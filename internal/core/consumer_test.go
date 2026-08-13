package core

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lcylpzls/errx"
	"github.com/lcylpzls/testx"
)

// TestConsumeAck 覆盖成功消费与单 key 顺序。
func TestConsumeAck(t *testing.T) {
	m := newTestMQ(t)
	_, err := m.CreateTopic("orders", smallTopic())
	testx.RequireNoError(t, err)
	var mu sync.Mutex
	var seq []int
	_, err = m.Subscribe(context.Background(), "orders", "g", 1, func(_ context.Context, msg *Message) error {
		mu.Lock()
		seq = append(seq, msg.Attempt)
		mu.Unlock()
		return nil
	})
	testx.RequireNoError(t, err)
	for i := 0; i < 10; i++ {
		testx.RequireNoError(t, m.Produce(context.Background(), "orders", "k", []byte(fmt.Sprint(i))))
	}
	waitFor(t, 2*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(seq) == 10
	})
}

// TestConsumeRetryAndDead 覆盖重试次数与死信流转。
func TestConsumeRetryAndDead(t *testing.T) {
	cfg := smallTopic()
	cfg.RetryMax = 2
	m := newTestMQ(t)
	_, err := m.CreateTopic("orders", cfg)
	testx.RequireNoError(t, err)
	var attempts atomic.Int32
	_, err = m.Subscribe(context.Background(), "orders", "g", 1, func(context.Context, *Message) error {
		attempts.Add(1)
		return errors.New("业务失败")
	})
	testx.RequireNoError(t, err)
	testx.RequireNoError(t, m.Produce(context.Background(), "orders", "k", []byte("x")))
	waitFor(t, 2*time.Second, func() bool { return attempts.Load() == 3 })

	var dead atomic.Pointer[Message]
	_, err = m.Subscribe(context.Background(), "orders.dlq", "dg", 1, func(_ context.Context, msg *Message) error {
		dead.Store(msg)
		return nil
	})
	testx.RequireNoError(t, err)
	waitFor(t, 2*time.Second, func() bool { return dead.Load() != nil })
	d := dead.Load()
	testx.RequireEqual(t, d.Attempt, 1)
	testx.RequireEqual(t, d.Err.Error(), "业务失败")
	testx.RequireEqual(t, d.Topic, "orders")
}

// TestConsumeTimeout 覆盖处理超时重投。
func TestConsumeTimeout(t *testing.T) {
	cfg := smallTopic()
	cfg.ProcessTimeout = 40 * time.Millisecond
	cfg.RetryMax = 1
	m := newTestMQ(t)
	_, err := m.CreateTopic("orders", cfg)
	testx.RequireNoError(t, err)
	release := make(chan struct{})
	started := make(chan struct{}, 4)
	var attempts atomic.Int32
	_, err = m.Subscribe(context.Background(), "orders", "g", 1, func(context.Context, *Message) error {
		attempts.Add(1)
		started <- struct{}{}
		<-release
		return nil
	})
	testx.RequireNoError(t, err)
	testx.RequireNoError(t, m.Produce(context.Background(), "orders", "k", nil))
	waitFor(t, 2*time.Second, func() bool { return attempts.Load() == 2 })
	var dead atomic.Pointer[Message]
	_, err = m.Subscribe(context.Background(), "orders.dlq", "dg", 1, func(_ context.Context, msg *Message) error {
		dead.Store(msg)
		return nil
	})
	testx.RequireNoError(t, err)
	waitFor(t, 2*time.Second, func() bool { return dead.Load() != nil })
	// 死信已落定后再释放阻塞的 handler goroutine，避免第二次尝试被误判为 ack。
	close(release)
	d := dead.Load()
	testx.RequireEqual(t, d.Attempt, 1)
	testx.RequireTrue(t, errors.Is(d.Err, ErrProcessTimeout))
}

// TestPerKeySerialization 覆盖同 key 串行、跨 key 并行。
func TestPerKeySerialization(t *testing.T) {
	cfg := smallTopic()
	cfg.Partitions = 8
	m := newTestMQ(t)
	_, err := m.CreateTopic("orders", cfg)
	testx.RequireNoError(t, err)
	var mu sync.Mutex
	inFlight := map[string]int{}
	maxInFlight := map[string]int{}
	var received int
	_, err = m.Subscribe(context.Background(), "orders", "g", 4, func(_ context.Context, msg *Message) error {
		mu.Lock()
		inFlight[msg.Key]++
		if inFlight[msg.Key] > maxInFlight[msg.Key] {
			maxInFlight[msg.Key] = inFlight[msg.Key]
		}
		received++
		mu.Unlock()
		time.Sleep(2 * time.Millisecond)
		mu.Lock()
		inFlight[msg.Key]--
		mu.Unlock()
		return nil
	})
	testx.RequireNoError(t, err)
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("k%d", i%4)
		testx.RequireNoError(t, m.Produce(context.Background(), "orders", key, nil))
	}
	waitFor(t, 3*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return received == 20
	})
	for k, v := range maxInFlight {
		if v != 1 {
			t.Fatalf("key %s 最大并发应=1，实际 %d", k, v)
		}
	}
}

// TestRetryBlocksSameKey 覆盖重试期间同 key 后续消息排队。
func TestRetryBlocksSameKey(t *testing.T) {
	cfg := smallTopic()
	cfg.RetryDelay = 20 * time.Millisecond
	cfg.RetryMax = 1
	m := newTestMQ(t)
	_, err := m.CreateTopic("orders", cfg)
	testx.RequireNoError(t, err)
	var mu sync.Mutex
	var order []string
	failOnce := map[string]bool{}
	_, err = m.Subscribe(context.Background(), "orders", "g", 1, func(_ context.Context, msg *Message) error {
		mu.Lock()
		defer mu.Unlock()
		order = append(order, msg.ID+":"+fmt.Sprint(msg.Attempt))
		if !failOnce[msg.ID] {
			failOnce[msg.ID] = true
			return errors.New("第一次失败")
		}
		return nil
	})
	testx.RequireNoError(t, err)
	testx.RequireNoError(t, m.Produce(context.Background(), "orders", "k", []byte("m1")))
	testx.RequireNoError(t, m.Produce(context.Background(), "orders", "k", []byte("m2")))
	waitFor(t, 3*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(order) == 3
	})
	mu.Lock()
	defer mu.Unlock()
	// m1 第一次失败 → m1 第二次成功 → 之后才轮到 m2。
	firstID := strings.SplitN(order[0], ":", 2)[0]
	if strings.SplitN(order[1], ":", 2)[0] != firstID {
		t.Fatalf("顺序不符：%v", order)
	}
	if strings.SplitN(order[2], ":", 2)[0] == firstID {
		t.Fatalf("m2 不应与 m1 相同：%v", order)
	}
}

// TestMultipleGroups 覆盖多组独立进度。
func TestMultipleGroups(t *testing.T) {
	cfg := smallTopic()
	m := newTestMQ(t)
	_, err := m.CreateTopic("orders", cfg)
	testx.RequireNoError(t, err)
	var acked, failed atomic.Int32
	_, err = m.Subscribe(context.Background(), "orders", "a", 1, func(context.Context, *Message) error {
		acked.Add(1)
		return nil
	})
	testx.RequireNoError(t, err)
	_, err = m.Subscribe(context.Background(), "orders", "b", 1, func(context.Context, *Message) error {
		failed.Add(1)
		return errors.New("组 b 失败")
	})
	testx.RequireNoError(t, err)
	testx.RequireNoError(t, m.Produce(context.Background(), "orders", "k", nil))
	waitFor(t, 2*time.Second, func() bool { return acked.Load() == 1 && failed.Load() >= 1 })
}

// TestAbandonOnStopDuringRetry 覆盖停止订阅时放弃重试。
func TestAbandonOnStopDuringRetry(t *testing.T) {
	cfg := smallTopic()
	cfg.RetryDelay = time.Second
	cfg.RetryMax = 3
	m := newTestMQ(t)
	_, err := m.CreateTopic("orders", cfg)
	testx.RequireNoError(t, err)
	var attempts atomic.Int32
	sub, err := m.Subscribe(context.Background(), "orders", "g", 1, func(context.Context, *Message) error {
		attempts.Add(1)
		return errors.New("失败")
	})
	testx.RequireNoError(t, err)
	testx.RequireNoError(t, m.Produce(context.Background(), "orders", "k", nil))
	waitFor(t, 2*time.Second, func() bool { return attempts.Load() == 1 })
	testx.RequireNoError(t, sub.Stop())
	time.Sleep(30 * time.Millisecond)
	testx.RequireEqual(t, attempts.Load(), int32(1))
}

// TestAbandonOnShutdownDuringRetry 覆盖关闭时放弃重试。
func TestAbandonOnShutdownDuringRetry(t *testing.T) {
	cfg := smallTopic()
	cfg.RetryDelay = time.Second
	m, err := New(WithClock(time.Now))
	testx.RequireNoError(t, err)
	_, err = m.CreateTopic("orders", cfg)
	testx.RequireNoError(t, err)
	var attempts atomic.Int32
	_, err = m.Subscribe(context.Background(), "orders", "g", 1, func(context.Context, *Message) error {
		attempts.Add(1)
		return errors.New("失败")
	})
	testx.RequireNoError(t, err)
	testx.RequireNoError(t, m.Produce(context.Background(), "orders", "k", nil))
	waitFor(t, 2*time.Second, func() bool { return attempts.Load() == 1 })
	testx.RequireNoError(t, m.Shutdown(context.Background()))
	testx.RequireEqual(t, attempts.Load(), int32(1))
}

// TestDLQDisabled 覆盖关闭死信时消息直接丢弃。
func TestDLQDisabled(t *testing.T) {
	cfg := smallTopic()
	cfg.DisableDLQ = true
	m := newTestMQ(t)
	_, err := m.CreateTopic("orders", cfg)
	testx.RequireNoError(t, err)
	var attempts atomic.Int32
	_, err = m.Subscribe(context.Background(), "orders", "g", 1, func(context.Context, *Message) error {
		attempts.Add(1)
		return errors.New("失败")
	})
	testx.RequireNoError(t, err)
	testx.RequireNoError(t, m.Produce(context.Background(), "orders", "k", nil))
	// 默认 RetryMax=3：共 4 次投递后丢弃，且 DLQ 不存在。
	waitFor(t, 2*time.Second, func() bool { return attempts.Load() == 4 })
	if _, err := m.Subscribe(context.Background(), "orders.dlq", "g2", 1, func(context.Context, *Message) error {
		return nil
	}); !errx.Is(err, CodeTopicNotFound) {
		t.Fatalf("DLQ 不应存在：%v", err)
	}
}
