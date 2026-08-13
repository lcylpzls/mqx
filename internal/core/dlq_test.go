package core

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lcylpzls/errx"
	"github.com/lcylpzls/testx"
)

// dlqLen 返回死信队列当前长度（并发安全）。
func dlqLen(m *MQ, topic string) int {
	m.mu.RLock()
	d := m.topics[topic].dlq
	m.mu.RUnlock()
	if d == nil {
		return 0
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.msgs)
}

// dlqIDs 返回死信队列全部消息 ID（并发安全）。
func dlqIDs(m *MQ, topic string) []string {
	m.mu.RLock()
	d := m.topics[topic].dlq
	m.mu.RUnlock()
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]string, len(d.msgs))
	for i, msg := range d.msgs {
		out[i] = msg.ID
	}
	return out
}

// deadTopic 构造会把消息送入 DLQ 的主题。
func deadTopic(t *testing.T, m *MQ, name string, cfg TopicConfig) {
	t.Helper()
	if cfg.QueueSize == 0 {
		cfg = smallTopic()
	}
	if cfg.QueueSize < 8 {
		cfg.QueueSize = 8
	}
	cfg.RetryMax = 0
	_, err := m.CreateTopic(name, cfg)
	testx.RequireNoError(t, err)
	_, err = m.Subscribe(context.Background(), name, "g", 1, func(context.Context, *Message) error {
		return errors.New("失败")
	})
	testx.RequireNoError(t, err)
}

// TestReplayAll 覆盖全部死信重放。
func TestReplayAll(t *testing.T) {
	m := newTestMQ(t)
	cfg := smallTopic()
	deadTopic(t, m, "orders", cfg)
	testx.RequireNoError(t, m.Produce(context.Background(), "orders", "k1", []byte("a")))
	testx.RequireNoError(t, m.Produce(context.Background(), "orders", "k2", []byte("b")))
	waitFor(t, 2*time.Second, func() bool {
		return dlqLen(m, "orders") == 2
	})
	testx.RequireNoError(t, m.Replay(context.Background(), "orders.dlq"))

	var mu sync.Mutex
	var bodies []string
	_, err := m.Subscribe(context.Background(), "orders", "g2", 1, func(_ context.Context, msg *Message) error {
		mu.Lock()
		bodies = append(bodies, string(msg.Body))
		mu.Unlock()
		return nil
	})
	testx.RequireNoError(t, err)
	waitFor(t, 2*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(bodies) >= 2
	})
	hasA, hasB := false, false
	for _, b := range bodies {
		hasA = hasA || b == "a"
		hasB = hasB || b == "b"
	}
	testx.RequireTrue(t, hasA && hasB)
	testx.RequireEqual(t, dlqLen(m, "orders"), 0)
}

// TestReplayByIDs 覆盖按 ID 重放。
func TestReplayByIDs(t *testing.T) {
	m := newTestMQ(t)
	cfg := smallTopic()
	deadTopic(t, m, "orders", cfg)
	ids := make([]string, 2)
	for i := range ids {
		testx.RequireNoError(t, m.Produce(context.Background(), "orders", "k", []byte{byte(i)}))
	}
	waitFor(t, 2*time.Second, func() bool {
		return dlqLen(m, "orders") == 2
	})
	ids[0] = dlqIDs(m, "orders")[0]
	testx.RequireNoError(t, m.Replay(context.Background(), "orders.dlq", ids[0]))
	testx.RequireEqual(t, dlqLen(m, "orders"), 1)
}

// TestReplayInFlightSkip 覆盖在途死信不重放。
func TestReplayInFlightSkip(t *testing.T) {
	m := newTestMQ(t)
	cfg := smallTopic()
	cfg.QueueSize = 8
	cfg.RetryMax = 0
	_, err := m.CreateTopic("orders", cfg)
	testx.RequireNoError(t, err)
	_, err = m.Subscribe(context.Background(), "orders", "g", 1, func(context.Context, *Message) error {
		return errors.New("失败")
	})
	testx.RequireNoError(t, err)
	testx.RequireNoError(t, m.Produce(context.Background(), "orders", "k1", nil))
	testx.RequireNoError(t, m.Produce(context.Background(), "orders", "k2", nil))
	waitFor(t, 2*time.Second, func() bool {
		return dlqLen(m, "orders") == 2
	})
	release := make(chan struct{})
	started := make(chan struct{})
	_, err = m.Subscribe(context.Background(), "orders.dlq", "dg", 1, func(context.Context, *Message) error {
		select {
		case started <- struct{}{}:
		default:
		}
		<-release
		return nil
	})
	testx.RequireNoError(t, err)
	<-started
	s, _ := m.Stats("orders")
	testx.RequireEqual(t, s.DLQInFlight, 1)
	testx.RequireNoError(t, m.Replay(context.Background(), "orders.dlq"))
	testx.RequireEqual(t, dlqLen(m, "orders"), 1)
	close(release)
}

// TestDLQFullDrop 覆盖死信队列满丢弃。
func TestDLQFullDrop(t *testing.T) {
	cfg := smallTopic()
	cfg.QueueSize = 1
	cfg.RetryMax = 0
	var dropped atomic.Int32
	m := newTestMQ(t, WithMetrics(Metrics{DLQDropped: func(string) { dropped.Add(1) }}))
	_, err := m.CreateTopic("orders", cfg)
	testx.RequireNoError(t, err)
	_, err = m.Subscribe(context.Background(), "orders", "g", 1, func(context.Context, *Message) error {
		return errors.New("失败")
	})
	testx.RequireNoError(t, err)
	testx.RequireNoError(t, m.Produce(context.Background(), "orders", "k1", nil))
	testx.RequireNoError(t, m.Produce(context.Background(), "orders", "k2", nil))
	waitFor(t, 2*time.Second, func() bool { return dropped.Load() == 1 })
}

// TestDLQConsumerExhausted 覆盖死信消费失败丢弃。
func TestDLQConsumerExhausted(t *testing.T) {
	cfg := smallTopic()
	cfg.QueueSize = 8
	cfg.RetryMax = 0
	m := newTestMQ(t)
	_, err := m.CreateTopic("orders", cfg)
	testx.RequireNoError(t, err)
	_, err = m.Subscribe(context.Background(), "orders", "g", 1, func(context.Context, *Message) error {
		return errors.New("失败")
	})
	testx.RequireNoError(t, err)
	testx.RequireNoError(t, m.Produce(context.Background(), "orders", "k", nil))
	waitFor(t, 2*time.Second, func() bool {
		return dlqLen(m, "orders") == 1
	})
	var attempts atomic.Int32
	_, err = m.Subscribe(context.Background(), "orders.dlq", "dg", 1, func(context.Context, *Message) error {
		attempts.Add(1)
		return errors.New("死信消费失败")
	})
	testx.RequireNoError(t, err)
	// 默认 RetryMax=3：死信共投递 4 次后丢弃。
	waitFor(t, 3*time.Second, func() bool { return attempts.Load() == 4 })
	testx.RequireEqual(t, dlqLen(m, "orders"), 0)
}

// TestReplayFailure 覆盖重放失败放回死信。
func TestReplayFailure(t *testing.T) {
	m := newTestMQ(t)
	cfg := smallTopic()
	cfg.QueueSize = 1
	cfg.RetryMax = 0
	_, err := m.CreateTopic("orders", cfg)
	testx.RequireNoError(t, err)
	_, err = m.Subscribe(context.Background(), "orders", "g", 1, func(context.Context, *Message) error {
		return errors.New("失败")
	})
	testx.RequireNoError(t, err)
	testx.RequireNoError(t, m.Produce(context.Background(), "orders", "k", nil))
	waitFor(t, 2*time.Second, func() bool {
		return dlqLen(m, "orders") == 1
	})
	// 关闭后重放必然失败，消息放回死信。
	testx.RequireNoError(t, m.Shutdown(context.Background()))
	if err := m.Replay(context.Background(), "orders.dlq"); err == nil {
		t.Fatal("重放失败应返回错误")
	}
	testx.RequireEqual(t, dlqLen(m, "orders"), 1)
}

// TestReplayNotFound 覆盖重放未知主题与未启用 DLQ。
func TestReplayNotFound(t *testing.T) {
	m := newTestMQ(t)
	if err := m.Replay(context.Background(), "bad"); !errx.Is(err, CodeInvalidConfig) {
		t.Fatalf("非法死信名应报错：%v", err)
	}
	if err := m.Replay(context.Background(), "missing.dlq"); !errx.Is(err, CodeTopicNotFound) {
		t.Fatalf("未知死信主题应报错：%v", err)
	}
	_, err := m.CreateTopic("orders", TopicConfig{DisableDLQ: true})
	testx.RequireNoError(t, err)
	if err := m.Replay(context.Background(), "orders.dlq"); !errx.Is(err, CodeTopicNotFound) {
		t.Fatalf("未启用 DLQ 应报错：%v", err)
	}
}

// TestReplayAdjustsCursors 覆盖重放时对在途游标的调整。
func TestReplayAdjustsCursors(t *testing.T) {
	cfg := smallTopic()
	cfg.QueueSize = 8
	cfg.RetryMax = 0
	m := newTestMQ(t)
	_, err := m.CreateTopic("orders", cfg)
	testx.RequireNoError(t, err)
	_, err = m.Subscribe(context.Background(), "orders", "g", 1, func(context.Context, *Message) error {
		return errors.New("失败")
	})
	testx.RequireNoError(t, err)
	testx.RequireNoError(t, m.Produce(context.Background(), "orders", "k1", nil))
	testx.RequireNoError(t, m.Produce(context.Background(), "orders", "k2", nil))
	waitFor(t, 2*time.Second, func() bool {
		return dlqLen(m, "orders") == 2
	})
	firstID := dlqIDs(m, "orders")[0]
	release := make(chan struct{})
	started := make(chan struct{})
	firstAcked := make(chan struct{})
	_, err = m.Subscribe(context.Background(), "orders.dlq", "dg", 1, func(_ context.Context, msg *Message) error {
		if msg.ID == firstID {
			close(firstAcked)
			return nil
		}
		started <- struct{}{}
		<-release
		return nil
	})
	testx.RequireNoError(t, err)
	<-firstAcked
	<-started
	// 在途消息为第二条（index 1），重放第一条（index 0）应调整游标。
	testx.RequireNoError(t, m.Replay(context.Background(), "orders.dlq", firstID))
	testx.RequireEqual(t, dlqLen(m, "orders"), 1)
	close(release)
}

// TestDLQSubscriptionStop 覆盖死信订阅停止放弃重试。
func TestDLQSubscriptionStop(t *testing.T) {
	cfg := smallTopic()
	cfg.QueueSize = 8
	cfg.RetryDelay = 200 * time.Millisecond
	cfg.RetryMax = 3
	m := newTestMQ(t)
	_, err := m.CreateTopic("orders", cfg)
	testx.RequireNoError(t, err)
	_, err = m.Subscribe(context.Background(), "orders", "g", 1, func(context.Context, *Message) error {
		return errors.New("失败")
	})
	testx.RequireNoError(t, err)
	testx.RequireNoError(t, m.Produce(context.Background(), "orders", "k", nil))
	waitFor(t, 2*time.Second, func() bool {
		return dlqLen(m, "orders") == 1
	})
	var attempts atomic.Int32
	sub, err := m.Subscribe(context.Background(), "orders.dlq", "dg", 1, func(context.Context, *Message) error {
		attempts.Add(1)
		return errors.New("死信失败")
	})
	testx.RequireNoError(t, err)
	waitFor(t, 2*time.Second, func() bool { return attempts.Load() == 1 })
	testx.RequireNoError(t, sub.Stop())
	time.Sleep(30 * time.Millisecond)
	testx.RequireEqual(t, attempts.Load(), int32(1))
}

// TestDLQSubscriptionStopIdle 覆盖空闲死信订阅停止。
func TestDLQSubscriptionStopIdle(t *testing.T) {
	m := newTestMQ(t)
	_, err := m.CreateTopic("orders", smallTopic())
	testx.RequireNoError(t, err)
	sub, err := m.Subscribe(context.Background(), "orders.dlq", "dg", 1, func(context.Context, *Message) error {
		return nil
	})
	testx.RequireNoError(t, err)
	testx.RequireNoError(t, sub.Stop())
}
