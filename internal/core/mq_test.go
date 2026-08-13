package core

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lcylpzls/errx"
	"github.com/lcylpzls/testx"
)

// smallTopic 返回小队列测试主题配置。
func smallTopic() TopicConfig {
	cfg := TopicConfig{}
	cfg.withDefaults()
	cfg.QueueSize = 1
	cfg.RetryDelay = 10 * time.Millisecond
	cfg.ProcessTimeout = 100 * time.Millisecond
	cfg.Partitions = 4
	return cfg
}

// TestNewError 覆盖构造失败。
func TestNewError(t *testing.T) {
	if _, err := New(WithStore(nil)); !errx.Is(err, CodeInvalidConfig) {
		t.Fatalf("nil Store 应报错：%v", err)
	}
}

// TestCreateTopicValidation 覆盖主题创建校验。
func TestCreateTopicValidation(t *testing.T) {
	m := newTestMQ(t)
	cases := []string{"", strings.Repeat("x", 129), "a b", "orders.dlq"}
	for _, name := range cases {
		if _, err := m.CreateTopic(name, TopicConfig{}); !errx.Is(err, CodeInvalidConfig) {
			t.Fatalf("非法主题名 %q 应报错：%v", name, err)
		}
	}
	if _, err := m.CreateTopic("orders", TopicConfig{QueueFullPolicy: QueueFullPolicy(99)}); !errx.Is(err, CodeInvalidConfig) {
		t.Fatalf("非法策略应报错：%v", err)
	}
	if _, err := m.CreateTopic("orders", TopicConfig{}); err != nil {
		t.Fatal(err)
	}
	if _, err := m.CreateTopic("orders", TopicConfig{}); !errx.Is(err, CodeTopicExists) {
		t.Fatalf("重复创建应报错：%v", err)
	}
}

// TestProduceErrors 覆盖投递错误分支。
func TestProduceErrors(t *testing.T) {
	m := newTestMQ(t, WithIDGen(func(int) (string, error) {
		return "", errors.New("生成失败")
	}))
	if err := m.Produce(context.Background(), "missing", "k", nil); !errx.Is(err, CodeTopicNotFound) {
		t.Fatalf("未知主题应报错：%v", err)
	}
	_, err := m.CreateTopic("orders", smallTopic())
	testx.RequireNoError(t, err)
	if err := m.Produce(context.Background(), "orders", "k", []byte("x")); !errx.Is(err, CodeIDGenerateFailed) {
		t.Fatalf("ID 生成失败应报错：%v", err)
	}
}

// TestQueueFullPolicies 覆盖 Drop/Reject 与 Block 分支。
func TestQueueFullPolicies(t *testing.T) {
	for _, policy := range []QueueFullPolicy{QueueFullDrop, QueueFullReject} {
		m := newTestMQ(t)
		cfg := smallTopic()
		cfg.QueueFullPolicy = policy
		_, err := m.CreateTopic("orders", cfg)
		testx.RequireNoError(t, err)
		testx.RequireNoError(t, m.Produce(context.Background(), "orders", "k", nil))
		if err := m.Produce(context.Background(), "orders", "k", nil); !errx.Is(err, CodeQueueFull) {
			t.Fatalf("队列满应报错：%v", err)
		}
	}

	// Block：消费者排空后阻塞生产者恢复。
	m := newTestMQ(t)
	_, err := m.CreateTopic("orders", smallTopic())
	testx.RequireNoError(t, err)
	var consumed atomic.Int32
	_, err = m.Subscribe(context.Background(), "orders", "g", 1, func(ctx context.Context, msg *Message) error {
		consumed.Add(1)
		return nil
	})
	testx.RequireNoError(t, err)
	testx.RequireNoError(t, m.Produce(context.Background(), "orders", "k1", nil))
	testx.RequireNoError(t, m.Produce(context.Background(), "orders", "k2", nil))
	testx.RequireNoError(t, m.Produce(context.Background(), "orders", "k3", nil))
	waitFor(t, 2*time.Second, func() bool { return consumed.Load() == 3 })
}

// TestProduceBlockContextCancel 覆盖 Block 等待时上下文取消。
func TestProduceBlockContextCancel(t *testing.T) {
	m := newTestMQ(t)
	_, err := m.CreateTopic("orders", smallTopic())
	testx.RequireNoError(t, err)
	testx.RequireNoError(t, m.Produce(context.Background(), "orders", "k", nil))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := m.Produce(ctx, "orders", "k", nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("取消上下文应返回：%v", err)
	}
}

// TestProduceShutdownWhileBlocked 覆盖阻塞生产者被关闭唤醒。
func TestProduceShutdownWhileBlocked(t *testing.T) {
	m, err := New()
	testx.RequireNoError(t, err)
	_, err = m.CreateTopic("orders", smallTopic())
	testx.RequireNoError(t, err)
	testx.RequireNoError(t, m.Produce(context.Background(), "orders", "k", nil))
	blocked := make(chan error, 1)
	go func() {
		blocked <- m.Produce(context.Background(), "orders", "k", nil)
	}()
	time.Sleep(30 * time.Millisecond)
	testx.RequireNoError(t, m.Shutdown(context.Background()))
	select {
	case err := <-blocked:
		testx.RequireTrue(t, errx.Is(err, CodeShuttingDown))
	case <-time.After(2 * time.Second):
		t.Fatal("阻塞生产者未被关闭唤醒")
	}
}

// TestProduceRecheckShutdown 覆盖生产者被唤醒后重新检查关闭状态。
func TestProduceRecheckShutdown(t *testing.T) {
	m, err := New()
	testx.RequireNoError(t, err)
	_, err = m.CreateTopic("orders", smallTopic())
	testx.RequireNoError(t, err)
	testx.RequireNoError(t, m.Produce(context.Background(), "orders", "k", nil))
	blocked := make(chan error, 1)
	go func() {
		blocked <- m.Produce(context.Background(), "orders", "k", nil)
	}()
	time.Sleep(30 * time.Millisecond)
	// 先置关闭标记，再广播唤醒阻塞生产者：走“唤醒后重新检查”分支。
	m.shuttingDown.Store(true)
	m.mu.RLock()
	p := m.topics["orders"].partitionFor("k")
	m.mu.RUnlock()
	p.mu.Lock()
	p.signalLocked()
	p.mu.Unlock()
	select {
	case err := <-blocked:
		testx.RequireTrue(t, errx.Is(err, CodeShuttingDown))
	case <-time.After(2 * time.Second):
		t.Fatal("生产者未被唤醒")
	}
	testx.RequireNoError(t, m.Shutdown(context.Background()))
}

// TestSubscribeValidation 覆盖订阅校验。
func TestSubscribeValidation(t *testing.T) {
	m := newTestMQ(t)
	h := func(context.Context, *Message) error { return nil }
	if _, err := m.Subscribe(context.Background(), "orders", "", 1, h); !errx.Is(err, CodeInvalidConfig) {
		t.Fatalf("空组名应报错：%v", err)
	}
	if _, err := m.Subscribe(context.Background(), "orders", "g", 0, h); !errx.Is(err, CodeInvalidConfig) {
		t.Fatalf("零消费者应报错：%v", err)
	}
	if _, err := m.Subscribe(context.Background(), "orders", "g", 1, nil); !errx.Is(err, CodeInvalidConfig) {
		t.Fatalf("nil 处理器应报错：%v", err)
	}
	if _, err := m.Subscribe(context.Background(), "orders", "g", 1, h); !errx.Is(err, CodeTopicNotFound) {
		t.Fatalf("未知主题应报错：%v", err)
	}
	_, err := m.CreateTopic("orders", TopicConfig{DisableDLQ: true})
	testx.RequireNoError(t, err)
	if _, err := m.Subscribe(context.Background(), "orders.dlq", "g", 1, h); !errx.Is(err, CodeTopicNotFound) {
		t.Fatalf("未启用 DLQ 应报错：%v", err)
	}
}

// TestSubscriptionStop 覆盖订阅停止。
func TestSubscriptionStop(t *testing.T) {
	m := newTestMQ(t)
	_, err := m.CreateTopic("orders", smallTopic())
	testx.RequireNoError(t, err)
	var consumed atomic.Int32
	sub, err := m.Subscribe(context.Background(), "orders", "g", 1, func(context.Context, *Message) error {
		consumed.Add(1)
		return nil
	})
	testx.RequireNoError(t, err)
	testx.RequireEqual(t, sub.Group(), "g")
	testx.RequireNoError(t, m.Produce(context.Background(), "orders", "k1", nil))
	waitFor(t, 2*time.Second, func() bool { return consumed.Load() == 1 })
	testx.RequireNoError(t, sub.Stop())
	testx.RequireNoError(t, sub.Stop())
	testx.RequireNoError(t, m.Produce(context.Background(), "orders", "k2", nil))
	time.Sleep(50 * time.Millisecond)
	testx.RequireEqual(t, consumed.Load(), int32(1))
}

// TestShutdown 覆盖关闭与超时分支。
func TestShutdown(t *testing.T) {
	cfg := smallTopic()
	cfg.ProcessTimeout = 5 * time.Second
	m, err := New(WithClock(time.Now))
	testx.RequireNoError(t, err)
	_, err = m.CreateTopic("orders", cfg)
	testx.RequireNoError(t, err)
	release := make(chan struct{})
	started := make(chan struct{})
	_, err = m.Subscribe(context.Background(), "orders", "g", 1, func(context.Context, *Message) error {
		close(started)
		<-release
		return nil
	})
	testx.RequireNoError(t, err)
	testx.RequireNoError(t, m.Produce(context.Background(), "orders", "k1", nil))
	<-started
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := m.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("关闭超时应报错：%v", err)
	}
	close(release)
	testx.RequireNoError(t, m.Shutdown(context.Background()))

	// 关闭后拒绝投递与创建主题。
	if err := m.Produce(context.Background(), "orders", "k2", nil); !errx.Is(err, CodeShuttingDown) {
		t.Fatalf("关闭后投递应报错：%v", err)
	}
	if _, err := m.CreateTopic("other", TopicConfig{}); !errx.Is(err, CodeShuttingDown) {
		t.Fatalf("关闭后创建主题应报错：%v", err)
	}
	if _, err := m.Subscribe(context.Background(), "orders", "g2", 1, func(context.Context, *Message) error {
		return nil
	}); !errx.Is(err, CodeTopicNotFound) {
		t.Fatalf("关闭后订阅应报错：%v", err)
	}
}
