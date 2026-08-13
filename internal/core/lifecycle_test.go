package core

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lcylpzls/errx"
	"github.com/lcylpzls/testx"
)

// TestSubscriptionPauseResume 覆盖暂停/恢复。
func TestSubscriptionPauseResume(t *testing.T) {
	m := newTestMQ(t)
	cfg := smallTopic()
	cfg.QueueSize = 32
	_, err := m.CreateTopic("orders", cfg)
	testx.RequireNoError(t, err)
	var consumed atomic.Int32
	sub, err := m.Subscribe(context.Background(), "orders", "g", 1, func(context.Context, *Message) error {
		consumed.Add(1)
		return nil
	})
	testx.RequireNoError(t, err)
	for i := 0; i < 5; i++ {
		testx.RequireNoError(t, m.Produce(context.Background(), "orders", "k", nil))
	}
	waitFor(t, 2*time.Second, func() bool { return consumed.Load() == 5 })

	sub.Pause()
	for i := 0; i < 3; i++ {
		testx.RequireNoError(t, m.Produce(context.Background(), "orders", "k", nil))
	}
	time.Sleep(80 * time.Millisecond)
	testx.RequireEqual(t, consumed.Load(), int32(5))
	sub.Resume()
	waitFor(t, 2*time.Second, func() bool { return consumed.Load() == 8 })
}

// TestDLQPauseResume 覆盖死信订阅暂停/恢复。
func TestDLQPauseResume(t *testing.T) {
	cfg := smallTopic()
	cfg.QueueSize = 8
	m := newTestMQ(t)
	_, err := m.CreateTopic("orders", cfg)
	testx.RequireNoError(t, err)
	_, err = m.Subscribe(context.Background(), "orders", "g", 1, func(context.Context, *Message) error {
		return errors.New("失败")
	})
	testx.RequireNoError(t, err)
	var consumed atomic.Int32
	sub, err := m.Subscribe(context.Background(), "orders.dlq", "dg", 1, func(context.Context, *Message) error {
		consumed.Add(1)
		return nil
	})
	testx.RequireNoError(t, err)
	sub.Pause()
	testx.RequireNoError(t, m.Produce(context.Background(), "orders", "k", nil))
	waitFor(t, 2*time.Second, func() bool { return dlqLen(m, "orders") == 1 })
	testx.RequireEqual(t, consumed.Load(), int32(0))
	sub.Resume()
	waitFor(t, 2*time.Second, func() bool { return consumed.Load() == 1 })
}

// TestDLQPauseStop 覆盖死信订阅在暂停等待中停止。
func TestDLQPauseStop(t *testing.T) {
	cfg := smallTopic()
	cfg.QueueSize = 8
	m := newTestMQ(t)
	_, err := m.CreateTopic("orders", cfg)
	testx.RequireNoError(t, err)
	_, err = m.Subscribe(context.Background(), "orders", "g", 1, func(context.Context, *Message) error {
		return errors.New("失败")
	})
	testx.RequireNoError(t, err)
	sub, err := m.Subscribe(context.Background(), "orders.dlq", "dg", 1, func(context.Context, *Message) error {
		return nil
	})
	testx.RequireNoError(t, err)
	sub.Pause()
	testx.RequireNoError(t, m.Produce(context.Background(), "orders", "k", nil))
	waitFor(t, 2*time.Second, func() bool { return dlqLen(m, "orders") == 1 })
	testx.RequireNoError(t, sub.Stop())
}

// TestPauseStopAndShutdown 覆盖暂停等待期间停止与关闭。
func TestPauseStopAndShutdown(t *testing.T) {
	m, err := New()
	testx.RequireNoError(t, err)
	_, err = m.CreateTopic("orders", smallTopic())
	testx.RequireNoError(t, err)
	sub, err := m.Subscribe(context.Background(), "orders", "g", 1, func(context.Context, *Message) error {
		return nil
	})
	testx.RequireNoError(t, err)
	sub.Pause()
	testx.RequireNoError(t, sub.Stop())

	m2, err := New()
	testx.RequireNoError(t, err)
	_, err = m2.CreateTopic("orders", smallTopic())
	testx.RequireNoError(t, err)
	sub2, err := m2.Subscribe(context.Background(), "orders", "g", 1, func(context.Context, *Message) error {
		return nil
	})
	testx.RequireNoError(t, err)
	sub2.Pause()
	testx.RequireNoError(t, m2.Shutdown(context.Background()))

	m3, err := New()
	testx.RequireNoError(t, err)
	_, err = m3.CreateTopic("orders", smallTopic())
	testx.RequireNoError(t, err)
	sub3, err := m3.Subscribe(context.Background(), "orders.dlq", "dg", 1, func(context.Context, *Message) error {
		return nil
	})
	testx.RequireNoError(t, err)
	sub3.Pause()
	testx.RequireNoError(t, m3.Shutdown(context.Background()))
	_ = sub
}

// TestDeleteTopic 覆盖主题删除。
func TestDeleteTopic(t *testing.T) {
	m := newTestMQ(t)
	cfg := smallTopic()
	cfg.QueueSize = 8
	_, err := m.CreateTopic("orders", cfg)
	testx.RequireNoError(t, err)
	var consumed atomic.Int32
	sub, err := m.Subscribe(context.Background(), "orders", "g", 1, func(context.Context, *Message) error {
		consumed.Add(1)
		return nil
	})
	testx.RequireNoError(t, err)
	testx.RequireNoError(t, m.Produce(context.Background(), "orders", "k", nil))
	waitFor(t, 2*time.Second, func() bool { return consumed.Load() == 1 })

	testx.RequireNoError(t, m.DeleteTopic("orders"))
	testx.RequireTrue(t, errx.Is(m.DeleteTopic("orders"), CodeTopicNotFound))
	testx.RequireTrue(t, errx.Is(m.Produce(context.Background(), "orders", "k", nil), CodeTopicNotFound))
	if _, err := m.Stats("orders"); !errx.Is(err, CodeTopicNotFound) {
		t.Fatalf("删除后统计应报错：%v", err)
	}
	if _, err := m.Subscribe(context.Background(), "orders", "g2", 1, func(context.Context, *Message) error {
		return nil
	}); !errx.Is(err, CodeTopicNotFound) {
		t.Fatalf("删除后订阅应报错：%v", err)
	}
	if err := m.Replay(context.Background(), "orders.dlq"); !errx.Is(err, CodeTopicNotFound) {
		t.Fatalf("删除后重放应报错：%v", err)
	}
	// 已停止的订阅可再次 Stop（幂等）。
	testx.RequireNoError(t, sub.Stop())
}

// TestDeleteTopicWithDLQSub 覆盖删除含死信订阅的主题。
func TestDeleteTopicWithDLQSub(t *testing.T) {
	cfg := smallTopic()
	cfg.QueueSize = 8
	m := newTestMQ(t)
	_, err := m.CreateTopic("orders", cfg)
	testx.RequireNoError(t, err)
	_, err = m.Subscribe(context.Background(), "orders", "g", 1, func(context.Context, *Message) error {
		return errors.New("失败")
	})
	testx.RequireNoError(t, err)
	testx.RequireNoError(t, m.Produce(context.Background(), "orders", "k", nil))
	waitFor(t, 2*time.Second, func() bool { return dlqLen(m, "orders") == 1 })
	sub, err := m.Subscribe(context.Background(), "orders.dlq", "dg", 1, func(context.Context, *Message) error {
		return nil
	})
	testx.RequireNoError(t, err)
	testx.RequireNoError(t, m.DeleteTopic("orders"))
	testx.RequireNoError(t, sub.Stop())
}
