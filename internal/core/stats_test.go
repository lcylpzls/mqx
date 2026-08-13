package core

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/lcylpzls/errx"
	"github.com/lcylpzls/testx"
)

// TestStats 覆盖主题统计快照。
func TestStats(t *testing.T) {
	m := newTestMQ(t)
	cfg := smallTopic()
	cfg.QueueSize = 8
	_, err := m.CreateTopic("orders", cfg)
	testx.RequireNoError(t, err)
	if _, err := m.Stats("missing"); !errx.Is(err, CodeTopicNotFound) {
		t.Fatalf("未知主题应报错：%v", err)
	}
	testx.RequireNoError(t, m.Produce(context.Background(), "orders", "k1", nil))
	testx.RequireNoError(t, m.Produce(context.Background(), "orders", "k2", nil))
	stats, err := m.Stats("orders")
	testx.RequireNoError(t, err)
	testx.RequireEqual(t, stats.Name, "orders")
	testx.RequireEqual(t, stats.Pending, 2)
	testx.RequireEqual(t, stats.Consumed, int64(0))
	testx.RequireEqual(t, stats.Partitions, cfg.Partitions)

	_, err = m.Subscribe(context.Background(), "orders", "g", 1, func(context.Context, *Message) error {
		return nil
	})
	testx.RequireNoError(t, err)
	waitFor(t, 2*time.Second, func() bool {
		s, _ := m.Stats("orders")
		return s.Consumed == 2 && s.Pending == 0
	})

	// 死信统计。
	cfg2 := smallTopic()
	cfg2.QueueSize = 8
	_, err = m.CreateTopic("bad", cfg2)
	testx.RequireNoError(t, err)
	_, err = m.Subscribe(context.Background(), "bad", "g", 1, func(context.Context, *Message) error {
		return errors.New("失败")
	})
	testx.RequireNoError(t, err)
	testx.RequireNoError(t, m.Produce(context.Background(), "bad", "k", nil))
	waitFor(t, 2*time.Second, func() bool {
		s, _ := m.Stats("bad")
		return s.DLQPending == 1
	})
	_, err = m.Subscribe(context.Background(), "bad.dlq", "dg", 1, func(context.Context, *Message) error {
		return nil
	})
	testx.RequireNoError(t, err)
	waitFor(t, 2*time.Second, func() bool {
		s, _ := m.Stats("bad")
		return s.DLQPending == 0 && s.DLQConsumed >= 1
	})
}
