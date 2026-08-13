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

// TestProduceBatch 覆盖批量投递分支。
func TestProduceBatch(t *testing.T) {
	m := newTestMQ(t)
	cfg := smallTopic()
	cfg.MaxMessageBytes = 8
	_, err := m.CreateTopic("orders", cfg)
	testx.RequireNoError(t, err)

	if err := m.ProduceBatch(context.Background(), "orders", nil); !errx.Is(err, CodeInvalidConfig) {
		t.Fatalf("空批量应报错：%v", err)
	}
	if err := m.ProduceBatch(context.Background(), "missing", []ProduceItem{{Key: "k"}}); !errx.Is(err, CodeTopicNotFound) {
		t.Fatalf("未知主题应报错：%v", err)
	}
	if err := m.ProduceBatch(context.Background(), "orders", []ProduceItem{
		{Key: "k", Body: []byte("123456789")},
	}); !errx.Is(err, CodeMessageTooLarge) {
		t.Fatalf("超限应报错：%v", err)
	}

	var mu sync.Mutex
	received := map[string]*Message{}
	_, err = m.Subscribe(context.Background(), "orders", "g", 1, func(_ context.Context, msg *Message) error {
		mu.Lock()
		received[msg.Key] = msg
		mu.Unlock()
		return nil
	})
	testx.RequireNoError(t, err)
	testx.RequireNoError(t, m.ProduceBatch(context.Background(), "orders", []ProduceItem{
		{Key: "k1", Body: []byte("a"), Attrs: map[string]string{"trace": "t1"}},
		{Key: "k2", Body: []byte("b")},
	}))
	waitFor(t, 2*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(received) == 2
	})
	mu.Lock()
	defer mu.Unlock()
	testx.RequireEqual(t, received["k1"].Attrs["trace"], "t1")
	testx.RequireEqual(t, len(received["k2"].Attrs), 0)
}

// TestProduceBatchIDGenFailure 覆盖批量投递 ID 生成失败。
func TestProduceBatchIDGenFailure(t *testing.T) {
	m := newTestMQ(t, WithIDGen(func(int) (string, error) {
		return "", errors.New("生成失败")
	}))
	_, err := m.CreateTopic("orders", smallTopic())
	testx.RequireNoError(t, err)
	if err := m.ProduceBatch(context.Background(), "orders", []ProduceItem{{Key: "k"}}); !errx.Is(err, CodeIDGenerateFailed) {
		t.Fatalf("ID 失败应报错：%v", err)
	}
}

// TestProduceBatchQueueFull 覆盖批量投递队列满分支。
func TestProduceBatchQueueFull(t *testing.T) {
	var full atomic.Int32
	m := newTestMQ(t, WithMetrics(Metrics{QueueFull: func(string) { full.Add(1) }}))
	cfg := smallTopic()
	cfg.QueueFullPolicy = QueueFullDrop
	_, err := m.CreateTopic("orders", cfg)
	testx.RequireNoError(t, err)
	err = m.ProduceBatch(context.Background(), "orders", []ProduceItem{
		{Key: "k", Body: []byte("a")},
		{Key: "k", Body: []byte("b")},
	})
	testx.RequireTrue(t, errx.Is(err, CodeQueueFull))
	testx.RequireEqual(t, full.Load(), int32(1))
}
