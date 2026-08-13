package core

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lcylpzls/errx"
	"github.com/lcylpzls/logx"
	"github.com/lcylpzls/testx"
)

// lockedWriter 是并发安全的日志输出缓冲。
type lockedWriter struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (w *lockedWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}

func (w *lockedWriter) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.String()
}

// fakeHook 是测试用链路钩子。
type fakeHook struct {
	mu    sync.Mutex
	names []string
}

func (h *fakeHook) Start(ctx context.Context, name string, attrs ...TraceAttr) (context.Context, func(error)) {
	h.mu.Lock()
	h.names = append(h.names, name)
	h.mu.Unlock()
	return ctx, func(error) {}
}

// TestMetricsCallbacks 覆盖全部指标回调。
func TestMetricsCallbacks(t *testing.T) {
	var produced, queueFull, consumed, retried, dead, replayed, dlqDropped atomic.Int32
	metrics := Metrics{
		Produced:   func(string) { produced.Add(1) },
		QueueFull:  func(string) { queueFull.Add(1) },
		Consumed:   func(string, time.Duration) { consumed.Add(1) },
		Retried:    func(string, int) { retried.Add(1) },
		Dead:       func(string) { dead.Add(1) },
		Replayed:   func(string) { replayed.Add(1) },
		DLQDropped: func(string) { dlqDropped.Add(1) },
	}
	cfg := smallTopic()
	cfg.QueueSize = 1
	cfg.QueueFullPolicy = QueueFullDrop
	cfg.RetryMax = 1
	m := newTestMQ(t, WithMetrics(metrics))
	_, err := m.CreateTopic("orders", cfg)
	testx.RequireNoError(t, err)
	// 无消费者时先投两条同 key 消息：第一条占满队列，第二条触发队列满。
	testx.RequireNoError(t, m.Produce(context.Background(), "orders", "k", []byte("m1")))
	if err := m.Produce(context.Background(), "orders", "k", []byte("m2")); !errx.Is(err, CodeQueueFull) {
		t.Fatalf("第二条同 key 消息应触发队列满：%v", err)
	}
	waitFor(t, 2*time.Second, func() bool { return queueFull.Load() == 1 })
	var first atomic.Bool
	_, err = m.Subscribe(context.Background(), "orders", "g", 1, func(_ context.Context, msg *Message) error {
		if strings.Contains(string(msg.Body), "dead") {
			return errors.New("必败消息")
		}
		if !first.Swap(true) {
			return errors.New("失败")
		}
		return nil
	})
	testx.RequireNoError(t, err)
	testx.RequireNoError(t, m.Produce(context.Background(), "orders", "dead", []byte("dead")))
	waitFor(t, 2*time.Second, func() bool {
		return produced.Load() == 2 && consumed.Load() >= 1 && retried.Load() >= 1
	})
	waitFor(t, 2*time.Second, func() bool { return dead.Load() == 1 })
	testx.RequireNoError(t, m.Replay(context.Background(), "orders.dlq"))
	waitFor(t, 2*time.Second, func() bool { return replayed.Load() == 1 })
}

// TestTraceHook 覆盖链路钩子调用。
func TestTraceHook(t *testing.T) {
	hook := &fakeHook{}
	m := newTestMQ(t, WithTraceHook(hook))
	_, err := m.CreateTopic("orders", smallTopic())
	testx.RequireNoError(t, err)
	_, err = m.Subscribe(context.Background(), "orders", "g", 1, func(context.Context, *Message) error {
		return nil
	})
	testx.RequireNoError(t, err)
	testx.RequireNoError(t, m.Produce(context.Background(), "orders", "k", nil))
	waitFor(t, 2*time.Second, func() bool {
		hook.mu.Lock()
		defer hook.mu.Unlock()
		return len(hook.names) == 1 && hook.names[0] == "mqx.consume"
	})
}

// TestLogOutput 覆盖真实日志器输出。
func TestLogOutput(t *testing.T) {
	buf := &lockedWriter{}
	logger, err := logx.NewBuilder().EnableWriter(buf, logx.InfoLevel).Build()
	testx.RequireNoError(t, err)
	cfg := smallTopic()
	cfg.QueueSize = 1
	cfg.QueueFullPolicy = QueueFullDrop
	cfg.RetryMax = 0
	m, err := New(WithLogger(logger))
	testx.RequireNoError(t, err)
	_, err = m.CreateTopic("orders", cfg)
	testx.RequireNoError(t, err)
	_, err = m.Subscribe(context.Background(), "orders", "g", 1, func(context.Context, *Message) error {
		return errors.New("失败")
	})
	testx.RequireNoError(t, err)
	testx.RequireNoError(t, m.Produce(context.Background(), "orders", "k1", nil))
	testx.RequireNoError(t, m.Produce(context.Background(), "orders", "k2", nil))
	waitFor(t, 2*time.Second, func() bool {
		s := buf.String()
		return strings.Contains(s, "主题已创建") &&
			strings.Contains(s, "队列已满") &&
			strings.Contains(s, "重试耗尽")
	})
	testx.RequireNoError(t, m.Shutdown(context.Background()))
}

// TestRetryDelay 覆盖退避计算与封顶。
func TestRetryDelay(t *testing.T) {
	testx.RequireEqual(t, retryDelay(time.Second, 2), time.Second)
	testx.RequireEqual(t, retryDelay(time.Second, 3), 2*time.Second)
	testx.RequireEqual(t, retryDelay(time.Second, 99), time.Hour)
}
