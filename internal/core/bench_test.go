package core

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// BenchmarkProduceConsumeSingleKey 单 key 串行吞吐（含投递与消费）。
func BenchmarkProduceConsumeSingleKey(b *testing.B) {
	m, err := New()
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = m.Shutdown(context.Background()) }()
	cfg := TopicConfig{QueueSize: 4096, RetryMax: 0, RetryDelay: time.Millisecond,
		ProcessTimeout: time.Second, Partitions: 1}
	_, err = m.CreateTopic("bench", cfg)
	if err != nil {
		b.Fatal(err)
	}
	done := make(chan struct{})
	_, err = m.Subscribe(context.Background(), "bench", "g", 1, func(context.Context, *Message) error {
		return nil
	})
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	b.ResetTimer()
	go func() {
		for i := 0; i < b.N; i++ {
			_ = m.Produce(ctx, "bench", "k", nil)
		}
		close(done)
	}()
	<-done
	benchWait(b, 10*time.Second, func() bool {
		s, _ := m.Stats("bench")
		return s.Pending == 0
	})
}

// BenchmarkProduceConsumeMultiKey 多 key 并行吞吐。
func BenchmarkProduceConsumeMultiKey(b *testing.B) {
	m, err := New()
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = m.Shutdown(context.Background()) }()
	cfg := TopicConfig{QueueSize: 4096, RetryMax: 0, RetryDelay: time.Millisecond,
		ProcessTimeout: time.Second, Partitions: 8}
	_, err = m.CreateTopic("bench", cfg)
	if err != nil {
		b.Fatal(err)
	}
	_, err = m.Subscribe(context.Background(), "bench", "g", 8, func(context.Context, *Message) error {
		return nil
	})
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = m.Produce(ctx, "bench", fmt.Sprintf("k%d", i%16), nil)
	}
	benchWait(b, 10*time.Second, func() bool {
		s, _ := m.Stats("bench")
		return s.Pending == 0
	})
}

// benchWait 供基准使用的等待辅助。
func benchWait(b testing.TB, timeout time.Duration, cond func() bool) {
	b.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	b.Fatal("等待条件超时")
}
