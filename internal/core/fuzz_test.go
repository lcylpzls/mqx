package core

import (
	"testing"
	"time"
)

// FuzzKeyPartition 确保 key 分区映射稳定且不越界。
func FuzzKeyPartition(f *testing.F) {
	f.Add("order-10086")
	f.Add("")
	f.Add("中文键")
	f.Fuzz(func(t *testing.T, key string) {
		m, err := New()
		if err != nil {
			t.Fatal(err)
		}
		topic, err := m.CreateTopic("t", TopicConfig{Partitions: 16})
		if err != nil {
			t.Fatal(err)
		}
		p1 := topic.partitionFor(key)
		p2 := topic.partitionFor(key)
		if p1 != p2 {
			t.Fatalf("同一 key 分区不稳定：%v vs %v", p1.index, p2.index)
		}
		if p1.index < 0 || p1.index >= len(topic.partitions) {
			t.Fatalf("分区越界：%d", p1.index)
		}
		_ = m.Shutdown(t.Context())
	})
}

// FuzzConfig 确保任意主题配置不会 panic。
func FuzzConfig(f *testing.F) {
	f.Add(0, 0, int64(0), int64(0), 0)
	f.Add(-1, 99, int64(-1), int64(-1), -1)
	f.Fuzz(func(t *testing.T, queueSize, retryMax int, retryDelay int64, timeout int64, partitions int) {
		cfg := TopicConfig{
			QueueSize:       queueSize,
			QueueFullPolicy: QueueFullPolicy(uint8(retryMax) % 5),
			RetryMax:        retryMax,
			RetryDelay:      time.Duration(retryDelay),
			ProcessTimeout:  time.Duration(timeout),
			Partitions:      partitions,
		}
		cfg.withDefaults()
		_ = cfg.Validate()
	})
}
