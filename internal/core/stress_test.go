package core

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/lcylpzls/testx"
)

// TestStressConcurrent 并发生产消费压力：每 key 顺序不乱、不丢、不 panic。
func TestStressConcurrent(t *testing.T) {
	cfg := smallTopic()
	cfg.QueueSize = 4096
	cfg.Partitions = 8
	cfg.ProcessTimeout = 5 * time.Second
	m := newTestMQ(t)
	_, err := m.CreateTopic("stress", cfg)
	testx.RequireNoError(t, err)
	const perKey = 500
	const keys = 4
	var mu sync.Mutex
	expected := make(map[string]int, keys)
	received := 0
	badOrder := 0
	_, err = m.Subscribe(context.Background(), "stress", "g", 8, func(_ context.Context, msg *Message) error {
		n, _ := strconv.Atoi(string(msg.Body))
		mu.Lock()
		if n != expected[msg.Key] {
			badOrder++
		}
		expected[msg.Key] = n + 1
		received++
		mu.Unlock()
		return nil
	})
	testx.RequireNoError(t, err)

	var wg sync.WaitGroup
	for k := 0; k < keys; k++ {
		wg.Add(1)
		go func(k int) {
			defer wg.Done()
			key := fmt.Sprintf("k%d", k)
			for i := 0; i < perKey; i++ {
				testx.RequireNoError(t, m.Produce(context.Background(), "stress", key,
					[]byte(strconv.Itoa(i))))
			}
		}(k)
	}
	wg.Wait()
	waitFor(t, 10*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return received == perKey*keys
	})
	mu.Lock()
	defer mu.Unlock()
	testx.RequireEqual(t, received, perKey*keys)
	testx.RequireEqual(t, badOrder, 0)
}
