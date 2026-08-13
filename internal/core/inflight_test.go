package core

import (
	"context"
	"testing"
	"time"

	"github.com/lcylpzls/testx"
)

// TestStatsInFlight 覆盖投递中统计。
func TestStatsInFlight(t *testing.T) {
	cfg := smallTopic()
	cfg.ProcessTimeout = 5 * time.Second
	m := newTestMQ(t)
	_, err := m.CreateTopic("orders", cfg)
	testx.RequireNoError(t, err)
	release := make(chan struct{})
	started := make(chan struct{})
	_, err = m.Subscribe(context.Background(), "orders", "g", 1, func(context.Context, *Message) error {
		close(started)
		<-release
		return nil
	})
	testx.RequireNoError(t, err)
	testx.RequireNoError(t, m.Produce(context.Background(), "orders", "k", nil))
	<-started
	stats, err := m.Stats("orders")
	testx.RequireNoError(t, err)
	testx.RequireEqual(t, stats.InFlight, 1)
	close(release)
	waitFor(t, 2*time.Second, func() bool {
		s, _ := m.Stats("orders")
		return s.InFlight == 0
	})
}
