// ecosystem 示例：削峰 + 按 key 串行落库（模拟数据库同条目写冲突场景）。
package main

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lcylpzls/mqx"
)

func main() {
	if err := run(context.Background()); err != nil {
		panic(err)
	}
}

// db 是模拟数据库：同一 key 的写入必须串行。
type db struct {
	mu       sync.Mutex
	values   map[string]int
	inflight map[string]bool
	violated atomic.Bool
}

func newDB() *db {
	return &db{values: map[string]int{}, inflight: map[string]bool{}}
}

// write 模拟同条目写：若同一 key 已在写则记冲突。
func (d *db) write(key string, delta int) {
	d.mu.Lock()
	if d.inflight[key] {
		d.violated.Store(true)
	}
	d.inflight[key] = true
	d.values[key] += delta
	d.inflight[key] = false
	d.mu.Unlock()
}

// run 演示突发 1000 条增量在 8 分区下按 key 串行落库。
func run(ctx context.Context) error {
	mq, err := mqx.New()
	if err != nil {
		return err
	}
	defer func() { _ = mq.Shutdown(ctx) }()

	_, err = mq.CreateTopic("account.incr", mqx.TopicConfig{
		QueueSize:       4096,
		RetryMax:        1,
		RetryDelay:      10 * time.Millisecond,
		ProcessTimeout:  time.Second,
		Partitions:      8,
		QueueFullPolicy: mqx.QueueFullBlock,
	})
	if err != nil {
		return err
	}

	d := newDB()
	var processed atomic.Int32
	_, err = mq.Subscribe(ctx, "account.incr", "db-writer", 8, func(_ context.Context, msg *mqx.Message) error {
		delta, _ := strconv.Atoi(string(msg.Body))
		d.write(msg.Key, delta)
		processed.Add(1)
		return nil
	})
	if err != nil {
		return err
	}

	// 突发 1000 条：4 个账户各 250 次 +1。
	items := make([]mqx.ProduceItem, 0, 1000)
	for i := 0; i < 1000; i++ {
		items = append(items, mqx.ProduceItem{
			Key:  fmt.Sprintf("account-%d", i%4),
			Body: []byte("1"),
		})
	}
	if err := mq.ProduceBatch(ctx, "account.incr", items); err != nil {
		return err
	}
	waitFor(ctx, func() bool { return processed.Load() == 1000 })
	d.mu.Lock()
	defer d.mu.Unlock()
	for k := 0; k < 4; k++ {
		key := fmt.Sprintf("account-%d", k)
		if d.values[key] != 250 {
			return fmt.Errorf("账户 %s 累计值应为 250，实际 %d", key, d.values[key])
		}
	}
	if d.violated.Load() {
		return fmt.Errorf("检测到同一账户并发写（顺序保证失效）")
	}
	fmt.Println("削峰落库完成：4 个账户各累计 250，无并发写冲突")
	return nil
}

// waitFor 轮询等待条件。
func waitFor(ctx context.Context, cond func() bool) {
	for !cond() {
		select {
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Millisecond):
		}
	}
}
