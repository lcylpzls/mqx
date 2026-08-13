// advanced 示例：多消费者组、暂停/恢复与主题统计。
package main

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/lcylpzls/mqx"
)

func main() {
	if err := run(context.Background()); err != nil {
		panic(err)
	}
}

// run 演示 writer/audit 双组消费与暂停削峰。
func run(ctx context.Context) error {
	mq, err := mqx.New()
	if err != nil {
		return err
	}
	defer func() { _ = mq.Shutdown(ctx) }()

	_, err = mq.CreateTopic("orders.write", mqx.TopicConfig{
		QueueSize:       64,
		RetryMax:        1,
		RetryDelay:      20 * time.Millisecond,
		ProcessTimeout:  time.Second,
		Partitions:      8,
		QueueFullPolicy: mqx.QueueFullBlock,
	})
	if err != nil {
		return err
	}

	var written, audited atomic.Int32
	writer, err := mq.Subscribe(ctx, "orders.write", "writer", 4, func(context.Context, *mqx.Message) error {
		written.Add(1)
		return nil
	})
	if err != nil {
		return err
	}
	if _, err := mq.Subscribe(ctx, "orders.write", "audit", 2, func(context.Context, *mqx.Message) error {
		audited.Add(1)
		return nil
	}); err != nil {
		return err
	}

	for i := 0; i < 10; i++ {
		if err := mq.Produce(ctx, "orders.write", fmt.Sprintf("order-%d", i%4), nil); err != nil {
			return err
		}
	}
	waitFor(ctx, func() bool { return written.Load() == 10 && audited.Load() == 10 })
	printStats(mq, "orders.write")

	// 暂停 writer 组模拟维护窗口：消息排队，audit 组不受影响。
	writer.Pause()
	for i := 0; i < 2; i++ {
		if err := mq.Produce(ctx, "orders.write", "order-paused", nil); err != nil {
			return err
		}
	}
	waitFor(ctx, func() bool { return audited.Load() == 12 && written.Load() == 10 })
	stats, _ := mq.Stats("orders.write")
	fmt.Printf("暂停期间 pending=%d\n", stats.Pending)
	writer.Resume()
	waitFor(ctx, func() bool { return written.Load() == 12 })
	printStats(mq, "orders.write")
	return nil
}

// printStats 打印主题统计。
func printStats(mq *mqx.MQ, name string) {
	stats, err := mq.Stats(name)
	if err != nil {
		fmt.Printf("统计失败：%v\n", err)
		return
	}
	fmt.Printf("统计：分区=%d pending=%d consumed=%d dlq_pending=%d\n",
		stats.Partitions, stats.Pending, stats.Consumed, stats.DLQPending)
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
