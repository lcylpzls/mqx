// basic 示例：按业务键顺序执行的消息队列。
package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/lcylpzls/mqx"
)

func main() {
	if err := run(context.Background()); err != nil {
		panic(err)
	}
}

// run 演示生产消费、按 key 串行与死信重放。
func run(ctx context.Context) error {
	mq, err := mqx.New()
	if err != nil {
		return err
	}
	defer func() { _ = mq.Shutdown(ctx) }()

	_, err = mq.CreateTopic("orders.write", mqx.TopicConfig{
		QueueSize:       16,
		RetryMax:        1,
		RetryDelay:      20 * time.Millisecond,
		ProcessTimeout:  time.Second,
		Partitions:      4,
		QueueFullPolicy: mqx.QueueFullBlock,
	})
	if err != nil {
		return err
	}

	var mu sync.Mutex
	var received []string
	_, err = mq.Subscribe(ctx, "orders.write", "writer", 2, func(_ context.Context, msg *mqx.Message) error {
		if string(msg.Body) == "坏消息" {
			return fmt.Errorf("业务拒绝：%s", msg.Body)
		}
		mu.Lock()
		received = append(received, string(msg.Body))
		mu.Unlock()
		return nil
	})
	if err != nil {
		return err
	}

	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("order-%d", i%3)
		if err := mq.Produce(ctx, "orders.write", key, []byte(fmt.Sprintf("订单%d", i))); err != nil {
			return err
		}
	}
	// 一条必败消息进入死信队列。
	if err := mq.Produce(ctx, "orders.write", "order-0", []byte("坏消息")); err != nil {
		return err
	}
	time.Sleep(300 * time.Millisecond)

	mu.Lock()
	count := len(received)
	snapshot := append([]string(nil), received...)
	mu.Unlock()
	fmt.Printf("已消费 %d 条：%v\n", count, snapshot)
	fmt.Println("死信重放……")
	if err := mq.Replay(ctx, "orders.write.dlq"); err != nil {
		return err
	}
	time.Sleep(200 * time.Millisecond)
	mu.Lock()
	count = len(received)
	mu.Unlock()
	fmt.Printf("重放后共消费 %d 条\n", count)
	return nil
}
