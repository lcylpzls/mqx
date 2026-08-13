package mqx_test

import (
	"context"
	"testing"
	"time"

	"github.com/lcylpzls/errx"
	"github.com/lcylpzls/mqx"
)

// TestPublicAPI 黑盒冒烟测试：覆盖根包全部转发函数、类型别名与常量。
func TestPublicAPI(t *testing.T) {
	if mqx.Version != "v0.2.0" {
		t.Fatalf("Version 不符：%s", mqx.Version)
	}
	_ = []mqx.QueueFullPolicy{mqx.QueueFullBlock, mqx.QueueFullDrop, mqx.QueueFullReject}
	_ = []mqx.Handler{func(context.Context, *mqx.Message) error { return nil }}
	var _ mqx.MQ
	var _ mqx.Topic
	var _ mqx.Subscription
	var _ mqx.TopicConfig
	var _ mqx.Message
	var _ mqx.Metrics
	var _ mqx.TraceAttr
	var _ mqx.TraceHook
	var _ mqx.Store
	var _ mqx.TopicStats
	var _ mqx.Config
	var _ mqx.Option
	_ = mqx.CodeInvalidConfig
	_ = mqx.CodeMessageTooLarge
	_ = mqx.ErrTopicNotFound
	_ = mqx.ErrMessageTooLarge

	mq, err := mqx.New(
		mqx.WithLogger(nil),
		mqx.WithMetrics(mqx.Metrics{}),
		mqx.WithTraceHook(nil),
		mqx.WithStore(struct{}{}),
		mqx.WithClock(time.Now),
		mqx.WithIDGen(func(int) (string, error) { return "id", nil }),
	)
	if err != nil || mq == nil {
		t.Fatalf("New 失败：%v", err)
	}
	if _, err := mqx.New(mqx.WithStore(nil)); err == nil {
		t.Fatal("nil Store 应报错")
	}

	_, err = mq.CreateTopic("orders", mqx.TopicConfig{
		QueueSize:       8,
		RetryMax:        1,
		RetryDelay:      10 * time.Millisecond,
		ProcessTimeout:  100 * time.Millisecond,
		Partitions:      4,
		QueueFullPolicy: mqx.QueueFullDrop,
	})
	if err != nil {
		t.Fatalf("CreateTopic 失败：%v", err)
	}
	if err := mq.Produce(context.Background(), "orders", "k", []byte("x")); err != nil {
		t.Fatalf("Produce 失败：%v", err)
	}
	sub, err := mq.Subscribe(context.Background(), "orders", "g", 1, func(context.Context, *mqx.Message) error {
		return nil
	})
	if err != nil || sub == nil {
		t.Fatalf("Subscribe 失败：%v", err)
	}
	if sub.Group() != "g" {
		t.Fatal("Group 不符")
	}
	if err := mq.Replay(context.Background(), "orders.dlq"); err != nil {
		t.Fatalf("Replay 失败：%v", err)
	}
	if err := mq.Replay(context.Background(), "bad-name"); !errx.Is(err, mqx.CodeInvalidConfig) {
		t.Fatalf("非法 DLQ 名应报错：%v", err)
	}
	if err := sub.Stop(); err != nil {
		t.Fatalf("Stop 失败：%v", err)
	}
	if err := mq.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown 失败：%v", err)
	}
}
