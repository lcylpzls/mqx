package core

import (
	"context"
	"testing"

	"github.com/lcylpzls/errx"
	"github.com/lcylpzls/testx"
)

// TestTopicsAndGroups 覆盖主题与组枚举。
func TestTopicsAndGroups(t *testing.T) {
	m := newTestMQ(t)
	_, err := m.CreateTopic("orders", smallTopic())
	testx.RequireNoError(t, err)
	_, err = m.CreateTopic("audit", smallTopic())
	testx.RequireNoError(t, err)
	_, err = m.Subscribe(context.Background(), "orders", "writer", 1, func(context.Context, *Message) error {
		return nil
	})
	testx.RequireNoError(t, err)
	_, err = m.Subscribe(context.Background(), "orders", "audit", 1, func(context.Context, *Message) error {
		return nil
	})
	testx.RequireNoError(t, err)
	topics := m.Topics()
	testx.RequireEqual(t, len(topics), 2)
	groups, err := m.Groups("orders")
	testx.RequireNoError(t, err)
	testx.RequireEqual(t, len(groups), 2)
	if _, err := m.Groups("missing"); !errx.Is(err, CodeTopicNotFound) {
		t.Fatalf("未知主题应报错：%v", err)
	}
}
