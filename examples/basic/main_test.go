package main

import (
	"context"
	"testing"
)

// TestRun 验证示例主流程：顺序消费与死信重放。
func TestRun(t *testing.T) {
	if err := run(context.Background()); err != nil {
		t.Fatalf("示例运行失败：%v", err)
	}
}
