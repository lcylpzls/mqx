package main

import (
	"context"
	"testing"
)

// TestRun 验证高级示例主流程。
func TestRun(t *testing.T) {
	if err := run(context.Background()); err != nil {
		t.Fatalf("示例运行失败：%v", err)
	}
}
