package main

import (
	"context"
	"testing"
)

// TestRun 验证生态示例：按 key 串行落库无并发写冲突。
func TestRun(t *testing.T) {
	if err := run(context.Background()); err != nil {
		t.Fatalf("示例运行失败：%v", err)
	}
}
