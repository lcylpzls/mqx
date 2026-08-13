package core

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/lcylpzls/logx"
	"github.com/lcylpzls/testx"
)

// testLogger 构造写入丢弃目标的日志器。
func testLogger() logx.Logger {
	logger, err := logx.NewBuilder().EnableWriter(io.Discard, logx.InfoLevel).Build()
	if err != nil {
		panic(err)
	}
	return logger
}

// newTestMQ 构造测试用消息队列，并在测试结束时关闭。
func newTestMQ(t *testing.T, opts ...Option) *MQ {
	t.Helper()
	m, err := New(opts...)
	testx.RequireNoError(t, err)
	t.Cleanup(func() { _ = m.Shutdown(context.Background()) })
	return m
}

// waitFor 轮询等待条件成立。
func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("等待条件超时")
}
