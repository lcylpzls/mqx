package core

import (
	"errors"
	"testing"

	"github.com/lcylpzls/errx"
)

// TestErrorCodes 覆盖错误码与错误值引用。
func TestErrorCodes(t *testing.T) {
	_ = []errx.Code{
		CodeInvalidConfig, CodeTopicExists, CodeTopicNotFound, CodeQueueFull,
		CodeShuttingDown, CodeProcessTimeout, CodeRetryExhausted, CodeIDGenerateFailed,
	}
	_ = []error{
		ErrInvalidConfig, ErrTopicExists, ErrTopicNotFound, ErrQueueFull,
		ErrShuttingDown, ErrProcessTimeout, ErrRetryExhausted, ErrIDGenerateFailed,
	}
	for _, e := range []error{
		ErrInvalidConfig, ErrTopicExists, ErrTopicNotFound, ErrQueueFull,
		ErrShuttingDown, ErrProcessTimeout, ErrRetryExhausted, ErrIDGenerateFailed,
	} {
		if !errors.Is(e, e) {
			t.Fatalf("错误值应可被 errors.Is 匹配：%v", e)
		}
	}
	if !errx.Is(ErrQueueFull, CodeQueueFull) {
		t.Fatal("错误码匹配失败")
	}
}
