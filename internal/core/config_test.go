package core

import (
	"context"
	"testing"
	"time"

	"github.com/lcylpzls/errx"
	"github.com/lcylpzls/logx"
	"github.com/lcylpzls/testx"
)

// typedNilLogger 是测试类型化 nil 的假日志器。
type typedNilLogger struct{ n int }

func (l *typedNilLogger) IsDebugEnabled() bool                        { _ = l.n; return false }
func (l *typedNilLogger) Debug(msg string, fields logx.FieldGroup)    { _ = l.n }
func (l *typedNilLogger) Info(msg string, fields logx.FieldGroup)     { _ = l.n }
func (l *typedNilLogger) Warn(msg string, fields logx.FieldGroup)     { _ = l.n }
func (l *typedNilLogger) Error(msg string, fields logx.FieldGroup)    { _ = l.n }
func (l *typedNilLogger) Panic(msg string, fields logx.FieldGroup)    { _ = l.n }
func (l *typedNilLogger) Fatal(msg string, fields logx.FieldGroup)    { _ = l.n }
func (l *typedNilLogger) Debugf(format string, args ...any)           { _ = l.n }
func (l *typedNilLogger) Infof(format string, args ...any)            { _ = l.n }
func (l *typedNilLogger) Warnf(format string, args ...any)            { _ = l.n }
func (l *typedNilLogger) Errorf(format string, args ...any)           { _ = l.n }
func (l *typedNilLogger) Panicf(format string, args ...any)           { _ = l.n }
func (l *typedNilLogger) Fatalf(format string, args ...any)           { _ = l.n }
func (l *typedNilLogger) WithContext(ctx context.Context) logx.Logger { _ = l.n; return l }
func (l *typedNilLogger) WithField(key string, val any) logx.Logger   { _ = l.n; return l }
func (l *typedNilLogger) Sync() error                                 { _ = l.n; return nil }
func (l *typedNilLogger) Close() error                                { _ = l.n; return nil }
func (l *typedNilLogger) SafeExit(f func())                           { _ = l.n; f() }

// TestTopicConfigDefaults 覆盖默认值与非法配置。
func TestTopicConfigDefaults(t *testing.T) {
	cfg := TopicConfig{}
	cfg.withDefaults()
	testx.RequireEqual(t, cfg.QueueSize, defaultQueueSize)
	testx.RequireEqual(t, cfg.QueueFullPolicy, QueueFullBlock)
	testx.RequireEqual(t, cfg.RetryMax, defaultRetryMax)
	testx.RequireEqual(t, cfg.RetryDelay, defaultRetryDelay)
	testx.RequireEqual(t, cfg.ProcessTimeout, defaultProcessTimeout)
	testx.RequireEqual(t, cfg.Partitions, defaultPartitions)
	testx.RequireEqual(t, cfg.MaxMessageBytes, defaultMaxMessageBytes)
	testx.RequireEqual(t, cfg.DisableDLQ, false)
	testx.RequireNoError(t, cfg.validate())

	bad := TopicConfig{QueueSize: 0}
	if err := bad.validate(); err == nil {
		t.Fatal("队列容量非法应报错")
	}
	bad2 := TopicConfig{QueueSize: 1, QueueFullPolicy: QueueFullPolicy(99), RetryMax: 1, RetryDelay: time.Second, ProcessTimeout: time.Second, Partitions: 1}
	if err := bad2.validate(); err == nil {
		t.Fatal("非法策略应报错")
	}
	bad3 := TopicConfig{QueueSize: 1, RetryMax: -1, RetryDelay: time.Second, ProcessTimeout: time.Second, Partitions: 1}
	if err := bad3.validate(); err == nil {
		t.Fatal("负重试次数应报错")
	}
	bad4 := TopicConfig{QueueSize: 1, RetryMax: 1, RetryDelay: -1, ProcessTimeout: time.Second, Partitions: 1}
	if err := bad4.validate(); err == nil {
		t.Fatal("负退避应报错")
	}
	bad5 := TopicConfig{QueueSize: 1, RetryMax: 1, RetryDelay: time.Second, ProcessTimeout: 0, Partitions: 1}
	if err := bad5.validate(); err == nil {
		t.Fatal("零超时应报错")
	}
	bad6 := TopicConfig{QueueSize: 1, RetryMax: 1, RetryDelay: time.Second, ProcessTimeout: time.Second, Partitions: 0}
	if err := bad6.validate(); err == nil {
		t.Fatal("零分区应报错")
	}
	bad7 := TopicConfig{QueueSize: 1, RetryMax: 1, RetryDelay: time.Second, ProcessTimeout: time.Second, Partitions: 1, MaxMessageBytes: 0}
	if err := bad7.validate(); err == nil {
		t.Fatal("零消息大小上限应报错")
	}
}

// TestNormalizeLogger 覆盖日志器归一。
func TestNormalizeLogger(t *testing.T) {
	if got := normalizeLogger(nil); got == nil {
		t.Fatal("未类型化 nil 应归一为非 nil")
	}
	var typed *typedNilLogger
	if got := normalizeLogger(typed); got == nil {
		t.Fatal("类型化 nil 应归一为非 nil")
	}
	real := testLogger()
	if got := normalizeLogger(real); got != real {
		t.Fatal("有效 logger 应原样返回")
	}
}

// TestDefaultConfig 覆盖默认全局配置。
func TestDefaultConfig(t *testing.T) {
	cfg := defaultConfig()
	if cfg.logger == nil || cfg.now == nil || cfg.idgen == nil {
		t.Fatal("默认配置字段不完整")
	}
}

// TestOptions 覆盖全部 Option 分支。
func TestOptions(t *testing.T) {
	cfg := defaultConfig()
	testx.RequireNoError(t, WithLogger(nil)(&cfg))
	testx.RequireNoError(t, WithLogger((*typedNilLogger)(nil))(&cfg))
	testx.RequireNoError(t, WithLogger(testLogger())(&cfg))
	testx.RequireNoError(t, WithMetrics(Metrics{})(&cfg))
	testx.RequireNoError(t, WithTraceHook(nil)(&cfg))
	testx.RequireNoError(t, WithStore(struct{}{})(&cfg))
	if err := WithStore(nil)(&cfg); err == nil {
		t.Fatal("nil Store 应报错")
	}
	testx.RequireNoError(t, WithClock(time.Now)(&cfg))
	if err := WithClock(nil)(&cfg); err == nil {
		t.Fatal("nil 时间源应报错")
	}
	testx.RequireNoError(t, WithIDGen(func(int) (string, error) { return "id", nil })(&cfg))
	if err := WithIDGen(nil)(&cfg); err == nil {
		t.Fatal("nil ID 生成函数应报错")
	}
}

// TestErrInvalidConfig 覆盖配置错误构造。
func TestErrInvalidConfig(t *testing.T) {
	if err := errInvalidConfig("测试"); !errx.Is(err, CodeInvalidConfig) {
		t.Fatalf("配置错误应可匹配：%v", err)
	}
}
