package mqx

import (
	"time"

	"github.com/lcylpzls/logx"
	"github.com/lcylpzls/mqx/internal/core"
)

// Version 是当前库版本，与 git tag 保持一致。
const Version = core.Version

const (
	CodeInvalidConfig    = core.CodeInvalidConfig
	CodeTopicExists      = core.CodeTopicExists
	CodeTopicNotFound    = core.CodeTopicNotFound
	CodeQueueFull        = core.CodeQueueFull
	CodeShuttingDown     = core.CodeShuttingDown
	CodeProcessTimeout   = core.CodeProcessTimeout
	CodeRetryExhausted   = core.CodeRetryExhausted
	CodeIDGenerateFailed = core.CodeIDGenerateFailed
	CodeMessageTooLarge  = core.CodeMessageTooLarge
	CodeStoreFailed      = core.CodeStoreFailed
)

var (
	ErrInvalidConfig    = core.ErrInvalidConfig
	ErrTopicExists      = core.ErrTopicExists
	ErrTopicNotFound    = core.ErrTopicNotFound
	ErrQueueFull        = core.ErrQueueFull
	ErrShuttingDown     = core.ErrShuttingDown
	ErrProcessTimeout   = core.ErrProcessTimeout
	ErrRetryExhausted   = core.ErrRetryExhausted
	ErrIDGenerateFailed = core.ErrIDGenerateFailed
	ErrMessageTooLarge  = core.ErrMessageTooLarge
	ErrStoreFailed      = core.ErrStoreFailed
)

type (
	QueueFullPolicy = core.QueueFullPolicy
	TopicConfig     = core.TopicConfig
	Message         = core.Message
	ProduceItem     = core.ProduceItem
	Handler         = core.Handler
	Metrics         = core.Metrics
	TraceAttr       = core.TraceAttr
	TraceHook       = core.TraceHook
	Store           = core.Store
	Config          = core.Config
	Option          = core.Option
	MQ              = core.MQ
	Topic           = core.Topic
	Subscription    = core.Subscription
	TopicStats      = core.TopicStats
)

const (
	QueueFullBlock  = core.QueueFullBlock
	QueueFullDrop   = core.QueueFullDrop
	QueueFullReject = core.QueueFullReject
)

// New 创建消息队列实例。
func New(opts ...Option) (*MQ, error) {
	return core.New(opts...)
}

// WithLogger 注入结构化日志器；nil（含类型化 nil）自动归一为 no-op。
func WithLogger(logger logx.Logger) Option {
	return core.WithLogger(logger)
}

// WithMetrics 注入指标回调（全部可选）。
func WithMetrics(m Metrics) Option {
	return core.WithMetrics(m)
}

// WithTraceHook 注入链路追踪钩子。
func WithTraceHook(h TraceHook) Option {
	return core.WithTraceHook(h)
}

// WithStore 注入持久化 Store。
func WithStore(s Store) Option {
	return core.WithStore(s)
}

// NewFileStore 创建基于追加日志的进程级文件存储。
func NewFileStore(path string) (Store, error) {
	return core.NewFileStore(path)
}

// WithClock 注入时间源（测试用）。
func WithClock(now func() time.Time) Option {
	return core.WithClock(now)
}

// WithIDGen 注入消息 ID 生成函数（测试用）。
func WithIDGen(fn func(int) (string, error)) Option {
	return core.WithIDGen(fn)
}
