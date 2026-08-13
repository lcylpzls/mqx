package core

import (
	"reflect"
	"time"

	"github.com/lcylpzls/errx"
	"github.com/lcylpzls/idgenx"
	"github.com/lcylpzls/logx"
	"github.com/lcylpzls/tracex/contract"
)

// 默认配置值。
const (
	defaultQueueSize      = 1024
	defaultRetryMax       = 3
	defaultRetryDelay     = time.Second
	defaultProcessTimeout = 30 * time.Second
	defaultPartitions     = 16
	messageIDBytes        = 16
)

// QueueFullPolicy 队列满时的投递策略。
type QueueFullPolicy uint8

const (
	// QueueFullBlock 阻塞等待队列空间（默认，保证不丢）。
	QueueFullBlock QueueFullPolicy = iota
	// QueueFullDrop 丢弃新消息并返回 ErrQueueFull。
	QueueFullDrop
	// QueueFullReject 拒绝新消息并返回 ErrQueueFull。
	QueueFullReject
)

// TopicConfig 主题配置。
type TopicConfig struct {
	// QueueSize 有界队列容量（默认 1024）。
	QueueSize int
	// QueueFullPolicy 队列满策略（默认 Block）。
	QueueFullPolicy QueueFullPolicy
	// RetryMax 失败最大重试次数（默认 3；0 表示失败直接进 DLQ）。
	RetryMax int
	// RetryDelay 首次重试退避（默认 1s，指数增长）。
	RetryDelay time.Duration
	// ProcessTimeout 单条消息处理超时（默认 30s，超时按未确认重投）。
	ProcessTimeout time.Duration
	// Partitions 分区数（默认 16，创建后固定；建议不小于最大消费者数）。
	Partitions int
	// DisableDLQ 关闭死信队列（默认 false，即默认启用 DLQ）。
	DisableDLQ bool
}

// withDefaults 填充默认值。
func (c *TopicConfig) withDefaults() {
	if c.QueueSize <= 0 {
		c.QueueSize = defaultQueueSize
	}
	if c.RetryMax <= 0 {
		c.RetryMax = defaultRetryMax
	}
	if c.RetryDelay <= 0 {
		c.RetryDelay = defaultRetryDelay
	}
	if c.ProcessTimeout <= 0 {
		c.ProcessTimeout = defaultProcessTimeout
	}
	if c.Partitions <= 0 {
		c.Partitions = defaultPartitions
	}
}

// validate 校验配置并返回错误。
func (c *TopicConfig) validate() error {
	if c.QueueSize <= 0 {
		return errInvalidConfig("队列容量必须为正")
	}
	if c.QueueFullPolicy > QueueFullReject {
		return errInvalidConfig("非法队列满策略")
	}
	if c.RetryMax < 0 {
		return errInvalidConfig("重试次数不能为负")
	}
	if c.RetryDelay < 0 {
		return errInvalidConfig("重试退避不能为负")
	}
	if c.ProcessTimeout <= 0 {
		return errInvalidConfig("处理超时必须为正")
	}
	if c.Partitions <= 0 {
		return errInvalidConfig("分区数必须为正")
	}
	return nil
}

// Metrics 外部注入的消息队列指标回调（全部可选，nil 跳过）。
type Metrics struct {
	// Produced 投递成功。
	Produced func(topic string)
	// QueueFull 队列满（Drop/Reject 或阻塞后成功不算）。
	QueueFull func(topic string)
	// Consumed 消费成功。
	Consumed func(topic string, duration time.Duration)
	// Retried 安排重试（attempt 为即将执行的次数）。
	Retried func(topic string, attempt int)
	// Dead 进入死信队列。
	Dead func(topic string)
	// Replayed 从死信队列重放。
	Replayed func(topic string)
	// DLQDropped 死信队列已满丢弃。
	DLQDropped func(topic string)
}

// TraceHook 链路追踪钩子（家族统一契约）。
type TraceHook = contract.TraceHook

// TraceAttr 链路追踪属性（家族统一契约）。
type TraceAttr = contract.TraceAttr

// Config 是消息队列全局配置。
type Config struct {
	logger  logx.Logger
	metrics Metrics
	trace   TraceHook
	store   Store
	now     func() time.Time
	idgen   func(int) (string, error)
}

// Option 修改全局配置。
type Option func(*Config) error

// WithLogger 注入结构化日志器；nil（含类型化 nil）自动归一为 no-op。
func WithLogger(logger logx.Logger) Option {
	return func(c *Config) error {
		c.logger = normalizeLogger(logger)
		return nil
	}
}

// WithMetrics 注入指标回调（全部可选）。
func WithMetrics(m Metrics) Option {
	return func(c *Config) error {
		c.metrics = m
		return nil
	}
}

// WithTraceHook 注入链路追踪钩子。
func WithTraceHook(h TraceHook) Option {
	return func(c *Config) error {
		c.trace = h
		return nil
	}
}

// WithClock 注入时间源（测试用）。
func WithClock(now func() time.Time) Option {
	return func(c *Config) error {
		if now == nil {
			return errInvalidConfig("时间源不能为空")
		}
		c.now = now
		return nil
	}
}

// WithIDGen 注入消息 ID 生成函数（测试用）。
func WithIDGen(fn func(int) (string, error)) Option {
	return func(c *Config) error {
		if fn == nil {
			return errInvalidConfig("消息 ID 生成函数不能为空")
		}
		c.idgen = fn
		return nil
	}
}

// defaultConfig 返回默认全局配置。
func defaultConfig() Config {
	return Config{
		logger: logx.NewNopLogger(),
		now:    time.Now,
		idgen:  func(n int) (string, error) { return idgenx.RandomHex(n) },
	}
}

// normalizeLogger 归一日志器：未类型化 nil 与类型化 nil（接口内装 nil
// 指针）统一替换为 no-op logger，保证配置完成后 logger 恒非 nil。
func normalizeLogger(logger logx.Logger) logx.Logger {
	if logger == nil {
		return logx.NewNopLogger()
	}
	v := reflect.ValueOf(logger)
	if v.Kind() == reflect.Ptr && v.IsNil() {
		return logx.NewNopLogger()
	}
	return logger
}

// errInvalidConfig 构造配置错误。
func errInvalidConfig(msg string) error {
	return errx.New(errx.KindInvalid, CodeInvalidConfig, msg)
}
