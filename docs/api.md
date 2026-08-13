# mqx API 定版草案

> 版本：v0.1.0 · 已实现签名与代码一致。

## 1. 公开类型

```go
type TopicConfig struct {
	QueueSize       int             // 有界队列容量，默认 1024
	QueueFullPolicy QueueFullPolicy // 默认 Block
	RetryMax        int             // 失败最大重试次数，默认 3
	RetryDelay      time.Duration   // 首次重试退避，默认 1s，指数增长
	ProcessTimeout  time.Duration   // 单条消息处理超时，默认 30s
	Partitions      int             // 分区数（0 = 默认 16，创建后固定）
	DisableDLQ      bool            // 默认 false（即默认启用 DLQ）
}

type QueueFullPolicy uint8
const (
	QueueFullBlock QueueFullPolicy = iota // 阻塞等待（默认）
	QueueFullDrop                          // 丢弃并返回 ErrQueueFull
	QueueFullReject                        // 拒绝并返回 ErrQueueFull
)

type Message struct {
	ID        string // 全局唯一（idgenx）
	Topic     string
	Key       string // 业务键，决定分区
	Body      []byte
	Attempt   int    // 当前投递次数（1 起；进入 DLQ 后重置为 1）
	EnqueueAt time.Time
	Err       error  // 仅 DLQ 消息携带失败原因
}

type Handler func(ctx context.Context, msg *Message) error
```

## 2. 门面 API

```go
func New(opts ...Option) (*MQ, error)
func (m *MQ) CreateTopic(name string, cfg TopicConfig) (*Topic, error)
func (m *MQ) Produce(ctx context.Context, topic, key string, body []byte) error
func (m *MQ) Subscribe(ctx context.Context, topic, group string, consumers int, h Handler) (*Subscription, error)
func (m *MQ) Shutdown(ctx context.Context) error
```

语义：

- `CreateTopic`：显式创建并校验配置；重复创建返回 `ErrTopicExists`；
- `Produce`：topic 不存在返回 `ErrTopicNotFound`；队列满按策略处理；
- `Subscribe`：组内 `consumers` 为分区归属粒度（`p % consumers`），
  每个分区独立串行投递，实际并发度取决于分区数；同一 topic 可注册
  多个组，组间独立；
- handler 返回 nil = 确认；error = 重试；超时未返回 = 重投；
- `Shutdown`：幂等，停止投递与消费并等待 in-flight 落定。

## 3. 订阅对象

```go
type Subscription struct{ /* 不可直接构造 */ }
func (s *Subscription) Group() string
func (s *Subscription) Stop() error // 停止该组消费（保留队列）
```

## 4. 默认值

| 配置 | 默认 |
| --- | --- |
| QueueSize | 1024 |
| QueueFullPolicy | Block |
| RetryMax | 3 |
| RetryDelay | 1s（指数退避） |
| ProcessTimeout | 30s |
| Partitions | 16（创建后固定） |
| DisableDLQ | false（DLQ 默认启用） |

## 5. 错误码（草案）

```go
const (
	CodeInvalidConfig   = "mqx_invalid_config"
	CodeTopicExists     = "mqx_topic_exists"
	CodeTopicNotFound   = "mqx_topic_not_found"
	CodeQueueFull       = "mqx_queue_full"
	CodeShuttingDown    = "mqx_shutting_down"
	CodeProcessTimeout  = "mqx_process_timeout"
	CodeRetryExhausted  = "mqx_retry_exhausted"
	CodeIDGenerateFailed = "mqx_id_generate_failed"
)
```

## 6. 明确不做（v0.1）

- 延迟消息（复用 jobx）；
- 跨进程网络协议与集群；
- 消费者组动态扩缩容；
- 内置落盘持久化（仅预留 `Store` 接口）。
