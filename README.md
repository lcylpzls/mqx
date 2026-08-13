# mqx

自研内置通用消息队列：生产消费、削峰、按业务键顺序执行，
与 errx / logx / tracex / metricsx / idgenx 家族生态打通。

> 当前状态：**v0.3.0**。

## 定位

mqx **不是分布式消息中间件**，不解决跨进程消息传递；它解决单进程内
每个业务都要重复的部分：

- **按 key 分区保序**：同一业务键（订单号、用户 ID、SKU）的消息严格
  串行处理，不同键并行——结构性消除“同一数据库行并发写导致锁冲突/
  死锁/事务重试”的问题；
- **生产消费 + 削峰**：有界队列吸收突发，消费者按自身节奏消费；
- **至少一次投递**：手动确认（handler 返回 nil=成功）、处理超时重投、
  失败自动重试、超限进死信队列（DLQ）；
- **消费者组**：组内按 key 固定分配，多组独立进度；
- **可观测性**：logx 结构化日志、metricsx 指标、tracex 钩子；
- **错误语义**：统一 errx 错误码。

所有组件并发安全，可在多个 goroutine 间共享。

## 核心特性

- 按 key 分区保序（同 key 串行、跨 key 并行）；
- at-least-once：handler 返回值即确认信号，处理超时（默认 30s）重投；
- 消费者组：组内 N 个消费者固定分配，运行期不动态扩缩容；
- 有界队列 + 队列满策略：`Block`（默认）/ `Drop` / `Reject`；
- 失败重试（默认 3 次、1s 起指数退避）+ DLQ 重放；
- DLQ 默认启用（`DisableDLQ` 可关闭），死信消息 `Attempt` 从 1 重新计数；
- v0.1 纯内存 + `Store` 接口预留（已 ack 不丢、未 ack 可重投）；
- 显式 `CreateTopic`，投递/订阅不存在的 topic 返回明确错误；
- 消息体大小上限（默认 1 MiB）与主题统计快照（`Stats`）；
- 订阅暂停/恢复（`Pause`/`Resume`，维护窗口与削峰）与主题删除（`DeleteTopic`）；
- 消息体 `[]byte`，序列化由业务负责，库保持零第三方依赖。

## 明确不做（v0.1）

- 延迟消息/定时投递（复用 jobx 的延迟与调度能力）；
- 跨进程网络协议、集群、消费位点外置；
- 消费者组动态扩缩容（重平衡语义留待后续版本）。

## 快速上手（API 草案）

```go
mq, _ := mqx.New()

_, _ = mq.CreateTopic("orders.write", mqx.TopicConfig{
	QueueSize:       1024,
	RetryMax:        3,
	RetryDelay:      time.Second,
	ProcessTimeout:  30 * time.Second,
	QueueFullPolicy: mqx.QueueFullBlock,
})

_ = mq.Produce(context.Background(), "orders.write", "order-10086", body)

_, _ = mq.Subscribe(context.Background(), "orders.write", "writer", func(ctx context.Context, msg *mqx.Message) error {
	// 同一 order-10086 的消息串行到达；返回 nil 表示确认。
	return nil
})
```

## 文档索引

- [docs/README.md](docs/README.md) — 文档索引
- [docs/architecture.md](docs/architecture.md) — 架构详解（顺序模型/状态机/削峰/重试）
- [docs/api.md](docs/api.md) — API 定版草案（签名与语义）
- [docs/benchmark.md](docs/benchmark.md) — 基准测试报告

## License

MIT © [lcylpzls](https://github.com/lcylpzls)
