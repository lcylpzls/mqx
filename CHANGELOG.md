# 更新日志

本项目遵循语义化版本（SemVer）。v1.0.0 之前允许破坏性变更。

## [v0.2.0] - 2026-08-14

## [v0.3.0] - 2026-08-14

## [v0.4.0] - 2026-08-14

## [v0.5.0] - 2026-08-14

## [v0.6.0] - 2026-08-14

## [v0.7.0] - 2026-08-14

## [v0.8.0] - 2026-08-14

## [v0.9.0] - 2026-08-14

### 新增

- `NewFileStore(path, WithSync())`：文件存储同步模式，每次写入后
  fsync，断电不丢已确认写入（吞吐较低）；
- 同步失败按 `CodeStoreFailed` 返回/记录。

### 质量

- 根包与 internal/core 覆盖率 100%；race / vet / staticcheck /
  fuzz / govulncheck 全绿。

## [v0.8.0] - 2026-08-14

### 新增

- 死信队列持久化闭环：进入 DLQ 时落盘（存储标识 `dlq:<id>` 与主队列
  隔离）、`Recover` 恢复死信、DLQ 消费后删除；
- 死信落盘失败时保留内存副本并记录错误，删除失败不阻断消费。

### 质量

- 根包与 internal/core 覆盖率 100%；race / vet / staticcheck /
  fuzz / govulncheck 全绿。

## [v0.7.0] - 2026-08-14

### 新增

- `Store` 接口正式接入：入队前同步落盘（失败则投递失败）、消息被
  全部消费者组越过时删除、`Recover` 恢复未删除消息；
- `NewFileStore`：追加日志 + 墓碑 + 自动压实的进程级文件存储；
- 最低持久化承诺落地：已 ack 不丢、未 ack 可重投（进程正常退出/重启）。

### 质量

- 根包与 internal/core 覆盖率 100%；race / vet / staticcheck /
  fuzz / govulncheck 全绿。

## [v0.6.0] - 2026-08-14

### 新增

- `TopicConfig.Validate()` 公开预检（CreateTopic 前可复用）；
- `Subscription.Consumers()` 查询组内消费者数量；
- 生态示例 `examples/ecosystem`：突发 1000 条增量按 key 串行落库，
  断言无同条目并发写冲突（削峰 + 顺序保证演示）；
- 架构文档补充与 eventx / jobx 的定位区别。

### 质量

- 根包与 internal/core 覆盖率 100%；race / vet / staticcheck /
  fuzz / govulncheck 全绿。

## [v0.5.0] - 2026-08-14

### 新增

- `MQ.Topics()` / `MQ.Groups(topic)`：主题与消费者组枚举；
- 并发压力测试：4 key × 500 条并发生产，断言不丢且每 key 顺序不乱；
- `docs/errors.md` 错误码手册。

### 修复

- 暂停/恢复的丢失唤醒竞态：暂停检查与恢复通道捕获改为原子操作，
  避免 Resume 落在两者之间导致消费者永久等待。

### 质量

- 根包与 internal/core 覆盖率 100%；race / vet / staticcheck /
  fuzz / govulncheck 全绿。

## [v0.4.0] - 2026-08-14

### 新增

- `ProduceBatch`：批量投递（按顺序逐条入队，非事务）；
- `Message.Attrs`：消息元数据（trace_id、来源服务等），入队与进入
  DLQ 时深拷贝；
- `TopicStats` 增加 `InFlight` / `DLQInFlight`：当前投递中（含重试
  等待）的消息数，供并发水位监控。

### 质量

- 根包与 internal/core 覆盖率 100%；race / vet / staticcheck /
  fuzz / govulncheck 全绿。

## [v0.3.0] - 2026-08-14

### 新增

- `Subscription.Pause/Resume`：暂停期间消息在分区内排队（在途消息
  继续处理），恢复后自动续消费；多组互相独立；
- `MQ.DeleteTopic`：停止主题全部订阅并移除路由，之后投递/订阅/
  统计/重放返回 `ErrTopicNotFound`；
- 高级示例 `examples/advanced`：writer/audit 双组消费、暂停削峰
  与主题统计演示。

### 质量

- 根包与 internal/core 覆盖率 100%；race / vet / staticcheck /
  fuzz / govulncheck 全绿。

## [v0.2.0] - 2026-08-14

### 新增

- `TopicConfig.MaxMessageBytes`：消息体大小上限（默认 1 MiB），
  超限投递返回 `ErrMessageTooLarge`；
- `MQ.Stats(topic)`：主题统计快照（待处理、累计消费、死信水位），
  供监控面板与排障使用；
- 基准测试：单 key 串行与多 key 并行吞吐报告（docs/benchmark.md）。

### 质量

- 根包与 internal/core 覆盖率 100%；race / vet / staticcheck /
  fuzz / govulncheck 全绿。

## [v0.1.0] - 2026-08-14

### 新增

- 内置消息队列核心语义：
  - 按 key 分区保序（同 key 串行、跨 key 并行）；
  - at-least-once：handler 返回 nil=ack、error=重试、超时=未确认重投；
  - 消费者组：组内按 `p % consumers` 固定分配，多组独立进度；
  - 有界队列 + `Block`（默认）/ `Drop` / `Reject` 队列满策略；
  - 失败重试（默认 3 次、1s 起指数退避，封顶 1 小时）+ DLQ 重放；
  - 处理超时默认 30s，超时重投；
  - 显式 `CreateTopic`，消息体 `[]byte`，`Message.ID` 由 idgenx 生成；
  - `Store` 接口预留（v0.1 纯内存，已 ack 不丢、未 ack 可重投）。
- 家族生态：logx 日志、metricsx 指标回调、tracex 链路钩子、
  errx 错误码、testx 测试底座。

### 质量

- 根包与 internal/core 覆盖率 100%；race / vet / staticcheck /
  fuzz / govulncheck 全绿；
- 示例 `examples/basic`：顺序消费与死信重放演示。


### 待评审

- 架构与 API 草案已就绪（README / docs/architecture / docs/api）；
- 核心语义已达成共识：按 key 保序、at-least-once 手动确认、
  消费者组、有界队列背压、重试 + DLQ、纯内存 + Store 预留。

### 计划

- v0.1.0 起进入全自动编码 / CI / Release 循环，1.0 由维护者拍板。
