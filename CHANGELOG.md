# 更新日志

本项目遵循语义化版本（SemVer）。v1.0.0 之前允许破坏性变更。

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
