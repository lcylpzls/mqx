# mqx 架构详解

> 版本：v0.1 草案 · 本文是项目最需要跟进的部分。

## 1. 定位与边界

```
业务（生产者）──Produce──► mqx（进程内）
                              │ 按 key 分区保序
                              │ 有界队列 + 背压
                              │ 消费者组固定分配
                              ▼
业务（消费者 handler）── 返回 nil=ack / error=重试 / 超时=重投
                              │ 重试耗尽
                              ▼
                           DLQ（可重放）
```

非目标（v0.1）：跨进程网络协议、集群、延迟消息、动态扩缩容。

### 1.1 与家族库的定位区别

- `eventx`：进程内事件总线（通知语义：通配符、过滤器、同步/异步投递），
  不保证送达、无 ack/位点/消费者组；
- `jobx`：任务执行与调度（我要跑任务：handler 模型、延迟/定时、重试），
  明确不是消息队列；
- `mqx`：消息传递与消费管理（我要收发消息：topic/分区/消费者组、
  ack/位点、削峰背压、DLQ）。

同一需求应只选一个：异步任务用 jobx，事件通知用 eventx，
需要顺序保证与消费进度的消息流用 mqx。

## 2. 组件

```
mqx
├── MQ              # 门面：CreateTopic / Produce / Subscribe / Shutdown
├── Topic           # 队列 + 分区 + 重试/DLQ 配置
├── Partition       # 按 key 取模/哈希的静态分区，内部 FIFO
├── ConsumerGroup   # 组内 N 个消费者，key → 消费者固定分配
├── DLQ             # 死信队列（独立可消费、可重放）
├── Store           # 可选持久化接口（v0.1 预留，默认内存）
└── observability   # logx / metricsx / tracex 钩子
```

依赖方向：`mqx` → 各内部组件；核心不依赖任何第三方库。

## 3. 顺序保证模型

- 每个 topic 有固定数量的分区（`Partitions`，默认按消费者组大小推导，
  或显式配置）；
- 消息按 `key` 哈希静态映射到分区（`hash(key) % partitions`）；
- 分区是严格 FIFO；同一分区同一时刻只被一个消费者处理；
- 结论：**同一 key 永远落在同一分区 → 同一分区串行 → 同 key 串行**；
  不同 key 可能在同一分区（串行）或不同分区（并行），顺序与并发均由
  库保证，不依赖业务约定。

## 4. 消息状态机

```
Produced → Queued（分区 FIFO）→ Delivering（in-flight）
            │                        │
            │                        ├─ handler 返回 nil → Acked（推进位点）
            │                        ├─ handler 返回 error → Retrying（重试次数内）
            │                        ├─ 处理超时（默认 30s）→ Retrying（未确认重投）
            │                        └─ 重试耗尽 → DLQ
            │
            └─ 队列满：Block（默认）/ Drop / Reject
```

重试期间，同一 key 的后续消息继续排队等待，直到该消息 Ack 或进 DLQ；
不同 key 不受影响。DLQ 消息保留 `Message{ID, Key, Body, Attempt, Err}`，
可独立订阅消费，也可手动重放回原 topic。

## 5. 消费者组

- 注册时指定组名与消费者数量 N（固定，运行期不扩缩容）；
- 组内 key → 消费者分配为静态映射，同一 key 不换手；
- 多组互相独立：各自的消费进度、ack、重试与 DLQ 策略；
- 典型用法：`writer` 组负责落库（保证顺序），`audit` 组只读观察。

## 6. 削峰与背压

- 有界队列（`QueueSize`，默认 1024）吸收突发；
- 队列满策略：
  - `Block`（默认）：生产者阻塞等待，保证不丢，与 at-least-once 一致；
  - `Drop`：丢弃新消息并返回 `ErrQueueFull`；
  - `Reject`：拒绝新消息并返回 `ErrQueueFull`（与 Drop 的差异在指标与
    语义表达，v0.1 两者行为等价，保留枚举便于后续扩展）；
- 消费者速率由“分区串行”天然限流，削峰不靠丢消息，靠排队。

## 7. at-least-once 与确认

- handler 返回 `nil` = 确认（ack），推进位点；
- handler 返回 `error` = 未确认，按 `RetryMax`/`RetryDelay` 重试；
- 处理超时（`ProcessTimeout`，默认 30s）未返回 = 未确认，重投；
- 幂等：`Message.ID` 由 idgenx 生成且全局唯一，业务侧以 ID 做幂等键，
  重投不产生重复副作用。

## 8. 持久化（Store 预留）

- v0.1 默认内存实现：进程重启消息丢失；
- `Store` 接口抽象消息体、位点与 in-flight 状态；
- 启用 Store 后的最低承诺：**已 ack 不丢、未 ack 可重投**；
- 具体落盘实现（WAL/数据库）为后续版本，不改变公开 API。

## 9. 并发与生命周期

- 全组件并发安全；分区串行由内部每分区单消费者保证；
- `Shutdown(ctx)`：停止接受投递 → 停止新消费 → 等待 in-flight
  在 `ProcessTimeout` 内落定 → 排空/丢弃策略由配置决定；
- 优雅关闭期间未确认消息保留在队列，下次启动可继续消费（配合 Store）。

## 10. 可观测性

- logx：创建 topic、投递拒绝、重试、DLQ、关闭等事件的结构化日志；
- metricsx：投递数、队列深度、消费耗时、重试/死信计数、in-flight 水位；
- tracex：Produce → 分区入队 → handler 执行的链路钩子。
