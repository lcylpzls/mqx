# mqx 基准测试报告

> 环境：Windows 11 · AMD Ryzen 5 7600 · Go 1.26.5
> 方法：`go test -bench 'BenchmarkProduceConsume' -benchtime=1s -run '^$' ./internal/core`

## 结果

| 场景 | 吞吐 | 单次操作耗时 |
| --- | --- | --- |
| 单 key 串行（1 分区，1 消费者） | ~87.8 万 ops/s | 1139 ns/op |
| 多 key 并行（8 分区，8 消费者） | ~144.5 万 ops/s | 692 ns/op |

## 说明

- 基准覆盖“生产入队 → 分区串行投递 → 消费者 ack”完整闭环，
  含锁、通道广播与游标推进；
- 单 key 场景受“同 key 串行”约束，吞吐即单分区上限；
- 多 key 场景体现按分区并行带来的吞吐提升（约 1.65×）；
- 数值随机器与 Go 版本波动，建议在目标部署环境复测。
