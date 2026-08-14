# mqx 错误码手册

所有错误统一走 errx：`CodeXxx` 常量 + `ErrXxx` 预定义值，
可用 `errx.Is(err, mqx.CodeXxx)` 或 `errors.Is` 匹配。

| 错误码 | 场景 |
| --- | --- |
| `mqx_invalid_config` | 配置非法（主题名、队列容量、策略、超时等） |
| `mqx_topic_exists` | `CreateTopic` 重复创建 |
| `mqx_topic_not_found` | 投递/订阅/统计/重放/删除不存在的主题 |
| `mqx_queue_full` | 队列满且策略为 Drop/Reject，或批量投递中途满 |
| `mqx_shutting_down` | 关闭后投递/创建主题 |
| `mqx_process_timeout` | 消息处理超时（按未确认重投） |
| `mqx_retry_exhausted` | 重试耗尽进入死信 |
| `mqx_id_generate_failed` | 消息 ID 生成失败 |
| `mqx_message_too_large` | 消息体超过 `MaxMessageBytes` |
| `mqx_store_failed` | 消息存储操作失败（落盘/删除/恢复） |

## 匹配示例

```go
if errx.Is(err, mqx.CodeQueueFull) {
	// 队列满：按业务策略重试或降级。
}
```
