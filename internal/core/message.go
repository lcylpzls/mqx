package core

import "time"

// Message 是队列中的一条消息。
type Message struct {
	// ID 全局唯一消息 ID（idgenx 生成），业务侧幂等键。
	ID string
	// Topic 原始主题名（DLQ 消息同样指向原始主题）。
	Topic string
	// Key 业务键，决定分区与顺序归属。
	Key string
	// Body 消息体（[]byte，序列化由业务负责）。
	Body []byte
	// Attempt 当前投递次数（1 起，重试递增）。
	Attempt int
	// EnqueueAt 首次入队时间。
	EnqueueAt time.Time
	// Attrs 消息元数据（可选，如 trace_id、来源服务）。
	Attrs map[string]string
	// Err 仅 DLQ 消息携带失败原因。
	Err error
}

// copyForDLQ 返回进入 DLQ 的消息副本。
func (m *Message) copyForDLQ(cause error) *Message {
	cp := *m
	cp.Body = append([]byte(nil), m.Body...)
	cp.Attrs = cloneAttrs(m.Attrs)
	// 进入死信队列后 Attempt 重置为 1，表示死信队列内的投递次数。
	cp.Attempt = 1
	cp.Err = cause
	return &cp
}

// cloneAttrs 深拷贝消息元数据。
func cloneAttrs(attrs map[string]string) map[string]string {
	if len(attrs) == 0 {
		return nil
	}
	out := make(map[string]string, len(attrs))
	for k, v := range attrs {
		out[k] = v
	}
	return out
}
