package core

import "context"

// Store 是消息持久化接口（v0.7 正式接入）。
//
// 启用 Store 后的最低承诺为“已 ack 不丢、未 ack 可重投”：
// - SaveMessage 在消息入队前同步落盘，失败则投递失败；
// - DeleteMessage 在消息被全部消费者组越过（retire）时调用；
// - LoadMessages 在 Recover 时按保存顺序返回全部未删除消息。
type Store interface {
	// SaveMessage 持久化一条已入队消息。
	SaveMessage(ctx context.Context, msg *Message) error
	// DeleteMessage 删除一条已确认/已消费的消息。
	DeleteMessage(ctx context.Context, id string) error
	// LoadMessages 恢复全部未删除消息（按保存顺序）。
	LoadMessages(ctx context.Context) ([]*Message, error)
}

// WithStore 注入持久化 Store。
func WithStore(s Store) Option {
	return func(c *Config) error {
		if s == nil {
			return errInvalidConfig("消息存储不能为空")
		}
		c.store = s
		return nil
	}
}
