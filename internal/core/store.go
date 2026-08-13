package core

// Store 是消息持久化接口的占位契约（v0.1 预留）。
//
// 后续版本将抽象消息体、消费位点与 in-flight 状态的落盘：
// 启用 Store 后的最低承诺为“已 ack 不丢、未 ack 可重投”。
// v0.1 为纯内存实现，无需实现本接口。
type Store interface{}

// WithStore 注入持久化 Store（v0.1 仅校验非 nil，行为与内存实现一致）。
func WithStore(s Store) Option {
	return func(c *Config) error {
		if s == nil {
			return errInvalidConfig("消息存储不能为空")
		}
		c.store = s
		return nil
	}
}
