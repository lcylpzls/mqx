package core

import (
	"context"
	"strings"
	"sync"
)

// dlqSuffix 死信主题名后缀。
const dlqSuffix = ".dlq"

// Topic 是一个消息主题：按 key 静态分区分片，每分区内部 FIFO。
type Topic struct {
	mq         *MQ
	name       string
	cfg        TopicConfig
	partitions []*partition
	dlq        *dlqQueue
}

// partition 是主题下的一个顺序分区。
type partition struct {
	topic  *Topic
	index  int
	mu     sync.Mutex
	start  int
	msgs   []*Message
	loops  map[*loop]struct{}
	notify chan struct{}
	// delivered 累计已消费消息数（压缩不清零）。
	delivered int64
}

// newPartition 创建分区。
func newPartition(t *Topic, index int) *partition {
	return &partition{
		topic:  t,
		index:  index,
		loops:  make(map[*loop]struct{}),
		notify: make(chan struct{}),
	}
}

// partitionFor 返回 key 所属的静态分区（FNV-1a 哈希，零分配）。
func (t *Topic) partitionFor(key string) *partition {
	var h uint32 = 2166136261
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= 16777619
	}
	return t.partitions[int(h)%len(t.partitions)]
}

// produce 投递一条消息到分区（按队列满策略处理）。
func (p *partition) produce(ctx context.Context, msg *Message) error {
	cfg := p.topic.cfg
	p.mu.Lock()
	for {
		if p.topic.mq.shuttingDown.Load() {
			p.mu.Unlock()
			return ErrShuttingDown
		}
		if len(p.msgs)-p.minCursorLocked() < cfg.QueueSize {
			p.msgs = append(p.msgs, msg)
			p.signalLocked()
			p.mu.Unlock()
			return nil
		}
		if cfg.QueueFullPolicy != QueueFullBlock {
			p.mu.Unlock()
			return ErrQueueFull
		}
		// 容量检查与通道捕获在同一临界区内，避免信号落在检查与捕获之间。
		ch := p.notify
		p.mu.Unlock()
		select {
		case <-ch:
		case <-p.topic.mq.stopCh:
			return ErrShuttingDown
		case <-ctx.Done():
			return ctx.Err()
		}
		p.mu.Lock()
	}
}

// signalLocked 广播唤醒等待中的生产者与消费者（需持有锁）。
// 采用“关闭旧通道并替换新通道”的模式，避免信号被单个等待者吞掉。
func (p *partition) signalLocked() {
	close(p.notify)
	p.notify = make(chan struct{})
}

// minCursorLocked 返回所有消费者组循环的最小游标（需持有锁）。
func (p *partition) minCursorLocked() int {
	if len(p.loops) == 0 {
		return p.start
	}
	min := len(p.msgs)
	for l := range p.loops {
		if l.cursor < min {
			min = l.cursor
		}
	}
	return min
}

// compactLocked 推进已消费前缀并在阈值触发时压缩（需持有锁）。
func (p *partition) compactLocked() {
	p.start = p.minCursorLocked()
	if p.start > 0 && (p.start >= 1024 || p.start*2 > len(p.msgs)) {
		removed := p.start
		p.msgs = append([]*Message(nil), p.msgs[p.start:]...)
		p.start = 0
		for l := range p.loops {
			l.cursor -= removed
		}
	}
}

// validateTopicName 校验主题名：非空、长度受限、不得占用 DLQ 后缀。
func validateTopicName(name string) error {
	if name == "" || len(name) > 128 {
		return errInvalidConfig("主题名必须为 1..128 个字符")
	}
	if strings.ContainsAny(name, " \t\r\n") {
		return errInvalidConfig("主题名不能包含空白字符")
	}
	if strings.HasSuffix(name, dlqSuffix) {
		return errInvalidConfig("主题名不能以 .dlq 结尾（保留给死信队列）")
	}
	return nil
}
