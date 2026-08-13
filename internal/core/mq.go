package core

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/lcylpzls/errx"
	"github.com/lcylpzls/logx"
)

// MQ 是内置消息队列的门面：主题、投递、订阅与关闭。
// 所有方法并发安全。
type MQ struct {
	cfg          Config
	mu           sync.RWMutex
	topics       map[string]*Topic
	stopCh       chan struct{}
	shutdownOnce sync.Once
	shuttingDown atomic.Bool
	wg           sync.WaitGroup
}

// New 创建消息队列实例。
func New(opts ...Option) (*MQ, error) {
	cfg := defaultConfig()
	for _, opt := range opts {
		if opt != nil {
			if err := opt(&cfg); err != nil {
				return nil, err
			}
		}
	}
	return &MQ{
		cfg:    cfg,
		topics: make(map[string]*Topic),
		stopCh: make(chan struct{}),
	}, nil
}

// CreateTopic 显式创建主题（重复创建返回 ErrTopicExists）。
func (m *MQ) CreateTopic(name string, cfg TopicConfig) (*Topic, error) {
	if err := validateTopicName(name); err != nil {
		return nil, err
	}
	cfg.withDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.shuttingDown.Load() {
		return nil, ErrShuttingDown
	}
	if _, ok := m.topics[name]; ok {
		return nil, ErrTopicExists
	}
	t := &Topic{mq: m, name: name, cfg: cfg}
	t.partitions = make([]*partition, cfg.Partitions)
	for i := range t.partitions {
		t.partitions[i] = newPartition(t, i)
	}
	if !cfg.DisableDLQ {
		t.dlq = newDLQ(t)
	}
	m.topics[name] = t
	m.logInfo("mqx：主题已创建", logx.String("mqx_topic", name))
	return t, nil
}

// Produce 投递一条消息（按 key 静态路由到分区）。
func (m *MQ) Produce(ctx context.Context, topicName, key string, body []byte) error {
	t := m.topic(topicName)
	if t == nil {
		return ErrTopicNotFound
	}
	if m.shuttingDown.Load() {
		return ErrShuttingDown
	}
	id, err := m.cfg.idgen(messageIDBytes)
	if err != nil {
		return errx.Wrap(err, errx.KindUnavailable, CodeIDGenerateFailed, "消息 ID 生成失败")
	}
	msg := &Message{
		ID:        id,
		Topic:     topicName,
		Key:       key,
		Body:      append([]byte(nil), body...),
		Attempt:   1,
		EnqueueAt: m.cfg.now(),
	}
	if err := t.partitionFor(key).produce(ctx, msg); err != nil {
		if errx.Is(err, CodeQueueFull) {
			m.metricQueueFull(topicName)
			m.logWarn("mqx：队列已满，投递被拒绝",
				logx.String("mqx_topic", topicName),
				logx.String("mqx_key", key))
		}
		return err
	}
	m.metricProduced(topicName)
	return nil
}

// Subscribe 注册消费者组；组内按 key 静态归属，多组互相独立。
func (m *MQ) Subscribe(_ context.Context, topicName, group string, consumers int, h Handler) (*Subscription, error) {
	if group == "" {
		return nil, errInvalidConfig("消费者组名不能为空")
	}
	if consumers < 1 {
		return nil, errInvalidConfig("消费者数量必须为正")
	}
	if h == nil {
		return nil, errInvalidConfig("消费者处理器不能为空")
	}
	base, isDLQ := strings.CutSuffix(topicName, dlqSuffix)
	m.mu.RLock()
	t, ok := m.topics[base]
	shuttingDown := m.shuttingDown.Load()
	m.mu.RUnlock()
	if !ok || shuttingDown {
		return nil, ErrTopicNotFound
	}
	if isDLQ {
		if t.cfg.DisableDLQ || t.dlq == nil {
			return nil, ErrTopicNotFound
		}
		return m.subscribeDLQ(t, topicName, group, consumers, h)
	}
	return m.subscribeTopic(t, group, consumers, h)
}

// Replay 把死信消息重放回原始主题；ids 为空表示全部重放。
func (m *MQ) Replay(ctx context.Context, dlqName string, ids ...string) error {
	base, ok := strings.CutSuffix(dlqName, dlqSuffix)
	if !ok {
		return errInvalidConfig("死信主题名必须以 .dlq 结尾")
	}
	m.mu.RLock()
	t, exists := m.topics[base]
	m.mu.RUnlock()
	if !exists || t.dlq == nil || t.cfg.DisableDLQ {
		return ErrTopicNotFound
	}
	return t.dlq.replay(ctx, ids)
}

// Shutdown 停止投递与消费并等待在途消息落定（幂等）。
func (m *MQ) Shutdown(ctx context.Context) error {
	var err error
	m.shutdownOnce.Do(func() {
		m.shuttingDown.Store(true)
		close(m.stopCh)
		done := make(chan struct{})
		go func() {
			m.wg.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-ctx.Done():
			err = ctx.Err()
		}
	})
	return err
}

// topic 按名查找主题。
func (m *MQ) topic(name string) *Topic {
	m.mu.RLock()
	t := m.topics[name]
	m.mu.RUnlock()
	return t
}

// subscribeTopic 为主题注册消费者组：每分区一个串行投递循环。
// consumers 决定分区归属（p % consumers），实际串行粒度是分区。
func (m *MQ) subscribeTopic(t *Topic, group string, consumers int, h Handler) (*Subscription, error) {
	sub := &Subscription{
		mq:    m,
		topic: t.name,
		group: group,
		stop:  make(chan struct{}),
	}
	for _, p := range t.partitions {
		l := &loop{
			topic:     t,
			partition: p,
			group:     group,
			handler:   h,
			stopCh:    sub.stop,
		}
		p.mu.Lock()
		p.loops[l] = struct{}{}
		p.mu.Unlock()
		m.wg.Add(1)
		sub.wg.Add(1)
		go func() {
			defer m.wg.Done()
			defer sub.wg.Done()
			l.run()
		}()
	}
	m.logInfo("mqx：消费者组已订阅",
		logx.String("mqx_topic", t.name),
		logx.String("mqx_group", group),
		logx.Int("mqx_consumers", consumers))
	return sub, nil
}

// subscribeDLQ 为死信队列注册消费者组（单分区串行）。
func (m *MQ) subscribeDLQ(t *Topic, dlqName, group string, consumers int, h Handler) (*Subscription, error) {
	sub := &Subscription{
		mq:    m,
		topic: dlqName,
		group: group,
		stop:  make(chan struct{}),
	}
	l := &dlqLoop{
		topic:    t,
		group:    group,
		handler:  h,
		inFlight: -1,
		stopCh:   sub.stop,
	}
	t.dlq.mu.Lock()
	t.dlq.loops[l] = struct{}{}
	t.dlq.mu.Unlock()
	m.wg.Add(1)
	sub.wg.Add(1)
	go func() {
		defer m.wg.Done()
		defer sub.wg.Done()
		l.run()
	}()
	m.logInfo("mqx：死信消费者组已订阅",
		logx.String("mqx_topic", t.name),
		logx.String("mqx_group", group),
		logx.Int("mqx_consumers", consumers))
	return sub, nil
}

// moveToDLQ 把最终失败的消息送入死信队列。
func (t *Topic) moveToDLQ(msg *Message, cause error) {
	if t.cfg.DisableDLQ || t.dlq == nil {
		return
	}
	t.dlq.enqueue(msg.copyForDLQ(cause))
}

// Subscription 是一次消费者组订阅。
type Subscription struct {
	mq       *MQ
	topic    string
	group    string
	stop     chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
}

// Group 返回消费者组名。
func (s *Subscription) Group() string {
	return s.group
}

// Stop 停止该组消费（队列与消息保留，幂等）。
func (s *Subscription) Stop() error {
	s.stopOnce.Do(func() { close(s.stop) })
	s.wg.Wait()
	return nil
}
