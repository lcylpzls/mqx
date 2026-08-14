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
	if err := cfg.Validate(); err != nil {
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
	t.subs = make(map[*Subscription]struct{})
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
	if len(body) > t.cfg.MaxMessageBytes {
		return ErrMessageTooLarge
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
	if m.cfg.store != nil {
		if err := m.cfg.store.SaveMessage(ctx, msg); err != nil {
			return errx.Wrap(err, errx.KindUnavailable, CodeStoreFailed, "消息落盘失败")
		}
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

// ProduceItem 是批量投递的单项。
type ProduceItem struct {
	// Key 业务键，决定分区与顺序归属。
	Key string
	// Body 消息体。
	Body []byte
	// Attrs 消息元数据（可选）。
	Attrs map[string]string
}

// ProduceBatch 批量投递同一主题（按顺序逐条入队，非事务；
// 中途失败时此前条目已入队）。
func (m *MQ) ProduceBatch(ctx context.Context, topicName string, items []ProduceItem) error {
	if len(items) == 0 {
		return errInvalidConfig("批量投递条目不能为空")
	}
	t := m.topic(topicName)
	if t == nil {
		return ErrTopicNotFound
	}
	for _, item := range items {
		if len(item.Body) > t.cfg.MaxMessageBytes {
			return ErrMessageTooLarge
		}
		id, err := m.cfg.idgen(messageIDBytes)
		if err != nil {
			return errx.Wrap(err, errx.KindUnavailable, CodeIDGenerateFailed, "消息 ID 生成失败")
		}
		msg := &Message{
			ID:        id,
			Topic:     topicName,
			Key:       item.Key,
			Body:      append([]byte(nil), item.Body...),
			Attempt:   1,
			EnqueueAt: m.cfg.now(),
			Attrs:     cloneAttrs(item.Attrs),
		}
		if m.cfg.store != nil {
			if err := m.cfg.store.SaveMessage(ctx, msg); err != nil {
				return errx.Wrap(err, errx.KindUnavailable, CodeStoreFailed, "消息落盘失败")
			}
		}
		if err := t.partitionFor(item.Key).produce(ctx, msg); err != nil {
			if errx.Is(err, CodeQueueFull) {
				m.metricQueueFull(topicName)
				m.logWarn("mqx：批量投递队列已满，投递被拒绝",
					logx.String("mqx_topic", topicName))
			}
			return err
		}
		m.metricProduced(topicName)
	}
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

// DeleteTopic 删除主题并停止其全部订阅（幂等删除后返回 ErrTopicNotFound）。
func (m *MQ) DeleteTopic(name string) error {
	m.mu.Lock()
	t, ok := m.topics[name]
	if !ok {
		m.mu.Unlock()
		return ErrTopicNotFound
	}
	delete(m.topics, name)
	m.mu.Unlock()
	t.stopSubs()
	return nil
}

// stopSubs 停止主题的全部订阅并等待退出。
func (t *Topic) stopSubs() {
	t.subsMu.Lock()
	subs := make([]*Subscription, 0, len(t.subs))
	for s := range t.subs {
		subs = append(subs, s)
	}
	t.subsMu.Unlock()
	for _, s := range subs {
		_ = s.Stop()
	}
}

// Stats 返回主题统计快照（队列深度、累计消费与死信水位）。
func (m *MQ) Stats(topicName string) (TopicStats, error) {
	t := m.topic(topicName)
	if t == nil {
		return TopicStats{}, ErrTopicNotFound
	}
	stats := TopicStats{Name: topicName, Partitions: len(t.partitions)}
	for _, p := range t.partitions {
		p.mu.Lock()
		stats.Pending += len(p.msgs) - p.minCursorLocked()
		stats.Consumed += p.delivered
		for l := range p.loops {
			if l.inFlight >= 0 {
				stats.InFlight++
			}
		}
		p.mu.Unlock()
	}
	if t.dlq != nil {
		t.dlq.mu.Lock()
		stats.DLQPending = len(t.dlq.msgs) - t.dlq.start
		stats.DLQConsumed = t.dlq.delivered
		for l := range t.dlq.loops {
			if l.inFlight >= 0 {
				stats.DLQInFlight++
			}
		}
		t.dlq.mu.Unlock()
	}
	return stats, nil
}

// Topics 返回全部主题名（无序）。
func (m *MQ) Topics() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]string, 0, len(m.topics))
	for name := range m.topics {
		out = append(out, name)
	}
	return out
}

// Groups 返回主题的全部消费者组名（无序）。
func (m *MQ) Groups(topicName string) ([]string, error) {
	t := m.topic(topicName)
	if t == nil {
		return nil, ErrTopicNotFound
	}
	t.subsMu.Lock()
	defer t.subsMu.Unlock()
	seen := make(map[string]struct{}, len(t.subs))
	for s := range t.subs {
		seen[s.group] = struct{}{}
	}
	out := make([]string, 0, len(seen))
	for g := range seen {
		out = append(out, g)
	}
	return out, nil
}

// Recover 从 Store 恢复全部未删除消息（需先 CreateTopic）。
// 无 Store 时为 no-op；未知主题或加载失败返回错误。
func (m *MQ) Recover(ctx context.Context) error {
	if m.cfg.store == nil {
		return nil
	}
	msgs, err := m.cfg.store.LoadMessages(ctx)
	if err != nil {
		return errx.Wrap(err, errx.KindUnavailable, CodeStoreFailed, "消息恢复加载失败")
	}
	for _, msg := range msgs {
		t := m.topic(msg.Topic)
		if t == nil {
			return errx.NewCode(CodeStoreFailed, "存储中存在未创建主题："+msg.Topic)
		}
		if err := t.partitionFor(msg.Key).produce(ctx, msg); err != nil {
			return errx.Wrap(err, errx.KindUnavailable, CodeStoreFailed, "消息恢复入队失败")
		}
	}
	m.logInfo("mqx：消息恢复完成", logx.Int("mqx_recovered", len(msgs)))
	return nil
}

// TopicStats 是主题统计快照。
type TopicStats struct {
	// Name 主题名。
	Name string
	// Partitions 分区数。
	Partitions int
	// Pending 待处理消息数（未被任意组消费）。
	Pending int
	// Consumed 已消费消息数（当前在内存中的累计游标）。
	Consumed int64
	// InFlight 当前投递中（含重试等待）的消息数。
	InFlight int
	// DLQPending 死信队列待处理数。
	DLQPending int
	// DLQConsumed 死信队列已消费数。
	DLQConsumed int64
	// DLQInFlight 死信队列当前投递中的消息数。
	DLQInFlight int
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
		t:     t,
		topic: t.name,
		group: group,
		consumers: consumers,
		stop:  make(chan struct{}),
		pause: newPauseState(),
	}
	t.subsMu.Lock()
	t.subs[sub] = struct{}{}
	t.subsMu.Unlock()
	for _, p := range t.partitions {
		l := &loop{
			topic:     t,
			partition: p,
			group:     group,
			handler:   h,
			inFlight:  -1,
			stopCh:    sub.stop,
			pause:     sub.pause,
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
		t:     t,
		topic: dlqName,
		group: group,
		consumers: consumers,
		stop:  make(chan struct{}),
		pause: newPauseState(),
	}
	t.subsMu.Lock()
	t.subs[sub] = struct{}{}
	t.subsMu.Unlock()
	l := &dlqLoop{
		topic:    t,
		group:    group,
		handler:  h,
		inFlight: -1,
		stopCh:   sub.stop,
		pause:    sub.pause,
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
	t        *Topic
	topic    string
	group    string
	consumers int
	stop     chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
	pause    *pauseState
}

// Group 返回消费者组名。
func (s *Subscription) Group() string {
	return s.group
}

// Consumers 返回该组配置的消费者数量。
func (s *Subscription) Consumers() int {
	return s.consumers
}

// Stop 停止该组消费（队列与消息保留，幂等）。
func (s *Subscription) Stop() error {
	s.stopOnce.Do(func() {
		close(s.stop)
		s.t.subsMu.Lock()
		delete(s.t.subs, s)
		s.t.subsMu.Unlock()
	})
	s.wg.Wait()
	return nil
}

// Pause 暂停该组消费（在途消息继续处理，后续消息排队等待）。
func (s *Subscription) Pause() {
	s.pause.Pause()
}

// Resume 恢复该组消费并唤醒所有分区循环。
func (s *Subscription) Resume() {
	s.pause.Resume()
}
