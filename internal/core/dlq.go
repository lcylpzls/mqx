package core

import (
	"context"
	"sync"

	"github.com/lcylpzls/logx"
)

// dlqQueue 是主题的死信队列（单分区，容量与主题队列一致）。
type dlqQueue struct {
	topic  *Topic
	mu     sync.Mutex
	start  int
	msgs   []*Message
	loops  map[*dlqLoop]struct{}
	notify chan struct{}
}

// newDLQ 创建死信队列。
func newDLQ(t *Topic) *dlqQueue {
	return &dlqQueue{
		topic:  t,
		loops:  make(map[*dlqLoop]struct{}),
		notify: make(chan struct{}),
	}
}

// enqueue 追加一条死信；队列已满时丢弃并记录错误。
func (d *dlqQueue) enqueue(msg *Message) {
	d.mu.Lock()
	if len(d.msgs)-d.start >= d.topic.cfg.QueueSize {
		d.mu.Unlock()
		d.topic.mq.metricDLQDropped(d.topic.name)
		d.topic.mq.logError("mqx：死信队列已满，消息丢弃",
			logx.String("mqx_topic", d.topic.name),
			logx.String("mqx_id", msg.ID))
		return
	}
	d.msgs = append(d.msgs, msg)
	d.signalLocked()
	d.mu.Unlock()
}

// signalLocked 广播唤醒死信消费者（需持有锁）。
func (d *dlqQueue) signalLocked() {
	close(d.notify)
	d.notify = make(chan struct{})
}

// replay 把指定 ID（空集合为全部）的死信重放回原始主题。
func (d *dlqQueue) replay(ctx context.Context, ids []string) error {
	d.mu.Lock()
	var replay []*Message
	for i := 0; i < len(d.msgs); i++ {
		msg := d.msgs[i]
		if len(ids) > 0 && !containsID(ids, msg.ID) {
			continue
		}
		inFlight := false
		for l := range d.loops {
			if l.inFlight == i {
				inFlight = true
				break
			}
		}
		if inFlight {
			continue
		}
		replay = append(replay, msg)
		d.msgs = append(d.msgs[:i], d.msgs[i+1:]...)
		i--
		for l := range d.loops {
			if l.cursor > i {
				l.cursor--
			}
			if l.inFlight > i {
				l.inFlight--
			}
		}
	}
	d.compactLocked()
	d.mu.Unlock()

	for _, msg := range replay {
		cp := *msg
		cp.Attempt = 1
		cp.Err = nil
		cp.EnqueueAt = d.topic.mq.cfg.now()
		if err := d.topic.partitionFor(msg.Key).produce(ctx, &cp); err != nil {
			d.enqueue(msg)
			d.topic.mq.logError("mqx：死信重放失败，消息放回死信队列",
				logx.String("mqx_topic", d.topic.name),
				logx.String("mqx_id", msg.ID),
				logx.Any("error", err))
			return err
		}
		d.topic.mq.metricReplayed(d.topic.name)
	}
	return nil
}

// compactLocked 推进已消费前缀并在阈值触发时压缩（需持有锁）。
func (d *dlqQueue) compactLocked() {
	d.start = d.minCursorLocked()
	if d.start > 0 && (d.start >= 1024 || d.start*2 > len(d.msgs)) {
		removed := d.start
		d.msgs = append([]*Message(nil), d.msgs[d.start:]...)
		d.start = 0
		for l := range d.loops {
			l.cursor -= removed
		}
	}
}

// minCursorLocked 返回死信队列的最小消费游标（需持有锁）。
func (d *dlqQueue) minCursorLocked() int {
	if len(d.loops) == 0 {
		return d.start
	}
	min := len(d.msgs)
	for l := range d.loops {
		if l.cursor < min {
			min = l.cursor
		}
	}
	return min
}

// dlqLoop 是死信队列的消费者循环（单分区串行）。
type dlqLoop struct {
	topic    *Topic
	group    string
	handler  Handler
	cursor   int
	inFlight int
	stopCh   chan struct{}
}

// run 消费死信队列直到停止。
func (l *dlqLoop) run() {
	d := l.topic.dlq
	defer func() {
		d.mu.Lock()
		delete(d.loops, l)
		d.mu.Unlock()
	}()
	for {
		d.mu.Lock()
		if l.cursor < len(d.msgs) {
			msg := d.msgs[l.cursor]
			l.inFlight = l.cursor
			d.mu.Unlock()
			delivery := *msg
			res, _ := deliverWithRetry(l.topic.mq, l.topic.cfg, l.group,
				l.stopCh, &delivery, l.handler)
			d.mu.Lock()
			l.inFlight = -1
			if res == outcomeAbandoned {
				d.mu.Unlock()
				return
			}
			if res == outcomeDead {
				// 死信消费失败且重试耗尽：丢弃并记录（不再二次入死信）。
				l.topic.mq.logError("mqx：死信消息消费失败且重试耗尽，丢弃",
					logx.String("mqx_topic", l.topic.name),
					logx.String("mqx_group", l.group),
					logx.String("mqx_id", delivery.ID),
					logx.Any("error", delivery.Err))
			}
			l.cursor++
			d.compactLocked()
			d.mu.Unlock()
			continue
		}
		ch := d.notify
		d.mu.Unlock()
		select {
		case <-ch:
		case <-l.stopCh:
			return
		case <-l.topic.mq.stopCh:
			return
		}
	}
}

// containsID 判断 ID 是否在集合中。
func containsID(ids []string, id string) bool {
	for _, v := range ids {
		if v == id {
			return true
		}
	}
	return false
}
