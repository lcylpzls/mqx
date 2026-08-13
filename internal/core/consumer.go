package core

import (
	"context"
	"strconv"
	"time"

	"github.com/lcylpzls/logx"
)

// Handler 是消息消费者：返回 nil 表示确认（ack），返回 error 表示未确认，
// 按主题配置自动重试，耗尽后进入死信队列。
type Handler func(ctx context.Context, msg *Message) error

// outcome 是单条消息投递的最终结果。
type outcome int

const (
	outcomeAcked outcome = iota
	outcomeDead
	outcomeAbandoned
)

// loop 是 (topic, partition, group) 的单消费者循环：分区内串行投递。
type loop struct {
	topic     *Topic
	partition *partition
	group     string
	handler   Handler
	cursor    int
	inFlight  int
	stopCh    chan struct{}
	pause     *pauseState
}

// run 消费分区队列直到停止。
func (l *loop) run() {
	p := l.partition
	defer func() {
		p.mu.Lock()
		delete(p.loops, l)
		p.mu.Unlock()
	}()
	for {
		if l.pause != nil {
			ch, paused := l.pause.waitChannel()
			if !paused {
				goto process
			}
			select {
			case <-ch:
			case <-l.stopCh:
				return
			case <-l.topic.mq.stopCh:
				return
			}
			continue
		}
	process:
		p.mu.Lock()
		if l.cursor < len(p.msgs) {
			msg := p.msgs[l.cursor]
			l.inFlight = l.cursor
			p.mu.Unlock()
			// 每组独立投递副本，避免多组并发修改 Attempt。
			delivery := *msg
			delivery.Attrs = cloneAttrs(msg.Attrs)
			res, cause := deliverWithRetry(l.topic.mq, l.topic.cfg, l.group,
				l.stopCh, &delivery, l.handler)
			if res == outcomeAbandoned {
				return
			}
			if res == outcomeDead {
				l.topic.moveToDLQ(&delivery, cause)
			}
			p.mu.Lock()
			l.inFlight = -1
			l.cursor++
			p.delivered++
			p.compactLocked()
			// 排空后唤醒等待空间的生产者。
			p.signalLocked()
			p.mu.Unlock()
			continue
		}
		ch := p.notify
		p.mu.Unlock()
		select {
		case <-ch:
		case <-l.stopCh:
			return
		case <-l.topic.mq.stopCh:
			return
		}
	}
}

// deliverWithRetry 投递消息直到确认 / 重试耗尽 / 关闭放弃。
// 返回 outcomeDead 时 cause 为最终失败原因。
func deliverWithRetry(mq *MQ, cfg TopicConfig, group string, stopCh chan struct{},
	msg *Message, handler Handler) (outcome, error) {
	for {
		ctx, end := mq.startTrace(context.Background(), "mqx.consume",
			TraceAttr{Key: "mqx.topic", Value: msg.Topic},
			TraceAttr{Key: "mqx.group", Value: group},
			TraceAttr{Key: "mqx.key", Value: msg.Key},
			TraceAttr{Key: "mqx.attempt", Value: strconv.Itoa(msg.Attempt)},
		)
		start := mq.cfg.now()
		err := invoke(handler, ctx, msg, cfg.ProcessTimeout)
		end(err)
		if err == nil {
			mq.metricConsumed(msg.Topic, mq.cfg.now().Sub(start))
			return outcomeAcked, nil
		}
		if msg.Attempt <= cfg.RetryMax {
			msg.Attempt++
			mq.metricRetried(msg.Topic, msg.Attempt)
			mq.logWarn("mqx：消息处理失败，安排重试",
				logx.String("mqx_topic", msg.Topic),
				logx.String("mqx_group", group),
				logx.String("mqx_id", msg.ID),
				logx.Int("mqx_attempt", msg.Attempt),
				logx.Any("error", err))
			delay := retryDelay(cfg.RetryDelay, msg.Attempt)
			select {
			case <-time.After(delay):
			case <-stopCh:
				return outcomeAbandoned, nil
			case <-mq.stopCh:
				return outcomeAbandoned, nil
			}
			continue
		}
		msg.Err = err
		mq.metricDead(msg.Topic)
		mq.logError("mqx：消息重试耗尽，进入死信队列",
			logx.String("mqx_topic", msg.Topic),
			logx.String("mqx_group", group),
			logx.String("mqx_id", msg.ID),
			logx.Int("mqx_attempt", msg.Attempt),
			logx.Any("error", err))
		return outcomeDead, err
	}
}

// invoke 在独立 goroutine 中执行处理器并等待结果或超时。
// 超时后原 goroutine 可能仍在运行，其副作用由业务幂等键兜底。
func invoke(handler Handler, ctx context.Context, msg *Message, timeout time.Duration) error {
	result := make(chan error, 1)
	go func() { result <- handler(ctx, msg) }()
	select {
	case err := <-result:
		return err
	case <-time.After(timeout):
		return ErrProcessTimeout
	}
}

// retryDelay 计算第 attempt 次投递前的退避时长（指数增长，封顶 1 小时）。
func retryDelay(base time.Duration, attempt int) time.Duration {
	delay := base
	for i := 1; i < attempt-1 && delay < time.Hour; i++ {
		delay *= 2
	}
	if delay > time.Hour {
		return time.Hour
	}
	return delay
}
