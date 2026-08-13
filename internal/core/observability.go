package core

import (
	"context"
	"time"

	"github.com/lcylpzls/logx"
)

// startTrace 开始链路（无钩子时 no-op）。
func (m *MQ) startTrace(ctx context.Context, name string, attrs ...TraceAttr) (context.Context, func(error)) {
	if m.cfg.trace == nil {
		return ctx, func(error) {}
	}
	return m.cfg.trace.Start(ctx, name, attrs...)
}

// logInfo 输出结构化信息日志。
func (m *MQ) logInfo(msg string, fields ...logx.Field) {
	m.cfg.logger.Info(msg, logx.Fields(fields...))
}

// logWarn 输出结构化告警日志。
func (m *MQ) logWarn(msg string, fields ...logx.Field) {
	m.cfg.logger.Warn(msg, logx.Fields(fields...))
}

// logError 输出结构化错误日志。
func (m *MQ) logError(msg string, fields ...logx.Field) {
	m.cfg.logger.Error(msg, logx.Fields(fields...))
}

// metricProduced 记录投递成功。
func (m *MQ) metricProduced(topic string) {
	if m.cfg.metrics.Produced != nil {
		m.cfg.metrics.Produced(topic)
	}
}

// metricQueueFull 记录队列满。
func (m *MQ) metricQueueFull(topic string) {
	if m.cfg.metrics.QueueFull != nil {
		m.cfg.metrics.QueueFull(topic)
	}
}

// metricConsumed 记录消费成功与耗时。
func (m *MQ) metricConsumed(topic string, duration time.Duration) {
	if m.cfg.metrics.Consumed != nil {
		m.cfg.metrics.Consumed(topic, duration)
	}
}

// metricRetried 记录重试。
func (m *MQ) metricRetried(topic string, attempt int) {
	if m.cfg.metrics.Retried != nil {
		m.cfg.metrics.Retried(topic, attempt)
	}
}

// metricDead 记录进入死信。
func (m *MQ) metricDead(topic string) {
	if m.cfg.metrics.Dead != nil {
		m.cfg.metrics.Dead(topic)
	}
}

// metricReplayed 记录死信重放。
func (m *MQ) metricReplayed(topic string) {
	if m.cfg.metrics.Replayed != nil {
		m.cfg.metrics.Replayed(topic)
	}
}

// metricDLQDropped 记录死信队列满丢弃。
func (m *MQ) metricDLQDropped(topic string) {
	if m.cfg.metrics.DLQDropped != nil {
		m.cfg.metrics.DLQDropped(topic)
	}
}
