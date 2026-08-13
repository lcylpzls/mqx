// Package mqx 提供内置通用消息队列：生产消费、削峰、按业务键顺序执行。
// 核心语义：按 key 分区保序、at-least-once 手动确认、消费者组、
// 有界队列背压、重试与死信队列。实现主体位于 internal/core。
package mqx
