package core

import (
	"sync"
	"sync/atomic"
)

// pauseState 是订阅暂停/恢复的共享状态（同一订阅的所有分区循环共用）。
type pauseState struct {
	paused atomic.Bool
	mu     sync.Mutex
	resume chan struct{}
}

// newPauseState 创建暂停状态。
func newPauseState() *pauseState {
	return &pauseState{resume: make(chan struct{})}
}

// Pause 暂停消费（在途消息继续处理，暂停期间消息排队）。
func (p *pauseState) Pause() {
	p.paused.Store(true)
}

// Resume 恢复消费并唤醒所有等待中的分区循环。
func (p *pauseState) Resume() {
	p.mu.Lock()
	close(p.resume)
	p.resume = make(chan struct{})
	p.mu.Unlock()
	p.paused.Store(false)
}

// isPaused 返回是否暂停。
func (p *pauseState) isPaused() bool {
	return p.paused.Load()
}

// channel 返回当前恢复信号通道（需在暂停状态下捕获后等待）。
func (p *pauseState) channel() chan struct{} {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.resume
}
