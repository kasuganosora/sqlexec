package performance

import (
	"sync"
	"time"
)

// BatchExecutor 批量执行器
type BatchExecutor struct {
	batchSize     int
	flushInterval time.Duration
	batch         []interface{}
	timer         *time.Timer
	mu            sync.Mutex
	flushFunc      func([]interface{}) error
}

// NewBatchExecutor 创建批量执行器
func NewBatchExecutor(batchSize int, flushInterval time.Duration, flushFunc func([]interface{}) error) *BatchExecutor {
	be := &BatchExecutor{
		batchSize:     batchSize,
		flushInterval: flushInterval,
		flushFunc:     flushFunc,
	}
	be.timer = time.AfterFunc(flushInterval, func() { be.flush() })
	return be
}

// Add 添加到批次
func (be *BatchExecutor) Add(item interface{}) error {
	be.mu.Lock()
	defer be.mu.Unlock()

	be.batch = append(be.batch, item)

	if len(be.batch) >= be.batchSize {
		return be.flush()
	}

	return nil
}

// flush 刷新批次
func (be *BatchExecutor) flush() error {
	be.mu.Lock()
	defer be.mu.Unlock()

	if len(be.batch) == 0 {
		be.timer.Reset(be.flushInterval)
		return nil
	}

	items := be.batch
	be.batch = make([]interface{}, 0, be.batchSize)

	err := be.flushFunc(items)
	if err != nil {
		return err
	}

	be.timer.Reset(be.flushInterval)
	return nil
}

// Flush 手动刷新
func (be *BatchExecutor) Flush() error {
	return be.flush()
}

// Close 关闭批量执行器
func (be *BatchExecutor) Close() error {
	be.timer.Stop()
	return be.Flush()
}
