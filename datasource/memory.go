package datasource

import (
	"fmt"
	"runtime"
	"time"
)

// MemoryMonitor 内存监控器
type MemoryMonitor struct {
	maxMemory int64
	stopChan  chan struct{}
}

// NewMemoryMonitor 创建内存监控器
func NewMemoryMonitor(maxMemory int64) *MemoryMonitor {
	return &MemoryMonitor{
		maxMemory: maxMemory,
		stopChan:  make(chan struct{}),
	}
}

// Start 启动内存监控
func (m *MemoryMonitor) Start() error {
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				var mem runtime.MemStats
				runtime.ReadMemStats(&mem)
				if mem.Alloc > uint64(m.maxMemory) {
					// 触发GC
					runtime.GC()
					// 如果仍然超过限制，返回错误
					runtime.ReadMemStats(&mem)
					if mem.Alloc > uint64(m.maxMemory) {
						return
					}
				}
			case <-m.stopChan:
				return
			}
		}
	}()
	return nil
}

// Stop 停止内存监控
func (m *MemoryMonitor) Stop() {
	close(m.stopChan)
}

// Check 检查内存使用
func (m *MemoryMonitor) Check() error {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	if mem.Alloc > uint64(m.maxMemory) {
		// 触发GC
		runtime.GC()
		// 再次检查内存使用
		runtime.ReadMemStats(&mem)
		if mem.Alloc > uint64(m.maxMemory) {
			return fmt.Errorf("内存使用超过限制: %d > %d", mem.Alloc, m.maxMemory)
		}
	}
	return nil
}
