package datasource

import (
	"testing"
	"time"
)

func TestMemoryMonitor(t *testing.T) {
	// 创建一个内存限制为 100MB 的监控器
	maxMemory := int64(100 * 1024 * 1024) // 100MB
	monitor := NewMemoryMonitor(maxMemory)

	// 测试启动监控
	err := monitor.Start()
	if err != nil {
		t.Errorf("启动内存监控失败: %v", err)
	}

	// 测试内存检查
	err = monitor.Check()
	if err != nil {
		t.Errorf("内存检查失败: %v", err)
	}

	// 测试停止监控
	monitor.Stop()

	// 等待一段时间确保 goroutine 已经停止
	time.Sleep(200 * time.Millisecond)
}
