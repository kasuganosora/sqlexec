package performance

import (
	"sync"
)

// MemoryPool 内存池（用于重用对象减少GC压力）
type MemoryPool struct {
	pools map[string]interface{}
	mu    sync.RWMutex
}

// NewMemoryPool 创建内存池
func NewMemoryPool() *MemoryPool {
	return &MemoryPool{
		pools: make(map[string]interface{}),
	}
}

// GetPool 获取指定类型的池
func (mp *MemoryPool) GetPool(key string) interface{} {
	mp.mu.RLock()
	defer mp.mu.RUnlock()
	return mp.pools[key]
}

// SetPool 设置指定类型的池
func (mp *MemoryPool) SetPool(key string, pool interface{}) {
	mp.mu.Lock()
	defer mp.mu.Unlock()
	mp.pools[key] = pool
}
