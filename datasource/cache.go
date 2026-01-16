package datasource

import (
	"sync"
	"time"
)

// Cache 数据缓存
type Cache struct {
	data    map[string]interface{}
	expire  map[string]time.Time
	mu      sync.RWMutex
	ttl     time.Duration
	maxSize int
}

// NewCache 创建缓存
func NewCache() *Cache {
	return &Cache{
		data:    make(map[string]interface{}),
		expire:  make(map[string]time.Time),
		ttl:     5 * time.Minute,
		maxSize: 1000,
	}
}

// Get 获取缓存数据
func (c *Cache) Get(key string) (interface{}, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// 检查是否过期
	if expire, ok := c.expire[key]; ok {
		if time.Now().After(expire) {
			delete(c.data, key)
			delete(c.expire, key)
			return nil, false
		}
	}

	value, ok := c.data[key]
	return value, ok
}

// Set 设置缓存数据
func (c *Cache) Set(key string, value interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 检查缓存大小
	if len(c.data) >= c.maxSize {
		// 删除最旧的缓存
		var oldestKey string
		var oldestTime time.Time
		for k, t := range c.expire {
			if oldestKey == "" || t.Before(oldestTime) {
				oldestKey = k
				oldestTime = t
			}
		}
		if oldestKey != "" {
			delete(c.data, oldestKey)
			delete(c.expire, oldestKey)
		}
	}

	c.data[key] = value
	c.expire[key] = time.Now().Add(c.ttl)
}

// Delete 删除缓存数据
func (c *Cache) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.data, key)
	delete(c.expire, key)
}

// Clear 清空缓存
func (c *Cache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.data = make(map[string]interface{})
	c.expire = make(map[string]time.Time)
}

// SetTTL 设置缓存过期时间
func (c *Cache) SetTTL(ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.ttl = ttl
}

// SetMaxSize 设置缓存最大大小
func (c *Cache) SetMaxSize(size int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.maxSize = size
}
