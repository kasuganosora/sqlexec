package dataaccess

import (
	"fmt"
	"sync"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// Router 数据源路由器
type Router struct {
	mu                    sync.RWMutex
	routes                map[string]string // 表名到数据源名称的映射
	defaultDataSourceName string
	manager               *Manager
}

// NewRouter 创建路由器
func NewRouter() *Router {
	return &Router{
		routes:                make(map[string]string),
		defaultDataSourceName: "default",
	}
}

// Route 路由表到数据源
func (r *Router) Route(tableName string) (domain.DataSource, error) {
	r.mu.RLock()
	if r.manager == nil {
		r.mu.RUnlock()
		return nil, fmt.Errorf("router manager not initialized")
	}
	dataSourceName := r.defaultDataSourceName
	if route, exists := r.routes[tableName]; exists {
		dataSourceName = route
	}
	r.mu.RUnlock()

	return r.manager.GetDataSource(dataSourceName)
}

// AddRoute 添加路由
func (r *Router) AddRoute(tableName, dataSourceName string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.routes[tableName] = dataSourceName
}

// RemoveRoute 移除路由
func (r *Router) RemoveRoute(tableName string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.routes, tableName)
}

// SetDefaultDataSource 设置默认数据源
func (r *Router) SetDefaultDataSource(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.defaultDataSourceName = name
}

// GetRoutes 获取所有路由
func (r *Router) GetRoutes() map[string]string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	routes := make(map[string]string, len(r.routes))
	for k, v := range r.routes {
		routes[k] = v
	}
	return routes
}

// SetManager 设置管理器
func (r *Router) SetManager(manager *Manager) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.manager = manager
}
