package dataaccess

import (
	"context"
	"fmt"
	"sync"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// Manager 数据源管理器
type Manager struct {
	dataSources map[string]domain.DataSource
	connections map[string]struct{} // 跟踪活跃连接
	mu          sync.RWMutex
}

// NewManager 创建数据源管理器
func NewManager(dataSource domain.DataSource) *Manager {
	return &Manager{
		dataSources: map[string]domain.DataSource{"default": dataSource},
		connections: make(map[string]struct{}),
	}
}

// RegisterDataSource 注册数据源
func (m *Manager) RegisterDataSource(name string, ds domain.DataSource) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.dataSources[name]; exists {
		return fmt.Errorf("data source already registered: %s", name)
	}

	m.dataSources[name] = ds
	return nil
}

// GetDataSource 获取数据源
func (m *Manager) GetDataSource(name string) (domain.DataSource, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	ds, ok := m.dataSources[name]
	if !ok {
		return nil, fmt.Errorf("data source not found: %s", name)
	}
	return ds, nil
}

// AcquireConnection 获取连接
func (m *Manager) AcquireConnection(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.connections[name]; exists {
		return fmt.Errorf("connection already acquired: %s", name)
	}

	m.connections[name] = struct{}{}
	return nil
}

// ReleaseConnection 释放连接
func (m *Manager) ReleaseConnection(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.connections, name)
}

// GetStats 获取统计信息
func (m *Manager) GetStats() map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return map[string]interface{}{
		"data_sources": len(m.dataSources),
		"connections":  len(m.connections),
	}
}

// HealthCheck 健康检查
func (m *Manager) HealthCheck(ctx context.Context) map[string]bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	results := make(map[string]bool)
	for name, ds := range m.dataSources {
		_, err := ds.GetTableInfo(ctx, "test")
		results[name] = err == nil
	}
	return results
}
