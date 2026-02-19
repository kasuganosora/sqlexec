package application

import (
	"fmt"
	"sync"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// ==================== 工厂注册表 ====================

// Registry 数据源工厂注册表
type Registry struct {
	factories map[domain.DataSourceType]domain.DataSourceFactory
	mu        sync.RWMutex
}

// NewRegistry 创建注册表
func NewRegistry() *Registry {
	return &Registry{
		factories: make(map[domain.DataSourceType]domain.DataSourceFactory),
	}
}

// Register 注册数据源工厂
func (r *Registry) Register(factory domain.DataSourceFactory) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	factoryType := factory.GetType()
	if _, exists := r.factories[factoryType]; exists {
		return fmt.Errorf("factory %s already registered", factoryType)
	}

	r.factories[factoryType] = factory
	return nil
}

// Unregister 注销数据源工厂
func (r *Registry) Unregister(factoryType domain.DataSourceType) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.factories[factoryType]; !exists {
		return fmt.Errorf("factory %s not found", factoryType)
	}

	delete(r.factories, factoryType)
	return nil
}

// Get 获取数据源工厂
func (r *Registry) Get(factoryType domain.DataSourceType) (domain.DataSourceFactory, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	factory, ok := r.factories[factoryType]
	if !ok {
		return nil, fmt.Errorf("factory %s not found", factoryType)
	}
	return factory, nil
}

// Create 使用工厂创建数据源
func (r *Registry) Create(config *domain.DataSourceConfig) (domain.DataSource, error) {
	factory, err := r.Get(config.Type)
	if err != nil {
		return nil, err
	}
	return factory.Create(config)
}

// List 列出所有已注册的工厂
func (r *Registry) List() []domain.DataSourceType {
	r.mu.RLock()
	defer r.mu.RUnlock()

	types := make([]domain.DataSourceType, 0, len(r.factories))
	for t := range r.factories {
		types = append(types, t)
	}
	return types
}

// ListFactories returns all registered factories with their metadata
func (r *Registry) ListFactories() map[domain.DataSourceType]domain.DataSourceFactory {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make(map[domain.DataSourceType]domain.DataSourceFactory, len(r.factories))
	for t, f := range r.factories {
		result[t] = f
	}
	return result
}

// Exists 检查工厂是否存在
func (r *Registry) Exists(factoryType domain.DataSourceType) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	_, exists := r.factories[factoryType]
	return exists
}

// Clear 清空所有注册的工厂
func (r *Registry) Clear() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.factories = make(map[domain.DataSourceType]domain.DataSourceFactory)
}

// ==================== 全局注册表 ====================

var (
	globalRegistry = NewRegistry()
	registryOnce   sync.Once
)

// GetRegistry 获取全局注册表
func GetRegistry() *Registry {
	return globalRegistry
}

// RegisterFactory 注册数据源工厂（全局便捷函数）
func RegisterFactory(factory domain.DataSourceFactory) error {
	return globalRegistry.Register(factory)
}

// UnregisterFactory 注销数据源工厂（全局便捷函数）
func UnregisterFactory(factoryType domain.DataSourceType) error {
	return globalRegistry.Unregister(factoryType)
}

// GetFactory 获取数据源工厂（全局便捷函数）
func GetFactory(factoryType domain.DataSourceType) (domain.DataSourceFactory, error) {
	return globalRegistry.Get(factoryType)
}

// CreateDataSource 使用全局注册表创建数据源
func CreateDataSource(config *domain.DataSourceConfig) (domain.DataSource, error) {
	return globalRegistry.Create(config)
}

// GetSupportedTypes 获取支持的数据源类型
func GetSupportedTypes() []domain.DataSourceType {
	return globalRegistry.List()
}
