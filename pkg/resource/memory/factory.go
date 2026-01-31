package memory

import (
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// ==================== Factory ====================

// MemoryFactory 内存数据源工厂
type MemoryFactory struct{}

// NewMemoryFactory 创建内存数据源工厂
func NewMemoryFactory() *MemoryFactory {
	return &MemoryFactory{}
}

// GetType 实现DataSourceFactory接口
func (f *MemoryFactory) GetType() domain.DataSourceType {
	return domain.DataSourceTypeMemory
}

// Create 实现DataSourceFactory接口
func (f *MemoryFactory) Create(config *domain.DataSourceConfig) (domain.DataSource, error) {
	// 内存数据源默认可写
	writable := true
	name := "memory"
	if config != nil {
		writable = config.Writable
		if config.Name != "" {
			name = config.Name
		}
	}
	return NewMVCCDataSource(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Name:     name,
		Writable: writable,
	}), nil
}
