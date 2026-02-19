package memory

import (
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// ==================== Factory ====================

// MemoryFactory 内存数据源工厂
type MemoryFactory struct {
	pagingCfg *PagingConfig
}

// NewMemoryFactory 创建内存数据源工厂
// An optional *PagingConfig controls the buffer pool for memory paging.
func NewMemoryFactory(opts ...*PagingConfig) *MemoryFactory {
	var cfg *PagingConfig
	if len(opts) > 0 {
		cfg = opts[0]
	}
	return &MemoryFactory{pagingCfg: cfg}
}

// GetType 实现DataSourceFactory接口
func (f *MemoryFactory) GetType() domain.DataSourceType {
	return domain.DataSourceTypeMemory
}

// GetMetadata 实现DataSourceFactory接口
func (f *MemoryFactory) GetMetadata() domain.DriverMetadata {
	return domain.DriverMetadata{
		Comment:      "MVCC-based in-memory storage with transaction support",
		Transactions: "YES",
		XA:           "NO",
		Savepoints:   "NO",
	}
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
	}, f.pagingCfg), nil
}
