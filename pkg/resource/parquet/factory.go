package parquet

import (
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// ParquetFactory Parquet 数据源工厂
type ParquetFactory struct{}

// NewParquetFactory 创建 Parquet 数据源工厂
func NewParquetFactory() *ParquetFactory {
	return &ParquetFactory{}
}

// GetType 实现DataSourceFactory接口
func (f *ParquetFactory) GetType() domain.DataSourceType {
	return domain.DataSourceTypeParquet
}

// Create 实现DataSourceFactory接口
func (f *ParquetFactory) Create(config *domain.DataSourceConfig) (domain.DataSource, error) {
	// 使用ParquetAdapter（继承MVCCDataSource）
	return NewParquetAdapter(config, config.Name), nil
}
