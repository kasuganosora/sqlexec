package csv

import (
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// CSVFactory CSV 数据源工厂
type CSVFactory struct{}

// NewCSVFactory 创建 CSV 数据源工厂
func NewCSVFactory() *CSVFactory {
	return &CSVFactory{}
}

// GetType 实现DataSourceFactory接口
func (f *CSVFactory) GetType() domain.DataSourceType {
	return domain.DataSourceTypeCSV
}

// Create 实现DataSourceFactory接口
func (f *CSVFactory) Create(config *domain.DataSourceConfig) (domain.DataSource, error) {
	// 使用CSVAdapter（继承MVCCDataSource）
	return NewCSVAdapter(config, config.Name), nil
}
