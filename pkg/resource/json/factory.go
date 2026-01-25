package json

import (
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// JSONFactory JSON 数据源工厂
type JSONFactory struct{}

// NewJSONFactory 创建 JSON 数据源工厂
func NewJSONFactory() *JSONFactory {
	return &JSONFactory{}
}

// GetType 实现 DataSourceFactory 接口
func (f *JSONFactory) GetType() domain.DataSourceType {
	return domain.DataSourceTypeJSON
}

// Create 实现 DataSourceFactory 接口
func (f *JSONFactory) Create(config *domain.DataSourceConfig) (domain.DataSource, error) {
	// 使用JSONAdapter（继承MVCCDataSource）
	return NewJSONAdapter(config, config.Name), nil
}
