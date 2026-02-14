package json

import (
	"fmt"

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
	if config == nil {
		return nil, fmt.Errorf("json factory: config cannot be nil")
	}
	filePath := config.Database
	if config.Options != nil {
		if p, ok := config.Options["path"]; ok {
			if str, ok := p.(string); ok && str != "" {
				filePath = str
			}
		}
	}
	if filePath == "" {
		return nil, fmt.Errorf("json factory: file path required (set config.Database or options[\"path\"])")
	}
	return NewJSONAdapter(config, filePath), nil
}
