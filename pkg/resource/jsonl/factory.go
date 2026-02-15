package jsonl

import (
	"fmt"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// JSONLFactory JSONL 数据源工厂
type JSONLFactory struct{}

// NewJSONLFactory 创建 JSONL 数据源工厂
func NewJSONLFactory() *JSONLFactory {
	return &JSONLFactory{}
}

// GetType 实现 DataSourceFactory 接口
func (f *JSONLFactory) GetType() domain.DataSourceType {
	return domain.DataSourceTypeJSONL
}

// Create 实现 DataSourceFactory 接口
func (f *JSONLFactory) Create(config *domain.DataSourceConfig) (domain.DataSource, error) {
	if config == nil {
		return nil, fmt.Errorf("jsonl factory: config cannot be nil")
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
		return nil, fmt.Errorf("jsonl factory: file path required (set config.Database or options[\"path\"])")
	}
	return NewJSONLAdapter(config, filePath), nil
}
