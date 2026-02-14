package excel

import (
	"fmt"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// ExcelFactory Excel 数据源工厂
type ExcelFactory struct{}

// NewExcelFactory 创建 Excel 数据源工厂
func NewExcelFactory() *ExcelFactory {
	return &ExcelFactory{}
}

// GetType 实现DataSourceFactory接口
func (f *ExcelFactory) GetType() domain.DataSourceType {
	return domain.DataSourceTypeExcel
}

// Create 实现DataSourceFactory接口
func (f *ExcelFactory) Create(config *domain.DataSourceConfig) (domain.DataSource, error) {
	if config == nil {
		return nil, fmt.Errorf("excel factory: config cannot be nil")
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
		return nil, fmt.Errorf("excel factory: file path required (set config.Database or options[\"path\"])")
	}
	return NewExcelAdapter(config, filePath), nil
}
