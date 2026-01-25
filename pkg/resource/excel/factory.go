package excel

import (
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
	// 使用ExcelAdapter（继承MVCCDataSource）
	return NewExcelAdapter(config, config.Name), nil
}
