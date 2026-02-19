package csv

import (
	"fmt"

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

// GetMetadata 实现DataSourceFactory接口
func (f *CSVFactory) GetMetadata() domain.DriverMetadata {
	return domain.DriverMetadata{
		Comment:      "CSV storage engine with MVCC transaction support",
		Transactions: "YES",
		XA:           "NO",
		Savepoints:   "NO",
	}
}

// Create 实现DataSourceFactory接口
func (f *CSVFactory) Create(config *domain.DataSourceConfig) (domain.DataSource, error) {
	if config == nil {
		return nil, fmt.Errorf("csv factory: config cannot be nil")
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
		return nil, fmt.Errorf("csv factory: file path required (set config.Database or options[\"path\"])")
	}
	return NewCSVAdapter(config, filePath), nil
}
