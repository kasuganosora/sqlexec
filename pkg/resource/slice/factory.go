package slice

import (
	"fmt"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// Factory 数据源工厂
type Factory struct{}

// NewFactory 创建新的工厂
func NewFactory() *Factory {
	return &Factory{}
}

// Create 创建数据源
func (f *Factory) Create(config *domain.DataSourceConfig) (domain.DataSource, error) {
	if config == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}

	// 从配置中获取数据
	data, ok := config.Options["data"]
	if !ok {
		return nil, fmt.Errorf("missing 'data' option in config")
	}

	// 获取表名
	tableName, ok := config.Options["table_name"].(string)
	if !ok {
		return nil, fmt.Errorf("missing or invalid 'table_name' option in config")
	}

	// 获取数据库名
	databaseName, ok := config.Options["database_name"].(string)
	if !ok {
		databaseName = "default" // 默认数据库名
	}

	// 获取是否可写
	writable := config.Writable
	if val, ok := config.Options["writable"].(bool); ok {
		writable = val
	}

	// 获取是否支持 MVCC
	mvccSupported := true
	if val, ok := config.Options["mvcc_supported"].(bool); ok {
		mvccSupported = val
	}

	// 通过 New 构造器创建 adapter
	opts := []Option{
		WithDatabaseName(databaseName),
		WithWritable(writable),
		WithMVCC(mvccSupported),
	}
	return New(data, tableName, opts...)
}

// GetType 获取数据源类型
func (f *Factory) GetType() domain.DataSourceType {
	return "slice"
}

// GetMetadata 实现DataSourceFactory接口
func (f *Factory) GetMetadata() domain.DriverMetadata {
	return domain.DriverMetadata{
		Comment:      "Slice data source for []map[string]any or []struct with MVCC transaction support",
		Transactions: "YES",
		XA:           "NO",
		Savepoints:   "NO",
	}
}

// Description 工厂描述
func (f *Factory) Description() string {
	return "Slice data source for []map[string]any or []struct"
}
