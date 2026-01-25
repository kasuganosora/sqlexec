package domain

// DataSourceFactory 数据源工厂接口
type DataSourceFactory interface {
	// Create 创建数据源
	Create(config *DataSourceConfig) (DataSource, error)

	// GetType 支持的数据源类型
	GetType() DataSourceType
}
