package domain

// DataSourceFactory 数据源工厂接口
type DataSourceFactory interface {
	// Create 创建数据源
	Create(config *DataSourceConfig) (DataSource, error)

	// GetType 支持的数据源类型
	GetType() DataSourceType

	// GetMetadata 获取驱动元数据（用于 information_schema.ENGINES）
	GetMetadata() DriverMetadata
}

// DriverMetadata 驱动元数据
type DriverMetadata struct {
	Comment      string // 引擎描述
	Transactions string // 是否支持事务: YES/NO
	XA           string // 是否支持XA事务: YES/NO
	Savepoints   string // 是否支持保存点: YES/NO
}
