package memory

import "context"

// VectorIndex 向量索引接口
type VectorIndex interface {
	// Build 构建索引
	Build(ctx context.Context, loader VectorDataLoader) error
	// Search 搜索最近邻
	Search(ctx context.Context, query []float32, k int, filter *VectorFilter) (*VectorSearchResult, error)
	// Insert 插入向量
	Insert(id int64, vector []float32) error
	// Delete 删除向量
	Delete(id int64) error
	// GetConfig 获取索引配置
	GetConfig() *VectorIndexConfig
	// Stats 获取索引统计信息
	Stats() VectorIndexStats
	// Close 关闭索引
	Close() error
}

// VectorDataLoader 向量数据加载器接口
type VectorDataLoader interface {
	Load(ctx context.Context) ([]VectorRecord, error)
	Count() int64
}

// VectorRecord 向量记录
type VectorRecord struct {
	ID     int64
	Vector []float32
}

// VectorFilter 向量搜索过滤器
type VectorFilter struct {
	IDs []int64
}

// VectorSearchResult 向量搜索结果
type VectorSearchResult struct {
	IDs       []int64
	Distances []float32
}

// VectorIndexStats 向量索引统计信息
type VectorIndexStats struct {
	Type       IndexType
	Metric     VectorMetricType
	Dimension  int
	Count      int64
	MemorySize int64
}
