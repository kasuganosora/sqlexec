package plan

// VectorScanConfig 向量扫描配置
type VectorScanConfig struct {
	TableName   string    `json:"table_name"`
	ColumnName  string    `json:"column_name"`
	IndexType   string    `json:"index_type"`
	QueryVector []float32 `json:"query_vector"`
	K           int       `json:"k"`
	MetricType  string    `json:"metric_type"`
}

// NewVectorScanPlan 创建向量扫描计划
func NewVectorScanPlan(tableName, columnName, indexType string, queryVector []float32, k int, metricType string) *Plan {
	config := &VectorScanConfig{
		TableName:   tableName,
		ColumnName:  columnName,
		IndexType:   indexType,
		QueryVector: queryVector,
		K:           k,
		MetricType:  metricType,
	}

	return &Plan{
		ID:       "vector_scan_" + tableName + "_" + columnName,
		Type:     TypeVectorScan,
		Config:   config,
		Children: nil,
	}
}
