package plan

import (
	"testing"
)

func TestVectorScanConfig(t *testing.T) {
	tests := []struct {
		name        string
		tableName   string
		columnName  string
		indexType   string
		queryVector []float32
		k           int
		metricType  string
	}{
		{
			name:        "Simple vector scan",
			tableName:   "documents",
			columnName:  "embedding",
			indexType:   "HNSW",
			queryVector: []float32{0.1, 0.2, 0.3, 0.4},
			k:           10,
			metricType:  "cosine",
		},
		{
			name:        "Large vector scan",
			tableName:   "images",
			columnName:  "feature_vector",
			indexType:   "IVFFlat",
			queryVector: make([]float32, 768), // 768-dimensional vector
			k:           100,
			metricType:  "euclidean",
		},
		{
			name:        "Small K value",
			tableName:   "items",
			columnName:  "vector",
			indexType:   "Flat",
			queryVector: []float32{1.0, 2.0, 3.0},
			k:           1,
			metricType:  "inner_product",
		},
		{
			name:        "Empty vector",
			tableName:   "test",
			columnName:  "vec",
			indexType:   "Flat",
			queryVector: []float32{},
			k:           5,
			metricType:  "l2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := &VectorScanConfig{
				TableName:   tt.tableName,
				ColumnName:  tt.columnName,
				IndexType:   tt.indexType,
				QueryVector: tt.queryVector,
				K:           tt.k,
				MetricType:  tt.metricType,
			}

			if config.TableName != tt.tableName {
				t.Errorf("TableName = %v, want %v", config.TableName, tt.tableName)
			}

			if config.ColumnName != tt.columnName {
				t.Errorf("ColumnName = %v, want %v", config.ColumnName, tt.columnName)
			}

			if config.IndexType != tt.indexType {
				t.Errorf("IndexType = %v, want %v", config.IndexType, tt.indexType)
			}

			if len(config.QueryVector) != len(tt.queryVector) {
				t.Errorf("QueryVector length = %v, want %v", len(config.QueryVector), len(tt.queryVector))
			}

			if config.K != tt.k {
				t.Errorf("K = %v, want %v", config.K, tt.k)
			}

			if config.MetricType != tt.metricType {
				t.Errorf("MetricType = %v, want %v", config.MetricType, tt.metricType)
			}
		})
	}
}

func TestVectorScanConfigWithPlan(t *testing.T) {
	queryVector := []float32{0.5, 0.6, 0.7, 0.8}

	vectorScanConfig := &VectorScanConfig{
		TableName:   "documents",
		ColumnName:  "embedding",
		IndexType:   "HNSW",
		QueryVector: queryVector,
		K:           50,
		MetricType:  "cosine",
	}

	plan := &Plan{
		ID:     "vector_scan_documents_embedding",
		Type:   TypeVectorScan,
		Config: vectorScanConfig,
	}

	if plan.Type != TypeVectorScan {
		t.Errorf("Plan.Type = %v, want %v", plan.Type, TypeVectorScan)
	}

	retrievedConfig, ok := plan.Config.(*VectorScanConfig)
	if !ok {
		t.Fatal("Failed to retrieve VectorScanConfig from Plan")
	}

	if retrievedConfig.TableName != "documents" {
		t.Errorf("TableName = %v, want documents", retrievedConfig.TableName)
	}

	if retrievedConfig.ColumnName != "embedding" {
		t.Errorf("ColumnName = %v, want embedding", retrievedConfig.ColumnName)
	}

	if retrievedConfig.IndexType != "HNSW" {
		t.Errorf("IndexType = %v, want HNSW", retrievedConfig.IndexType)
	}

	if len(retrievedConfig.QueryVector) != 4 {
		t.Errorf("QueryVector length = %v, want 4", len(retrievedConfig.QueryVector))
	}

	if retrievedConfig.K != 50 {
		t.Errorf("K = %v, want 50", retrievedConfig.K)
	}

	if retrievedConfig.MetricType != "cosine" {
		t.Errorf("MetricType = %v, want cosine", retrievedConfig.MetricType)
	}

	if plan.Explain() != "VectorScan[vector_scan_documents_embedding]" {
		t.Errorf("Plan.Explain() = %v, want VectorScan[vector_scan_documents_embedding]", plan.Explain())
	}
}

func TestNewVectorScanPlan(t *testing.T) {
	tests := []struct {
		name        string
		tableName   string
		columnName  string
		indexType   string
		queryVector []float32
		k           int
		metricType  string
	}{
		{
			name:        "Create vector scan plan",
			tableName:   "test_docs",
			columnName:  "vector_col",
			indexType:   "Flat",
			queryVector: []float32{1.0, 2.0, 3.0},
			k:           20,
			metricType:  "l2",
		},
		{
			name:        "High dimensional vector",
			tableName:   "embeddings",
			columnName:  "embedding",
			indexType:   "HNSW",
			queryVector: make([]float32, 1536),
			k:           100,
			metricType:  "cosine",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan := NewVectorScanPlan(tt.tableName, tt.columnName, tt.indexType, tt.queryVector, tt.k, tt.metricType)

			if plan == nil {
				t.Fatal("NewVectorScanPlan returned nil")
			}

			if plan.Type != TypeVectorScan {
				t.Errorf("Plan.Type = %v, want %v", plan.Type, TypeVectorScan)
			}

			config, ok := plan.Config.(*VectorScanConfig)
			if !ok {
				t.Fatal("Failed to retrieve VectorScanConfig from Plan")
			}

			if config.TableName != tt.tableName {
				t.Errorf("TableName = %v, want %v", config.TableName, tt.tableName)
			}

			if config.ColumnName != tt.columnName {
				t.Errorf("ColumnName = %v, want %v", config.ColumnName, tt.columnName)
			}

			if config.IndexType != tt.indexType {
				t.Errorf("IndexType = %v, want %v", config.IndexType, tt.indexType)
			}

			if len(config.QueryVector) != len(tt.queryVector) {
				t.Errorf("QueryVector length = %v, want %v", len(config.QueryVector), len(tt.queryVector))
			}

			if config.K != tt.k {
				t.Errorf("K = %v, want %v", config.K, tt.k)
			}

			if config.MetricType != tt.metricType {
				t.Errorf("MetricType = %v, want %v", config.MetricType, tt.metricType)
			}

			// Check ID format
			expectedID := "vector_scan_" + tt.tableName + "_" + tt.columnName
			if plan.ID != expectedID {
				t.Errorf("Plan.ID = %v, want %v", plan.ID, expectedID)
			}

			// Check Children is nil
			if plan.Children != nil {
				t.Errorf("Plan.Children should be nil, got %v", plan.Children)
			}
		})
	}
}

func TestVectorScanConfigMetricTypes(t *testing.T) {
	metricTypes := []string{
		"cosine",
		"euclidean",
		"l2",
		"inner_product",
		"manhattan",
		"hamming",
	}

	for _, metricType := range metricTypes {
		t.Run(metricType, func(t *testing.T) {
			config := &VectorScanConfig{
				TableName:   "test",
				ColumnName:  "vector",
				IndexType:   "Flat",
				QueryVector: []float32{1.0, 2.0, 3.0},
				K:           10,
				MetricType:  metricType,
			}

			if config.MetricType != metricType {
				t.Errorf("MetricType = %v, want %v", config.MetricType, metricType)
			}
		})
	}
}

func TestVectorScanConfigIndexTypes(t *testing.T) {
	indexTypes := []string{
		"Flat",
		"HNSW",
		"IVFFlat",
		"IVFPQ",
		"SCANN",
		"DiskANN",
	}

	for _, indexType := range indexTypes {
		t.Run(indexType, func(t *testing.T) {
			config := &VectorScanConfig{
				TableName:   "test",
				ColumnName:  "vector",
				IndexType:   indexType,
				QueryVector: []float32{1.0, 2.0, 3.0},
				K:           10,
				MetricType:  "cosine",
			}

			if config.IndexType != indexType {
				t.Errorf("IndexType = %v, want %v", config.IndexType, indexType)
			}
		})
	}
}

func TestVectorScanConfigZeroK(t *testing.T) {
	config := &VectorScanConfig{
		TableName:   "test",
		ColumnName:  "vector",
		IndexType:   "Flat",
		QueryVector: []float32{1.0, 2.0, 3.0},
		K:           0,
		MetricType:  "cosine",
	}

	if config.K != 0 {
		t.Errorf("K = %v, want 0", config.K)
	}
}
