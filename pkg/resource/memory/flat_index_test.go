package memory

import (
	"context"
	"testing"
)

// TestNewFlatIndex 测试创建Flat索引
func TestNewFlatIndex(t *testing.T) {
	t.Run("valid_config", func(t *testing.T) {
		config := &VectorIndexConfig{
			MetricType: VectorMetricCosine,
			Dimension:  128,
		}
		idx, err := NewFlatIndex("embedding", config)
		if err != nil {
			t.Fatalf("NewFlatIndex failed: %v", err)
		}
		if idx == nil {
			t.Fatal("NewFlatIndex returned nil")
		}
		if idx.columnName != "embedding" {
			t.Errorf("Expected column name 'embedding', got %s", idx.columnName)
		}
	})

	t.Run("invalid_metric", func(t *testing.T) {
		config := &VectorIndexConfig{
			MetricType: "invalid_metric",
			Dimension:  128,
		}
		_, err := NewFlatIndex("embedding", config)
		if err == nil {
			t.Error("NewFlatIndex should fail with invalid metric")
		}
	})
}

// TestFlatIndexInsert 测试插入向量
func TestFlatIndexInsert(t *testing.T) {
	config := &VectorIndexConfig{
		MetricType: VectorMetricCosine,
		Dimension:  64,
	}
	idx, _ := NewFlatIndex("embedding", config)

	t.Run("valid_insert", func(t *testing.T) {
		vec := randomVector(64)
		err := idx.Insert(1, vec)
		if err != nil {
			t.Errorf("Insert failed: %v", err)
		}
	})

	t.Run("dimension_mismatch", func(t *testing.T) {
		vec := randomVector(32) // 错误维度
		err := idx.Insert(2, vec)
		if err == nil {
			t.Error("Insert should fail with dimension mismatch")
		}
	})

	t.Run("insert_duplicate_id", func(t *testing.T) {
		vec := randomVector(64)
		err := idx.Insert(1, vec) // 重复ID，应该覆盖
		if err != nil {
			t.Errorf("Insert with duplicate ID failed: %v", err)
		}
		stats := idx.Stats()
		if stats.Count != 1 {
			t.Errorf("Expected count 1, got %d", stats.Count)
		}
	})

	t.Run("insert_multiple", func(t *testing.T) {
		for i := int64(10); i < 110; i++ {
			vec := randomVector(64)
			err := idx.Insert(i, vec)
			if err != nil {
				t.Fatalf("Insert %d failed: %v", i, err)
			}
		}
		stats := idx.Stats()
		if stats.Count != 101 { // 1 + 100
			t.Errorf("Expected count 101, got %d", stats.Count)
		}
	})

	t.Run("concurrent_insert", func(t *testing.T) {
		done := make(chan bool, 10)
		for i := 0; i < 10; i++ {
			go func(id int) {
				vec := randomVector(64)
				_ = idx.Insert(int64(200+id), vec)
				done <- true
			}(i)
		}
		for i := 0; i < 10; i++ {
			<-done
		}
	})
}

// TestFlatIndexDelete 测试删除向量
func TestFlatIndexDelete(t *testing.T) {
	ctx := context.Background()
	config := &VectorIndexConfig{
		MetricType: VectorMetricCosine,
		Dimension:  64,
	}
	idx, _ := NewFlatIndex("embedding", config)

	// 插入测试数据
	for i := 0; i < 100; i++ {
		idx.Insert(int64(i), randomVector(64))
	}

	t.Run("delete_existing", func(t *testing.T) {
		err := idx.Delete(50)
		if err != nil {
			t.Errorf("Delete failed: %v", err)
		}
		stats := idx.Stats()
		if stats.Count != 99 {
			t.Errorf("Expected count 99 after delete, got %d", stats.Count)
		}
	})

	t.Run("delete_nonexistent", func(t *testing.T) {
		err := idx.Delete(9999) // 不存在的ID
		if err != nil {
			t.Errorf("Delete nonexistent should not error: %v", err)
		}
	})

	t.Run("search_after_delete", func(t *testing.T) {
		query := randomVector(64)
		result, err := idx.Search(ctx, query, 10, nil)
		if err != nil {
			t.Fatalf("Search after delete failed: %v", err)
		}
		// 检查删除的ID不在结果中
		for _, id := range result.IDs {
			if id == 50 {
				t.Error("Deleted ID should not appear in search results")
			}
		}
	})
}

// TestFlatIndexSearch 测试搜索功能
func TestFlatIndexSearch(t *testing.T) {
	ctx := context.Background()
	config := &VectorIndexConfig{
		MetricType: VectorMetricCosine,
		Dimension:  64,
	}
	idx, _ := NewFlatIndex("embedding", config)

	// 插入测试数据
	for i := 0; i < 1000; i++ {
		idx.Insert(int64(i), randomVector(64))
	}

	t.Run("basic_search", func(t *testing.T) {
		query := randomVector(64)
		result, err := idx.Search(ctx, query, 10, nil)
		if err != nil {
			t.Fatalf("Search failed: %v", err)
		}
		if len(result.IDs) != 10 {
			t.Errorf("Expected 10 results, got %d", len(result.IDs))
		}
		if len(result.Distances) != 10 {
			t.Errorf("Expected 10 distances, got %d", len(result.Distances))
		}
	})

	t.Run("search_with_k_larger_than_count", func(t *testing.T) {
		query := randomVector(64)
		result, err := idx.Search(ctx, query, 2000, nil)
		if err != nil {
			t.Fatalf("Search failed: %v", err)
		}
		if len(result.IDs) != 1000 {
			t.Errorf("Expected 1000 results (all), got %d", len(result.IDs))
		}
	})

	t.Run("search_with_filter", func(t *testing.T) {
		query := randomVector(64)
		filter := &VectorFilter{
			IDs: []int64{1, 2, 3, 4, 5},
		}
		result, err := idx.Search(ctx, query, 10, filter)
		if err != nil {
			t.Fatalf("Search with filter failed: %v", err)
		}
		if len(result.IDs) != 5 {
			t.Errorf("Expected 5 results with filter, got %d", len(result.IDs))
		}
		// 验证所有结果都在过滤列表中
		for _, id := range result.IDs {
			found := false
			for _, fid := range filter.IDs {
				if id == fid {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("Result ID %d not in filter list", id)
			}
		}
	})

	t.Run("search_with_empty_filter", func(t *testing.T) {
		query := randomVector(64)
		filter := &VectorFilter{
			IDs: []int64{},
		}
		result, err := idx.Search(ctx, query, 10, filter)
		if err != nil {
			t.Fatalf("Search with empty filter failed: %v", err)
		}
		if len(result.IDs) != 0 {
			t.Errorf("Expected 0 results with empty filter, got %d", len(result.IDs))
		}
	})

	t.Run("search_dimension_mismatch", func(t *testing.T) {
		query := randomVector(32) // 错误维度
		_, err := idx.Search(ctx, query, 10, nil)
		if err == nil {
			t.Error("Search should fail with dimension mismatch")
		}
	})

	t.Run("search_k_zero", func(t *testing.T) {
		query := randomVector(64)
		result, err := idx.Search(ctx, query, 0, nil)
		if err != nil {
			t.Fatalf("Search with k=0 failed: %v", err)
		}
		if len(result.IDs) != 0 {
			t.Errorf("Expected 0 results with k=0, got %d", len(result.IDs))
		}
	})

	t.Run("search_returns_sorted_results", func(t *testing.T) {
		// 插入已知的向量
		idx2, _ := NewFlatIndex("test", &VectorIndexConfig{
			MetricType: VectorMetricL2,
			Dimension:  2,
		})
		idx2.Insert(1, []float32{0.0, 0.0})
		idx2.Insert(2, []float32{1.0, 1.0})
		idx2.Insert(3, []float32{2.0, 2.0})
		idx2.Insert(4, []float32{3.0, 3.0})

		query := []float32{0.0, 0.0}
		result, err := idx2.Search(ctx, query, 3, nil)
		if err != nil {
			t.Fatalf("Search failed: %v", err)
		}

		// 验证结果是按距离排序的
		for i := 1; i < len(result.Distances); i++ {
			if result.Distances[i] < result.Distances[i-1] {
				t.Error("Results should be sorted by distance")
			}
		}
	})
}

// TestFlatIndexBuild 测试构建索引
func TestFlatIndexBuild(t *testing.T) {
	ctx := context.Background()
	config := &VectorIndexConfig{
		MetricType: VectorMetricCosine,
		Dimension:  64,
	}
	idx, _ := NewFlatIndex("embedding", config)

	t.Run("build_with_loader", func(t *testing.T) {
		loader := &testVectorDataLoader{
			records: []VectorRecord{
				{ID: 1, Vector: randomVector(64)},
				{ID: 2, Vector: randomVector(64)},
				{ID: 3, Vector: randomVector(64)},
			},
		}
		err := idx.Build(ctx, loader)
		if err != nil {
			t.Fatalf("Build failed: %v", err)
		}
		stats := idx.Stats()
		if stats.Count != 3 {
			t.Errorf("Expected count 3, got %d", stats.Count)
		}
	})

	t.Run("build_with_nil_loader", func(t *testing.T) {
		idx2, _ := NewFlatIndex("test", config)
		err := idx2.Build(ctx, nil)
		if err == nil {
			t.Error("Build should fail with nil loader")
		}
	})
}

// TestFlatIndexStats 测试统计信息
func TestFlatIndexStats(t *testing.T) {
	config := &VectorIndexConfig{
		MetricType: VectorMetricCosine,
		Dimension:  128,
		Params:     map[string]interface{}{"key": "value"},
	}
	idx, _ := NewFlatIndex("embedding", config)

	// 插入数据
	for i := 0; i < 100; i++ {
		idx.Insert(int64(i), randomVector(128))
	}

	stats := idx.Stats()

	t.Run("stats_count", func(t *testing.T) {
		if stats.Count != 100 {
			t.Errorf("Expected Count 100, got %d", stats.Count)
		}
	})

	t.Run("stats_type", func(t *testing.T) {
		if stats.Type != IndexTypeVectorFlat {
			t.Errorf("Expected Type %s, got %s", IndexTypeVectorFlat, stats.Type)
		}
	})

	t.Run("stats_metric", func(t *testing.T) {
		if stats.Metric != VectorMetricCosine {
			t.Errorf("Expected Metric %s, got %s", VectorMetricCosine, stats.Metric)
		}
	})

	t.Run("stats_dimension", func(t *testing.T) {
		if stats.Dimension != 128 {
			t.Errorf("Expected Dimension 128, got %d", stats.Dimension)
		}
	})

	t.Run("stats_memory", func(t *testing.T) {
		expectedMemory := int64(100 * 128 * 4) // 100 vectors * 128 dims * 4 bytes
		if stats.MemorySize != expectedMemory {
			t.Errorf("Expected MemorySize %d, got %d", expectedMemory, stats.MemorySize)
		}
	})
}

// TestFlatIndexClose 测试关闭索引
func TestFlatIndexClose(t *testing.T) {
	config := &VectorIndexConfig{
		MetricType: VectorMetricCosine,
		Dimension:  64,
	}
	idx, _ := NewFlatIndex("embedding", config)

	// 插入数据
	for i := 0; i < 100; i++ {
		idx.Insert(int64(i), randomVector(64))
	}

	err := idx.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	stats := idx.Stats()
	if stats.Count != 0 {
		t.Errorf("Expected count 0 after close, got %d", stats.Count)
	}
}

// TestFlatIndexGetConfig 测试获取配置
func TestFlatIndexGetConfig(t *testing.T) {
	config := &VectorIndexConfig{
		MetricType: VectorMetricCosine,
		Dimension:  128,
	}
	idx, _ := NewFlatIndex("embedding", config)

	gotConfig := idx.GetConfig()
	if gotConfig == nil {
		t.Fatal("GetConfig returned nil")
	}
	if gotConfig.MetricType != VectorMetricCosine {
		t.Errorf("Expected MetricType Cosine, got %s", gotConfig.MetricType)
	}
	if gotConfig.Dimension != 128 {
		t.Errorf("Expected Dimension 128, got %d", gotConfig.Dimension)
	}
}

// TestFlatIndexDifferentMetrics 测试不同距离度量
func TestFlatIndexDifferentMetrics(t *testing.T) {
	ctx := context.Background()
	metrics := []VectorMetricType{VectorMetricCosine, VectorMetricL2, VectorMetricIP}

	for _, metric := range metrics {
		t.Run(string(metric), func(t *testing.T) {
			config := &VectorIndexConfig{
				MetricType: metric,
				Dimension:  64,
			}
			idx, err := NewFlatIndex("embedding", config)
			if err != nil {
				t.Fatalf("NewFlatIndex failed: %v", err)
			}

			// 插入数据
			for i := 0; i < 50; i++ {
				idx.Insert(int64(i), randomVector(64))
			}

			// 搜索
			query := randomVector(64)
			result, err := idx.Search(ctx, query, 10, nil)
			if err != nil {
				t.Fatalf("Search failed: %v", err)
			}
			if len(result.IDs) != 10 {
				t.Errorf("Expected 10 results, got %d", len(result.IDs))
			}
		})
	}
}

// testVectorDataLoader 测试用的数据加载器
type testVectorDataLoader struct {
	records []VectorRecord
	err     error
}

func (l *testVectorDataLoader) Load(ctx context.Context) ([]VectorRecord, error) {
	if l.err != nil {
		return nil, l.err
	}
	return l.records, nil
}

func (l *testVectorDataLoader) Count() int64 {
	return int64(len(l.records))
}
