package memory

import (
	"context"
	"math/rand"
	"testing"
)

// TestDistanceFunctions 测试距离函数
func TestDistanceFunctions(t *testing.T) {
	v1 := []float32{1.0, 2.0, 3.0}
	v2 := []float32{1.0, 2.0, 3.0}
	v3 := []float32{-1.0, -2.0, -3.0}

	// 测试余弦距离
	t.Run("CosineDistance", func(t *testing.T) {
		dist := CosineDistance(v1, v2)
		if dist != 0 {
			t.Errorf("CosineDistance(v1, v1) = %f, want 0", dist)
		}

		dist = CosineDistance(v1, v3)
		if dist != 2.0 {
			t.Errorf("CosineDistance(v1, v3) = %f, want 2.0", dist)
		}
	})

	// 测试L2距离
	t.Run("L2Distance", func(t *testing.T) {
		dist := L2Distance(v1, v2)
		if dist != 0 {
			t.Errorf("L2Distance(v1, v1) = %f, want 0", dist)
		}

		dist = L2Distance(v1, v3)
		expected := float32(7.483315)
		if dist < expected-0.001 || dist > expected+0.001 {
			t.Errorf("L2Distance(v1, v3) = %f, want %f", dist, expected)
		}
	})

	// 测试内积距离
	t.Run("InnerProductDistance", func(t *testing.T) {
		dist := InnerProductDistance(v1, v2)
		expected := float32(-14.0) // -(1*1 + 2*2 + 3*3)
		if dist != expected {
			t.Errorf("InnerProductDistance(v1, v1) = %f, want %f", dist, expected)
		}
	})
}

// TestFlatIndex 测试Flat索引
func TestFlatIndex(t *testing.T) {
	ctx := context.Background()
	config := &VectorIndexConfig{
		MetricType: VectorMetricCosine,
		Dimension:  128,
	}

	idx, err := NewFlatIndex("embedding", config)
	if err != nil {
		t.Fatalf("NewFlatIndex failed: %v", err)
	}

	// 插入测试向量
	for i := 0; i < 100; i++ {
		vec := randomVector(128)
		err := idx.Insert(int64(i), vec)
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}
	}

	// 验证索引统计
	stats := idx.Stats()
	if stats.Count != 100 {
		t.Errorf("Stats.Count = %d, want 100", stats.Count)
	}
	if stats.Type != IndexTypeVectorFlat {
		t.Errorf("Stats.Type = %s, want %s", stats.Type, IndexTypeVectorFlat)
	}
	if stats.Metric != VectorMetricCosine {
		t.Errorf("Stats.Metric = %s, want %s", stats.Metric, VectorMetricCosine)
	}

	// 搜索测试
	query := randomVector(128)
	result, err := idx.Search(ctx, query, 10, nil)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	if len(result.IDs) != 10 {
		t.Errorf("len(result.IDs) = %d, want 10", len(result.IDs))
	}

	if len(result.Distances) != 10 {
		t.Errorf("len(result.Distances) = %d, want 10", len(result.Distances))
	}

	// 关闭索引
	err = idx.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

// TestHNSWIndex 测试HNSW索引
func TestHNSWIndex(t *testing.T) {
	ctx := context.Background()
	config := &VectorIndexConfig{
		MetricType: VectorMetricL2,
		Dimension:  64,
	}

	idx, err := NewHNSWIndex("embedding", config)
	if err != nil {
		t.Fatalf("NewHNSWIndex failed: %v", err)
	}

	// 插入测试向量
	for i := 0; i < 100; i++ {
		vec := randomVector(64)
		err := idx.Insert(int64(i), vec)
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}
	}

	// 验证索引统计
	stats := idx.Stats()
	if stats.Count != 100 {
		t.Errorf("Stats.Count = %d, want 100", stats.Count)
	}
	if stats.Type != IndexTypeVectorHNSW {
		t.Errorf("Stats.Type = %s, want %s", stats.Type, IndexTypeVectorHNSW)
	}

	// 搜索测试
	query := randomVector(64)
	result, err := idx.Search(ctx, query, 10, nil)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	if len(result.IDs) != 10 {
		t.Errorf("len(result.IDs) = %d, want 10", len(result.IDs))
	}

	// 关闭索引
	err = idx.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

// TestIndexManagerVector 测试IndexManager的向量索引功能
func TestIndexManagerVector(t *testing.T) {
	mgr := NewIndexManager()

	// 创建向量索引
	idx, err := mgr.CreateVectorIndex("articles", "embedding", VectorMetricCosine, IndexTypeVectorFlat, 128, nil)
	if err != nil {
		t.Fatalf("CreateVectorIndex failed: %v", err)
	}

	if idx == nil {
		t.Fatal("CreateVectorIndex returned nil index")
	}

	// 插入向量
	for i := 0; i < 50; i++ {
		vec := randomVector(128)
		err := idx.Insert(int64(i), vec)
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}
	}

	// 获取向量索引
	gotIdx, err := mgr.GetVectorIndex("articles", "embedding")
	if err != nil {
		t.Fatalf("GetVectorIndex failed: %v", err)
	}

	if gotIdx == nil {
		t.Fatal("GetVectorIndex returned nil")
	}

	// 搜索
	ctx := context.Background()
	query := randomVector(128)
	result, err := gotIdx.Search(ctx, query, 5, nil)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	if len(result.IDs) != 5 {
		t.Errorf("len(result.IDs) = %d, want 5", len(result.IDs))
	}

	// 删除向量索引
	err = mgr.DropVectorIndex("articles", "embedding")
	if err != nil {
		t.Fatalf("DropVectorIndex failed: %v", err)
	}

	// 验证已删除
	_, err = mgr.GetVectorIndex("articles", "embedding")
	if err == nil {
		t.Error("GetVectorIndex should return error after drop")
	}
}

// randomVector 生成随机向量
func randomVector(dim int) []float32 {
	vec := make([]float32, dim)
	for i := 0; i < dim; i++ {
		vec[i] = rand.Float32()
	}
	return vec
}
