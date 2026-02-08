package memory

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestVectorIndexCompleteWorkflow 测试向量搜索完整工作流
func TestVectorIndexCompleteWorkflow(t *testing.T) {
	ctx := context.Background()
	
	// 1. 创建索引管理器
	idxMgr := NewIndexManager()
	require.NotNil(t, idxMgr)
	
	// 2. 创建向量索引
	idx, err := idxMgr.CreateVectorIndex(
		"test_articles",
		"embedding",
		VectorMetricCosine,
		IndexTypeVectorHNSW,
		128,
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, idx)
	
	// 3. 添加测试向量
	numVectors := 1000
	vectors := make([][]float32, numVectors)
	for i := 0; i < numVectors; i++ {
		vectors[i] = randomVector(128)
		err := idx.Insert(int64(i), vectors[i])
		require.NoError(t, err)
	}
	
	// 4. 执行向量搜索
	queryVector := randomVector(128)
	result, err := idx.Search(ctx, queryVector, 10, nil)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Len(t, result.IDs, 10)
	require.Len(t, result.Distances, 10)
	
	// 5. 验证搜索结果合理性
	for i := 1; i < len(result.Distances); i++ {
		// 距离应该是非递减的
		require.GreaterOrEqual(t, result.Distances[i], result.Distances[i-1])
	}
	
	// 6. 测试索引统计信息
	stats := idx.Stats()
	require.Equal(t, IndexTypeVectorHNSW, stats.Type)
	require.Equal(t, VectorMetricCosine, stats.Metric)
	require.Equal(t, 128, stats.Dimension)
	require.Equal(t, int64(numVectors), stats.Count)
	require.Greater(t, stats.MemorySize, int64(0))
	
	// 7. 测试删除操作
	err = idx.Delete(int64(0))
	require.NoError(t, err)
	
	stats = idx.Stats()
	require.Equal(t, int64(numVectors-1), stats.Count)
	
	// 8. 测试Flat索引
	flatIdx, err := idxMgr.CreateVectorIndex(
		"test_flat",
		"embedding",
		VectorMetricL2,
		IndexTypeVectorFlat,
		64,
		nil,
	)
	require.NoError(t, err)
	
	// 添加向量并搜索
	for i := 0; i < 100; i++ {
		vec := randomVector(64)
		err := flatIdx.Insert(int64(i), vec)
		require.NoError(t, err)
	}
	
	query2 := randomVector(64)
	result2, err := flatIdx.Search(ctx, query2, 5, nil)
	require.NoError(t, err)
	require.Len(t, result2.IDs, 5)
	
	// 9. 测试获取索引
	retrievedIdx, err := idxMgr.GetVectorIndex("test_articles", "embedding")
	require.NoError(t, err)
	require.NotNil(t, retrievedIdx)
	
	// 10. 测试删除索引
	err = idxMgr.DropVectorIndex("test_flat", "embedding")
	require.NoError(t, err)
	
	_, err = idxMgr.GetVectorIndex("test_flat", "embedding")
	require.Error(t, err)
}

// TestVectorIndexRecall 测试向量索引召回率（参考 Milvus 实现）
func TestVectorIndexRecall(t *testing.T) {
	ctx := context.Background()
	
	// 创建Flat索引（精确搜索）和HNSW索引（近似搜索）进行对比
	idxMgr := NewIndexManager()
	
	flatIdx, err := idxMgr.CreateVectorIndex(
		"test_recall",
		"embedding",
		VectorMetricCosine,
		IndexTypeVectorFlat,
		32,
		nil,
	)
	require.NoError(t, err)
	
	hnswIdx, err := idxMgr.CreateVectorIndex(
		"test_recall",
		"embedding_hnsw",
		VectorMetricCosine,
		IndexTypeVectorHNSW,
		32,
		nil,
	)
	require.NoError(t, err)
	
	// 添加相同向量到两个索引
	numTestVectors := 500
	for i := 0; i < numTestVectors; i++ {
		vec := randomVector(32)
		err := flatIdx.Insert(int64(i), vec)
		require.NoError(t, err)
		
		err = hnswIdx.Insert(int64(i), vec)
		require.NoError(t, err)
	}
	
	// 测试多个查询向量（批量召回率测试）
	numQueries := 20
	k := 10
	
	// 存储所有查询的真实结果和近似结果
	trueIDs := make([][]int64, numQueries)
	resultIDs := make([][]int64, numQueries)
	
	for i := 0; i < numQueries; i++ {
		query := randomVector(32)
		
		// 精确搜索结果（作为 ground truth）
		flatResult, err := flatIdx.Search(ctx, query, k, nil)
		require.NoError(t, err)
		trueIDs[i] = flatResult.IDs
		
		// 近似搜索结果
		hnswResult, err := hnswIdx.Search(ctx, query, k, nil)
		require.NoError(t, err)
		resultIDs[i] = hnswResult.IDs
		
		// 打印单个查询召回率
		singleRecall := calculateRecall(trueIDs[i], resultIDs[i])
		t.Logf("查询 %d 召回率: %.3f", i, singleRecall)
	}
	
	// 使用批量召回率计算（参考 Milvus 实现）
	avgRecall := GetRecallValue(trueIDs, resultIDs)
	t.Logf("平均召回率@K=%d: %.3f", k, avgRecall)
	
	// 使用简化的HNSW实现，目标召回率应该大于70%
	require.GreaterOrEqual(t, avgRecall, 0.7, "批量召回率应该大于等于70%")
	
	// 测试不同K值的召回率
	kValues := []int{5, 10, 20}
	for _, testK := range kValues {
		recallAtK := GetRecallValueAtK([][]int64{trueIDs[0]}, [][]int64{resultIDs[0]}, testK)
		t.Logf("召回率@K=%d: %.3f", testK, recallAtK)
	}
}

// calculateRecall 计算召回率（单查询）
func calculateRecall(groundTruth, result []int64) float64 {
	if len(groundTruth) == 0 {
		return 0
	}
	
	truthSet := make(map[int64]bool)
	for _, id := range groundTruth {
		truthSet[id] = true
	}
	
	hitCount := 0
	for _, id := range result {
		if truthSet[id] {
			hitCount++
		}
	}
	
	return float64(hitCount) / float64(len(groundTruth))
}



// TestVectorDataLoaderIntegration 测试向量数据加载器
func TestVectorDataLoaderIntegration(t *testing.T) {
	ctx := context.Background()
	
	// 创建测试数据
	vectors := make([]VectorRecord, 100)
	for i := 0; i < 100; i++ {
		vectors[i] = VectorRecord{
			ID:     int64(i),
			Vector: randomVector(64),
		}
	}
	
	// 创建数据加载器
	loader := &mockVectorDataLoader{
		records: vectors,
		count:   int64(len(vectors)),
	}
	
	// 创建索引并构建
	idxMgr := NewIndexManager()
	idx, err := idxMgr.CreateVectorIndex(
		"test_loader",
		"embedding",
		VectorMetricL2,
		IndexTypeVectorHNSW,
		64,
		nil,
	)
	require.NoError(t, err)
	
	err = idx.Build(ctx, loader)
	require.NoError(t, err)
	
	// 验证数据已加载
	stats := idx.Stats()
	require.Equal(t, int64(100), stats.Count)
	
	// 执行搜索
	query := randomVector(64)
	result, err := idx.Search(ctx, query, 10, nil)
	require.NoError(t, err)
	require.Len(t, result.IDs, 10)
}

// mockVectorDataLoader 模拟向量数据加载器
type mockVectorDataLoader struct {
	records []VectorRecord
	count   int64
}

func (m *mockVectorDataLoader) Load(ctx context.Context) ([]VectorRecord, error) {
	return m.records, nil
}

func (m *mockVectorDataLoader) Count() int64 {
	return m.count
}

// TestVectorIndexAccuracy 测试向量索引准确率（类似 Milvus AccuracyRunner）
func TestVectorIndexAccuracy(t *testing.T) {
	ctx := context.Background()
	
	// 测试不同索引类型和参数的召回率
	testCases := []struct {
		name        string
		indexType   IndexType
		metricType  VectorMetricType
		dimension   int
		datasetSize int
		nq          int // number of queries
		k           int // top-k
		minRecall   float64
	}{
		{
			name:        "HNSW_Cosine_1000_20_10",
			indexType:   IndexTypeVectorHNSW,
			metricType:  VectorMetricCosine,
			dimension:   64,
			datasetSize: 1000,
			nq:          20,
			k:           10,
			minRecall:   0.70,
		},
		{
			name:        "Flat_L2_500_10_5",
			indexType:   IndexTypeVectorFlat,
			metricType:  VectorMetricL2,
			dimension:   32,
			datasetSize: 500,
			nq:          10,
			k:           5,
			minRecall:   0.95, // Flat索引应该有更高的召回率
		},
		{
			name:        "HNSW_IP_2000_50_20",
			indexType:   IndexTypeVectorHNSW,
			metricType:  VectorMetricIP,
			dimension:   128,
			datasetSize: 2000,
			nq:          50,
			k:           20,
			minRecall:   0.65,
		},
	}
	
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// 创建Flat索引（ground truth）和测试索引
			idxMgr := NewIndexManager()
			
			flatIdx, err := idxMgr.CreateVectorIndex(
				"accuracy_test",
				"flat",
				tc.metricType,
				IndexTypeVectorFlat,
				tc.dimension,
				nil,
			)
			require.NoError(t, err)
			
			testIdx, err := idxMgr.CreateVectorIndex(
				"accuracy_test",
				tc.name,
				tc.metricType,
				tc.indexType,
				tc.dimension,
				nil,
			)
			require.NoError(t, err)
			
			// 添加相同向量到两个索引
			for i := 0; i < tc.datasetSize; i++ {
				vec := randomVector(tc.dimension)
				err := flatIdx.Insert(int64(i), vec)
				require.NoError(t, err)
				
				err = testIdx.Insert(int64(i), vec)
				require.NoError(t, err)
			}
			
			// 准备查询向量
			trueIDs := make([][]int64, tc.nq)
			resultIDs := make([][]int64, tc.nq)
			
			for i := 0; i < tc.nq; i++ {
				query := randomVector(tc.dimension)
				
				// 获取 ground truth
				flatResult, err := flatIdx.Search(ctx, query, tc.k, nil)
				require.NoError(t, err)
				trueIDs[i] = flatResult.IDs
				
				// 获取测试结果
				testResult, err := testIdx.Search(ctx, query, tc.k, nil)
				require.NoError(t, err)
				resultIDs[i] = testResult.IDs
			}
			
			// 计算批量召回率
			recall := GetRecallValue(trueIDs, resultIDs)
			t.Logf("测试 %s: 召回率=%.3f, 最小要求=%.3f", tc.name, recall, tc.minRecall)
			
			require.GreaterOrEqual(t, recall, tc.minRecall,
				"召回率 %.3f 低于最小要求 %.3f", recall, tc.minRecall)
		})
	}
}

// TestVectorFilter 测试向量过滤器
func TestVectorFilter(t *testing.T) {
	ctx := context.Background()
	
	idxMgr := NewIndexManager()
	idx, err := idxMgr.CreateVectorIndex(
		"test_filter",
		"embedding",
		VectorMetricCosine,
		IndexTypeVectorFlat,
		32,
		nil,
	)
	require.NoError(t, err)
	
	// 添加向量
	for i := 0; i < 50; i++ {
		vec := randomVector(32)
		err := idx.Insert(int64(i), vec)
		require.NoError(t, err)
	}
	
	// 搜索并过滤特定ID
	query := randomVector(32)
	filter := &VectorFilter{
		IDs: []int64{5, 10, 15, 20, 25},
	}
	
	result, err := idx.Search(ctx, query, 10, filter)
	require.NoError(t, err)
	
	// 验证结果都在过滤器中
	for _, id := range result.IDs {
		found := false
		for _, fid := range filter.IDs {
			if id == fid {
				found = true
				break
			}
		}
		require.True(t, found, "结果ID %d 不在过滤器中", id)
	}
}
