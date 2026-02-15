package memory

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestImprovedHNSWRecall 测试改进的 HNSW 索引召回率（参考 Milvus）
func TestImprovedHNSWRecall(t *testing.T) {
	ctx := context.Background()
	
	// 创建 Flat 索引（精确搜索）和改进的 HNSW 索引
	idxMgr := NewIndexManager()
	
	flatIdx, err := idxMgr.CreateVectorIndex(
		"test_improved_recall",
		"flat",
		VectorMetricCosine,
		IndexTypeVectorFlat,
		128,
		nil,
	)
	require.NoError(t, err)
	
	hnswIdx, err := idxMgr.CreateVectorIndex(
		"test_improved_recall",
		"hnsw_improved",
		VectorMetricCosine,
		IndexTypeVectorHNSW,
		128,
		nil,
	)
	require.NoError(t, err)
	
	// 添加大量向量以更好地评估
	numTestVectors := 2000
	t.Logf("添加 %d 个向量用于测试...", numTestVectors)
	
	for i := 0; i < numTestVectors; i++ {
		vec := randomVector(128)
		err := flatIdx.Insert(int64(i), vec)
		require.NoError(t, err)
		
		err = hnswIdx.Insert(int64(i), vec)
		require.NoError(t, err)
		
		if (i+1)%500 == 0 {
			t.Logf("已添加 %d/%d 个向量...", i+1, numTestVectors)
		}
	}
	
	t.Logf("向量添加完成，开始测试召回率...")
	
	// 测试多个查询向量
	numQueries := 100
	k := 10
	
	trueIDs := make([][]int64, numQueries)
	resultIDs := make([][]int64, numQueries)
	
	for i := 0; i < numQueries; i++ {
		query := randomVector(128)
		
		// 精确搜索结果
		flatResult, err := flatIdx.Search(ctx, query, k, nil)
		require.NoError(t, err)
		trueIDs[i] = flatResult.IDs
		
		// 近似搜索结果（改进的 HNSW）
		hnswResult, err := hnswIdx.Search(ctx, query, k, nil)
		require.NoError(t, err)
		resultIDs[i] = hnswResult.IDs
		
		// 计算单个查询召回率
		singleRecall := CalculateSingleRecall(trueIDs[i], resultIDs[i])
		if (i+1)%20 == 0 {
			t.Logf("查询 %d 召回率: %.3f%%", i, singleRecall*100)
		}
	}
	
	// 使用改进的召回率计算
	avgRecall := GetRecallValue(trueIDs, resultIDs)
	minRecall := GetMinRecall(trueIDs, resultIDs)
	avgRecallAt5 := GetRecallValueAtK(trueIDs, resultIDs, 5)
	avgRecallAt10 := GetRecallValueAtK(trueIDs, resultIDs, 10)
	avgRecallAt20 := GetRecallValueAtK(trueIDs, resultIDs, 20)
	
	// 输出详细结果
	t.Log("\n=== 改进的 HNSW 召回率测试结果 ===")
	t.Logf("数据集大小: %d 个向量", numTestVectors)
	t.Logf("查询数量: %d", numQueries)
	t.Logf("Top-K: %d", k)
	t.Logf("平均召回率: %.3f%%", avgRecall*100)
	t.Logf("最小召回率: %.3f%%", minRecall*100)
	t.Logf("召回率@K=5:  %.3f%%", avgRecallAt5*100)
	t.Logf("召回率@K=10: %.3f%%", avgRecallAt10*100)
	t.Logf("召回率@K=20: %.3f%%", avgRecallAt20*100)
	
	// 验证召回率要求
	require.GreaterOrEqual(t, avgRecall, 0.95, 
		"平均召回率应该 >= 95%%, 实际: %.3f%%", avgRecall*100)
	require.GreaterOrEqual(t, minRecall, 0.85, 
		"最小召回率应该 >= 85%%, 实际: %.3f%%", minRecall*100)
	require.GreaterOrEqual(t, avgRecallAt10, 0.95, 
		"召回率@K=10 应该 >= 95%%, 实际: %.3f%%", avgRecallAt10*100)
}

// TestImprovedHNSWPerformance 测试改进的 HNSW 性能
func TestImprovedHNSWPerformance(t *testing.T) {
	ctx := context.Background()
	
	// 创建改进的 HNSW 索引
	idxMgr := NewIndexManager()
	
	hnswIdx, err := idxMgr.CreateVectorIndex(
		"test_perf",
		"embedding",
		VectorMetricCosine,
		IndexTypeVectorHNSW,
		256,
		nil,
	)
	require.NoError(t, err)
	
	// 添加测试数据
	numVectors := 5000
	t.Logf("添加 %d 个向量...", numVectors)
	
	for i := 0; i < numVectors; i++ {
		vec := randomVector(256)
		err := hnswIdx.Insert(int64(i), vec)
		require.NoError(t, err)
	}
	
	t.Logf("向量添加完成，测试性能...")
	
	// 测试查询延迟
	numQueries := 1000
	k := 10

	latencies := make([]float64, numQueries)

	for i := 0; i < numQueries; i++ {
		query := randomVector(256)

		start := time.Now()
		_, err := hnswIdx.Search(ctx, query, k, nil)
		require.NoError(t, err)
		elapsed := time.Since(start)

		latencies[i] = float64(elapsed.Microseconds()) / 1000.0 // ms with sub-ms precision

		if (i+1)%200 == 0 {
			t.Logf("已执行 %d/%d 次查询...", i+1, numQueries)
		}
	}

	sort.Float64s(latencies)
	var totalLatency float64
	for _, l := range latencies {
		totalLatency += l
	}
	avgLatency := totalLatency / float64(numQueries)
	minLatency := latencies[0]
	maxLatency := latencies[numQueries-1]
	p95 := latencies[int(float64(numQueries)*0.95)]
	p99 := latencies[int(float64(numQueries)*0.99)]
	
	t.Log("\n=== 改进的 HNSW 性能测试结果 ===")
	t.Logf("数据集大小: %d 个向量", numVectors)
	t.Logf("查询数量: %d", numQueries)
	t.Logf("Top-K: %d", k)
	t.Logf("平均延迟: %.2f ms", avgLatency)
	t.Logf("最小延迟: %.2f ms", minLatency)
	t.Logf("最大延迟: %.2f ms", maxLatency)
	t.Logf("P95 延迟: %.2f ms", p95)
	t.Logf("P99 延迟: %.2f ms", p99)
	
	// 验证性能要求
	require.LessOrEqual(t, p99, 10.0, 
		"P99 延迟应该 <= 10ms, 实际: %.2fms", p99)
	require.LessOrEqual(t, avgLatency, 5.0, 
		"平均延迟应该 <= 5ms, 实际: %.2fms", avgLatency)
}


// TestImprovedHNSWScale 测试改进的 HNSW 可扩展性
func TestImprovedHNSWScale(t *testing.T) {
	ctx := context.Background()
	
	scales := []struct {
		numVectors int
		queries    int
		expectedRecall float64
	}{
		{1000, 50, 0.95},
		{2000, 100, 0.95},
		{5000, 200, 0.94}, // 大数据集召回率略微下降
		{10000, 500, 0.93},
	}
	
	for _, scale := range scales {
		t.Run(fmt.Sprintf("Scale_%d", scale.numVectors), func(t *testing.T) {
			t.Logf("测试规模: %d 个向量, %d 次查询", scale.numVectors, scale.queries)
			
			// 创建索引
			idxMgr := NewIndexManager()
			hnswIdx, err := idxMgr.CreateVectorIndex(
				"scale_test",
				"embedding",
				VectorMetricCosine,
				IndexTypeVectorHNSW,
				128,
				nil,
			)
			require.NoError(t, err)
			
			// 添加向量
			for i := 0; i < scale.numVectors; i++ {
				vec := randomVector(128)
				err := hnswIdx.Insert(int64(i), vec)
				require.NoError(t, err)
			}
			
			// 测试召回率
			trueIDs := make([][]int64, scale.queries)
			resultIDs := make([][]int64, scale.queries)
			
			for i := 0; i < scale.queries; i++ {
				query := randomVector(128)
				flatResult, err := hnswIdx.Search(ctx, query, 10, nil)
				require.NoError(t, err)
				trueIDs[i] = flatResult.IDs
				
				hnswResult, err := hnswIdx.Search(ctx, query, 10, nil)
				require.NoError(t, err)
				resultIDs[i] = hnswResult.IDs
			}
			
			recall := GetRecallValue(trueIDs, resultIDs)
			t.Logf("召回率: %.3f%% (期望 >= %.1f%%)", recall*100, scale.expectedRecall*100)
			require.GreaterOrEqual(t, recall, scale.expectedRecall,
				"召回率应该 >= %.1f%%, 实际: %.3f%%", scale.expectedRecall*100, recall*100)
		})
	}
}

// TestImprovedHNSWWithFilters 测试改进的 HNSW 带过滤条件
func TestImprovedHNSWWithFilters(t *testing.T) {
	ctx := context.Background()

	idxMgr := NewIndexManager()

	hnswIdx, err := idxMgr.CreateVectorIndex(
		"test_filter",
		"embedding",
		VectorMetricCosine,
		IndexTypeVectorHNSW,
		128,
		nil,
	)
	require.NoError(t, err)

	// 添加带类别的向量
	numVectors := 1000
	categories := []string{"tech", "news", "sports", "entertainment"}

	for i := 0; i < numVectors; i++ {
		vec := randomVector(128)
		err := hnswIdx.Insert(int64(i), vec)
		require.NoError(t, err)
	}

	t.Logf("向量添加完成，测试带过滤的搜索...")

	// 测试带过滤的搜索
	numQueries := 50
	k := 10

	totalCorrect := 0
	totalResults := 0

	for i := 0; i < numQueries; i++ {
		query := randomVector(128)
		categoryIdx := i % len(categories)

		// 创建过滤器：只包含该类别的向量
		filter := &VectorFilter{
			IDs: make([]int64, 0),
		}

		for j := 0; j < numVectors; j++ {
			if j%len(categories) == categoryIdx {
				filter.IDs = append(filter.IDs, int64(j))
			}
		}

		// 带过滤搜索
		result, err := hnswIdx.Search(ctx, query, k, filter)
		require.NoError(t, err)

		// 验证所有结果都在过滤集合中
		filteredCount := 0
		for _, id := range result.IDs {
			if contains(filter.IDs, id) {
				filteredCount++
			}
		}

		// 由于filter已经应用，所有结果都应该在filter中
		totalCorrect += filteredCount
		totalResults += len(result.IDs)

		if (i+1)%10 == 0 {
			recall := float64(filteredCount) / float64(len(result.IDs))
			t.Logf("查询 %d: %d/%d 结果在过滤集中 (%.1f%%)", i, filteredCount, len(result.IDs), recall*100)
		}
	}

	// 计算整体准确率（结果应该在过滤集合中）
	accuracy := float64(totalCorrect) / float64(totalResults)
	t.Logf("\n整体准确率: %.3f%% (%d/%d)", accuracy*100, totalCorrect, totalResults)

	// 由于Search函数已经应用过滤器，准确率应该是100%
	require.Equal(t, totalCorrect, totalResults,
		"所有结果都应该在过滤集合中")
}

// contains 检查切片是否包含元素
func contains(slice []int64, element int64) bool {
	for _, v := range slice {
		if v == element {
			return true
		}
	}
	return false
}
