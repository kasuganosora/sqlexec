package memory

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestGetRecallValue 测试批量召回率计算（参考 Milvus）
func TestGetRecallValue(t *testing.T) {
	testCases := []struct {
		name      string
		trueIDs   [][]int64
		resultIDs [][]int64
		expected  float64
	}{
		{
			name: "perfect_recall",
			trueIDs: [][]int64{
				{1, 2, 3, 4, 5},
				{10, 20, 30, 40, 50},
			},
			resultIDs: [][]int64{
				{1, 2, 3, 4, 5},
				{10, 20, 30, 40, 50},
			},
			expected: 1.0,
		},
		{
			name: "partial_recall",
			trueIDs: [][]int64{
				{1, 2, 3, 4, 5},
				{10, 20, 30, 40, 50},
			},
			resultIDs: [][]int64{
				{1, 2, 6, 7, 8},      // 2/5 = 0.4
				{10, 20, 30, 60, 70}, // 3/5 = 0.6
			},
			expected: 0.5, // (0.4 + 0.6) / 2 = 0.5
		},
		{
			name: "zero_recall",
			trueIDs: [][]int64{
				{1, 2, 3, 4, 5},
				{10, 20, 30, 40, 50},
			},
			resultIDs: [][]int64{
				{6, 7, 8, 9, 10},
				{60, 70, 80, 90, 100},
			},
			expected: 0.0,
		},
		{
			name: "empty_results",
			trueIDs: [][]int64{
				{1, 2, 3, 4, 5},
				{10, 20, 30, 40, 50},
			},
			resultIDs: [][]int64{
				{},
				{},
			},
			expected: 0.0,
		},
		{
			name: "single_query",
			trueIDs: [][]int64{
				{1, 2, 3, 4, 5},
			},
			resultIDs: [][]int64{
				{1, 2, 6, 7, 8},
			},
			expected: 0.4,
		},
		{
			name: "different_lengths",
			trueIDs: [][]int64{
				{1, 2, 3, 4, 5},
				{10, 20, 30, 40, 50},
			},
			resultIDs: [][]int64{
				{1, 2, 3}, // 3/3 = 1.0
				{10, 20},  // 2/2 = 1.0
			},
			expected: 1.0,
		},
		{
			name: "milvus_example",
			trueIDs: [][]int64{
				{100, 200, 300, 400, 500},
				{1000, 2000, 3000, 4000, 5000},
				{10, 20, 30, 40, 50},
			},
			resultIDs: [][]int64{
				{100, 200, 600, 700, 800},
				{1000, 2000, 3000, 9000, 10000},
				{15, 25, 35, 45, 55},
			},
			expected: 0.333, // (2/5 + 3/5 + 0/5) / 3 = 1.0/3 = 0.333
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := GetRecallValue(tc.trueIDs, tc.resultIDs)
			require.InDelta(t, tc.expected, result, 0.001,
				"期望召回率 %.3f, 实际 %.3f", tc.expected, result)
		})
	}
}

// TestGetRecallValueAtK 测试指定K值的召回率
func TestGetRecallValueAtK(t *testing.T) {
	trueIDs := [][]int64{
		{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		{10, 20, 30, 40, 50, 60, 70, 80, 90, 100},
	}

	resultIDs := [][]int64{
		{1, 2, 11, 12, 13, 14, 15, 16, 17, 18},
		{10, 20, 31, 32, 33, 34, 35, 36, 37, 38},
	}

	testCases := []struct {
		name     string
		k        int
		expected float64
	}{
		{
			name:     "k_5",
			k:        5,
			expected: 0.4, // (2/5 + 2/5) / 2 = 0.4
		},
		{
			name:     "k_3",
			k:        3,
			expected: 0.667, // (2/3 + 2/3) / 2 = 0.667
		},
		{
			name:     "k_10",
			k:        10,
			expected: 0.2, // (2/10 + 2/10) / 2 = 0.2
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := GetRecallValueAtK(trueIDs, resultIDs, tc.k)
			t.Logf("K=%d, 召回率=%.3f", tc.k, result)
			require.InDelta(t, tc.expected, result, 0.001)
		})
	}
}

// TestGetIntersectionSize 测试交集大小计算
func TestGetIntersectionSize(t *testing.T) {
	testCases := []struct {
		name     string
		list1    []int64
		list2    []int64
		expected int
	}{
		{
			name:     "full_intersection",
			list1:    []int64{1, 2, 3, 4, 5},
			list2:    []int64{1, 2, 3, 4, 5},
			expected: 5,
		},
		{
			name:     "partial_intersection",
			list1:    []int64{1, 2, 3, 4, 5},
			list2:    []int64{4, 5, 6, 7, 8},
			expected: 2,
		},
		{
			name:     "no_intersection",
			list1:    []int64{1, 2, 3},
			list2:    []int64{4, 5, 6},
			expected: 0,
		},
		{
			name:     "empty_lists",
			list1:    []int64{},
			list2:    []int64{},
			expected: 0,
		},
		{
			name:     "duplicate_values",
			list1:    []int64{1, 2, 2, 3},
			list2:    []int64{2, 2, 4, 5},
			expected: 2, // 只计算唯一值
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := GetIntersectionSize(tc.list1, tc.list2)
			require.Equal(t, tc.expected, result)
		})
	}
}

// TestCalculateSingleRecall 测试单个查询召回率
func TestCalculateSingleRecall(t *testing.T) {
	testCases := []struct {
		name      string
		trueIDs   []int64
		resultIDs []int64
		expected  float64
	}{
		{
			name:      "perfect_recall",
			trueIDs:   []int64{1, 2, 3, 4, 5},
			resultIDs: []int64{1, 2, 3, 4, 5},
			expected:  1.0,
		},
		{
			name:      "half_recall",
			trueIDs:   []int64{1, 2, 3, 4, 5},
			resultIDs: []int64{1, 2, 6, 7, 8},
			expected:  0.4, // 2/5 = 0.4
		},
		{
			name:      "zero_recall",
			trueIDs:   []int64{1, 2, 3},
			resultIDs: []int64{4, 5, 6},
			expected:  0.0,
		},
		{
			name:      "empty_result",
			trueIDs:   []int64{1, 2, 3},
			resultIDs: []int64{},
			expected:  0.0,
		},
		{
			name:      "empty_groundtruth",
			trueIDs:   []int64{},
			resultIDs: []int64{1, 2, 3},
			expected:  0.0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := CalculateSingleRecall(tc.trueIDs, tc.resultIDs)
			require.InDelta(t, tc.expected, result, 0.001)
		})
	}
}

// TestGetMinRecall 测试最小召回率
func TestGetMinRecall(t *testing.T) {
	trueIDs := [][]int64{
		{1, 2, 3, 4, 5},
		{10, 20, 30, 40, 50},
		{100, 200, 300, 400, 500},
	}

	resultIDs := [][]int64{
		{1, 2, 3, 4, 5},           // recall = 1.0
		{10, 20, 60, 70, 80},      // recall = 0.4
		{100, 200, 300, 400, 500}, // recall = 1.0
	}

	minRecall := GetMinRecall(trueIDs, resultIDs)
	require.InDelta(t, 0.4, minRecall, 0.001)
}

// TestGetRecallStats 测试召回率统计信息
func TestGetRecallStats(t *testing.T) {
	trueIDs := [][]int64{
		{1, 2, 3, 4, 5},
		{10, 20, 30, 40, 50},
		{100, 200, 300, 400, 500},
		{1000, 2000, 3000, 4000, 5000},
	}

	resultIDs := [][]int64{
		{1, 2, 3, 4, 5},                // recall = 1.0
		{10, 20, 60, 70, 80},           // recall = 0.4
		{100, 200, 300, 900, 1000},     // recall = 0.6
		{5000, 6000, 7000, 8000, 9000}, // recall = 0.2
	}

	avg, min, max, std := GetRecallStats(trueIDs, resultIDs)

	// 期望平均值
	expectedAvg := (1.0 + 0.4 + 0.6 + 0.2) / 4.0
	require.InDelta(t, expectedAvg, avg, 0.001)

	// 期望最小值
	require.InDelta(t, 0.2, min, 0.001)

	// 期望最大值
	require.InDelta(t, 1.0, max, 0.001)

	// 期望标准差（可以验证是否大于0）
	require.Greater(t, std, 0.0)

	t.Logf("召回率统计 - 平均: %.3f, 最小: %.3f, 最大: %.3f, 标准差: %.3f",
		avg, min, max, std)
}

// TestRecallWithRealVectorSearch 使用真实向量搜索测试召回率
func TestRecallWithRealVectorSearch(t *testing.T) {
	ctx := context.Background()

	// 创建索引
	idxMgr := NewIndexManager()

	// 使用Flat索引作为ground truth
	flatIdx, err := idxMgr.CreateVectorIndex(
		"test_real_recall",
		"flat",
		VectorMetricCosine,
		IndexTypeVectorFlat,
		64,
		nil,
	)
	require.NoError(t, err)

	// 使用HNSW索引作为测试索引
	hnswIdx, err := idxMgr.CreateVectorIndex(
		"test_real_recall",
		"hnsw",
		VectorMetricCosine,
		IndexTypeVectorHNSW,
		64,
		nil,
	)
	require.NoError(t, err)

	// 添加测试数据
	numVectors := 1000
	for i := 0; i < numVectors; i++ {
		vec := randomVector(64)
		err := flatIdx.Insert(int64(i), vec)
		require.NoError(t, err)

		err = hnswIdx.Insert(int64(i), vec)
		require.NoError(t, err)
	}

	// 执行多个查询
	numQueries := 50
	k := 20

	trueIDs := make([][]int64, numQueries)
	resultIDs := make([][]int64, numQueries)

	for i := 0; i < numQueries; i++ {
		query := randomVector(64)

		// 获取ground truth
		flatResult, err := flatIdx.Search(ctx, query, k, nil)
		require.NoError(t, err)
		trueIDs[i] = flatResult.IDs

		// 获取测试结果
		hnswResult, err := hnswIdx.Search(ctx, query, k, nil)
		require.NoError(t, err)
		resultIDs[i] = hnswResult.IDs
	}

	// 计算各种召回率指标
	avgRecall := GetRecallValue(trueIDs, resultIDs)
	minRecall := GetMinRecall(trueIDs, resultIDs)
	avgRecallAt5 := GetRecallValueAtK(trueIDs, resultIDs, 5)
	avgRecallAt10 := GetRecallValueAtK(trueIDs, resultIDs, 10)
	avgRecallAt20 := GetRecallValueAtK(trueIDs, resultIDs, 20)

	t.Logf("向量搜索召回率测试结果:")
	t.Logf("  平均召回率: %.3f", avgRecall)
	t.Logf("  最小召回率: %.3f", minRecall)
	t.Logf("  召回率@5: %.3f", avgRecallAt5)
	t.Logf("  召回率@10: %.3f", avgRecallAt10)
	t.Logf("  召回率@20: %.3f", avgRecallAt20)

	// 验证召回率要求
	require.GreaterOrEqual(t, avgRecall, 0.6, "平均召回率应该 >= 0.6")
	require.GreaterOrEqual(t, minRecall, 0.4, "最小召回率应该 >= 0.4")
	require.GreaterOrEqual(t, avgRecallAt10, 0.65, "召回率@10应该 >= 0.65")
	require.GreaterOrEqual(t, avgRecallAt20, 0.6, "召回率@20应该 >= 0.6")
}
