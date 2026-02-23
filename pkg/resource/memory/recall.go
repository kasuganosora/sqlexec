package memory

import (
	"math"
)

// RecallMetrics 召回率计算工具（参考 Milvus 实现）

// GetRecallValue 计算批量召回率（参考 Milvus 的 get_recall_value）
// trueIDs: 每个查询的真实邻居ID列表，形状为 [numQueries, k]
// resultIDs: 每个查询的搜索结果ID列表，形状为 [numQueries, k]
// 返回平均召回率，保留3位小数
func GetRecallValue(trueIDs [][]int64, resultIDs [][]int64) float64 {
	if len(trueIDs) == 0 || len(resultIDs) == 0 {
		return 0.0
	}

	if len(trueIDs) != len(resultIDs) {
		return 0.0
	}

	sumRatio := 0.0
	numQueries := len(trueIDs)

	for i := 0; i < numQueries; i++ {
		if len(resultIDs[i]) == 0 {
			continue
		}

		// 计算当前查询的召回率
		// recall = |trueIDs[i] ∩ resultIDs[i]| / |resultIDs[i]|
		hitCount := 0
		trueSet := make(map[int64]bool, len(trueIDs[i]))

		for _, id := range trueIDs[i] {
			trueSet[id] = true
		}

		for _, id := range resultIDs[i] {
			if trueSet[id] {
				hitCount++
			}
		}

		sumRatio += float64(hitCount) / float64(len(resultIDs[i]))
	}

	// 计算平均召回率并保留3位小数
	avgRecall := sumRatio / float64(numQueries)
	return math.Round(avgRecall*1000) / 1000.0
}

// GetRecallValueAtK 计算指定K值的召回率
// 对每个查询只考虑前K个结果
func GetRecallValueAtK(trueIDs [][]int64, resultIDs [][]int64, k int) float64 {
	if len(trueIDs) == 0 || len(resultIDs) == 0 || k <= 0 {
		return 0.0
	}

	if len(trueIDs) != len(resultIDs) {
		return 0.0
	}

	// 截取前K个结果
	truncatedResultIDs := make([][]int64, len(resultIDs))
	for i := range resultIDs {
		if len(resultIDs[i]) <= k {
			truncatedResultIDs[i] = resultIDs[i]
		} else {
			truncatedResultIDs[i] = resultIDs[i][:k]
		}
	}

	return GetRecallValue(trueIDs, truncatedResultIDs)
}

// GetIntersectionSize 计算两个ID列表的交集大小
func GetIntersectionSize(list1, list2 []int64) int {
	if len(list1) == 0 || len(list2) == 0 {
		return 0
	}

	set := make(map[int64]bool, len(list1))
	for _, id := range list1 {
		set[id] = true
	}

	count := 0
	for _, id := range list2 {
		if set[id] {
			count++
		}
	}

	return count
}

// CalculateSingleRecall 计算单个查询的召回率
func CalculateSingleRecall(trueIDs, resultIDs []int64) float64 {
	if len(trueIDs) == 0 || len(resultIDs) == 0 {
		return 0.0
	}

	hitCount := GetIntersectionSize(trueIDs, resultIDs)
	return float64(hitCount) / float64(len(trueIDs))
}

// GetMinRecall 获取最小召回率（用于评估最坏情况）
func GetMinRecall(trueIDs [][]int64, resultIDs [][]int64) float64 {
	if len(trueIDs) == 0 || len(resultIDs) == 0 {
		return 0.0
	}

	if len(trueIDs) != len(resultIDs) {
		return 0.0
	}

	minRecall := 1.0
	for i := 0; i < len(trueIDs); i++ {
		recall := CalculateSingleRecall(trueIDs[i], resultIDs[i])
		if recall < minRecall {
			minRecall = recall
		}
	}

	return minRecall
}

// GetRecallStats 获取召回率统计信息
// 返回: 平均值, 最小值, 最大值, 标准差
func GetRecallStats(trueIDs [][]int64, resultIDs [][]int64) (avg, min, max, std float64) {
	if len(trueIDs) == 0 || len(resultIDs) == 0 || len(trueIDs) != len(resultIDs) {
		return 0.0, 0.0, 0.0, 0.0
	}

	numQueries := len(trueIDs)
	recalls := make([]float64, numQueries)

	for i := 0; i < numQueries; i++ {
		recalls[i] = CalculateSingleRecall(trueIDs[i], resultIDs[i])
	}

	// 计算平均值
	sum := 0.0
	for _, r := range recalls {
		sum += r
	}
	avg = sum / float64(numQueries)

	// 计算最小值和最大值
	min = 1.0
	max = 0.0
	for _, r := range recalls {
		if r < min {
			min = r
		}
		if r > max {
			max = r
		}
	}

	// 计算标准差
	variance := 0.0
	for _, r := range recalls {
		diff := r - avg
		variance += diff * diff
	}
	variance /= float64(numQueries)
	std = math.Sqrt(variance)

	return avg, min, max, std
}
