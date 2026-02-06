package optimizer

import (
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/parser"
	"github.com/stretchr/testify/assert"
)

func TestNewIndexCostEstimator(t *testing.T) {
	estimator := NewIndexCostEstimator()
	assert.NotNil(t, estimator)
	assert.Equal(t, 0.1, estimator.ioCostFactor)
	assert.Equal(t, 0.01, estimator.cpuCostFactor)
	assert.Equal(t, 0.001, estimator.memoryCostFactor)
}

func TestEstimateIndexScanCostWithStats(t *testing.T) {
	estimator := NewIndexCostEstimator()

	// 测试1: 有统计信息的索引
	stats := &HypotheticalIndexStats{
		NDV:           100,
		Selectivity:   0.1,
		EstimatedSize: 102400, // 100KB
		NullFraction:  0.05,
		Correlation:   0.1,
	}

	cost := estimator.EstimateIndexScanCost(stats, 10000, 0.1)
	assert.Greater(t, cost, 0.0)
	assert.Less(t, cost, 500.0) // 应该比全表扫描便宜

	// 测试2: 没有统计信息
	cost2 := estimator.EstimateIndexScanCost(nil, 10000, 0.1)
	assert.Greater(t, cost2, cost) // 没有统计信息应该更贵

	// 测试3: 高选择性（扫描更少行）
	cost3 := estimator.EstimateIndexScanCost(stats, 10000, 0.01)
	assert.Less(t, cost3, cost) // 高选择性更便宜
}

func TestEstimateIndexLookupCost(t *testing.T) {
	estimator := NewIndexCostEstimator()

	stats := &HypotheticalIndexStats{
		NDV:           1000,
		Selectivity:   0.1,
		EstimatedSize: 51200,
		NullFraction:  0.05,
		Correlation:   0.1,
	}

	cost := estimator.EstimateIndexLookupCost(stats, 10000)
	assert.Greater(t, cost, 0.0)
	assert.Less(t, cost, 200.0) // 查找比扫描更便宜

	// 测试没有统计信息
	cost2 := estimator.EstimateIndexLookupCost(nil, 10000)
	assert.Greater(t, cost2, cost)
}

func TestEstimateIndexRangeScanCost(t *testing.T) {
	estimator := NewIndexCostEstimator()

	stats := &HypotheticalIndexStats{
		NDV:           1000,
		Selectivity:   0.2,
		EstimatedSize: 51200,
		NullFraction:  0.05,
		Correlation:   0.1,
	}

	cost := estimator.EstimateIndexRangeScanCost(stats, 10000, 0.2)
	assert.Greater(t, cost, 0.0)
	assert.Less(t, cost, 500.0)
}

func TestEstimateMultiColumnIndexCost(t *testing.T) {
	estimator := NewIndexCostEstimator()

	stats := &HypotheticalIndexStats{
		NDV:           500,
		Selectivity:   0.05,
		EstimatedSize: 102400,
		NullFraction:  0.05,
		Correlation:   0.1,
	}

	// 多列索引的前缀选择性
	leadingSelectivity := []float64{0.1, 0.5} // user_id选择性0.1, product_id选择性0.5

	cost := estimator.EstimateMultiColumnIndexCost(stats, 10000, 0.05, leadingSelectivity)
	assert.Greater(t, cost, 0.0)

	// 测试空前缀选择性
	cost2 := estimator.EstimateMultiColumnIndexCost(stats, 10000, 0.05, []float64{})
	assert.Greater(t, cost2, 0.0)
}

func TestEstimateSelectivityFromNDV(t *testing.T) {
	estimator := NewIndexCostEstimator()

	// 测试1: 高NDV（高选择性）
	selectivity := estimator.EstimateSelectivityFromNDV(1000, 10000)
	assert.Equal(t, 0.001, selectivity)

	// 测试2: 低NDV（低选择性）
	selectivity2 := estimator.EstimateSelectivityFromNDV(10, 10000)
	assert.Equal(t, 0.1, selectivity2)

	// 测试3: NDV > 行数
	selectivity3 := estimator.EstimateSelectivityFromNDV(20000, 10000)
	assert.Equal(t, 0.1, selectivity3) // 限制为0.1

	// 测试4: 边界条件
	selectivity4 := estimator.EstimateSelectivityFromNDV(0, 10000)
	assert.Equal(t, 1.0, selectivity4)

	selectivity5 := estimator.EstimateSelectivityFromNDV(100, 0)
	assert.Equal(t, 1.0, selectivity5)
}

func TestEstimateRangeSelectivity(t *testing.T) {
	estimator := NewIndexCostEstimator()

	// 测试 < 条件
	condition1 := &parser.Expression{Operator: "<"}
	selectivity1 := estimator.EstimateRangeSelectivity(1000, 10000, condition1)
	assert.Greater(t, selectivity1, 0.0)
	assert.Less(t, selectivity1, 1.0)

	// 测试 <= 条件
	condition2 := &parser.Expression{Operator: "<="}
	selectivity2 := estimator.EstimateRangeSelectivity(1000, 10000, condition2)
	assert.Greater(t, selectivity2, selectivity1) // <= 应该比 < 选择性更高

	// 测试 BETWEEN 条件
	condition3 := &parser.Expression{Operator: "BETWEEN"}
	selectivity3 := estimator.EstimateRangeSelectivity(1000, 10000, condition3)
	assert.Greater(t, selectivity3, selectivity2) // BETWEEN 应该有最低选择性

	// 测试边界条件
	condition4 := &parser.Expression{Operator: "UNKNOWN"}
	selectivity4 := estimator.EstimateRangeSelectivity(1000, 10000, condition4)
	assert.Greater(t, selectivity4, 0.0)
}

func TestEstimateJoinCost(t *testing.T) {
	estimator := NewIndexCostEstimator()

	// 测试1: 有索引的连接
	innerIndexStats := &HypotheticalIndexStats{
		NDV:           5000,
		Selectivity:   0.1,
		EstimatedSize: 51200,
		NullFraction:  0.05,
		Correlation:   0.1,
	}

	cost := estimator.EstimateJoinCost(1000, 10000, innerIndexStats, 0.1)
	assert.Greater(t, cost, 0.0)
	assert.Less(t, cost, 20000.0) // 比嵌套循环便宜

	// 测试2: 没有索引的连接
	cost2 := estimator.EstimateJoinCost(1000, 10000, nil, 0.1)
	assert.Greater(t, cost2, cost) // 没有索引应该更贵

	// 测试3: 高选择性连接
	cost3 := estimator.EstimateJoinCost(1000, 10000, innerIndexStats, 0.01)
	assert.Less(t, cost3, cost) // 高选择性更便宜
}

func TestEstimateAggregateCost(t *testing.T) {
	estimator := NewIndexCostEstimator()

	// 测试1: 有覆盖索引
	indexStats := &HypotheticalIndexStats{
		NDV:           1000,
		Selectivity:   0.1,
		EstimatedSize: 51200,
		NullFraction:  0.05,
		Correlation:   0.1,
	}
	indexColumns := []string{"category", "count"}

	cost1 := estimator.EstimateAggregateCost(10000, []string{"category"}, indexStats, indexColumns)
	assert.Greater(t, cost1, 0.0)

	// 测试2: 没有索引
	cost2 := estimator.EstimateAggregateCost(10000, []string{"category"}, nil, nil)
	assert.Greater(t, cost2, cost1) // 没有索引应该更贵

	// 测试3: 多列GROUP BY
	cost3 := estimator.EstimateAggregateCost(10000, []string{"category", "status"}, indexStats, indexColumns)
	assert.Greater(t, cost3, cost1) // 多列GROUP BY可能更贵
}

func TestEstimateBenefit(t *testing.T) {
	estimator := NewIndexCostEstimator()

	// 测试1: 正收益
	benefit1 := estimator.EstimateBenefit(1000.0, 500.0)
	assert.Equal(t, 0.5, benefit1) // (1000-500)/1000 = 0.5

	// 测试2: 零收益
	benefit2 := estimator.EstimateBenefit(500.0, 500.0)
	assert.Equal(t, 0.0, benefit2)

	// 测试3: 负收益（索引更差）
	benefit3 := estimator.EstimateBenefit(500.0, 1000.0)
	assert.Equal(t, 0.0, benefit3) // 限制为0

	// 测试4: 完全消除成本
	benefit4 := estimator.EstimateBenefit(1000.0, 0.0)
	assert.Equal(t, 1.0, benefit4)

	// 测试5: 边界条件
	benefit5 := estimator.EstimateBenefit(0.0, 100.0)
	assert.Equal(t, 0.0, benefit5)
}

func TestAdjustCostForSkew(t *testing.T) {
	estimator := NewIndexCostEstimator()

	baseCost := 100.0

	// 测试1: 无偏斜
	cost1 := estimator.AdjustCostForSkew(baseCost, 0.0)
	assert.Equal(t, baseCost, cost1)

	// 测试2: 中等偏斜
	cost2 := estimator.AdjustCostForSkew(baseCost, 0.5)
	assert.Greater(t, cost2, baseCost)
	assert.Equal(t, 150.0, cost2) // 100 * (1 + 0.5) = 150

	// 测试3: 严重偏斜
	cost3 := estimator.AdjustCostForSkew(baseCost, 1.0)
	assert.Greater(t, cost3, cost2)
	assert.Equal(t, 200.0, cost3) // 100 * (1 + 1.0) = 200
}

func TestEstimateCardinality(t *testing.T) {
	estimator := NewIndexCostEstimator()

	// 测试1: 正常情况
	cardinality1 := estimator.EstimateCardinality(1000, 0.1, 10000)
	assert.Equal(t, int64(1000), cardinality1)

	// 测试2: 选择性超过NDV限制
	cardinality2 := estimator.EstimateCardinality(100, 0.5, 10000)
	assert.Equal(t, int64(100), cardinality2) // 不能超过NDV

	// 测试3: 选择性超过行数
	cardinality3 := estimator.EstimateCardinality(1000, 2.0, 10000)
	assert.Equal(t, int64(10000), cardinality3) // 不能超过总行数

	// 测试4: 边界条件
	cardinality4 := estimator.EstimateCardinality(0, 0.1, 10000)
	assert.Equal(t, int64(10000), cardinality4)
}
