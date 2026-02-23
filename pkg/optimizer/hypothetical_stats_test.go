package optimizer

import (
	"fmt"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/optimizer/statistics"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// TestNewHypotheticalStatsGenerator 测试创建统计生成器
func TestNewHypotheticalStatsGenerator(t *testing.T) {
	// 创建一个 mock estimator
	estimator := &mockCardinalityEstimator{}
	generator := NewHypotheticalStatsGenerator(estimator)

	if generator == nil {
		t.Fatal("NewHypotheticalStatsGenerator returned nil")
	}
	if generator.estimator != estimator {
		t.Error("Estimator not set correctly")
	}
}

// TestGenerateStatsWithTableStats 测试使用表统计信息生成统计
func TestGenerateStatsWithTableStats(t *testing.T) {
	estimator := &mockCardinalityEstimator{
		stats: createMockTableStats("test_table", 10000, map[string]*statistics.ColumnStatistics{
			"id": {
				Name:          "id",
				DataType:      "INT",
				DistinctCount: 10000,
				NullCount:     0,
				AvgWidth:      4,
				MinValue:      1,
				MaxValue:      10000,
			},
			"status": {
				Name:          "status",
				DataType:      "VARCHAR",
				DistinctCount: 5,
				NullCount:     100,
				AvgWidth:      10,
				MinValue:      "active",
				MaxValue:      "inactive",
			},
		}),
	}

	generator := NewHypotheticalStatsGenerator(estimator)

	// 测试单列索引
	stats, err := generator.GenerateStats("test_table", []string{"id"}, true)
	if err != nil {
		t.Fatalf("GenerateStats failed: %v", err)
	}

	if stats.NDV != 10000 {
		t.Errorf("Expected NDV 10000, got %d", stats.NDV)
	}
	if stats.Selectivity <= 0 || stats.Selectivity > 0.5 {
		t.Errorf("Expected selectivity in (0, 0.5], got %f", stats.Selectivity)
	}
	if stats.EstimatedSize <= 0 {
		t.Errorf("Expected positive estimated size, got %d", stats.EstimatedSize)
	}
	if stats.NullFraction != 0 {
		t.Errorf("Expected null fraction 0, got %f", stats.NullFraction)
	}

	// 测试非唯一索引
	stats2, err := generator.GenerateStats("test_table", []string{"status"}, false)
	if err != nil {
		t.Fatalf("GenerateStats failed: %v", err)
	}

	if stats2.NDV != 5 {
		t.Errorf("Expected NDV 5, got %d", stats2.NDV)
	}
	if stats2.NullFraction <= 0 || stats2.NullFraction > 1 {
		t.Errorf("Expected null fraction in (0, 1], got %f", stats2.NullFraction)
	}
}

// TestGenerateStatsMultiColumn 测试多列索引
func TestGenerateStatsMultiColumn(t *testing.T) {
	estimator := &mockCardinalityEstimator{
		stats: createMockTableStats("test_table", 10000, map[string]*statistics.ColumnStatistics{
			"user_id": {
				Name:          "user_id",
				DataType:      "INT",
				DistinctCount: 1000,
				NullCount:     0,
				AvgWidth:      4,
			},
			"status": {
				Name:          "status",
				DataType:      "VARCHAR",
				DistinctCount: 5,
				NullCount:     100,
				AvgWidth:      10,
			},
		}),
	}

	generator := NewHypotheticalStatsGenerator(estimator)

	stats, err := generator.GenerateStats("test_table", []string{"user_id", "status"}, false)
	if err != nil {
		t.Fatalf("GenerateStats failed: %v", err)
	}

	// 多列索引的 NDV 应该小于或等于最小单列 NDV
	minNDV := int64(5) // status 的 NDV
	if stats.NDV > minNDV {
		t.Errorf("Expected NDV <= %d, got %d", minNDV, stats.NDV)
	}
	if stats.Correlation <= 0 || stats.Correlation > 1 {
		t.Errorf("Expected correlation in (0, 1], got %f", stats.Correlation)
	}
}

// TestGenerateStatsWithoutTableStats 测试没有表统计信息的情况
func TestGenerateStatsWithoutTableStats(t *testing.T) {
	estimator := &mockCardinalityEstimator{} // 没有统计信息
	generator := NewHypotheticalStatsGenerator(estimator)

	stats, err := generator.GenerateStats("test_table", []string{"id"}, true)
	if err != nil {
		t.Fatalf("GenerateStats failed: %v", err)
	}

	if stats == nil {
		t.Fatal("Expected stats to be generated")
	}
	if stats.NDV <= 0 {
		t.Errorf("Expected positive NDV, got %d", stats.NDV)
	}
	if stats.Selectivity <= 0 {
		t.Errorf("Expected positive selectivity, got %f", stats.Selectivity)
	}
}

// TestEstimateSelectivity 测试选择性估算
func TestEstimateSelectivity(t *testing.T) {
	testCases := []struct {
		name           string
		ndv            int64
		rowCount       int64
		minSelectivity float64
		maxSelectivity float64
	}{
		{"High NDV", 10000, 100000, 0.0001, 0.5},
		{"Low NDV", 10, 1000, 0.1, 0.5},
		{"Unique", 1000, 1000, 0.001, 0.5},
		{"Large table", 100000, 1000000, 0.0001, 0.5},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			estimator := &mockCardinalityEstimator{}
			generator := NewHypotheticalStatsGenerator(estimator)

			sel := generator.estimateSelectivity(tc.ndv, tc.rowCount)

			if sel < tc.minSelectivity || sel > tc.maxSelectivity {
				t.Errorf("Selectivity %f out of range [%f, %f]", sel, tc.minSelectivity, tc.maxSelectivity)
			}
		})
	}
}

// TestEstimateIndexSizeForStats 测试索引大小估算
func TestEstimateIndexSizeForStats(t *testing.T) {
	stats := &statistics.TableStatistics{
		RowCount: 10000,
		ColumnStats: map[string]*statistics.ColumnStatistics{
			"id": {
				Name:     "id",
				DataType: "INT",
				AvgWidth: 4,
			},
			"name": {
				Name:     "name",
				DataType: "VARCHAR",
				AvgWidth: 20,
			},
		},
	}

	testCases := []struct {
		name    string
		columns []string
		minSize int64
		maxSize int64
	}{
		{"Single int column", []string{"id"}, 200000, 400000},
		{"Single varchar column", []string{"name"}, 400000, 800000},
		{"Two columns", []string{"id", "name"}, 500000, 800000},
	}

	estimator := &mockCardinalityEstimator{}
	generator := NewHypotheticalStatsGenerator(estimator)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			size := generator.estimateIndexSize(stats, tc.columns)

			if size < tc.minSize || size > tc.maxSize {
				t.Errorf("Size %d out of range [%d, %d]", size, tc.minSize, tc.maxSize)
			}
		})
	}
}

// TestEstimateIndexScanCost 测试索引扫描成本
func TestEstimateIndexScanCost(t *testing.T) {
	estimator := &mockCardinalityEstimator{}
	generator := NewHypotheticalStatsGenerator(estimator)

	stats := &HypotheticalIndexStats{
		NDV:         1000,
		Selectivity: 0.01,
	}

	rowCount := int64(10000)
	cost := generator.EstimateIndexScanCost(stats, rowCount)

	if cost <= 0 {
		t.Errorf("Expected positive cost, got %f", cost)
	}

	// 成本应该小于全表扫描
	fullScanCost := float64(rowCount)
	if cost >= fullScanCost {
		t.Errorf("Expected cost %f < full scan cost %f", cost, fullScanCost)
	}
}

// TestCompareWithFullScan 测试与全表扫描比较
func TestCompareWithFullScan(t *testing.T) {
	estimator := &mockCardinalityEstimator{}
	generator := NewHypotheticalStatsGenerator(estimator)

	testCases := []struct {
		name            string
		selectivity     float64
		ndv             int64
		expectedBenefit bool
	}{
		{"Highly selective", 0.001, 1000, true},
		{"Medium selective", 0.1, 1000, true}, // 10% 选择性，1000 NDV，索引仍有显著收益
		{"Low selective", 0.8, 10, false},     // 80% 选择性，索引无收益
	}

	rowCount := int64(10000)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			stats := &HypotheticalIndexStats{
				NDV:         tc.ndv,
				Selectivity: tc.selectivity,
			}

			_, benefit := generator.CompareWithFullScan(stats, rowCount)

			if benefit != tc.expectedBenefit {
				t.Errorf("Expected benefit %v, got %v", tc.expectedBenefit, benefit)
			}
		})
	}
}

// TestEstimateIndexJoinCost 测试索引 JOIN 成本
func TestEstimateIndexJoinCost(t *testing.T) {
	estimator := &mockCardinalityEstimator{}
	generator := NewHypotheticalStatsGenerator(estimator)

	leftStats := &HypotheticalIndexStats{
		NDV:         1000,
		Selectivity: 0.01,
	}
	rightStats := &HypotheticalIndexStats{
		NDV:         500,
		Selectivity: 0.02,
	}

	cost := generator.EstimateIndexJoinCost(leftStats, rightStats, 10000, 10000)

	if cost <= 0 {
		t.Errorf("Expected positive cost, got %f", cost)
	}
}

// TestHeuristicNDV 测试启发式 NDV 估算
func TestHeuristicNDV(t *testing.T) {
	estimator := &mockCardinalityEstimator{}
	generator := NewHypotheticalStatsGenerator(estimator)

	testCases := []struct {
		rowCount int64
		expected int64
	}{
		{500, 500},       // 小表
		{5000, 500},      // 中等表
		{50000, 5000},    // 中等表
		{500000, 5000},   // 大表
		{5000000, 50000}, // 超大表
	}

	for _, tc := range testCases {
		ndv := generator.heuristicNDV(tc.rowCount)
		// 允许一定的误差
		if ndv < tc.expected/2 || ndv > tc.expected*2 {
			t.Errorf("For rowCount %d, expected NDV around %d, got %d", tc.rowCount, tc.expected, ndv)
		}
	}
}

// TestEstimateCorrelation 测试相关性估算
func TestEstimateCorrelation(t *testing.T) {
	estimator := &mockCardinalityEstimator{}
	generator := NewHypotheticalStatsGenerator(estimator)

	// 单列索引
	correlation := generator.estimateCorrelation(nil, []string{"id"})
	if correlation != 1.0 {
		t.Errorf("Expected correlation 1.0 for single column, got %f", correlation)
	}

	// 多列索引
	stats := &statistics.TableStatistics{
		RowCount: 10000,
		ColumnStats: map[string]*statistics.ColumnStatistics{
			"user_id": {DistinctCount: 1000},
			"status":  {DistinctCount: 10},
		},
	}
	correlation = generator.estimateCorrelation(stats, []string{"user_id", "status"})
	if correlation <= 0.5 || correlation > 1.0 {
		t.Errorf("Expected correlation in (0.5, 1.0], got %f", correlation)
	}
}

// TestUpdateTableInfo 测试更新表信息
func TestUpdateTableInfo(t *testing.T) {
	estimator := &mockCardinalityEstimator{}
	generator := NewHypotheticalStatsGenerator(estimator)

	tableInfo := &domain.TableInfo{
		Name:    "test_table",
		Columns: []domain.ColumnInfo{{Name: "id", Type: "INT"}},
	}

	generator.UpdateTableInfo("test_table", tableInfo)

	if len(generator.tableInfo) != 1 {
		t.Errorf("Expected 1 table info, got %d", len(generator.tableInfo))
	}

	info, exists := generator.tableInfo["test_table"]
	if !exists {
		t.Error("Table info not found")
	}
	if info.Name != "test_table" {
		t.Errorf("Expected table name 'test_table', got %s", info.Name)
	}
}

// === Mock implementations ===

type mockCardinalityEstimator struct {
	stats *statistics.TableStatistics
}

func (m *mockCardinalityEstimator) EstimateTableScan(tableName string) int64 {
	return 10000
}

func (m *mockCardinalityEstimator) EstimateFilter(tableName string, filters []domain.Filter) int64 {
	return 1000
}

func (m *mockCardinalityEstimator) GetStatistics(tableName string) (*statistics.TableStatistics, error) {
	if m.stats == nil {
		return nil, fmt.Errorf("statistics not found")
	}
	return m.stats, nil
}

// createMockTableStats 创建模拟表统计信息
func createMockTableStats(tableName string, rowCount int64, colStats map[string]*statistics.ColumnStatistics) *statistics.TableStatistics {
	return &statistics.TableStatistics{
		Name:              tableName,
		RowCount:          rowCount,
		ColumnStats:       colStats,
		Histograms:        make(map[string]*statistics.Histogram),
		EstimatedRowCount: rowCount,
	}
}
