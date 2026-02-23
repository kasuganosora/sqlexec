package optimizer

import (
	"fmt"
	"strings"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/optimizer/statistics"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// mockEstimator 模拟的估算器
type mockEstimator struct {
	stats map[string]*statistics.TableStatistics
}

func newMockEstimator() *mockEstimator {
	return &mockEstimator{
		stats: make(map[string]*statistics.TableStatistics),
	}
}

func (m *mockEstimator) EstimateTableScan(tableName string) int64 {
	if stats, exists := m.stats[tableName]; exists {
		return stats.RowCount
	}
	return 10000
}

func (m *mockEstimator) EstimateFilter(tableName string, filters []domain.Filter) int64 {
	return m.EstimateTableScan(tableName)
}

func (m *mockEstimator) GetStatistics(tableName string) (*statistics.TableStatistics, error) {
	if stats, exists := m.stats[tableName]; exists {
		return stats, nil
	}
	return nil, fmt.Errorf("statistics not found for table: %s", tableName)
}

func (m *mockEstimator) setStatistics(tableName string, stats *statistics.TableStatistics) {
	m.stats[tableName] = stats
}

// createMockTableStatistics 创建模拟的表统计信息
func createMockTableStatistics() *statistics.TableStatistics {
	return &statistics.TableStatistics{
		Name:              "test_table",
		RowCount:          100000,
		EstimatedRowCount: 100000,
		ColumnStats: map[string]*statistics.ColumnStatistics{
			"id": {
				Name:          "id",
				DistinctCount: 100000,
				NullCount:     0,
				AvgWidth:      8,
				MinValue:      int64(1),
				MaxValue:      int64(100000),
			},
			"status": {
				Name:          "status",
				DistinctCount: 5,
				NullCount:     0,
				AvgWidth:      10,
				MinValue:      "active",
				MaxValue:      "deleted",
			},
			"age": {
				Name:          "age",
				DistinctCount: 100,
				NullCount:     1000,
				AvgWidth:      4,
				MinValue:      int64(18),
				MaxValue:      int64(80),
			},
		},
		Histograms: map[string]*statistics.Histogram{
			"id": {
				Type:        statistics.EquiWidthHistogram,
				MinValue:    int64(1),
				MaxValue:    int64(100000),
				BucketCount: 10,
				NDV:         100000,
				NullCount:   0,
				Buckets: []*statistics.HistogramBucket{
					{LowerBound: int64(1), UpperBound: int64(10000), Count: 10000, NDV: 10000},
					{LowerBound: int64(10001), UpperBound: int64(20000), Count: 10000, NDV: 10000},
					{LowerBound: int64(20001), UpperBound: int64(30000), Count: 10000, NDV: 10000},
					{LowerBound: int64(30001), UpperBound: int64(40000), Count: 10000, NDV: 10000},
					{LowerBound: int64(40001), UpperBound: int64(50000), Count: 10000, NDV: 10000},
					{LowerBound: int64(50001), UpperBound: int64(60000), Count: 10000, NDV: 10000},
					{LowerBound: int64(60001), UpperBound: int64(70000), Count: 10000, NDV: 10000},
					{LowerBound: int64(70001), UpperBound: int64(80000), Count: 10000, NDV: 10000},
					{LowerBound: int64(80001), UpperBound: int64(90000), Count: 10000, NDV: 10000},
					{LowerBound: int64(90001), UpperBound: int64(100000), Count: 10000, NDV: 10000},
				},
			},
			"status": {
				Type:        statistics.FrequencyHistogram,
				BucketCount: 3,
				NDV:         5,
				NullCount:   0,
				Buckets: []*statistics.HistogramBucket{
					{LowerBound: "active", UpperBound: "active", Count: 50000, NDV: 1},
					{LowerBound: "pending", UpperBound: "pending", Count: 30000, NDV: 1},
					{LowerBound: "inactive", UpperBound: "deleted", Count: 20000, NDV: 3},
				},
			},
		},
	}
}

// TestNewStatisticsIntegrator 测试创建统计信息集成器
func TestNewStatisticsIntegrator(t *testing.T) {
	estimator := newMockEstimator()
	integrator := NewStatisticsIntegrator(estimator)

	if integrator == nil {
		t.Fatal("Expected non-nil integrator")
	}

	if integrator.estimator == nil {
		t.Error("Expected non-nil estimator")
	}
}

// TestGetRealStatistics 测试获取真实统计信息
func TestGetRealStatistics(t *testing.T) {
	estimator := newMockEstimator()
	stats := createMockTableStatistics()
	estimator.setStatistics("test_table", stats)

	integrator := NewStatisticsIntegrator(estimator)

	// 第一次获取
	retrievedStats, err := integrator.GetRealStatistics("test_table")
	if err != nil {
		t.Fatalf("Failed to get statistics: %v", err)
	}

	if retrievedStats == nil {
		t.Fatal("Expected non-nil statistics")
	}

	if retrievedStats.RowCount != 100000 {
		t.Errorf("Expected RowCount=100000, got %d", retrievedStats.RowCount)
	}

	// 第二次获取（应该从缓存）
	retrievedStats2, err := integrator.GetRealStatistics("test_table")
	if err != nil {
		t.Fatalf("Failed to get statistics from cache: %v", err)
	}

	if retrievedStats != retrievedStats2 {
		t.Error("Expected same instance from cache")
	}

	// 获取不存在的表
	_, err = integrator.GetRealStatistics("non_existent_table")
	if err == nil {
		t.Error("Expected error for non-existent table")
	}
}

// TestGetHistogram 测试获取直方图
func TestGetHistogram(t *testing.T) {
	estimator := newMockEstimator()
	stats := createMockTableStatistics()
	estimator.setStatistics("test_table", stats)

	integrator := NewStatisticsIntegrator(estimator)

	// 获取存在的直方图
	histogram, err := integrator.GetHistogram("test_table", "id")
	if err != nil {
		t.Fatalf("Failed to get histogram: %v", err)
	}

	if histogram == nil {
		t.Fatal("Expected non-nil histogram")
	}

	if histogram.NDV != 100000 {
		t.Errorf("Expected NDV=100000, got %d", histogram.NDV)
	}

	if len(histogram.Buckets) != 10 {
		t.Errorf("Expected 10 buckets, got %d", len(histogram.Buckets))
	}

	// 获取不存在的直方图
	_, err = integrator.GetHistogram("test_table", "non_existent_column")
	if err == nil {
		t.Error("Expected error for non-existent column")
	}
}

// TestEstimateNDVFromRealStats 测试从真实统计信息估算 NDV
func TestEstimateNDVFromRealStats(t *testing.T) {
	estimator := newMockEstimator()
	stats := createMockTableStatistics()
	estimator.setStatistics("test_table", stats)

	integrator := NewStatisticsIntegrator(estimator)

	// 测试有直方图的列
	ndv, err := integrator.EstimateNDVFromRealStats("test_table", "id")
	if err != nil {
		t.Fatalf("Failed to estimate NDV: %v", err)
	}

	if ndv != 100000.0 {
		t.Errorf("Expected NDV=100000.0, got %f", ndv)
	}

	// 测试只有列统计信息的列
	ndv, err = integrator.EstimateNDVFromRealStats("test_table", "status")
	if err != nil {
		t.Fatalf("Failed to estimate NDV: %v", err)
	}

	if ndv != 5.0 {
		t.Errorf("Expected NDV=5.0, got %f", ndv)
	}
}

// TestEstimateNDVFromRealStatsMultiColumn 测试多列 NDV 估算
func TestEstimateNDVFromRealStatsMultiColumn(t *testing.T) {
	estimator := newMockEstimator()
	stats := createMockTableStatistics()
	estimator.setStatistics("test_table", stats)

	integrator := NewStatisticsIntegrator(estimator)

	// 测试单列
	ndv, err := integrator.EstimateNDVFromRealStatsMultiColumn("test_table", []string{"id"})
	if err != nil {
		t.Fatalf("Failed to estimate NDV: %v", err)
	}

	if ndv != 100000.0 {
		t.Errorf("Expected NDV=100000.0, got %f", ndv)
	}

	// 测试多列
	ndv, err = integrator.EstimateNDVFromRealStatsMultiColumn("test_table", []string{"id", "status"})
	if err != nil {
		t.Fatalf("Failed to estimate NDV: %v", err)
	}

	// 多列的 NDV 应该小于单列最小 NDV
	if ndv >= 5.0 {
		t.Errorf("Expected NDV < 5.0 for multi-column index, got %f", ndv)
	}

	// 测试空列
	_, err = integrator.EstimateNDVFromRealStatsMultiColumn("test_table", []string{})
	if err == nil {
		t.Error("Expected error for empty columns")
	}
}

// TestEstimateSelectivityFromRealStats 测试从真实统计信息估算选择性
func TestEstimateSelectivityFromRealStats(t *testing.T) {
	estimator := newMockEstimator()
	stats := createMockTableStatistics()
	estimator.setStatistics("test_table", stats)

	integrator := NewStatisticsIntegrator(estimator)

	// 测试有直方图的列
	sel, err := integrator.EstimateSelectivityFromRealStats("test_table", "id", nil)
	if err != nil {
		t.Fatalf("Failed to estimate selectivity: %v", err)
	}

	// 选择性应该在合理范围内
	if sel < 0.0001 || sel > 0.5 {
		t.Errorf("Expected selectivity in [0.0001, 0.5], got %f", sel)
	}
}

// TestEstimateNullFractionFromRealStats 测试估算 NULL 比例
func TestEstimateNullFractionFromRealStats(t *testing.T) {
	estimator := newMockEstimator()
	stats := createMockTableStatistics()
	estimator.setStatistics("test_table", stats)

	integrator := NewStatisticsIntegrator(estimator)

	// 测试没有 NULL 的列
	nullFrac, err := integrator.EstimateNullFractionFromRealStats("test_table", "id")
	if err != nil {
		t.Fatalf("Failed to estimate null fraction: %v", err)
	}

	if nullFrac != 0.0 {
		t.Errorf("Expected null fraction=0.0, got %f", nullFrac)
	}

	// 测试有 NULL 的列
	nullFrac, err = integrator.EstimateNullFractionFromRealStats("test_table", "age")
	if err != nil {
		t.Fatalf("Failed to estimate null fraction: %v", err)
	}

	expectedNullFrac := float64(1000) / float64(100000)
	if nullFrac != expectedNullFrac {
		t.Errorf("Expected null fraction=%f, got %f", expectedNullFrac, nullFrac)
	}
}

// TestEstimateCorrelationFromRealStats 测试估算相关性
func TestEstimateCorrelationFromRealStats(t *testing.T) {
	estimator := newMockEstimator()
	stats := createMockTableStatistics()
	estimator.setStatistics("test_table", stats)

	integrator := NewStatisticsIntegrator(estimator)

	// 测试单列索引
	corr, err := integrator.EstimateCorrelationFromRealStats("test_table", []string{"id"})
	if err != nil {
		t.Fatalf("Failed to estimate correlation: %v", err)
	}

	if corr != 1.0 {
		t.Errorf("Expected correlation=1.0 for single column, got %f", corr)
	}

	// 测试双列索引
	corr, err = integrator.EstimateCorrelationFromRealStats("test_table", []string{"id", "status"})
	if err != nil {
		t.Fatalf("Failed to estimate correlation: %v", err)
	}

	// 相关性应该在 [0.5, 1.0] 范围内
	if corr < 0.5 || corr > 1.0 {
		t.Errorf("Expected correlation in [0.5, 1.0], got %f", corr)
	}
}

// TestEstimateIndexSizeFromRealStats 测试估算索引大小
func TestEstimateIndexSizeFromRealStats(t *testing.T) {
	estimator := newMockEstimator()
	stats := createMockTableStatistics()
	estimator.setStatistics("test_table", stats)

	integrator := NewStatisticsIntegrator(estimator)

	// 测试单列索引
	size, err := integrator.EstimateIndexSizeFromRealStats("test_table", []string{"id"})
	if err != nil {
		t.Fatalf("Failed to estimate index size: %v", err)
	}

	if size <= 0 {
		t.Errorf("Expected positive index size, got %d", size)
	}

	// 索引大小应该合理（约行数 * (列大小 + 指针) * 填充因子）
	// 100000 行 * (8 + 8) 字节 / 0.75 * 1.2 ≈ 2.5MB
	expectedMinSize := int64(100000 * 16 * 1.2 / 0.75)
	expectedMaxSize := expectedMinSize * 2

	if size < expectedMinSize || size > expectedMaxSize {
		t.Errorf("Expected index size in [%d, %d], got %d", expectedMinSize, expectedMaxSize, size)
	}
}

// TestGenerateIndexStatsFromRealStats 测试从真实统计信息生成虚拟索引统计
func TestGenerateIndexStatsFromRealStats(t *testing.T) {
	estimator := newMockEstimator()
	stats := createMockTableStatistics()
	estimator.setStatistics("test_table", stats)

	integrator := NewStatisticsIntegrator(estimator)

	// 测试普通索引
	indexStats, err := integrator.GenerateIndexStatsFromRealStats("test_table", []string{"id"}, false)
	if err != nil {
		t.Fatalf("Failed to generate index stats: %v", err)
	}

	if indexStats == nil {
		t.Fatal("Expected non-nil index stats")
	}

	if indexStats.NDV != 100000 {
		t.Errorf("Expected NDV=100000, got %d", indexStats.NDV)
	}

	if indexStats.Selectivity <= 0 || indexStats.Selectivity > 0.5 {
		t.Errorf("Expected selectivity in (0, 0.5], got %f", indexStats.Selectivity)
	}

	if indexStats.EstimatedSize <= 0 {
		t.Errorf("Expected positive estimated size, got %d", indexStats.EstimatedSize)
	}

	// 测试唯一索引
	uniqueStats, err := integrator.GenerateIndexStatsFromRealStats("test_table", []string{"id"}, true)
	if err != nil {
		t.Fatalf("Failed to generate unique index stats: %v", err)
	}

	if uniqueStats.NDV != 100000 {
		t.Errorf("Expected NDV=100000 for unique index, got %d", uniqueStats.NDV)
	}

	// 测试多列索引
	multiStats, err := integrator.GenerateIndexStatsFromRealStats("test_table", []string{"id", "status"}, false)
	if err != nil {
		t.Fatalf("Failed to generate multi-column index stats: %v", err)
	}

	// 多列索引的 NDV 应该小于单列最小 NDV
	if multiStats.NDV >= 5 {
		t.Errorf("Expected NDV < 5 for multi-column index, got %d", multiStats.NDV)
	}
}

// TestClearCache 测试清理缓存
func TestClearCache(t *testing.T) {
	estimator := newMockEstimator()
	stats := createMockTableStatistics()
	estimator.setStatistics("test_table", stats)

	integrator := NewStatisticsIntegrator(estimator)

	// 获取统计信息（应该缓存）
	_, err := integrator.GetRealStatistics("test_table")
	if err != nil {
		t.Fatalf("Failed to get statistics: %v", err)
	}

	// 检查缓存
	if len(integrator.statsCache) != 1 {
		t.Errorf("Expected 1 cached item, got %d", len(integrator.statsCache))
	}

	// 清理缓存
	integrator.ClearCache()

	// 检查缓存是否清理
	if len(integrator.statsCache) != 0 {
		t.Errorf("Expected 0 cached items after clear, got %d", len(integrator.statsCache))
	}
}

// TestGetStatisticsSummary 测试获取统计信息摘要
func TestGetStatisticsSummary(t *testing.T) {
	estimator := newMockEstimator()
	stats := createMockTableStatistics()
	estimator.setStatistics("test_table", stats)

	integrator := NewStatisticsIntegrator(estimator)

	summary := integrator.GetStatisticsSummary("test_table")

	if summary == "" {
		t.Error("Expected non-empty summary")
	}

	// Verify summary contains key information
	if !strings.Contains(summary, "test_table") {
		t.Error("Expected summary to contain table name")
	}

	if !strings.Contains(summary, "RowCount") {
		t.Error("Expected summary to contain RowCount")
	}

	if !strings.Contains(summary, "100000") {
		t.Error("Expected summary to contain row count value")
	}
}
