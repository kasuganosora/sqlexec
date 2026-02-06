package optimizer

import (
	"fmt"
	"math"

	"github.com/kasuganosora/sqlexec/pkg/optimizer/statistics"
)

// StatisticsIntegrator 统计信息集成器
// 用于将真实统计信息与虚拟索引统计信息集成
type StatisticsIntegrator struct {
	estimator statistics.CardinalityEstimator
	statsCache map[string]*statistics.TableStatistics
}

// NewStatisticsIntegrator 创建统计信息集成器
func NewStatisticsIntegrator(estimator statistics.CardinalityEstimator) *StatisticsIntegrator {
	return &StatisticsIntegrator{
		estimator: estimator,
		statsCache: make(map[string]*statistics.TableStatistics),
	}
}

// GetRealStatistics 获取表的真实统计信息
func (si *StatisticsIntegrator) GetRealStatistics(tableName string) (*statistics.TableStatistics, error) {
	if si.estimator == nil {
		return nil, fmt.Errorf("estimator not available")
	}

	// 先从缓存获取
	if stats, exists := si.statsCache[tableName]; exists {
		return stats, nil
	}

	// 从 estimator 获取
	stats, err := si.estimator.GetStatistics(tableName)
	if err != nil {
		return nil, err
	}

	// 缓存结果
	si.statsCache[tableName] = stats
	return stats, nil
}

// GetHistogram 获取列的直方图
func (si *StatisticsIntegrator) GetHistogram(tableName, columnName string) (*statistics.Histogram, error) {
	stats, err := si.GetRealStatistics(tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to get table statistics: %w", err)
	}

	histogram, exists := stats.Histograms[columnName]
	if !exists {
		return nil, fmt.Errorf("histogram not found for column: %s", columnName)
	}

	return histogram, nil
}

// EstimateNDVFromRealStats 从真实统计信息估算 NDV
func (si *StatisticsIntegrator) EstimateNDVFromRealStats(tableName, columnName string) (float64, error) {
	stats, err := si.GetRealStatistics(tableName)
	if err != nil {
		return 0, fmt.Errorf("failed to get table statistics: %w", err)
	}

	// 优先使用直方图的 NDV
	if histogram, exists := stats.Histograms[columnName]; exists {
		if histogram.NDV > 0 {
			return float64(histogram.NDV), nil
		}
	}

	// 回退到列统计信息
	if colStats, exists := stats.ColumnStats[columnName]; exists {
		if colStats.DistinctCount > 0 {
			return float64(colStats.DistinctCount), nil
		}
	}

	// 使用启发式估算
	return si.heuristicNDV(tableName), nil
}

// EstimateNDVFromRealStatsMultiColumn 从真实统计信息估算多列的 NDV
func (si *StatisticsIntegrator) EstimateNDVFromRealStatsMultiColumn(tableName string, columns []string) (float64, error) {
	if len(columns) == 0 {
		return 0, fmt.Errorf("no columns specified")
	}

	if len(columns) == 1 {
		return si.EstimateNDVFromRealStats(tableName, columns[0])
	}

	// 多列索引的 NDV 估算
	// 使用最小 NDV 的列作为基础，然后应用组合因子
	minNDV := math.MaxFloat64
	hasValidNDV := false

	for _, col := range columns {
		ndv, err := si.EstimateNDVFromRealStats(tableName, col)
		if err == nil && ndv > 0 {
			if ndv < minNDV {
				minNDV = ndv
			}
			hasValidNDV = true
		}
	}

	if !hasValidNDV {
		// 回退到启发式估算
		return si.heuristicNDV(tableName) / float64(len(columns)), nil
	}

	// 多列索引的 NDV 通常低于单列最小 NDV
	// 使用组合因子：随着列数增加，NDV 递减
	comboFactor := 1.0 / math.Pow(2.0, float64(len(columns)-1))
	estimatedNDV := minNDV * comboFactor

	// 确保 NDV 至少为 1
	if estimatedNDV < 1 {
		estimatedNDV = 1
	}

	return estimatedNDV, nil
}

// EstimateSelectivityFromRealStats 从真实统计信息估算选择性
func (si *StatisticsIntegrator) EstimateSelectivityFromRealStats(tableName, columnName string, filter interface{}) (float64, error) {
	histogram, err := si.GetHistogram(tableName, columnName)
	if err != nil {
		// 没有直方图，使用 NDV 估算
		ndv, err := si.EstimateNDVFromRealStats(tableName, columnName)
		if err != nil {
			return 0.1, nil // 默认选择性
		}
		return 1.0 / ndv, nil
	}

	// 使用直方图估算（这里简化，实际应该解析 filter）
	// 假设等值查询
	sel := histogram.EstimateEqualitySelectivity(nil)
	if sel <= 0 {
		return 0.1, nil
	}

	// 限制选择性范围
	if sel < 0.0001 {
		sel = 0.0001
	}
	if sel > 0.5 {
		sel = 0.5
	}

	return sel, nil
}

// EstimateNullFractionFromRealStats 从真实统计信息估算 NULL 比例
func (si *StatisticsIntegrator) EstimateNullFractionFromRealStats(tableName, columnName string) (float64, error) {
	stats, err := si.GetRealStatistics(tableName)
	if err != nil {
		return 0.1, nil // 默认 10% NULL
	}

	// 获取列统计信息
	colStats, exists := stats.ColumnStats[columnName]
	if !exists {
		return 0.1, nil // 默认 10% NULL
	}

	if stats.RowCount <= 0 {
		return 0.0, nil
	}

	// 计算实际 NULL 比例
	nullFraction := float64(colStats.NullCount) / float64(stats.RowCount)

	// 限制范围 [0, 1]
	if nullFraction < 0 {
		nullFraction = 0
	}
	if nullFraction > 1 {
		nullFraction = 1
	}

	return nullFraction, nil
}

// EstimateCorrelationFromRealStats 从真实统计信息估算列相关性
func (si *StatisticsIntegrator) EstimateCorrelationFromRealStats(tableName string, columns []string) (float64, error) {
	if len(columns) <= 1 {
		return 1.0, nil // 单列索引相关性总是 1.0
	}

	if len(columns) == 2 {
		// 两列索引：基于 NDV 比率估算相关性
		ndv1, err1 := si.EstimateNDVFromRealStats(tableName, columns[0])
		ndv2, err2 := si.EstimateNDVFromRealStats(tableName, columns[1])

		if err1 != nil || err2 != nil {
			return 0.5, nil // 默认中等相关性
		}

		if ndv1 <= 0 || ndv2 <= 0 {
			return 0.5, nil
		}

		// NDV 越接近，相关性越高
		smaller := math.Min(ndv1, ndv2)
		larger := math.Max(ndv1, ndv2)
		ratio := smaller / larger

		// 相关系数 = 0.5 + ratio * 0.5
		// 当 ratio = 1 时，相关性 = 1.0
		// 当 ratio = 0 时，相关性 = 0.5
		return 0.5 + ratio*0.5, nil
	}

	// 多列（>2）索引：使用简化估算
	// 计算相邻列的相关性平均值
	totalCorrelation := 0.0
	correlationCount := 0

	for i := 0; i < len(columns)-1; i++ {
		corr, _ := si.EstimateCorrelationFromRealStats(tableName, []string{columns[i], columns[i+1]})
		totalCorrelation += corr
		correlationCount++
	}

	if correlationCount > 0 {
		return totalCorrelation / float64(correlationCount), nil
	}

	return 0.5, nil // 默认中等相关性
}

// EstimateIndexSizeFromRealStats 从真实统计信息估算索引大小
func (si *StatisticsIntegrator) EstimateIndexSizeFromRealStats(tableName string, columns []string) (int64, error) {
	stats, err := si.GetRealStatistics(tableName)
	if err != nil {
		// 使用默认估算
		return si.estimateDefaultIndexSize(tableName, columns), nil
	}

	if stats.RowCount == 0 {
		return 0, nil
	}

	// 计算列的平均大小
	avgColSize := int64(8) // 默认 8 字节（整数或指针）
	for _, col := range columns {
		if colStats, exists := stats.ColumnStats[col]; exists {
			// 使用列的平均长度
			avgColSize += int64(colStats.AvgWidth)
		}
	}

	// B-Tree 索引大小估算
	entrySize := avgColSize + 8 // 8 字节行指针

	// B-Tree 填充率约 75%
	fillFactor := 0.75

	// 计算总大小
	totalSize := float64(stats.RowCount*entrySize) / fillFactor

	// 添加内部节点的开销（约 20%）
	totalSize *= 1.2

	return int64(totalSize), nil
}

// GenerateIndexStatsFromRealStats 从真实统计信息生成虚拟索引统计
func (si *StatisticsIntegrator) GenerateIndexStatsFromRealStats(
	tableName string,
	columns []string,
	isUnique bool,
) (*HypotheticalIndexStats, error) {
	// 估算 NDV
	var ndv int64
	if isUnique {
		// 唯一索引的 NDV = 行数
		stats, err := si.GetRealStatistics(tableName)
		if err != nil {
			ndv = int64(si.heuristicNDV(tableName))
		} else {
			ndv = stats.RowCount
		}
	} else {
		ndvFloat, err := si.EstimateNDVFromRealStatsMultiColumn(tableName, columns)
		if err != nil {
			ndv = int64(si.heuristicNDV(tableName))
		} else {
			ndv = int64(ndvFloat)
		}
	}

	// 估算选择性
	selectivity := si.estimateSelectivity(ndv)

	// 估算索引大小
	size, err := si.EstimateIndexSizeFromRealStats(tableName, columns)
	if err != nil {
		size = si.estimateDefaultIndexSize(tableName, columns)
	}

	// 估算 NULL 比例
	nullFraction := 0.1 // 默认 10% NULL
	if len(columns) > 0 {
		nf, err := si.EstimateNullFractionFromRealStats(tableName, columns[0])
		if err == nil {
			nullFraction = nf
		}
	}

	// 估算相关性
	correlation, _ := si.EstimateCorrelationFromRealStats(tableName, columns)

	return &HypotheticalIndexStats{
		NDV:           ndv,
		Selectivity:   selectivity,
		EstimatedSize: size,
		NullFraction:  nullFraction,
		Correlation:   correlation,
	}, nil
}

// heuristicNDV 启发式估算 NDV
func (si *StatisticsIntegrator) heuristicNDV(tableName string) float64 {
	// 尝试获取表统计信息
	stats, err := si.GetRealStatistics(tableName)
	if err != nil {
		return 1000.0 // 默认 NDV
	}

	rowCount := stats.RowCount
	if rowCount <= 0 {
		return 1000.0
	}

	// 启发式规则
	switch {
	case rowCount < 1000:
		return float64(rowCount)
	case rowCount < 100000:
		ndv := rowCount / 10
		if ndv < 100 {
			ndv = 100
		}
		return float64(ndv)
	default:
		ndv := rowCount / 100
		if ndv < 1000 {
			ndv = 1000
		}
		return float64(ndv)
	}
}

// estimateSelectivity 估算选择性
func (si *StatisticsIntegrator) estimateSelectivity(ndv int64) float64 {
	if ndv <= 0 {
		return 0.1
	}

	// 选择性 = 1 / NDV（等值查询）
	sel := 1.0 / float64(ndv)

	// 限制选择性范围 [0.0001, 0.5]
	if sel < 0.0001 {
		sel = 0.0001
	}
	if sel > 0.5 {
		sel = 0.5
	}

	return sel
}

// estimateDefaultIndexSize 估算默认索引大小
func (si *StatisticsIntegrator) estimateDefaultIndexSize(tableName string, columns []string) int64 {
	stats, err := si.GetRealStatistics(tableName)
	if err != nil {
		// 使用默认表大小
		stats = &statistics.TableStatistics{RowCount: 10000}
	}

	avgColSize := int64(len(columns) * 8) // 每列 8 字节
	entrySize := avgColSize + 8            // 加上行指针
	fillFactor := 0.75

	totalSize := float64(stats.RowCount*entrySize) / fillFactor
	totalSize *= 1.2 // 内部节点开销

	return int64(totalSize)
}

// ClearCache 清理缓存
func (si *StatisticsIntegrator) ClearCache() {
	si.statsCache = make(map[string]*statistics.TableStatistics)
}

// GetStatisticsSummary 获取统计信息摘要（用于调试）
func (si *StatisticsIntegrator) GetStatisticsSummary(tableName string) string {
	stats, err := si.GetRealStatistics(tableName)
	if err != nil {
		return fmt.Sprintf("Error: %v", err)
	}

	summary := fmt.Sprintf("Table: %s\n", tableName)
	summary += fmt.Sprintf("  RowCount: %d\n", stats.RowCount)
	summary += fmt.Sprintf("  EstimatedRowCount: %d\n", stats.EstimatedRowCount)
	summary += fmt.Sprintf("  Columns: %d\n", len(stats.ColumnStats))
	summary += fmt.Sprintf("  Histograms: %d\n", len(stats.Histograms))

	return summary
}
