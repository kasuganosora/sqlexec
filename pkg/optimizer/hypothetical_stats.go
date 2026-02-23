package optimizer

import (
	"fmt"
	"math"

	"github.com/kasuganosora/sqlexec/pkg/optimizer/statistics"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/utils"
)

// HypotheticalStatsGenerator 虚拟索引统计信息生成器
// 基于表统计信息生成虚拟索引的统计信息
type HypotheticalStatsGenerator struct {
	estimator statistics.CardinalityEstimator
	tableInfo map[string]*domain.TableInfo
}

// NewHypotheticalStatsGenerator 创建虚拟索引统计生成器
func NewHypotheticalStatsGenerator(estimator statistics.CardinalityEstimator) *HypotheticalStatsGenerator {
	return &HypotheticalStatsGenerator{
		estimator: estimator,
		tableInfo: make(map[string]*domain.TableInfo),
	}
}

// UpdateTableInfo 更新表信息
func (g *HypotheticalStatsGenerator) UpdateTableInfo(tableName string, info *domain.TableInfo) {
	g.tableInfo[tableName] = info
}

// GenerateStats 为虚拟索引生成统计信息
func (g *HypotheticalStatsGenerator) GenerateStats(tableName string, columns []string, isUnique bool) (*HypotheticalIndexStats, error) {
	// 获取表统计信息
	if g.estimator == nil {
		// 如果没有 estimator，使用默认值
		return g.generateDefaultStats(tableName, columns, isUnique)
	}

	stats, err := g.estimator.GetStatistics(tableName)
	if err != nil {
		// 如果没有统计信息，使用默认值
		return g.generateDefaultStats(tableName, columns, isUnique)
	}

	// 生成统计信息
	ndv := g.estimateNDV(stats, columns, isUnique)
	selectivity := g.estimateSelectivity(ndv, stats.RowCount)
	size := g.estimateIndexSize(stats, columns)
	nullFraction := g.estimateNullFraction(stats, columns)
	correlation := g.estimateCorrelation(stats, columns)

	return &HypotheticalIndexStats{
		NDV:           ndv,
		Selectivity:   selectivity,
		EstimatedSize: size,
		NullFraction:  nullFraction,
		Correlation:   correlation,
	}, nil
}

// estimateNDV 估算不同值数量
func (g *HypotheticalStatsGenerator) estimateNDV(stats *statistics.TableStatistics, columns []string, isUnique bool) int64 {
	if isUnique {
		// 唯一索引的 NDV = 行数
		return stats.RowCount
	}

	// 单列索引
	if len(columns) == 1 {
		colName := columns[0]
		if colStats, exists := stats.ColumnStats[colName]; exists {
			if colStats.DistinctCount > 0 {
				return colStats.DistinctCount
			}
		}
		// 如果没有列统计信息，使用启发式估算
		return g.heuristicNDV(stats.RowCount)
	}

	// 多列索引：使用组合估算
	// NDV(col1, col2, ...) ≈ min(NDV(col1), NDV(col2), ...) / 2
	minNDV := int64(math.MaxInt64)
	for _, col := range columns {
		if colStats, exists := stats.ColumnStats[col]; exists {
			if colStats.DistinctCount > 0 && colStats.DistinctCount < minNDV {
				minNDV = colStats.DistinctCount
			}
		}
	}

	if minNDV == math.MaxInt64 {
		// 没有任何列的统计信息
		return g.heuristicNDV(stats.RowCount)
	}

	// 多列索引的 NDV 低于单列最小 NDV
	return utils.MaxInt64(minNDV/2, 1)
}

// heuristicNDV 启发式估算 NDV
func (g *HypotheticalStatsGenerator) heuristicNDV(rowCount int64) int64 {
	// 启发式规则：
	// - 小表（< 1000 行）：NDV ≈ rowCount
	// - 中等表（1000-100000 行）：NDV ≈ min(rowCount, rowCount / 10)
	// - 大表（> 100000 行）：NDV ≈ min(rowCount, rowCount / 100)
	switch {
	case rowCount < 1000:
		return rowCount
	case rowCount < 100000:
		return utils.MaxInt64(rowCount/10, 100)
	default:
		return utils.MaxInt64(rowCount/100, 1000)
	}
}

// estimateSelectivity 估算选择性
func (g *HypotheticalStatsGenerator) estimateSelectivity(ndv, rowCount int64) float64 {
	if rowCount == 0 {
		return 0.0
	}

	// 选择性 = 1 / NDV（等值查询）
	// 范围查询的选择性大约是 10-30%
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

// estimateIndexSize 估算索引大小
func (g *HypotheticalStatsGenerator) estimateIndexSize(stats *statistics.TableStatistics, columns []string) int64 {
	if stats.RowCount == 0 {
		return 0
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
	// 每个索引项 = 列值 + 行指针
	entrySize := avgColSize + 8 // 8 字节行指针

	// B-Tree 填充率约 75%
	fillFactor := 0.75

	// 计算总大小
	totalSize := float64(stats.RowCount*entrySize) / fillFactor

	// 添加内部节点的开销（约 20%）
	totalSize *= 1.2

	return int64(totalSize)
}

// estimateNullFraction 估算 NULL 值比例
func (g *HypotheticalStatsGenerator) estimateNullFraction(stats *statistics.TableStatistics, columns []string) float64 {
	if stats.RowCount == 0 {
		return 0.0
	}

	totalNullCount := int64(0)
	for _, col := range columns {
		if colStats, exists := stats.ColumnStats[col]; exists {
			totalNullCount += colStats.NullCount
		}
	}

	// NULL 比例 = 总 NULL 数 / (列数 * 行数)
	nullFraction := float64(totalNullCount) / float64(int64(len(columns))*stats.RowCount)

	// 限制范围 [0, 1]
	if nullFraction < 0 {
		nullFraction = 0
	}
	if nullFraction > 1 {
		nullFraction = 1
	}

	return nullFraction
}

// estimateCorrelation 估算列相关性
func (g *HypotheticalStatsGenerator) estimateCorrelation(stats *statistics.TableStatistics, columns []string) float64 {
	// 单列索引的相关性总是 1.0
	if len(columns) == 1 {
		return 1.0
	}

	// 多列索引的相关性估算
	// 使用启发式：基于列的 NDV 比率
	if len(columns) == 2 {
		ndv1, ndv2 := g.getColumnNDV(stats, columns[0]), g.getColumnNDV(stats, columns[1])
		if ndv1 > 0 && ndv2 > 0 {
			var smaller int64
			if ndv1 < ndv2 {
				smaller = ndv1
			} else {
				smaller = ndv2
			}
			var larger int64
			if ndv1 > ndv2 {
				larger = ndv1
			} else {
				larger = ndv2
			}
			ratio := float64(smaller) / float64(larger)
			// NDV 越接近，相关性越高
			return 0.5 + ratio*0.5
		}
	}

	// 默认中等相关性
	return 0.5
}

// getColumnNDV 获取列的 NDV
func (g *HypotheticalStatsGenerator) getColumnNDV(stats *statistics.TableStatistics, columnName string) int64 {
	if colStats, exists := stats.ColumnStats[columnName]; exists {
		if colStats.DistinctCount > 0 {
			return colStats.DistinctCount
		}
	}
	return 0
}

// generateDefaultStats 生成默认统计信息
func (g *HypotheticalStatsGenerator) generateDefaultStats(tableName string, columns []string, isUnique bool) (*HypotheticalIndexStats, error) {
	// 使用保守的默认值
	rowCount := int64(10000) // 默认表大小

	ndv := rowCount
	if !isUnique {
		ndv = g.heuristicNDV(rowCount)
		if len(columns) > 1 {
			ndv = utils.MaxInt64(ndv/2, 1)
		}
	}

	selectivity := g.estimateSelectivity(ndv, rowCount)
	size := g.estimateDefaultSize(rowCount, columns)

	return &HypotheticalIndexStats{
		NDV:           ndv,
		Selectivity:   selectivity,
		EstimatedSize: size,
		NullFraction:  0.1, // 默认 10% NULL
		Correlation:   g.estimateCorrelation(nil, columns),
	}, nil
}

// estimateDefaultSize 估算默认索引大小
func (g *HypotheticalStatsGenerator) estimateDefaultSize(rowCount int64, columns []string) int64 {
	avgColSize := int64(len(columns) * 8) // 每列 8 字节
	entrySize := avgColSize + 8           // 加上行指针
	fillFactor := 0.75

	totalSize := float64(rowCount*entrySize) / fillFactor
	totalSize *= 1.2 // 内部节点开销

	return int64(totalSize)
}

// EstimateIndexScanCost 估算索引扫描成本
func (g *HypotheticalStatsGenerator) EstimateIndexScanCost(stats *HypotheticalIndexStats, rowCount int64) float64 {
	if stats == nil || stats.Selectivity <= 0 {
		return float64(rowCount)
	}

	// 索引扫描成本 = 查找成本 + 扫描成本
	// 查找成本 = log2(NDV)
	// 扫描成本 = selectivity * rowCount
	lookupCost := math.Log2(float64(stats.NDV))
	scanCost := stats.Selectivity * float64(rowCount)

	return lookupCost + scanCost
}

// EstimateIndexJoinCost 估算使用索引的 JOIN 成本
func (g *HypotheticalStatsGenerator) EstimateIndexJoinCost(leftStats, rightStats *HypotheticalIndexStats, leftRows, rightRows int64) float64 {
	if leftStats == nil || rightStats == nil {
		return float64(leftRows + rightRows)
	}

	// Nested Loop Join with Index lookup
	// Cost = outer rows * (index lookup + inner rows selectivity)
	cost := float64(leftRows) * (math.Log2(float64(leftStats.NDV)) + rightStats.Selectivity*float64(rightRows))

	return cost
}

// CompareWithFullScan 比较索引扫描与全表扫描
func (g *HypotheticalStatsGenerator) CompareWithFullScan(indexStats *HypotheticalIndexStats, rowCount int64) (float64, bool) {
	indexCost := g.EstimateIndexScanCost(indexStats, rowCount)
	fullScanCost := float64(rowCount)

	benefitRatio := fullScanCost / indexCost
	return benefitRatio, benefitRatio > 1.5 // 索引收益 > 50% 时推荐
}

// GetColumnStatistics 获取列统计信息（用于调试）
func (g *HypotheticalStatsGenerator) GetColumnStatistics(tableName, columnName string) (*statistics.ColumnStatistics, error) {
	stats, err := g.estimator.GetStatistics(tableName)
	if err != nil {
		return nil, fmt.Errorf("table statistics not found: %w", err)
	}

	colStats, exists := stats.ColumnStats[columnName]
	if !exists {
		return nil, fmt.Errorf("column statistics not found: %s", columnName)
	}

	return colStats, nil
}
