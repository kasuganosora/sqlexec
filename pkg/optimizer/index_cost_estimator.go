package optimizer

import (
	"math"

	"github.com/kasuganosora/sqlexec/pkg/parser"
)

// IndexCostEstimator 索引成本估算器
// 用于更精确地估算索引扫描和查找的成本
type IndexCostEstimator struct {
	ioCostFactor     float64 // IO 成本因子
	cpuCostFactor    float64 // CPU 成本因子
	memoryCostFactor float64 // 内存成本因子
}

// NewIndexCostEstimator 创建索引成本估算器
func NewIndexCostEstimator() *IndexCostEstimator {
	return &IndexCostEstimator{
		ioCostFactor:     0.1,  // 默认IO成本因子
		cpuCostFactor:    0.01, // 默认CPU成本因子
		memoryCostFactor: 0.001, // 默认内存成本因子
	}
}

// EstimateIndexScanCost 估算索引扫描成本
func (ice *IndexCostEstimator) EstimateIndexScanCost(
	indexStats *HypotheticalIndexStats,
	rowCount int64,
	selectivity float64,
) float64 {
	if indexStats == nil {
		// 如果没有统计信息，使用全表扫描成本
		return float64(rowCount) * ice.ioCostFactor
	}

	// 计算扫描的行数
	scannedRows := float64(rowCount) * selectivity
	if scannedRows > float64(rowCount) {
		scannedRows = float64(rowCount)
	}

	// 计算索引层级（B树的高度）
	// 假设B树每个节点存储100个键
	logValue := math.Log(float64(indexStats.NDV)) / math.Log(100.0)
	indexLevels := int(math.Ceil(logValue + 1))

	// IO 成本：读取索引页 + 读取数据页
	ioCost := (float64(indexLevels) + scannedRows*0.1) * ice.ioCostFactor

	// CPU 成本：键比较 + 数据处理
	cpuCost := (float64(indexLevels)*0.01 + scannedRows*0.05) * ice.cpuCostFactor

	// 内存成本：索引缓存
	memoryCost := float64(indexStats.EstimatedSize) * ice.memoryCostFactor

	totalCost := ioCost + cpuCost + memoryCost

	return totalCost
}

// EstimateIndexLookupCost 估算索引查找成本（等值查找）
func (ice *IndexCostEstimator) EstimateIndexLookupCost(
	indexStats *HypotheticalIndexStats,
	rowCount int64,
) float64 {
	if indexStats == nil {
		return float64(rowCount) * ice.ioCostFactor
	}

	// 索引查找成本（等值条件）
	// 计算索引层级
	logValue := math.Log(float64(indexStats.NDV)) / math.Log(100.0)
	indexLevels := int(math.Ceil(logValue + 1))

	// IO 成本：读取索引页 + 读取少量数据页（假设1-10行）
	ioCost := (float64(indexLevels) + 5.0) * ice.ioCostFactor

	// CPU 成本：键比较
	cpuCost := float64(indexLevels) * 0.01 * ice.cpuCostFactor

	// 内存成本
	memoryCost := float64(indexStats.EstimatedSize) * ice.memoryCostFactor * 0.5 // 查找比扫描使用更少缓存

	totalCost := ioCost + cpuCost + memoryCost

	return totalCost
}

// EstimateIndexRangeScanCost 估算索引范围扫描成本
func (ice *IndexCostEstimator) EstimateIndexRangeScanCost(
	indexStats *HypotheticalIndexStats,
	rowCount int64,
	selectivity float64,
) float64 {
	if indexStats == nil {
		return float64(rowCount) * ice.ioCostFactor
	}

	// 范围扫描：比等值查找更贵，但比全表扫描更便宜
	scannedRows := float64(rowCount) * selectivity
	if scannedRows > float64(rowCount) {
		scannedRows = float64(rowCount)
	}

	// 计算索引层级
	logValue := math.Log(float64(indexStats.NDV)) / math.Log(100.0)
	indexLevels := int(math.Ceil(logValue + 1))

	// IO 成本：读取索引页 + 读取数据页
	ioCost := (float64(indexLevels)*2 + scannedRows*0.2) * ice.ioCostFactor

	// CPU 成本
	cpuCost := (float64(indexLevels)*0.02 + scannedRows*0.1) * ice.cpuCostFactor

	// 内存成本
	memoryCost := float64(indexStats.EstimatedSize) * ice.memoryCostFactor * 0.8

	totalCost := ioCost + cpuCost + memoryCost

	return totalCost
}

// EstimateMultiColumnIndexCost 估算多列索引的成本
func (ice *IndexCostEstimator) EstimateMultiColumnIndexCost(
	indexStats *HypotheticalIndexStats,
	rowCount int64,
	selectivity float64,
	leadingColumnSelectivity []float64,
) float64 {
	if indexStats == nil || len(leadingColumnSelectivity) == 0 {
		return ice.EstimateIndexScanCost(indexStats, rowCount, selectivity)
	}

	// 多列索引的成本：考虑前缀选择性
	// 第一列的选择性最重要
	prefixSelectivity := leadingColumnSelectivity[0]
	for i := 1; i < len(leadingColumnSelectivity); i++ {
		prefixSelectivity *= leadingColumnSelectivity[i]
	}

	// 使用前缀选择性调整整体选择性
	adjustedSelectivity := selectivity * prefixSelectivity
	if adjustedSelectivity > selectivity {
		adjustedSelectivity = selectivity
	}

	// 多列索引通常比单列索引更大，IO成本略高
	adjustedIOCost := ice.EstimateIndexScanCost(indexStats, rowCount, adjustedSelectivity) * 1.1

	return adjustedIOCost
}

// EstimateSelectivityFromNDV 根据NDV估算选择性
func (ice *IndexCostEstimator) EstimateSelectivityFromNDV(ndv float64, rowCount int64) float64 {
	if ndv <= 0 {
		return 1.0
	}
	if rowCount <= 0 {
		return 1.0
	}

	// 选择性 = 1 / NDV
	selectivity := 1.0 / ndv

	// 如果选择性太高，调整到0.1
	if selectivity > 0.1 {
		selectivity = 0.1
	}

	// 如果NDV超过行数，选择性应该更高（但不能超过0.1）
	if ndv > float64(rowCount) {
		selectivity = 0.1
	}

	return selectivity
}

// EstimateRangeSelectivity 估算范围条件的选择性
func (ice *IndexCostEstimator) EstimateRangeSelectivity(
	ndv float64,
	rowCount int64,
	condition *parser.Expression,
) float64 {
	if ndv <= 0 || rowCount <= 0 {
		return 1.0
	}

	// 范围条件的选择性通常比等值条件高
	// 使用启发式规则：
	// - <, >: 选择性最低
	// - <=, >=: 选择性中等 (包含等值部分)
	// - BETWEEN: 选择性最高 (假设选择一个较大的范围)

	baseSelectivity := ice.EstimateSelectivityFromNDV(ndv, rowCount)

	var rangeFactor float64
	switch condition.Operator {
	case "<", ">":
		// 选择性最低
		rangeFactor = 1.5
	case "<=", ">=":
		// 选择性中等
		rangeFactor = 3.0
	case "BETWEEN":
		// 选择性最高
		rangeFactor = 10.0
	default:
		rangeFactor = 2.0
	}

	adjustedSelectivity := baseSelectivity * rangeFactor
	if adjustedSelectivity > 1.0 {
		adjustedSelectivity = 1.0
	}

	return adjustedSelectivity
}

// EstimateJoinCost 估算连接成本（基于索引）
func (ice *IndexCostEstimator) EstimateJoinCost(
	outerRowCount int64,
	innerRowCount int64,
	innerIndexStats *HypotheticalIndexStats,
	joinSelectivity float64,
) float64 {
	if innerIndexStats == nil {
		// 没有索引，使用嵌套循环连接
		return float64(outerRowCount*innerRowCount) * ice.ioCostFactor
	}

	// 有索引，使用索引嵌套循环连接
	// 外层表扫描
	outerScanCost := float64(outerRowCount) * ice.ioCostFactor

	// 内层表通过索引查找
	_ = float64(innerRowCount) * joinSelectivity
	lookupCost := ice.EstimateIndexLookupCost(innerIndexStats, innerRowCount)

	innerJoinCost := float64(outerRowCount) * lookupCost * joinSelectivity

	totalCost := outerScanCost + innerJoinCost

	return totalCost
}

// EstimateAggregateCost 估算聚合成本
func (ice *IndexCostEstimator) EstimateAggregateCost(
	rowCount int64,
	groupByColumns []string,
	indexStats *HypotheticalIndexStats,
	indexColumns []string, // 添加索引列参数
) float64 {
	if indexStats == nil || len(groupByColumns) == 0 || len(indexColumns) == 0 {
		// 没有索引，需要全表扫描和排序
		sortCost := float64(rowCount) * math.Log(float64(rowCount))
		scanCost := float64(rowCount) * ice.ioCostFactor
		return (scanCost + sortCost) * ice.cpuCostFactor
	}

	// 有索引，可能使用索引进行分组
	// 检查索引是否覆盖所有GROUP BY列
	isCovering := true
	for _, groupCol := range groupByColumns {
		found := false
		for _, idxCol := range indexColumns {
			if groupCol == idxCol {
				found = true
				break
			}
		}
		if !found {
			isCovering = false
			break
		}
	}

	if isCovering {
		// 覆盖索引：只需要扫描索引，不需要排序
		// 多列GROUP BY 的成本比单列略高
		groupByFactor := 1.0 + float64(len(groupByColumns)-1)*0.2
		scanCost := float64(rowCount) * ice.ioCostFactor * 0.5 * groupByFactor
		return scanCost
	}

	// 部分覆盖：需要扫描索引然后排序
	// 多列GROUP BY 的成本更高
	groupByFactor := 1.0 + float64(len(groupByColumns))*0.5
	scanCost := float64(rowCount) * ice.ioCostFactor * 0.7 * groupByFactor
	sortCost := float64(rowCount) * math.Log(float64(rowCount)) * 0.3 * groupByFactor

	totalCost := (scanCost + sortCost) * ice.cpuCostFactor

	return totalCost
}

// EstimateBenefit 估算索引收益
func (ice *IndexCostEstimator) EstimateBenefit(
	baseCost float64,
	indexCost float64,
) float64 {
	if baseCost <= 0 {
		return 0.0
	}

	// 收益 = (基础成本 - 索引成本) / 基础成本
	benefit := (baseCost - indexCost) / baseCost

	// 限制收益在 [0, 1] 范围内
	if benefit < 0 {
		benefit = 0
	}
	if benefit > 1 {
		benefit = 1
	}

	return benefit
}

// AdjustCostForSkew 考虑数据偏斜调整成本
func (ice *IndexCostEstimator) AdjustCostForSkew(
	cost float64,
	skewFactor float64, // 0-1, 1表示完全偏斜
) float64 {
	if skewFactor <= 0 {
		return cost
	}

	// 数据偏斜会导致某些查询更慢
	// 使用调整因子：完全偏斜时成本增加50%
	adjustmentFactor := 1.0 + skewFactor

	return cost * adjustmentFactor
}

// EstimateCardinality 估算结果基数
func (ice *IndexCostEstimator) EstimateCardinality(
	ndv float64,
	selectivity float64,
	rowCount int64,
) int64 {
	if ndv <= 0 {
		return rowCount
	}

	cardinality := float64(rowCount) * selectivity

	// 基数不能超过总行数
	if cardinality > float64(rowCount) {
		cardinality = float64(rowCount)
	}

	// 基数不能超过NDV（仅在选择性正常时限制）
	if selectivity <= 1.0 && cardinality > ndv {
		cardinality = ndv
	}

	return int64(cardinality)
}
