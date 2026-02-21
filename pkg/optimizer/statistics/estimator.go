package statistics

import (
	"fmt"
	"math"

	"github.com/kasuganosora/sqlexec/pkg/parser"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// CardinalityEstimator 基数估算器接口
type CardinalityEstimator interface {
	EstimateTableScan(tableName string) int64
	EstimateFilter(tableName string, filters []domain.Filter) int64
	GetStatistics(tableName string) (*TableStatistics, error)
}

// EnhancedCardinalityEstimator 增强的基数估算器
// 使用直方图提供更准确的估算
type EnhancedCardinalityEstimator struct {
	statsCache *StatisticsCache
	stats      map[string]*TableStatistics
}

// NewEnhancedCardinalityEstimator 创建增强的基数估算器
func NewEnhancedCardinalityEstimator(cache *StatisticsCache) *EnhancedCardinalityEstimator {
	return &EnhancedCardinalityEstimator{
		statsCache: cache,
		stats:      make(map[string]*TableStatistics),
	}
}

// UpdateStatistics 更新统计信息
func (e *EnhancedCardinalityEstimator) UpdateStatistics(tableName string, stats *TableStatistics) {
	e.stats[tableName] = stats
	e.statsCache.Set(tableName, stats)
}

// GetStatistics 获取统计信息（优先从缓存）
func (e *EnhancedCardinalityEstimator) GetStatistics(tableName string) (*TableStatistics, error) {
	// 先从内存获取
	if stats, exists := e.stats[tableName]; exists {
		return stats, nil
	}

	// 从缓存获取
	cachedStats, ok := e.statsCache.Get(tableName)
	if ok {
		e.stats[tableName] = cachedStats
		return cachedStats, nil
	}

	return nil, fmt.Errorf("statistics not found for table: %s", tableName)
}

// EstimateTableScan 估算表扫描基数
func (e *EnhancedCardinalityEstimator) EstimateTableScan(tableName string) int64 {
	stats, err := e.GetStatistics(tableName)
	if err != nil {
		// 没有统计信息，使用保守估计
		return 10000 // 更大的默认值
	}

	// 使用估计的行数（可能基于采样）
	if stats.EstimatedRowCount > 0 {
		return stats.EstimatedRowCount
	}
	return stats.RowCount
}

// EstimateFilter 估算过滤后的基数
func (e *EnhancedCardinalityEstimator) EstimateFilter(table string, filters []domain.Filter) int64 {
	baseRowCount := e.EstimateTableScan(table)
	if len(filters) == 0 {
		return baseRowCount
	}

	// 计算每个过滤器的选择率
	totalSelectivity := 1.0
	for _, filter := range filters {
		sel := e.estimateFilterSelectivity(table, filter)
		if filter.LogicOp == "AND" {
			// AND: 选择率相乘
			totalSelectivity *= sel
		} else if filter.LogicOp == "OR" {
			// OR: 处理OR子过滤器
			orSelectivity := e.estimateOrSelectivity(table, filter)
			totalSelectivity *= orSelectivity
		} else {
			// 单个条件
			totalSelectivity *= sel
		}
	}

	result := float64(baseRowCount) * totalSelectivity
	// 确保至少返回1行
	if result < 1 {
		return 1
	}
	return int64(result)
}

// estimateFilterSelectivity 估算单个过滤器的选择率
func (e *EnhancedCardinalityEstimator) estimateFilterSelectivity(table string, filter domain.Filter) float64 {
	// 处理逻辑组合
	if filter.LogicOp == "AND" || filter.LogicOp == "OR" {
		return e.estimateLogicSelectivity(table, filter)
	}

	stats, err := e.GetStatistics(table)
	if err != nil {
		// 没有统计信息时使用默认选择率
		return e.getDefaultSelectivity(filter.Operator)
	}

	colStats, colExists := stats.ColumnStats[filter.Field]
	if !colExists {
		return e.getDefaultSelectivity(filter.Operator)
	}

	// 优先使用直方图估算
	if histogram, histExists := stats.Histograms[filter.Field]; histExists {
		return histogram.EstimateSelectivity(filter)
	}

	// 回退到基础统计信息
	return e.estimateSelectivityUsingStats(filter, colStats)
}

// estimateOrSelectivity 估算OR条件的选择率
func (e *EnhancedCardinalityEstimator) estimateOrSelectivity(table string, filter domain.Filter) float64 {
	if len(filter.SubFilters) == 0 {
		return 1.0
	}

	// OR选择率 = 1 - (1-s1)*(1-s2)*...*(1-sn)
	complement := 1.0
	for _, subFilter := range filter.SubFilters {
		sel := e.estimateFilterSelectivity(table, subFilter)
		complement *= (1.0 - sel)
	}
	return 1.0 - complement
}

// estimateLogicSelectivity 估算逻辑组合的选择率
func (e *EnhancedCardinalityEstimator) estimateLogicSelectivity(table string, filter domain.Filter) float64 {
	if len(filter.SubFilters) == 0 {
		return 1.0
	}

	switch filter.LogicOp {
	case "AND":
		// AND: 选择率相乘
		sel := 1.0
		for _, subFilter := range filter.SubFilters {
			sel *= e.estimateFilterSelectivity(table, subFilter)
		}
		return sel
	case "OR":
		// OR: 选择率 = 1 - (1-s1)*(1-s2)*...*(1-sn)
		complement := 1.0
		for _, subFilter := range filter.SubFilters {
			subSel := e.estimateFilterSelectivity(table, subFilter)
			complement *= (1.0 - subSel)
		}
		return 1.0 - complement
	default:
		return 1.0
	}
}

// estimateSelectivityUsingStats 使用基础统计信息估算选择率
func (e *EnhancedCardinalityEstimator) estimateSelectivityUsingStats(filter domain.Filter, colStats *ColumnStatistics) float64 {
	switch filter.Operator {
	case "=":
		// 等值查询：选择率 = 1/NDV
		if colStats.DistinctCount > 0 {
			return 1.0 / float64(colStats.DistinctCount)
		}
		return 0.1

	case "!=":
		// 不等值查询：选择率 = (NDV-1)/NDV
		if colStats.DistinctCount > 0 {
			return (float64(colStats.DistinctCount) - 1.0) / float64(colStats.DistinctCount)
		}
		return 0.9

	case ">", ">=", "<", "<=":
		// 范围查询
		return e.estimateRangeSelectivityUsingStats(filter.Operator, filter.Value, colStats)

	case "IN":
		// IN操作
		if valList, ok := filter.Value.([]interface{}); ok && len(valList) > 0 {
			return math.Min(1.0, float64(len(valList))/float64(colStats.DistinctCount))
		}
		return 0.2

	case "BETWEEN":
		// BETWEEN操作
		if vals, ok := filter.Value.([]interface{}); ok && len(vals) == 2 {
			sel1 := e.estimateRangeSelectivityUsingStats(">=", vals[0], colStats)
			sel2 := e.estimateRangeSelectivityUsingStats("<=", vals[1], colStats)
			return sel1 * sel2
		}
		return 0.3

	case "LIKE":
		// LIKE操作：考虑前缀匹配
		if pattern, ok := filter.Value.(string); ok {
			// 前缀匹配选择性更高
			if len(pattern) > 0 && pattern[len(pattern)-1] == '%' {
				// 'abc%' 模式
				prefixLen := len(pattern) - 1
				// 估计：前缀越 长，选择性越高
				return math.Min(0.8, 1.0-math.Pow(0.9, float64(prefixLen)))
			}
		}
		return 0.25

	default:
		return e.getDefaultSelectivity(filter.Operator)
	}
}

// estimateRangeSelectivityUsingStats 使用统计信息估算范围查询选择率
func (e *EnhancedCardinalityEstimator) estimateRangeSelectivityUsingStats(operator string, value interface{}, colStats *ColumnStatistics) float64 {
	minVal := colStats.MinValue
	maxVal := colStats.MaxValue

	if minVal == nil || maxVal == nil {
		return 0.1
	}

	minFloat, ok := toFloat64(minVal)
	if !ok {
		return 0.1
	}
	maxFloat, ok := toFloat64(maxVal)
	if !ok {
		return 0.1
	}
	valFloat, ok := toFloat64(value)
	if !ok {
		return 0.1
	}

	if minFloat == maxFloat {
		return 1.0
	}

	rangeSize := maxFloat - minFloat
	if rangeSize == 0 {
		return 0.5
	}

	switch operator {
	case ">":
		// col > value: fraction of rows where col > value
		if valFloat >= maxFloat {
			return 0.0 // value >= max => no rows match
		}
		if valFloat < minFloat {
			return 1.0 // value < min => all rows match
		}
		return (maxFloat - valFloat) / rangeSize
	case ">=":
		// col >= value
		if valFloat > maxFloat {
			return 0.0
		}
		if valFloat <= minFloat {
			return 1.0
		}
		return (maxFloat - valFloat + 0.0001) / rangeSize
	case "<":
		// col < value: fraction of rows where col < value
		if valFloat <= minFloat {
			return 0.0 // value <= min => no rows match
		}
		if valFloat > maxFloat {
			return 1.0 // value > max => all rows match
		}
		return (valFloat - minFloat) / rangeSize
	case "<=":
		// col <= value
		if valFloat < minFloat {
			return 0.0
		}
		if valFloat >= maxFloat {
			return 1.0
		}
		return (valFloat - minFloat + 0.0001) / rangeSize
	default:
		return 0.5
	}
}

// EstimateJoin 估算JOIN的基数（增强版）
func (e *EnhancedCardinalityEstimator) EstimateJoin(left, right interface{}, joinType string) int64 {
	leftCount := int64(10000)
	rightCount := int64(10000)

	if leftCount == 0 || rightCount == 0 {
		return 0
	}

	// 简化实现：基于JOIN类型估算基数
	switch joinType {
	case "INNER":
		// INNER JOIN: left * right * selectivity
		selectivity := 0.1
		return int64(float64(leftCount*rightCount) * selectivity)
	case "LEFT":
		// LEFT JOIN: left
		return leftCount
	case "RIGHT":
		// RIGHT JOIN: right
		return rightCount
	case "FULL":
		// FULL JOIN: left + right
		return leftCount + rightCount
	default:
		return leftCount * rightCount
	}
}

// estimateJoinSelectivity 估算JOIN的选择性
func (e *EnhancedCardinalityEstimator) estimateJoinSelectivity(conditions []*parser.Expression, left, right interface{}) float64 {
	if len(conditions) == 0 {
		return 1.0
	}

	// 分析连接条件中的列
	leftTableName := getTableName(left)
	rightTableName := getTableName(right)

	totalSelectivity := 1.0
	for range conditions {
		// 简化：假设等值连接
		// 实际应该解析表达式获取列名
		selectivity := e.estimateEquijoinSelectivity(leftTableName, rightTableName)
		totalSelectivity *= selectivity
	}

	return totalSelectivity
}

// estimateEquijoinSelectivity 估算等值连接的选择性
func (e *EnhancedCardinalityEstimator) estimateEquijoinSelectivity(leftTable, rightTable string) float64 {
	// 获取两个表的NDV
	leftStats, err := e.GetStatistics(leftTable)
	if err != nil {
		return 0.1
	}

	rightStats, err := e.GetStatistics(rightTable)
	if err != nil {
		return 0.1
	}

	// 假设连接列的NDV相似
	// 选择性 = 1 / NDV
	avgNDV := float64(leftStats.RowCount + rightStats.RowCount) / 2.0
	if avgNDV > 0 {
		return 1.0 / math.Min(100.0, avgNDV)
	}

	return 0.1
}

// EstimateDistinct 估算DISTINCT后的行数
func (e *EnhancedCardinalityEstimator) EstimateDistinct(table string, columns []string) int64 {
	stats, err := e.GetStatistics(table)
	if err != nil {
		return e.EstimateTableScan(table) / 2
	}

	if len(columns) == 0 {
		return stats.RowCount
	}

	// 使用直方图或NDV估算
	minNDV := int64(math.MaxInt64)
	for _, col := range columns {
		if histogram, exists := stats.Histograms[col]; exists {
			// 使用直方图的NDV
			if histogram.NDV < minNDV {
				minNDV = histogram.NDV
			}
		} else if colStats, exists := stats.ColumnStats[col]; exists && colStats.DistinctCount > 0 {
			if colStats.DistinctCount < minNDV {
				minNDV = colStats.DistinctCount
			}
		}
	}

	if minNDV == math.MaxInt64 {
		return stats.RowCount / 2
	}

	// 考虑采样比例
	return minNDV
}

// estimateRowCount 估算逻辑计划的行数
func (e *EnhancedCardinalityEstimator) estimateRowCount(plan interface{}) int64 {
	// 简化实现：返回默认值
	// 完整实现需要处理不同的 LogicalPlan 类型
	_ = plan // 避免未使用警告
	return 10000
}

// getDefaultSelectivity 获取默认选择率
func (e *EnhancedCardinalityEstimator) getDefaultSelectivity(operator string) float64 {
	switch operator {
	case "=":
		return 0.1 // 等值查询：10%
	case "!=":
		return 0.9 // 不等值查询：90%
	case ">", ">=", "<", "<=":
		return 0.3 // 范围查询：30%
	case "IN":
		return 0.2 // IN查询：20%
	case "BETWEEN":
		return 0.3 // BETWEEN查询：30%
	case "LIKE":
		return 0.25 // LIKE查询：25%
	default:
		return 0.5 // 默认：50%
	}
}

// expressionToString 将表达式转换为字符串（简化）
func expressionToString(expr *parser.Expression) string {
	if expr == nil {
		return ""
	}
	if expr.Column != "" {
		return expr.Column
	}
	return fmt.Sprintf("%v", expr)
}

// getTableName 获取逻辑计划的表名
func getTableName(plan interface{}) string {
	// 简化实现：需要处理具体的 LogicalPlan 类型
	_ = plan
	return ""
}



