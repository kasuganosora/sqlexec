package optimizer

import (
	"context"
	"math"

	"github.com/kasuganosora/sqlexec/service/resource"
)

// TableStatistics 表统计信�?
type TableStatistics struct {
	Name       string
	RowCount   int64
	ColumnStats map[string]*ColumnStatistics
}

// ColumnStatistics 列统计信�?
type ColumnStatistics struct {
	Name          string
	DataType       string
	DistinctCount  int64  // NDV (Number of Distinct Values)
	NullCount      int64
	MinValue       interface{}
	MaxValue       interface{}
	NullFraction   float64
	AvgWidth      float64 // 平均字符串长�?
}

// CardinalityEstimator 基数估算器接�?
type CardinalityEstimator interface {
	// EstimateTableScan 估算表扫描的基数
	EstimateTableScan(tableName string) int64

	// EstimateFilter 估算过滤后的基数
	EstimateFilter(table string, filters []resource.Filter) int64

	// EstimateJoin 估算JOIN的输出行�?
	EstimateJoin(left, right LogicalPlan, joinType JoinType) int64

	// EstimateDistinct 估算DISTINCT后的行数
	EstimateDistinct(table string, columns []string) int64

	// UpdateStatistics 更新表的统计信息
	UpdateStatistics(tableName string, stats *TableStatistics)
}

// SimpleCardinalityEstimator 简化的基数估算�?
type SimpleCardinalityEstimator struct {
	stats map[string]*TableStatistics
}

// NewSimpleCardinalityEstimator 创建简化基数估算器
func NewSimpleCardinalityEstimator() *SimpleCardinalityEstimator {
	return &SimpleCardinalityEstimator{
		stats: make(map[string]*TableStatistics),
	}
}

// UpdateStatistics 更新统计信息
func (e *SimpleCardinalityEstimator) UpdateStatistics(tableName string, stats *TableStatistics) {
	e.stats[tableName] = stats
}

// EstimateTableScan 估算表扫描基�?
func (e *SimpleCardinalityEstimator) EstimateTableScan(tableName string) int64 {
	if stats, exists := e.stats[tableName]; exists {
		return stats.RowCount
	}
	// 默认估计：假�?000�?
	return 1000
}

// EstimateFilter 估算过滤后的基数
func (e *SimpleCardinalityEstimator) EstimateFilter(table string, filters []resource.Filter) int64 {
	baseRowCount := e.EstimateTableScan(table)
	if len(filters) == 0 {
		return baseRowCount
	}

	// 计算每个过滤器的选择�?
	totalSelectivity := 1.0
	for _, filter := range filters {
		sel := e.estimateFilterSelectivity(table, filter)
		if filter.LogicOp == "AND" {
			// AND: 选择率相�?
			totalSelectivity *= sel
		} else if filter.LogicOp == "OR" {
			// OR: 处理OR子过滤器
			// 简化：使用平均选择�?
			orSelectivity := 0.0
			for i := range filter.SubFilters {
				orSel := e.estimateFilterSelectivity(table, filter.SubFilters[i])
				orSelectivity += orSel
			}
			if len(filter.SubFilters) > 0 {
				totalSelectivity *= (orSelectivity / float64(len(filter.SubFilters)))
			}
		} else {
			// 单个条件
			totalSelectivity *= sel
		}
	}

	result := float64(baseRowCount) * totalSelectivity
	// 确保至少返回1�?
	if result < 1 {
		return 1
	}
	return int64(result)
}

// estimateFilterSelectivity 估算单个过滤器的选择�?
func (e *SimpleCardinalityEstimator) estimateFilterSelectivity(table string, filter resource.Filter) float64 {
	// 处理逻辑组合
	if filter.LogicOp == "AND" || filter.LogicOp == "OR" {
		return e.estimateLogicSelectivity(table, filter)
	}

	stats, exists := e.stats[table]
	if !exists {
		// 没有统计信息时使用默认选择�?
		return e.getDefaultSelectivity(filter.Operator)
	}

	colStats, colExists := stats.ColumnStats[filter.Field]
	if !colExists {
		return e.getDefaultSelectivity(filter.Operator)
	}

	switch filter.Operator {
	case "=", "!=":
		// 等值查询：选择�?= 1/NDV
		if colStats.DistinctCount > 0 {
			return 1.0 / float64(colStats.DistinctCount)
		}
		return 0.1

	case ">", ">=", "<", "<=":
		// 范围查询
		return e.estimateRangeSelectivity(filter.Operator, filter.Value, colStats)

	case "IN":
		// IN操作：假设平均每IN列表�?0个�?
		if valList, ok := filter.Value.([]interface{}); ok && len(valList) > 0 {
			return float64(len(valList)) / float64(colStats.DistinctCount)
		}
		return 0.1

	case "BETWEEN":
		// BETWEEN操作
		if vals, ok := filter.Value.([]interface{}); ok && len(vals) == 2 {
			return e.estimateRangeSelectivity(">=", vals[0], colStats) *
				e.estimateRangeSelectivity("<=", vals[1], colStats)
		}
		return 0.3

	case "LIKE":
		// LIKE操作：保守估�?.1-0.5
		return 0.25

	default:
		return e.getDefaultSelectivity(filter.Operator)
	}
}

// estimateLogicSelectivity 估算逻辑组合的选择�?
func (e *SimpleCardinalityEstimator) estimateLogicSelectivity(table string, filter resource.Filter) float64 {
	if len(filter.SubFilters) == 0 {
		return 1.0
	}

	switch filter.LogicOp {
	case "AND":
		// AND: 选择率相�?
		sel := 1.0
		for _, subFilter := range filter.SubFilters {
			sel *= e.estimateFilterSelectivity(table, subFilter)
		}
		return sel

	case "OR":
		// OR: 选择�?= 1 - (1-s1)*(1-s2)*...*(1-sn)
		// 简化：使用包含关系
		sel := 0.0
		for _, subFilter := range filter.SubFilters {
			subSel := e.estimateFilterSelectivity(table, subFilter)
			sel += subSel
		}
		// 避免超过1.0
		if sel > 0.95 {
			sel = 0.95
		}
		return sel

	default:
		return 1.0
	}
}

// estimateRangeSelectivity 估算范围查询的选择�?
func (e *SimpleCardinalityEstimator) estimateRangeSelectivity(operator string, value interface{}, colStats *ColumnStatistics) float64 {
	minVal := colStats.MinValue
	maxVal := colStats.MaxValue

	if minVal == nil || maxVal == nil {
		return 0.1
	}

	minFloat, _ := toFloat64(minVal)
	maxFloat, _ := toFloat64(maxVal)
	valFloat, _ := toFloat64(value)

	if minFloat == maxFloat {
		return 1.0
	}

	rangeSize := maxFloat - minFloat
	if rangeSize == 0 {
		return 0.5
	}

	switch operator {
	case ">":
		// value > min: (max - value) / (max - min)
		if valFloat <= minFloat {
			return 0.0
		}
		return (maxFloat - valFloat) / rangeSize

	case ">=":
		// value >= min: (max - value) / (max - min)
		if valFloat < minFloat {
			return 0.0
		}
		return (maxFloat - valFloat + 0.0001) / rangeSize

	case "<":
		// value < max: (value - min) / (max - min)
		if valFloat >= maxFloat {
			return 0.0
		}
		return (valFloat - minFloat) / rangeSize

	case "<=":
		// value <= max: (value - min) / (max - min)
		if valFloat > maxFloat {
			return 0.0
		}
		return (valFloat - minFloat + 0.0001) / rangeSize

	default:
		return 0.5
	}
}

// getDefaultSelectivity 获取默认选择�?
func (e *SimpleCardinalityEstimator) getDefaultSelectivity(operator string) float64 {
	switch operator {
	case "=", "!=":
		return 0.1 // 等值查询：10%
	case ">", ">=", "<", "<=":
		return 0.3 // 范围查询�?0%
	case "IN":
		return 0.2 // IN查询�?0%
	case "BETWEEN":
		return 0.3 // BETWEEN查询�?0%
	case "LIKE":
		return 0.25 // LIKE查询�?5%
	default:
		return 0.5 // 默认�?0%
	}
}

// EstimateJoin 估算JOIN的基�?
func (e *SimpleCardinalityEstimator) EstimateJoin(left, right LogicalPlan, joinType JoinType) int64 {
	// 获取左右表的基数
	leftCount := e.estimateRowCount(left)
	rightCount := e.estimateRowCount(right)

	if leftCount == 0 || rightCount == 0 {
		return 0
	}

	// 根据JOIN类型估算
	switch joinType {
	case InnerJoin:
		// INNER JOIN: 假设每个左表行匹配右表的1/N个行
		// 简化估�? min(left, right)
		return min(leftCount, rightCount)

	case LeftOuterJoin:
		// LEFT JOIN: 输出 = 左表行数
		return leftCount

	case RightOuterJoin:
		// RIGHT JOIN: 输出 = 右表行数
		return rightCount

	case FullOuterJoin:
		// FULL JOIN: 输出 �?left + right - matches
		return leftCount + rightCount/2

	default:
		return min(leftCount, rightCount)
	}
}

// estimateRowCount 估算逻辑计划的行�?
func (e *SimpleCardinalityEstimator) estimateRowCount(plan LogicalPlan) int64 {
	if dataSource, ok := plan.(*LogicalDataSource); ok {
		return e.EstimateTableScan(dataSource.TableName)
	}

	if selection, ok := plan.(*LogicalSelection); ok {
		if len(selection.children) > 0 {
			tableName := getTableName(selection.children[0])
			conditions := selection.Conditions()
			// 转换表达式到过滤�?
			filters := make([]resource.Filter, len(conditions))
			for i, cond := range conditions {
				filters[i] = resource.Filter{
					Field:    expressionToString(cond),
					Operator:  "=",
					Value:     cond.Value,
				}
			}
			return e.EstimateFilter(tableName, filters)
		}
	}

	// 其他算子：返回子节点的行�?
	children := plan.Children()
	if len(children) > 0 {
		return e.estimateRowCount(children[0])
	}

	return 1000 // 默认�?
}

// getTableName 获取逻辑计划的表�?
func getTableName(plan LogicalPlan) string {
	if dataSource, ok := plan.(*LogicalDataSource); ok {
		return dataSource.TableName
	}
	children := plan.Children()
	if len(children) > 0 {
		return getTableName(children[0])
	}
	return ""
}

// EstimateDistinct 估算DISTINCT后的行数
func (e *SimpleCardinalityEstimator) EstimateDistinct(table string, columns []string) int64 {
	stats, exists := e.stats[table]
	if !exists {
		return e.EstimateTableScan(table) / 2
	}

	if len(columns) == 0 {
		return stats.RowCount
	}

	// 简化：取最小NDV
	minNDV := int64(math.MaxInt64)
	for _, col := range columns {
		if colStats, ok := stats.ColumnStats[col]; ok && colStats.DistinctCount > 0 {
			if colStats.DistinctCount < minNDV {
				minNDV = colStats.DistinctCount
			}
		}
	}

	if minNDV == math.MaxInt64 {
		return stats.RowCount / 2
	}

	return minNDV
}

// CollectStatistics 从数据源收集统计信息（简化版�?
func CollectStatistics(dataSource resource.DataSource, tableName string) (*TableStatistics, error) {
	// 执行查询获取所有数�?
	result, err := dataSource.Query(context.Background(), tableName, &resource.QueryOptions{})
	if err != nil {
		return nil, err
	}

	stats := &TableStatistics{
		Name:       tableName,
		RowCount:   result.Total,
		ColumnStats: make(map[string]*ColumnStatistics),
	}

	// 为每列收集统计信�?
	for _, colInfo := range result.Columns {
		stats.ColumnStats[colInfo.Name] = collectColumnStatistics(result.Rows, colInfo.Name, colInfo.Type)
	}

	return stats, nil
}

// collectColumnStatistics 收集列的统计信息
func collectColumnStatistics(rows []resource.Row, columnName, columnType string) *ColumnStatistics {
	stats := &ColumnStatistics{
		Name:    columnName,
		DataType: columnType,
	}

	// 收集�?
	values := make([]interface{}, 0, len(rows))
	distinctValues := make(map[interface{}]bool)
	nullCount := int64(0)
	totalWidth := 0.0

	for _, row := range rows {
		val := row[columnName]
		values = append(values, val)

		if val == nil {
			nullCount++
			continue
		}

		distinctValues[val] = true

		// 对于字符串类型，计算平均宽度
		if s, ok := val.(string); ok {
			totalWidth += float64(len(s))
		}
	}

	// 计算统计信息
	stats.NullCount = nullCount
	stats.NullFraction = float64(nullCount) / float64(len(rows))
	stats.DistinctCount = int64(len(distinctValues))

	// 计算Min和Max
	if len(values) > 0 {
		stats.MinValue = values[0]
		stats.MaxValue = values[0]

		for _, val := range values {
			if val != nil {
				if compareValues(val, stats.MinValue) < 0 {
					stats.MinValue = val
				}
				if compareValues(val, stats.MaxValue) > 0 {
					stats.MaxValue = val
				}
			}
		}

		// 计算平均宽度
		if len(values)-int(nullCount) > 0 {
			stats.AvgWidth = totalWidth / float64(len(values)-int(nullCount))
		}
	}

	return stats
}
