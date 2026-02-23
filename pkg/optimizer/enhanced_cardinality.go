package optimizer

import (
	"fmt"
	"math"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// EnhancedCardinalityEstimator 增强的基数估算器
// 使用更精确的统计信息和算法进行估算
type EnhancedCardinalityEstimator struct {
	baseEstimator CardinalityEstimator
	stats         map[string]*EnhancedTableStatistics
	correlations  map[string]map[string]float64 // 列相关性矩阵
	fkRelations   map[string]string             // 外键关系
}

// EnhancedTableStatistics 增强的表统计信息
type EnhancedTableStatistics struct {
	Base         *TableStatistics
	Histograms   map[string]*Histogram // 列直方图
	Correlations map[string]float64    // 列相关性
	CV           float64               // 基数变异系数 (Cardinality Variability)
	Skewness     float64               // 数据偏度
}

// Histogram 直方图（等宽或等高）
type Histogram struct {
	ColumnName  string
	Buckets     []*HistogramBucket // 直方图桶
	BucketCount int                // 桶数量
	IsEquiDepth bool               // 是否等深直方图
	TotalNDV    int64              // 总的不同值数
	SampleCount int64              // 样本数
	NullCount   int64              // NULL值数
}

// HistogramBucket 直方图桶
type HistogramBucket struct {
	LowerBound    interface{} // 下界（包含）
	UpperBound    interface{} // 上界（不包含）
	RowCount      int64       // 行数
	DistinctCount int64       // 桶内不同值数
	RepeatCount   int64       // 重复值计数
}

// NewEnhancedCardinalityEstimator 创建增强基数估算器
func NewEnhancedCardinalityEstimator(baseEstimator CardinalityEstimator) *EnhancedCardinalityEstimator {
	return &EnhancedCardinalityEstimator{
		baseEstimator: baseEstimator,
		stats:         make(map[string]*EnhancedTableStatistics),
		correlations:  make(map[string]map[string]float64),
		fkRelations:   make(map[string]string),
	}
}

// EstimateTableScan 估算表扫描基数（使用直方图）
func (e *EnhancedCardinalityEstimator) EstimateTableScan(tableName string) int64 {
	// 委托给基础估算器
	return e.baseEstimator.EstimateTableScan(tableName)
}

// EstimateFilter 使用直方图和相关性精确估算过滤后的基数
func (e *EnhancedCardinalityEstimator) EstimateFilter(table string, filters []domain.Filter) int64 {
	if len(filters) == 0 {
		return e.EstimateTableScan(table)
	}

	// 处理逻辑组合
	if len(filters) == 1 {
		return e.estimateSingleFilter(table, filters[0])
	}

	// 处理AND和OR逻辑组合
	logicOp := filters[0].LogicOp
	if logicOp == "" {
		// 没有逻辑操作符，假设是AND
		logicOp = "AND"
	}

	if logicOp == "AND" {
		return e.estimateANDFilters(table, filters)
	}
	return e.estimateORFilters(table, filters)
}

// estimateSingleFilter 估算单个过滤器的选择率
func (e *EnhancedCardinalityEstimator) estimateSingleFilter(table string, filter domain.Filter) int64 {
	baseRowCount := e.EstimateTableScan(table)

	// 尝试使用直方图估算
	if stats, exists := e.stats[table]; exists {
		if histogram, ok := stats.Histograms[filter.Field]; ok {
			selectivity := e.estimateHistogramSelectivity(histogram, filter.Operator, filter.Value)
			return int64(float64(baseRowCount) * selectivity)
		}
	}

	// 回退到基础估算
	return e.baseEstimator.EstimateFilter(table, []domain.Filter{filter})
}

// estimateHistogramSelectivity 使用直方图估算选择率
func (e *EnhancedCardinalityEstimator) estimateHistogramSelectivity(histogram *Histogram, operator string, value interface{}) float64 {
	if len(histogram.Buckets) == 0 {
		return 0.1
	}

	// 总行数
	totalRows := int64(0)
	for _, bucket := range histogram.Buckets {
		totalRows += bucket.RowCount
	}

	if totalRows == 0 {
		return 0.0
	}

	// 根据操作符计算选择率
	switch operator {
	case "=", "!=":
		return e.estimateEqualitySelectivity(histogram, value, totalRows)
	case ">", ">=", "<", "<=":
		return e.estimateRangeSelectivity(histogram, operator, value, totalRows)
	case "IN":
		if valList, ok := value.([]interface{}); ok {
			return e.estimateINSelectivity(histogram, valList, totalRows)
		}
		return 0.1
	case "BETWEEN":
		if vals, ok := value.([]interface{}); ok && len(vals) == 2 {
			return e.estimateBetweenSelectivity(histogram, vals[0], vals[1], totalRows)
		}
		return 0.3
	case "LIKE":
		return 0.25
	default:
		return 0.1
	}
}

// estimateEqualitySelectivity 估算等值查询的选择率
func (e *EnhancedCardinalityEstimator) estimateEqualitySelectivity(histogram *Histogram, value interface{}, totalRows int64) float64 {
	// 在直方图中查找包含该值的桶
	for _, bucket := range histogram.Buckets {
		if e.valueInRange(value, bucket.LowerBound, bucket.UpperBound) {
			// 桶内选择率
			if bucket.RowCount > 0 {
				bucketSelectivity := float64(bucket.RowCount) / float64(totalRows)
				// 进一步调整：考虑桶内NDV
				if bucket.DistinctCount > 0 {
					// 假设值均匀分布在桶内
					return bucketSelectivity / float64(bucket.DistinctCount)
				}
				return bucketSelectivity
			}
		}
	}

	// 未找到，使用总NDV估算
	if histogram.TotalNDV > 0 {
		return 1.0 / float64(histogram.TotalNDV)
	}
	return 0.1
}

// estimateRangeSelectivity 估算范围查询的选择率
func (e *EnhancedCardinalityEstimator) estimateRangeSelectivity(histogram *Histogram, operator string, value interface{}, totalRows int64) float64 {
	valueFloat, _ := e.toFloat64(value)
	if math.IsNaN(valueFloat) {
		return 0.1
	}

	matchedRows := int64(0)

	for _, bucket := range histogram.Buckets {
		lowerFloat, _ := e.toFloat64(bucket.LowerBound)
		upperFloat, _ := e.toFloat64(bucket.UpperBound)

		if math.IsNaN(lowerFloat) || math.IsNaN(upperFloat) {
			matchedRows += bucket.RowCount
			continue
		}

		bucketMatches := false

		switch operator {
		case ">":
			bucketMatches = lowerFloat > valueFloat
		case ">=":
			bucketMatches = lowerFloat >= valueFloat
		case "<":
			bucketMatches = upperFloat < valueFloat
		case "<=":
			bucketMatches = upperFloat <= valueFloat
		}

		if bucketMatches {
			matchedRows += bucket.RowCount
		} else if e.valueInRange(valueFloat, lowerFloat, upperFloat) {
			// 部分匹配：估算部分行数
			bucketRange := upperFloat - lowerFloat
			if bucketRange > 0 {
				var fraction float64
				switch operator {
				case ">":
					fraction = (upperFloat - valueFloat) / bucketRange
				case ">=":
					fraction = (upperFloat - valueFloat) / bucketRange
				case "<":
					fraction = (valueFloat - lowerFloat) / bucketRange
				case "<=":
					fraction = (valueFloat - lowerFloat) / bucketRange
				}
				matchedRows += int64(float64(bucket.RowCount) * fraction)
			} else {
				matchedRows += bucket.RowCount / 2
			}
		}
	}

	selectivity := float64(matchedRows) / float64(totalRows)
	if selectivity < 0.001 {
		selectivity = 0.001
	} else if selectivity > 0.999 {
		selectivity = 0.999
	}

	return selectivity
}

// estimateINSelectivity 估算IN操作的选择率
func (e *EnhancedCardinalityEstimator) estimateINSelectivity(histogram *Histogram, values []interface{}, totalRows int64) float64 {
	if len(values) == 0 {
		return 0.0
	}

	// 使用直方图估算每个值的选择率
	totalSelectivity := 0.0
	for _, val := range values {
		totalSelectivity += e.estimateEqualitySelectivity(histogram, val, totalRows)
	}

	// 避免超过1.0
	if totalSelectivity > 0.95 {
		totalSelectivity = 0.95
	}

	return totalSelectivity
}

// estimateBetweenSelectivity 估算BETWEEN操作的选择率
func (e *EnhancedCardinalityEstimator) estimateBetweenSelectivity(histogram *Histogram, lowerVal, upperVal interface{}, totalRows int64) float64 {
	lowerFloat, _ := e.toFloat64(lowerVal)
	upperFloat, _ := e.toFloat64(upperVal)

	if math.IsNaN(lowerFloat) || math.IsNaN(upperFloat) || lowerFloat > upperFloat {
		return 0.3
	}

	matchedRows := int64(0)

	for _, bucket := range histogram.Buckets {
		bucketLower, _ := e.toFloat64(bucket.LowerBound)
		bucketUpper, _ := e.toFloat64(bucket.UpperBound)

		if math.IsNaN(bucketLower) || math.IsNaN(bucketUpper) {
			matchedRows += bucket.RowCount
			continue
		}

		// 完全包含
		if bucketLower >= lowerFloat && bucketUpper <= upperFloat {
			matchedRows += bucket.RowCount
			continue
		}

		// 部分重叠
		if e.rangesOverlap(lowerFloat, upperFloat, bucketLower, bucketUpper) {
			overlapStart := math.Max(lowerFloat, bucketLower)
			overlapEnd := math.Min(upperFloat, bucketUpper)
			bucketRange := bucketUpper - bucketLower

			if bucketRange > 0 {
				fraction := (overlapEnd - overlapStart) / bucketRange
				matchedRows += int64(float64(bucket.RowCount) * fraction)
			} else {
				matchedRows += bucket.RowCount / 2
			}
		}
	}

	selectivity := float64(matchedRows) / float64(totalRows)
	if selectivity < 0.001 {
		selectivity = 0.001
	} else if selectivity > 0.999 {
		selectivity = 0.999
	}

	return selectivity
}

// estimateANDFilters 估算AND逻辑组合的选择率（考虑相关性）
func (e *EnhancedCardinalityEstimator) estimateANDFilters(table string, filters []domain.Filter) int64 {
	baseRowCount := e.EstimateTableScan(table)
	totalSelectivity := 1.0

	// 估算每个过滤器的选择率
	selectivities := make([]float64, len(filters))
	for i, filter := range filters {
		sel := float64(e.estimateSingleFilter(table, filter)) / float64(baseRowCount)
		selectivities[i] = sel
	}

	// 考虑相关性调整选择率
	for i := 0; i < len(selectivities); i++ {
		for j := i + 1; j < len(selectivities); j++ {
			correlation := e.getCorrelation(table, filters[i].Field, filters[j].Field)
			// 正相关性：减少选择率
			// 负相关性：增加选择率
			if correlation > 0.5 {
				selectivities[j] *= (1.0 - correlation*0.3)
			} else if correlation < -0.5 {
				selectivities[j] *= (1.0 + math.Abs(correlation)*0.2)
			}
		}
	}

	// 应用调整后的选择率
	for _, sel := range selectivities {
		totalSelectivity *= sel
	}

	// 确保最小值
	if totalSelectivity < 0.0001 {
		totalSelectivity = 0.0001
	}

	return int64(float64(baseRowCount) * totalSelectivity)
}

// estimateORFilters 估算OR逻辑组合的选择率
func (e *EnhancedCardinalityEstimator) estimateORFilters(table string, filters []domain.Filter) int64 {
	baseRowCount := e.EstimateTableScan(table)

	// OR选择率 = 1 - (1-s1) * (1-s2) * ... * (1-sn)
	totalSelectivity := 1.0
	for _, filter := range filters {
		sel := float64(e.estimateSingleFilter(table, filter)) / float64(baseRowCount)
		totalSelectivity *= (1.0 - sel)
	}

	resultSelectivity := 1.0 - totalSelectivity
	if resultSelectivity > 0.95 {
		resultSelectivity = 0.95
	}

	return int64(float64(baseRowCount) * resultSelectivity)
}

// EstimateJoin 使用增强算法估算JOIN基数
func (e *EnhancedCardinalityEstimator) EstimateJoin(left, right LogicalPlan, joinType JoinType) int64 {
	// 获取左右表的基数
	leftCount := e.estimateRowCount(left)
	rightCount := e.estimateRowCount(right)

	if leftCount == 0 || rightCount == 0 {
		return 0
	}

	// 获取连接条件
	leftTable := e.getTableName(left)
	rightTable := e.getTableName(right)

	// 检查是否是外键关系
	if e.isForeignKey(leftTable, rightTable) {
		return e.estimateFKJoin(leftCount, rightCount, joinType)
	}

	// 使用标准JOIN估算
	return e.estimateStandardJoin(leftCount, rightCount, joinType)
}

// estimateStandardJoin 标准JOIN估算（考虑连接键NDV）
func (e *EnhancedCardinalityEstimator) estimateStandardJoin(leftCount, rightCount int64, joinType JoinType) int64 {
	// 假设连接键的选择性
	// 简化：使用较小的NDV作为估计
	avgNDV := math.Min(float64(leftCount), float64(rightCount))
	if avgNDV > 0 {
		// 假设连接键的NDV是行数的平方根（保守估计）
		joinKeyNDV := math.Sqrt(avgNDV)
		selectivity := 1.0 / joinKeyNDV
		if selectivity > 0.3 {
			selectivity = 0.3
		}
		if selectivity < 0.01 {
			selectivity = 0.01
		}

		estimated := int64(float64(leftCount) * float64(rightCount) * selectivity)
		return e.adjustForJoinType(estimated, leftCount, rightCount, joinType)
	}

	return e.adjustForJoinType(leftCount*rightCount/100, leftCount, rightCount, joinType)
}

// estimateFKJoin 外键JOIN估算
func (e *EnhancedCardinalityEstimator) estimateFKJoin(leftCount, rightCount int64, joinType JoinType) int64 {
	// 外键JOIN：假设每个外键值对应1个主键值
	estimated := leftCount
	return e.adjustForJoinType(estimated, leftCount, rightCount, joinType)
}

// adjustForJoinType 根据JOIN类型调整基数
func (e *EnhancedCardinalityEstimator) adjustForJoinType(estimated, leftCount, rightCount int64, joinType JoinType) int64 {
	switch joinType {
	case InnerJoin:
		return estimated
	case LeftOuterJoin:
		// LEFT JOIN: 至少返回左表所有行
		if estimated < leftCount {
			return leftCount
		}
		return estimated
	case RightOuterJoin:
		// RIGHT JOIN: 至少返回右表所有行
		if estimated < rightCount {
			return rightCount
		}
		return estimated
	case FullOuterJoin:
		// FULL JOIN: 左表 + 右表 - 交集
		union := leftCount + rightCount - estimated
		if union < leftCount || union < rightCount {
			return leftCount + rightCount
		}
		return union
	case SemiJoin:
		// SEMI JOIN: 返回匹配的左表行
		if estimated > leftCount {
			return leftCount
		}
		return estimated
	case AntiSemiJoin:
		// ANTI SEMI JOIN: 返回不匹配的左表行
		unmatched := leftCount - estimated
		if unmatched < 0 {
			return 0
		}
		return unmatched
	default:
		return estimated
	}
}

// EstimateDistinct 估算DISTINCT后的行数（使用增强统计）
func (e *EnhancedCardinalityEstimator) EstimateDistinct(table string, columns []string) int64 {
	if stats, exists := e.stats[table]; exists {
		if len(columns) == 1 {
			// 单列DISTINCT：直接使用列的NDV
			if histogram, ok := stats.Histograms[columns[0]]; ok {
				return histogram.TotalNDV
			}
		}

		// 多列DISTINCT：估算组合NDV
		return e.estimateMultiColumnDistinct(table, columns)
	}

	// 回退到基础估算
	return e.baseEstimator.EstimateDistinct(table, columns)
}

// estimateMultiColumnDistinct 估算多列组合的NDV
func (e *EnhancedCardinalityEstimator) estimateMultiColumnDistinct(table string, columns []string) int64 {
	if len(columns) == 0 {
		return e.EstimateTableScan(table)
	}

	stats, exists := e.stats[table]
	if !exists {
		return e.baseEstimator.EstimateDistinct(table, columns)
	}

	// 使用最小NDV作为保守估计
	minNDV := int64(math.MaxInt64)
	for _, col := range columns {
		if histogram, ok := stats.Histograms[col]; ok && histogram.TotalNDV > 0 {
			if histogram.TotalNDV < minNDV {
				minNDV = histogram.TotalNDV
			}
		}
	}

	if minNDV == math.MaxInt64 {
		return e.baseEstimator.EstimateDistinct(table, columns)
	}

	return minNDV
}

// UpdateStatistics 更新增强统计信息
func (e *EnhancedCardinalityEstimator) UpdateStatistics(tableName string, stats *TableStatistics) {
	e.baseEstimator.UpdateStatistics(tableName, stats)

	// 更新增强统计信息
	enhancedStats := &EnhancedTableStatistics{
		Base:         stats,
		Histograms:   make(map[string]*Histogram),
		Correlations: make(map[string]float64),
	}

	// 为每列创建直方图
	for colName, colStats := range stats.ColumnStats {
		histogram := e.createSimpleHistogram(colName, colStats)
		enhancedStats.Histograms[colName] = histogram
	}

	e.stats[tableName] = enhancedStats
}

// createSimpleHistogram 创建简单直方图
func (e *EnhancedCardinalityEstimator) createSimpleHistogram(colName string, colStats *ColumnStatistics) *Histogram {
	return &Histogram{
		ColumnName:  colName,
		Buckets:     []*HistogramBucket{},
		BucketCount: 10, // 默认10个桶
		IsEquiDepth: false,
		TotalNDV:    colStats.DistinctCount,
		SampleCount: colStats.DistinctCount * 10, // 假设
		NullCount:   colStats.NullCount,
	}
}

// Helper methods

func (e *EnhancedCardinalityEstimator) estimateRowCount(plan LogicalPlan) int64 {
	return e.baseEstimator.EstimateTableScan(e.getTableName(plan))
}

func (e *EnhancedCardinalityEstimator) getTableName(plan LogicalPlan) string {
	if dataSource, ok := plan.(*LogicalDataSource); ok {
		return dataSource.TableName
	}
	if join, ok := plan.(*LogicalJoin); ok {
		if join.LeftTable != "" {
			return join.LeftTable
		}
	}
	return ""
}

func (e *EnhancedCardinalityEstimator) getCorrelation(table, col1, col2 string) float64 {
	if correlations, ok := e.correlations[table]; ok {
		if corr, exists := correlations[col1+"."+col2]; exists {
			return corr
		}
		if corr, exists := correlations[col2+"."+col1]; exists {
			return corr
		}
	}
	return 0.0
}

func (e *EnhancedCardinalityEstimator) isForeignKey(table1, table2 string) bool {
	key := table1 + "->" + table2
	if _, exists := e.fkRelations[key]; exists {
		return true
	}
	return false
}

func (e *EnhancedCardinalityEstimator) valueInRange(value interface{}, lower, upper interface{}) bool {
	valueFloat, _ := e.toFloat64(value)
	lowerFloat, _ := e.toFloat64(lower)
	upperFloat, _ := e.toFloat64(upper)

	if math.IsNaN(valueFloat) || math.IsNaN(lowerFloat) || math.IsNaN(upperFloat) {
		return false
	}

	return valueFloat >= lowerFloat && valueFloat < upperFloat
}

func (e *EnhancedCardinalityEstimator) toFloat64(value interface{}) (float64, bool) {
	if value == nil {
		return 0, false
	}

	switch v := value.(type) {
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case int:
		return float64(v), true
	case int64:
		return float64(v), true
	case int32:
		return float64(v), true
	default:
		return math.NaN(), false
	}
}

func (e *EnhancedCardinalityEstimator) rangesOverlap(start1, end1, start2, end2 float64) bool {
	return !(end1 <= start2 || start1 >= end2)
}

// Explain 解释增强基数估算器
func (e *EnhancedCardinalityEstimator) Explain() string {
	return fmt.Sprintf(
		"EnhancedCardinalityEstimator(tables=%d, correlations=%d, fk_relations=%d)",
		len(e.stats),
		len(e.correlations),
		len(e.fkRelations),
	)
}

// estimateJoin 估算JOIN基数（供optimizerCardinalityAdapter使用）
func (e *EnhancedCardinalityEstimator) estimateJoin(leftTable, rightTable string, joinType JoinType, leftCard, rightCard int64) int64 {
	// 检查是否是外键关系
	if e.isForeignKey(leftTable, rightTable) {
		return e.adjustForJoinType(leftCard, leftCard, rightCard, joinType)
	}

	// 检查是否可以获取连接键NDV
	// 简化：使用标准JOIN估算
	return e.estimateStandardJoin(leftCard, rightCard, joinType)
}

// estimateDistinct 估算DISTINCT基数（供optimizerCardinalityAdapter使用）
func (e *EnhancedCardinalityEstimator) estimateDistinct(table string, columns []string, totalRows int64) int64 {
	if len(columns) == 1 {
		// 单列DISTINCT：尝试使用直方图
		if stats, exists := e.stats[table]; exists {
			if histogram, ok := stats.Histograms[columns[0]]; ok && histogram.TotalNDV > 0 {
				return histogram.TotalNDV
			}
		}
	}

	// 多列DISTINCT或无直方图：估算组合NDV
	minNDV := totalRows
	for _, col := range columns {
		if stats, exists := e.stats[table]; exists {
			if histogram, ok := stats.Histograms[col]; ok && histogram.TotalNDV > 0 {
				if histogram.TotalNDV < minNDV {
					minNDV = histogram.TotalNDV
				}
			}
		}
	}

	return minNDV
}
