package optimizer

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// OptimizedAggregate 优化的物理聚合算子
// 基于 DuckDB Perfect Hash Aggregation 和 TiDB 聚合优化策略
type OptimizedAggregate struct {
	AggFuncs    []*AggregationItem
	GroupByCols []string
	cost        float64
	children    []PhysicalPlan
}

// NewOptimizedAggregate 创建优化的聚合算子
func NewOptimizedAggregate(aggFuncs []*AggregationItem, groupByCols []string, child PhysicalPlan) *OptimizedAggregate {
	inputRows := int64(1000) // 假设
	groupCost := float64(inputRows) * float64(len(groupByCols)) * 0.02 // 降低成本估计
	aggCost := float64(inputRows) * float64(len(aggFuncs)) * 0.01
	cost := child.Cost() + groupCost + aggCost

	return &OptimizedAggregate{
		AggFuncs:    aggFuncs,
		GroupByCols:  groupByCols,
		cost:        cost,
		children:    []PhysicalPlan{child},
	}
}

// Children 获取子节点
func (p *OptimizedAggregate) Children() []PhysicalPlan {
	return p.children
}

// SetChildren 设置子节点
func (p *OptimizedAggregate) SetChildren(children ...PhysicalPlan) {
	p.children = children
}

// Schema 返回输出列
func (p *OptimizedAggregate) Schema() []ColumnInfo {
	columns := []ColumnInfo{}

	// 添加 GROUP BY 列
	for _, col := range p.GroupByCols {
		columns = append(columns, ColumnInfo{
			Name:     col,
			Type:     "unknown",
			Nullable: true,
		})
	}

	// 添加聚合函数列
	for _, agg := range p.AggFuncs {
		name := agg.Alias
		if name == "" {
			name = fmt.Sprintf("%s(%v)", agg.Type, agg.Expr)
		}
		columns = append(columns, ColumnInfo{
			Name:     name,
			Type:     "unknown",
			Nullable: true,
		})
	}

	return columns
}

// Cost 返回执行成本
func (p *OptimizedAggregate) Cost() float64 {
	return p.cost
}

// Execute 执行优化的聚合
// DEPRECATED: 执行逻辑已迁移到 pkg/executor 包，此方法保留仅为兼容性
func (p *OptimizedAggregate) Execute(ctx context.Context) (*domain.QueryResult, error) {
	return nil, fmt.Errorf("OptimizedAggregate.Execute is deprecated. Please use pkg/executor instead")
}

// canUsePerfectHash 检查是否可以使用 Perfect Hash Aggregation
func (p *OptimizedAggregate) canUsePerfectHash(input *domain.QueryResult, colName string) bool {
	if len(input.Rows) == 0 {
		return false
	}

	// 检查列是否为整数类型
	for _, col := range input.Columns {
		if col.Name == colName {
			return col.Type == "int" || col.Type == "int64" || col.Type == "integer"
		}
	}

	return false
}

// executePerfectHashAggregate 执行 Perfect Hash Aggregation
// 适用于整数列分组，性能提升 10-100 倍
func (p *OptimizedAggregate) executePerfectHashAggregate(input *domain.QueryResult) (*domain.QueryResult, error) {
	colName := p.GroupByCols[0]

	// 第一步：找到最小值和最大值
	minVal, maxVal, err := p.findMinMax(input, colName)
	if err != nil {
		return nil, err
	}

	minInt, ok := minVal.(int64)
	if !ok {
		// 转换失败，回退到普通 Hash Aggregation
		return p.executeHashAggregate(input)
	}

	maxInt, ok := maxVal.(int64)
	if !ok {
		return p.executeHashAggregate(input)
	}

	// 计算范围
	rangeSize := maxInt - minInt + 1

	// 如果范围太大（> 1,000,000），回退到普通 Hash Aggregation
	if rangeSize > 1000000 {
		return p.executeHashAggregate(input)
	}

	// 第二步：使用数组作为哈希表
	groups := make([]*aggregateState, rangeSize)
	valid := make([]bool, rangeSize)

	// 第三步：单次遍历计算聚合
	for _, row := range input.Rows {
		val, exists := row[colName]
		if !exists {
			continue
		}

		intVal, ok := val.(int64)
		if !ok {
			continue
		}

		// 计算数组索引
		idx := int(intVal - minInt)
		if idx < 0 || idx >= len(groups) {
			continue
		}

		// 获取或创建聚合状态
		if !valid[idx] {
			groups[idx] = &aggregateState{
				key:      intVal,
				count:    0,
				sum:      0.0,
				avgSum:   0.0,
				avgCount: 0,
				minVal:   nil,
				maxVal:   nil,
			}
			valid[idx] = true
		}

		// 更新聚合状态
		p.updateAggregateState(groups[idx], row)
	}

	// 第四步：构建结果
	output := []domain.Row{}
	for idx, isValid := range valid {
		if !isValid {
			continue
		}

		row := make(domain.Row)

		// 添加 GROUP BY 列
		row[colName] = groups[idx].key

		// 计算聚合函数
		for _, agg := range p.AggFuncs {
			result := p.calculateFinalAggregation(agg, groups[idx])
			colName := agg.Alias
			if colName == "" {
				colName = fmt.Sprintf("%s(%v)", agg.Type, agg.Expr)
			}
			row[colName] = result
		}

		output = append(output, row)
	}

	// 构建列信息
	columns := []domain.ColumnInfo{}
	for _, colName := range p.GroupByCols {
		columns = append(columns, domain.ColumnInfo{
			Name:     colName,
			Type:     "unknown",
			Nullable: true,
		})
	}
	for _, agg := range p.AggFuncs {
		colName := agg.Alias
		if colName == "" {
			colName = fmt.Sprintf("%s(%v)", agg.Type, agg.Expr)
		}
		columns = append(columns, domain.ColumnInfo{
			Name:     colName,
			Type:     "unknown",
			Nullable: true,
		})
	}

	return &domain.QueryResult{
		Columns: columns,
		Rows:    output,
		Total:   int64(len(output)),
	}, nil
}

// executeHashAggregate 执行优化的 Hash Aggregation
// 不存储所有行，只维护聚合状态
func (p *OptimizedAggregate) executeHashAggregate(input *domain.QueryResult) (*domain.QueryResult, error) {
	groups := make(map[string]*aggregateState)

	// 单次遍历计算聚合
	for _, row := range input.Rows {
		// 构建分组键 - 使用更高效的方式
		key := p.buildGroupKey(row)

		// 获取或创建聚合状态
		group, exists := groups[key]
		if !exists {
			group = &aggregateState{
				keyValues: make([]interface{}, len(p.GroupByCols)),
				count:     0,
				sum:       0.0,
				avgSum:    0.0,
				avgCount:  0,
				minVal:    nil,
				maxVal:    nil,
			}
			// 保存分组键值
			for i, colName := range p.GroupByCols {
				group.keyValues[i] = row[colName]
			}
			groups[key] = group
		}

		// 更新聚合状态
		p.updateAggregateState(group, row)
	}

	// 构建结果
	output := make([]domain.Row, 0, len(groups))
	for _, group := range groups {
		row := make(domain.Row)

		// 添加 GROUP BY 列
		for i, colName := range p.GroupByCols {
			if i < len(group.keyValues) {
				row[colName] = group.keyValues[i]
			}
		}

		// 计算聚合函数
		for _, agg := range p.AggFuncs {
			result := p.calculateFinalAggregation(agg, group)
			colName := agg.Alias
			if colName == "" {
				colName = fmt.Sprintf("%s(%v)", agg.Type, agg.Expr)
			}
			row[colName] = result
		}

		output = append(output, row)
	}

	// 构建列信息
	columns := []domain.ColumnInfo{}
	for _, colName := range p.GroupByCols {
		columns = append(columns, domain.ColumnInfo{
			Name:     colName,
			Type:     "unknown",
			Nullable: true,
		})
	}
	for _, agg := range p.AggFuncs {
		colName := agg.Alias
		if colName == "" {
			colName = fmt.Sprintf("%s(%v)", agg.Type, agg.Expr)
		}
		columns = append(columns, domain.ColumnInfo{
			Name:     colName,
			Type:     "unknown",
			Nullable: true,
		})
	}

	return &domain.QueryResult{
		Columns: columns,
		Rows:    output,
		Total:   int64(len(output)),
	}, nil
}

// buildGroupKey 构建分组键（优化版本）
func (p *OptimizedAggregate) buildGroupKey(row domain.Row) string {
	if len(p.GroupByCols) == 0 {
		return ""
	}

	// 单列分组，直接返回值作为字符串
	if len(p.GroupByCols) == 1 {
		val := row[p.GroupByCols[0]]
		return fmt.Sprintf("%v", val)
	}

	// 多列分组，构建组合键
	keyBuilder := getStringBuilder()
	defer putStringBuilder(keyBuilder)

	for i, colName := range p.GroupByCols {
		if i > 0 {
			keyBuilder.WriteString("|")
		}
		keyBuilder.WriteString(fmt.Sprintf("%v", row[colName]))
	}

	return keyBuilder.String()
}

// findMinMax 找到列的最小值和最大值
func (p *OptimizedAggregate) findMinMax(input *domain.QueryResult, colName string) (min, max interface{}, err error) {
	if len(input.Rows) == 0 {
		return nil, nil, fmt.Errorf("no rows")
	}

	min = input.Rows[0][colName]
	max = input.Rows[0][colName]

	for _, row := range input.Rows[1:] {
		val := row[colName]
		if p.compareValues(val, min) < 0 {
			min = val
		}
		if p.compareValues(val, max) > 0 {
			max = val
		}
	}

	return min, max, nil
}

// compareValues 比较两个值
func (p *OptimizedAggregate) compareValues(a, b interface{}) int {
	aInt, ok1 := a.(int64)
	bInt, ok2 := b.(int64)
	if ok1 && ok2 {
		if aInt < bInt {
			return -1
		} else if aInt > bInt {
			return 1
		}
		return 0
	}

	// 简化的字符串比较
	aStr := fmt.Sprintf("%v", a)
	bStr := fmt.Sprintf("%v", b)
	if aStr < bStr {
		return -1
	} else if aStr > bStr {
		return 1
	}
	return 0
}

// updateAggregateState 更新聚合状态
func (p *OptimizedAggregate) updateAggregateState(state *aggregateState, row domain.Row) {
	// 更新每个聚合函数
	hasCountFunc := false
	for _, agg := range p.AggFuncs {
		if agg.Type == Count {
			hasCountFunc = true
			break
		}
	}

	// 只在有 COUNT 聚合函数时更新一次 count
	if hasCountFunc {
		state.count++
	}

	// 更新每个聚合函数
	for _, agg := range p.AggFuncs {
		colName := agg.Expr.Column
		if colName == "" {
			continue
		}

		val, exists := row[colName]
		if !exists {
			continue
		}

		switch agg.Type {
		case Sum:
			if numVal, ok := p.toFloat64(val); ok {
				state.sum += numVal
			}
		case Avg:
			if numVal, ok := p.toFloat64(val); ok {
				state.avgSum += numVal
				state.avgCount++
			}
		case Min:
			if state.minVal == nil || p.compareValues(val, state.minVal) < 0 {
				state.minVal = val
			}
		case Max:
			if state.maxVal == nil || p.compareValues(val, state.maxVal) > 0 {
				state.maxVal = val
			}
		}
	}
}

// calculateFinalAggregation 计算最终的聚合值
func (p *OptimizedAggregate) calculateFinalAggregation(agg *AggregationItem, state *aggregateState) interface{} {
	switch agg.Type {
	case Count:
		return state.count
	case Sum:
		return state.sum
	case Avg:
		if state.avgCount > 0 {
			return state.avgSum / float64(state.avgCount)
		}
		return 0.0
	case Min:
		return state.minVal
	case Max:
		return state.maxVal
	default:
		return nil
	}
}

// toFloat64 将值转换为 float64
func (p *OptimizedAggregate) toFloat64(val interface{}) (float64, bool) {
	switch v := val.(type) {
	case int:
		return float64(v), true
	case int64:
		return float64(v), true
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case string:
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f, true
		}
	}
	return 0, false
}

// aggregateState 聚合状态（不存储所有行）
type aggregateState struct {
	key       interface{}      // Perfect Hash 时的单列键
	keyValues []interface{}    // 多列键值
	count     int64            // COUNT
	sum       float64          // SUM 总和
	avgSum    float64          // AVG 的总和
	avgCount  int64            // AVG 的计数
	minVal    interface{}      // MIN
	maxVal    interface{}      // MAX
}

// Explain 返回计划说明
func (p *OptimizedAggregate) Explain() string {
	aggStr := ""
	for i, agg := range p.AggFuncs {
		if i > 0 {
			aggStr += ", "
		}
		aggStr += fmt.Sprintf("%s(%v)", agg.Type, agg.Expr)
	}

	groupStr := ""
	if len(p.GroupByCols) > 0 {
		groupStr = " GROUP BY "
		for i, col := range p.GroupByCols {
			if i > 0 {
				groupStr += ", "
			}
			groupStr += col
		}
	}

	return fmt.Sprintf("OptimizedAggregate(%s%s)", aggStr, groupStr)
}

// StringBuilder pool for efficient string concatenation
var stringBuilderPool sync.Pool

func init() {
	stringBuilderPool = sync.Pool{
		New: func() interface{} {
			return &strings.Builder{}
		},
	}
}

func getStringBuilder() *strings.Builder {
	sb := stringBuilderPool.Get().(*strings.Builder)
	sb.Reset()
	return sb
}

func putStringBuilder(sb *strings.Builder) {
	stringBuilderPool.Put(sb)
}
