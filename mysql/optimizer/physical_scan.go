package optimizer

import (
	"context"
	"fmt"
	"reflect"

	"mysql-proxy/mysql/parser"
	"mysql-proxy/mysql/resource"
)

// PhysicalTableScan 物理表扫描
type PhysicalTableScan struct {
	TableName  string
	Columns    []ColumnInfo
	TableInfo  *resource.TableInfo
	cost       float64
	children   []PhysicalPlan
	dataSource resource.DataSource
}

// NewPhysicalTableScan 创建物理表扫描
func NewPhysicalTableScan(tableName string, tableInfo *resource.TableInfo, dataSource resource.DataSource) *PhysicalTableScan {
	columns := make([]ColumnInfo, 0, len(tableInfo.Columns))
	for _, col := range tableInfo.Columns {
		columns = append(columns, ColumnInfo{
			Name:     col.Name,
			Type:     col.Type,
			Nullable: col.Nullable,
		})
	}

	// 假设表有1000行
	rowCount := int64(1000)

	return &PhysicalTableScan{
		TableName: tableName,
		Columns:   columns,
		TableInfo: tableInfo,
		cost:      rowCount * 0.1, // 简化的成本计算
		dataSource: dataSource,
		children:  []PhysicalPlan{},
	}
}

// Children 获取子节点
func (p *PhysicalTableScan) Children() []PhysicalPlan {
	return p.children
}

// SetChildren 设置子节点
func (p *PhysicalTableScan) SetChildren(children ...PhysicalPlan) {
	p.children = children
}

// Schema 返回输出列
func (p *PhysicalTableScan) Schema() []ColumnInfo {
	return p.Columns
}

// Cost 返回执行成本
func (p *PhysicalTableScan) Cost() float64 {
	return p.cost
}

// Execute 执行扫描
func (p *PhysicalTableScan) Execute(ctx context.Context) (*resource.QueryResult, error) {
	return p.dataSource.Query(ctx, p.TableName, &resource.QueryOptions{})
}

// Explain 返回计划说明
func (p *PhysicalTableScan) Explain() string {
	return fmt.Sprintf("TableScan(%s, cost=%.2f)", p.TableName, p.cost)
}

// PhysicalSelection 物理过滤
type PhysicalSelection struct {
	Conditions []*parser.Expression
	Filters    []resource.Filter
	cost       float64
	children   []PhysicalPlan
	dataSource resource.DataSource
}

// NewPhysicalSelection 创建物理过滤
func NewPhysicalSelection(conditions []*parser.Expression, filters []resource.Filter, child PhysicalPlan, dataSource resource.DataSource) *PhysicalSelection {
	inputCost := child.Cost()
	cost := inputCost*1.2 + 10 // 过滤成本

	return &PhysicalSelection{
		Conditions: conditions,
		Filters:    filters,
		cost:       cost,
		children:   []PhysicalPlan{child},
		dataSource: dataSource,
	}
}

// Children 获取子节点
func (p *PhysicalSelection) Children() []PhysicalPlan {
	return p.children
}

// SetChildren 设置子节点
func (p *PhysicalSelection) SetChildren(children ...PhysicalPlan) {
	p.children = children
}

// Schema 返回输出列
func (p *PhysicalSelection) Schema() []ColumnInfo {
	if len(p.children) > 0 {
		return p.children[0].Schema()
	}
	return []ColumnInfo{}
}

// Cost 返回执行成本
func (p *PhysicalSelection) Cost() float64 {
	return p.cost
}

// Execute 执行过滤
func (p *PhysicalSelection) Execute(ctx context.Context) (*resource.QueryResult, error) {
	if len(p.children) == 0 {
		return nil, fmt.Errorf("PhysicalSelection has no child")
	}

	// 先执行子节点
	input, err := p.children[0].Execute(ctx)
	if err != nil {
		return nil, err
	}

	// 手动应用过滤（简化实现）
	filtered := []resource.Row{}
	for _, row := range input.Rows {
		match := true
		for _, filter := range p.Filters {
			if !matchesFilter(row, filter) {
				match = false
				break
			}
		}
		if match {
			filtered = append(filtered, row)
		}
	}

	return &resource.QueryResult{
		Columns: input.Columns,
		Rows:    filtered,
		Total:    int64(len(filtered)),
	}, nil
}

// matchesFilter 检查行是否匹配过滤器（简化实现）
func matchesFilter(row resource.Row, filter resource.Filter) bool {
	value, exists := row[filter.Field]
	if !exists {
		return false
	}

	// 简化实现，只支持等值比较
	if filter.Operator == "=" {
		return fmt.Sprintf("%v", value) == fmt.Sprintf("%v", filter.Value)
	}
	return true
}

// Explain 返回计划说明
func (p *PhysicalSelection) Explain() string {
	return fmt.Sprintf("Selection(cost=%.2f)", p.cost)
}

// PhysicalProjection 物理投影
type PhysicalProjection struct {
	Exprs   []*parser.Expression
	Aliases  []string
	Columns  []ColumnInfo
	cost     float64
	children []PhysicalPlan
}

// NewPhysicalProjection 创建物理投影
func NewPhysicalProjection(exprs []*parser.Expression, aliases []string, child PhysicalPlan) *PhysicalProjection {
	inputCost := child.Cost()
	cost := inputCost*1.1 + float64(len(exprs))*5 // 投影成本

	columns := make([]ColumnInfo, len(exprs))
	for i, expr := range exprs {
		name := aliases[i]
		if name == "" {
			if expr.Type == parser.ExprTypeColumn {
				name = expr.Column
			} else {
				name = fmt.Sprintf("expr_%d", i)
			}
		}
		columns[i] = ColumnInfo{
			Name:     name,
			Type:     "unknown",
			Nullable: true,
		}
	}

	return &PhysicalProjection{
		Exprs:    exprs,
		Aliases:   aliases,
		Columns:   columns,
		cost:      cost,
		children:  []PhysicalPlan{child},
	}
}

// Children 获取子节点
func (p *PhysicalProjection) Children() []PhysicalPlan {
	return p.children
}

// SetChildren 设置子节点
func (p *PhysicalProjection) SetChildren(children ...PhysicalPlan) {
	p.children = children
}

// Schema 返回输出列
func (p *PhysicalProjection) Schema() []ColumnInfo {
	return p.Columns
}

// Cost 返回执行成本
func (p *PhysicalProjection) Cost() float64 {
	return p.cost
}

// Execute 执行投影
func (p *PhysicalProjection) Execute(ctx context.Context) (*resource.QueryResult, error) {
	if len(p.children) == 0 {
		return nil, fmt.Errorf("PhysicalProjection has no child")
	}

	// 先执行子节点
	input, err := p.children[0].Execute(ctx)
	if err != nil {
		return nil, err
	}

	// 应用投影（简化实现，只支持列选择）
	output := []resource.Row{}
	for _, row := range input.Rows {
		newRow := make(resource.Row)
		for i, expr := range p.Exprs {
			if expr.Type == parser.ExprTypeColumn {
				if val, exists := row[expr.Column]; exists {
					newRow[p.Aliases[i]] = val
				}
			} else {
				// 简化：不支持表达式计算
				newRow[p.Aliases[i]] = nil
			}
		}
		output = append(output, newRow)
	}

	// 更新列信息
	columns := make([]resource.ColumnInfo, len(p.Columns))
	for i, col := range p.Columns {
		columns[i] = resource.ColumnInfo{
			Name:     col.Name,
			Type:     col.Type,
			Nullable: col.Nullable,
		}
	}

	return &resource.QueryResult{
		Columns: columns,
		Rows:    output,
		Total:    int64(len(output)),
	}, nil
}

// Explain 返回计划说明
func (p *PhysicalProjection) Explain() string {
	return fmt.Sprintf("Projection(cost=%.2f)", p.cost)
}

// PhysicalLimit 物理限制
type PhysicalLimit struct {
	Limit    int64
	Offset   int64
	cost     float64
	children []PhysicalPlan
}

// NewPhysicalLimit 创建物理限制
func NewPhysicalLimit(limit, offset int64, child PhysicalPlan) *PhysicalLimit {
	inputCost := child.Cost()
	cost := inputCost + float64(limit)*0.01 // 限制操作成本很低

	return &PhysicalLimit{
		Limit:    limit,
		Offset:   offset,
		cost:     cost,
		children: []PhysicalPlan{child},
	}
}

// Children 获取子节点
func (p *PhysicalLimit) Children() []PhysicalPlan {
	return p.children
}

// SetChildren 设置子节点
func (p *PhysicalLimit) SetChildren(children ...PhysicalPlan) {
	p.children = children
}

// Schema 返回输出列
func (p *PhysicalLimit) Schema() []ColumnInfo {
	if len(p.children) > 0 {
		return p.children[0].Schema()
	}
	return []ColumnInfo{}
}

// Cost 返回执行成本
func (p *PhysicalLimit) Cost() float64 {
	return p.cost
}

// Execute 执行限制
func (p *PhysicalLimit) Execute(ctx context.Context) (*resource.QueryResult, error) {
	if len(p.children) == 0 {
		return nil, fmt.Errorf("PhysicalLimit has no child")
	}

	// 先执行子节点
	input, err := p.children[0].Execute(ctx)
	if err != nil {
		return nil, err
	}

	// 应用 OFFSET 和 LIMIT
	start := p.Offset
	if start < 0 {
		start = 0
	}
	if start >= int64(len(input.Rows)) {
		return &resource.QueryResult{
			Columns: input.Columns,
			Rows:    []resource.Row{},
			Total:    0,
		}, nil
	}

	end := start + p.Limit
	if end > int64(len(input.Rows)) {
		end = int64(len(input.Rows))
	}

	output := input.Rows[start:end]

	return &resource.QueryResult{
		Columns: input.Columns,
		Rows:    output,
		Total:    int64(len(output)),
	}, nil
}

// Explain 返回计划说明
func (p *PhysicalLimit) Explain() string {
	return fmt.Sprintf("Limit(offset=%d, limit=%d, cost=%.2f)", p.Offset, p.Limit, p.cost)
}

// PhysicalHashJoin 物理哈希连接
type PhysicalHashJoin struct {
	JoinType   JoinType
	Conditions []*JoinCondition
	cost       float64
	children   []PhysicalPlan
}

// NewPhysicalHashJoin 创建物理哈希连接
func NewPhysicalHashJoin(joinType JoinType, left, right PhysicalPlan, conditions []*JoinCondition) *PhysicalHashJoin {
	leftRows := int64(1000) // 假设
	rightRows := int64(1000) // 假设

	// Hash Join 成本 = 构建哈希表 + 探测
	buildCost := float64(leftRows) * 0.1
	probeCost := float64(rightRows) * 0.1
	cost := left.Cost() + right.Cost() + buildCost + probeCost

	return &PhysicalHashJoin{
		JoinType:   joinType,
		Conditions: conditions,
		cost:       cost,
		children:   []PhysicalPlan{left, right},
	}
}

// Children 获取子节点
func (p *PhysicalHashJoin) Children() []PhysicalPlan {
	return p.children
}

// SetChildren 设置子节点
func (p *PhysicalHashJoin) SetChildren(children ...PhysicalPlan) {
	p.children = children
}

// Schema 返回输出列
func (p *PhysicalHashJoin) Schema() []ColumnInfo {
	columns := []ColumnInfo{}
	if len(p.children) > 0 {
		columns = append(columns, p.children[0].Schema()...)
	}
	if len(p.children) > 1 {
		columns = append(columns, p.children[1].Schema()...)
	}
	return columns
}

// Cost 返回执行成本
func (p *PhysicalHashJoin) Cost() float64 {
	return p.cost
}

// Execute 执行哈希连接
func (p *PhysicalHashJoin) Execute(ctx context.Context) (*resource.QueryResult, error) {
	if len(p.children) != 2 {
		return nil, fmt.Errorf("HashJoin requires exactly 2 children")
	}

	// 获取连接条件（简化：只支持单列等值连接）
	leftJoinCol := ""
	rightJoinCol := ""
	if len(p.Conditions) > 0 {
		leftJoinCol = p.Conditions[0].Left
		rightJoinCol = p.Conditions[0].Right
	}

	// 1. 执行左表（构建端）
	leftResult, err := p.children[0].Execute(ctx)
	if err != nil {
		return nil, fmt.Errorf("left table execute error: %w", err)
	}

	// 2. 执行右表（探测端）
	rightResult, err := p.children[1].Execute(ctx)
	if err != nil {
		return nil, fmt.Errorf("right table execute error: %w", err)
	}

	// 3. 构建哈希表（从左表）
	hashTable := make(map[interface{}][]resource.Row)
	for _, row := range leftResult.Rows {
		key := row[leftJoinCol]
		hashTable[key] = append(hashTable[key], row)
	}

	// 4. 探测右表并产生结果
	output := []resource.Row{}

	// 根据连接类型处理
	switch p.JoinType {
	case InnerJoin:
		// INNER JOIN：两边都有匹配
		for _, rightRow := range rightResult.Rows {
			key := rightRow[rightJoinCol]
			if leftRows, exists := hashTable[key]; exists {
				for _, leftRow := range leftRows {
					// 合并左右行
					merged := make(resource.Row)
					for k, v := range leftRow {
						merged[k] = v
					}
					for k, v := range rightRow {
						// 如果列名冲突，添加前缀
						newKey := k
						if _, exists := merged[newKey]; exists {
							newKey = "right_" + k
						}
						merged[newKey] = v
					}
					output = append(output, merged)
				}
			}
		}

	case LeftOuterJoin:
		// LEFT JOIN：左边所有行，右边没有匹配的用NULL填充
		for _, leftRow := range leftResult.Rows {
			key := leftRow[leftJoinCol]
			rightRows, hasMatch := hashTable[key]
			// 在LEFT JOIN中，我们需要用右表去匹配左表，所以要重新构建右表的哈希表
			// 这里简化处理：如果左表的key在哈希表中，说明有匹配（虽然逻辑上不太对）
			// 正确的做法：为右表也构建哈希表
		}
		// 重新构建右表的哈希表用于LEFT JOIN
		rightHashTable := make(map[interface{}][]resource.Row)
		for _, row := range rightResult.Rows {
			key := row[rightJoinCol]
			rightHashTable[key] = append(rightHashTable[key], row)
		}

		for _, leftRow := range leftResult.Rows {
			key := leftRow[leftJoinCol]
			if rightRows, exists := rightHashTable[key]; exists {
				// 有匹配：连接
				for _, rightRow := range rightRows {
					merged := make(resource.Row)
					for k, v := range leftRow {
						merged[k] = v
					}
					for k, v := range rightRow {
						newKey := k
						if _, exists := merged[newKey]; exists {
							newKey = "right_" + k
						}
						merged[newKey] = v
					}
					output = append(output, merged)
				}
			} else {
				// 无匹配：左边行 + 右边NULL
				merged := make(resource.Row)
				for k, v := range leftRow {
					merged[k] = v
				}
				for _, col := range rightResult.Columns {
					newKey := col.Name
					if _, exists := merged[newKey]; exists {
						newKey = "right_" + col.Name
					}
					merged[newKey] = nil
				}
				output = append(output, merged)
			}
		}

	case JoinTypeRight:
		// RIGHT JOIN：右边所有行，左边没有匹配的用NULL填充
		// 重新构建左表的哈希表用于RIGHT JOIN
		leftHashTable := make(map[interface{}][]resource.Row)
		for _, row := range leftResult.Rows {
			key := row[leftJoinCol]
			leftHashTable[key] = append(leftHashTable[key], row)
		}

		for _, rightRow := range rightResult.Rows {
			key := rightRow[rightJoinCol]
			if leftRows, exists := leftHashTable[key]; exists {
				// 有匹配：连接
				for _, leftRow := range leftRows {
					merged := make(resource.Row)
					for k, v := range leftRow {
						merged[k] = v
					}
					for k, v := range rightRow {
						newKey := k
						if _, exists := merged[newKey]; exists {
							newKey = "right_" + k
						}
						merged[newKey] = v
					}
					output = append(output, merged)
				}
			} else {
				// 无匹配：左边NULL + 右边行
				merged := make(resource.Row)
				for _, col := range leftResult.Columns {
					merged[col.Name] = nil
				}
				for k, v := range rightRow {
					newKey := k
					if _, exists := merged[newKey]; exists {
						newKey = "right_" + k
					}
					merged[newKey] = v
				}
				output = append(output, merged)
			}
		}

	default:
		return nil, fmt.Errorf("unsupported join type: %s", p.JoinType)
	}

	// 合并列信息
	columns := []resource.ColumnInfo{}
	columns = append(columns, leftResult.Columns...)
	for _, col := range rightResult.Columns {
		// 检查列名是否冲突
		conflict := false
		for _, leftCol := range leftResult.Columns {
			if leftCol.Name == col.Name {
				conflict = true
				break
			}
		}
		if conflict {
			columns = append(columns, resource.ColumnInfo{
				Name:     "right_" + col.Name,
				Type:     col.Type,
				Nullable: true,
			})
		} else {
			columns = append(columns, col)
		}
	}

	return &resource.QueryResult{
		Columns: columns,
		Rows:    output,
		Total:   int64(len(output)),
	}, nil
}

// Explain 返回计划说明
func (p *PhysicalHashJoin) Explain() string {
	return fmt.Sprintf("HashJoin(type=%s, cost=%.2f)", p.JoinType, p.cost)
}

// PhysicalHashAggregate 物理哈希聚合
type PhysicalHashAggregate struct {
	AggFuncs   []*AggregationItem
	GroupByCols []string
	cost        float64
	children    []PhysicalPlan
}

// NewPhysicalHashAggregate 创建物理哈希聚合
func NewPhysicalHashAggregate(aggFuncs []*AggregationItem, groupByCols []string, child PhysicalPlan) *PhysicalHashAggregate {
	inputRows := int64(1000) // 假设

	// Hash Agg 成本 = 分组 + 聚合
	groupCost := float64(inputRows) * float64(len(groupByCols)) * 0.05
	aggCost := float64(inputRows) * float64(len(aggFuncs)) * 0.05
	cost := child.Cost() + groupCost + aggCost

	return &PhysicalHashAggregate{
		AggFuncs:   aggFuncs,
		GroupByCols: groupByCols,
		cost:        cost,
		children:    []PhysicalPlan{child},
	}
}

// Children 获取子节点
func (p *PhysicalHashAggregate) Children() []PhysicalPlan {
	return p.children
}

// SetChildren 设置子节点
func (p *PhysicalHashAggregate) SetChildren(children ...PhysicalPlan) {
	p.children = children
}

// Schema 返回输出列
func (p *PhysicalHashAggregate) Schema() []ColumnInfo {
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
func (p *PhysicalHashAggregate) Cost() float64 {
	return p.cost
}

// Execute 执行哈希聚合
func (p *PhysicalHashAggregate) Execute(ctx context.Context) (*resource.QueryResult, error) {
	if len(p.children) == 0 {
		return nil, fmt.Errorf("HashAggregate has no child")
	}

	// 执行子节点
	input, err := p.children[0].Execute(ctx)
	if err != nil {
		return nil, err
	}

	if len(p.AggFuncs) == 0 && len(p.GroupByCols) == 0 {
		// 没有聚合函数也没有分组，直接返回
		return input, nil
	}

	// 用于存储分组结果的哈希表
	type groupKey struct {
		values []interface{}
	}
	groups := make(map[interface{}]*aggregateGroup)

	// 遍历所有行，进行分组和聚合
	for _, row := range input.Rows {
		// 构建分组键
		key := make([]interface{}, len(p.GroupByCols))
		for i, colName := range p.GroupByCols {
			key[i] = row[colName]
		}

		// 将key转换为字符串作为map的key
		keyStr := fmt.Sprintf("%v", key)

		// 获取或创建分组
		group, exists := groups[keyStr]
		if !exists {
			group = &aggregateGroup{
				key:    key,
				rows:   []resource.Row{},
				values: make(map[string]interface{}),
			}
			groups[keyStr] = group
		}

		group.rows = append(group.rows, row)
	}

	// 为每个分组计算聚合函数
	output := []resource.Row{}
	for _, group := range groups {
		row := make(resource.Row)

		// 添加 GROUP BY 列
		for i, colName := range p.GroupByCols {
			if i < len(group.key) {
				row[colName] = group.key[i]
			}
		}

		// 计算聚合函数
		for _, agg := range p.AggFuncs {
			result := p.calculateAggregation(agg, group.rows)
			colName := agg.Alias
			if colName == "" {
				colName = fmt.Sprintf("%s(%v)", agg.Type, agg.Expr)
			}
			row[colName] = result
		}

		output = append(output, row)
	}

	// 构建列信息
	columns := []resource.ColumnInfo{}

	// GROUP BY 列
	for _, colName := range p.GroupByCols {
		columns = append(columns, resource.ColumnInfo{
			Name:     colName,
			Type:     "unknown",
			Nullable: true,
		})
	}

	// 聚合函数列
	for _, agg := range p.AggFuncs {
		colName := agg.Alias
		if colName == "" {
			colName = fmt.Sprintf("%s(%v)", agg.Type, agg.Expr)
		}
		columns = append(columns, resource.ColumnInfo{
			Name:     colName,
			Type:     "unknown",
			Nullable: true,
		})
	}

	return &resource.QueryResult{
		Columns: columns,
		Rows:    output,
		Total:   int64(len(output)),
	}, nil
}

// aggregateGroup 表示一个分组
type aggregateGroup struct {
	key    []interface{}
	rows   []resource.Row
	values map[string]interface{}
}

// calculateAggregation 计算聚合函数
func (p *PhysicalHashAggregate) calculateAggregation(agg *AggregationItem, rows []resource.Row) interface{} {
	if len(rows) == 0 {
		return nil
	}

	switch agg.Type {
	case AggregationTypeCount:
		// COUNT(*) 或 COUNT(column)
		if agg.Expr == "*" {
			return int64(len(rows))
		}
		count := int64(0)
		for _, row := range rows {
			if _, exists := row[agg.Expr]; exists && row[agg.Expr] != nil {
				count++
			}
		}
		return count

	case AggregationTypeSum:
		sum := 0.0
		for _, row := range rows {
			if val, exists := row[agg.Expr]; exists && val != nil {
				sum += convertToFloat64(val)
			}
		}
		return sum

	case AggregationTypeAvg:
		sum := 0.0
		count := 0
		for _, row := range rows {
			if val, exists := row[agg.Expr]; exists && val != nil {
				sum += convertToFloat64(val)
				count++
			}
		}
		if count == 0 {
			return nil
		}
		return sum / float64(count)

	case AggregationTypeMax:
		max := float64(-1e308)
		hasValue := false
		for _, row := range rows {
			if val, exists := row[agg.Expr]; exists && val != nil {
				fval := convertToFloat64(val)
				if !hasValue || fval > max {
					max = fval
					hasValue = true
				}
			}
		}
		if !hasValue {
			return nil
		}
		return max

	case AggregationTypeMin:
		min := float64(1e308)
		hasValue := false
		for _, row := range rows {
			if val, exists := row[agg.Expr]; exists && val != nil {
				fval := convertToFloat64(val)
				if !hasValue || fval < min {
					min = fval
					hasValue = true
				}
			}
		}
		if !hasValue {
			return nil
		}
		return min

	default:
		return nil
	}
}

// convertToFloat64 将值转换为float64
func convertToFloat64(val interface{}) float64 {
	switch v := val.(type) {
	case int, int8, int16, int32, int64:
		return float64(reflect.ValueOf(v).Int())
	case uint, uint8, uint16, uint32, uint64:
		return float64(reflect.ValueOf(v).Uint())
	case float32, float64:
		return reflect.ValueOf(v).Float()
	default:
		// 尝试从字符串转换
		s := fmt.Sprintf("%v", v)
		var f float64
		fmt.Sscanf(s, "%f", &f)
		return f
	}
}

// Explain 返回计划说明
func (p *PhysicalHashAggregate) Explain() string {
	return fmt.Sprintf("HashAggregate(cost=%.2f)", p.cost)
}
