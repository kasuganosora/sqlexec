package optimizer

import (
	"context"
	"fmt"

	"mysql-proxy/mysql/parser"
	"mysql-proxy/mysql/resource"
)

// LogicalDataSource 逻辑数据源（表扫描）
type LogicalDataSource struct {
	TableName  string
	Columns    []ColumnInfo
	TableInfo  *resource.TableInfo
	Statistics *Statistics
	children   []LogicalPlan
}

// NewLogicalDataSource 创建逻辑数据源
func NewLogicalDataSource(tableName string, tableInfo *resource.TableInfo) *LogicalDataSource {
	columns := make([]ColumnInfo, 0, len(tableInfo.Columns))
	for _, col := range tableInfo.Columns {
		columns = append(columns, ColumnInfo{
			Name:     col.Name,
			Type:     col.Type,
			Nullable: col.Nullable,
		})
	}

	return &LogicalDataSource{
		TableName: tableName,
		Columns:   columns,
		TableInfo: tableInfo,
		children:  []LogicalPlan{},
	}
}

// Children 获取子节点
func (p *LogicalDataSource) Children() []LogicalPlan {
	return p.children
}

// SetChildren 设置子节点
func (p *LogicalDataSource) SetChildren(children ...LogicalPlan) {
	p.children = children
}

// Schema 返回输出列
func (p *LogicalDataSource) Schema() []ColumnInfo {
	return p.Columns
}

// RowCount 返回预估行数
func (p *LogicalDataSource) RowCount() int64 {
	if p.TableInfo != nil {
		return int64(len(p.TableInfo.Rows))
	}
	return 0
}

// Table 返回表名
func (p *LogicalDataSource) Table() string {
	return p.TableName
}

// Explain 返回计划说明
func (p *LogicalDataSource) Explain() string {
	return fmt.Sprintf("DataSource(%s)", p.TableName)
}

// LogicalSelection 逻辑过滤（选择）
type LogicalSelection struct {
	filterConditions []*parser.Expression
	children       []LogicalPlan
}

// NewLogicalSelection 创建逻辑过滤
func NewLogicalSelection(conditions []*parser.Expression, child LogicalPlan) *LogicalSelection {
	return &LogicalSelection{
		filterConditions: conditions,
		children:       []LogicalPlan{child},
	}
}

// Children 获取子节点
func (p *LogicalSelection) Children() []LogicalPlan {
	return p.children
}

// SetChildren 设置子节点
func (p *LogicalSelection) SetChildren(children ...LogicalPlan) {
	p.children = children
}

// Schema 返回输出列
func (p *LogicalSelection) Schema() []ColumnInfo {
	if len(p.children) > 0 {
		return p.children[0].Schema()
	}
	return []ColumnInfo{}
}

// Conditions 返回过滤条件
func (p *LogicalSelection) Conditions() []*parser.Expression {
	return p.filterConditions
}

// Selectivity 返回选择率
func (p *LogicalSelection) Selectivity() float64 {
	// 简化实现：默认0.1（10%的选择率）
	return 0.1
}

// Explain 返回计划说明
func (p *LogicalSelection) Explain() string {
	condStr := ""
	if len(p.Conditions) > 0 {
		condStr = fmt.Sprintf(" WHERE %v", p.Conditions[0])
	}
	return fmt.Sprintf("Selection%s", condStr)
}

// LogicalProjection 逻辑投影
type LogicalProjection struct {
	Exprs        []*parser.Expression
	columnAliases []string
	Columns      []ColumnInfo
	children     []LogicalPlan
}

// NewLogicalProjection 创建逻辑投影
func NewLogicalProjection(exprs []*parser.Expression, aliases []string, child LogicalPlan) *LogicalProjection {
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

	return &LogicalProjection{
		Exprs:        exprs,
		columnAliases:  aliases,
		Columns:       columns,
		children:      []LogicalPlan{child},
	}
}

// Children 获取子节点
func (p *LogicalProjection) Children() []LogicalPlan {
	return p.children
}

// SetChildren 设置子节点
func (p *LogicalProjection) SetChildren(children ...LogicalPlan) {
	p.children = children
}

// Schema 返回输出列
func (p *LogicalProjection) Schema() []ColumnInfo {
	return p.Columns
}

// Expressions 返回投影表达式
func (p *LogicalProjection) Expressions() []*parser.Expression {
	return p.Exprs
}

// Aliases 返回别名列表
func (p *LogicalProjection) Aliases() []string {
	return p.columnAliases
}

// Explain 返回计划说明
func (p *LogicalProjection) Explain() string {
	exprs := ""
	for i, expr := range p.Exprs {
		if i > 0 {
			exprs += ", "
		}
		if expr.Type == parser.ExprTypeColumn {
			exprs += expr.Column
		} else {
			exprs += fmt.Sprintf("%v", expr)
		}
	}
	return fmt.Sprintf("Projection(%s)", exprs)
}

// LogicalLimit 逻辑限制
type LogicalLimit struct {
	limitVal  int64
	offsetVal int64
	children  []LogicalPlan
}

// NewLogicalLimit 创建逻辑限制
func NewLogicalLimit(limit, offset int64, child LogicalPlan) *LogicalLimit {
	return &LogicalLimit{
		limitVal:  limit,
		offsetVal: offset,
		children:  []LogicalPlan{child},
	}
}

// Children 获取子节点
func (p *LogicalLimit) Children() []LogicalPlan {
	return p.children
}

// SetChildren 设置子节点
func (p *LogicalLimit) SetChildren(children ...LogicalPlan) {
	p.children = children
}

// Schema 返回输出列
func (p *LogicalLimit) Schema() []ColumnInfo {
	if len(p.children) > 0 {
		return p.children[0].Schema()
	}
	return []ColumnInfo{}
}

// Limit 返回LIMIT值
func (p *LogicalLimit) Limit() int64 {
	return p.limitVal
}

// Offset 返回OFFSET值
func (p *LogicalLimit) Offset() int64 {
	return p.offsetVal
}

// Explain 返回计划说明
func (p *LogicalLimit) Explain() string {
	return fmt.Sprintf("Limit(offset=%d, limit=%d)", p.Offset, p.Limit)
}

// LogicalSort 逻辑排序
type LogicalSort struct {
	OrderBy   []OrderByItem
	children  []LogicalPlan
}

// OrderByItem 排序项
type OrderByItem struct {
	Column    string
	Direction string // "ASC" or "DESC"
}

// NewLogicalSort 创建逻辑排序
func NewLogicalSort(orderBy []OrderByItem, child LogicalPlan) *LogicalSort {
	return &LogicalSort{
		OrderBy:  orderBy,
		children: []LogicalPlan{child},
	}
}

// Children 获取子节点
func (p *LogicalSort) Children() []LogicalPlan {
	return p.children
}

// SetChildren 设置子节点
func (p *LogicalSort) SetChildren(children ...LogicalPlan) {
	p.children = children
}

// Schema 返回输出列
func (p *LogicalSort) Schema() []ColumnInfo {
	if len(p.children) > 0 {
		return p.children[0].Schema()
	}
	return []ColumnInfo{}
}

// OrderByItems 返回排序列表
func (p *LogicalSort) OrderByItems() []*OrderByItem {
	return p.OrderByItems
}

// Explain 返回计划说明
func (p *LogicalSort) Explain() string {
	items := ""
	for i, item := range p.OrderBy {
		if i > 0 {
			items += ", "
		}
		items += fmt.Sprintf("%s %s", item.Column, item.Direction)
	}
	return fmt.Sprintf("Sort(%s)", items)
}

// LogicalJoin 逻辑连接
type LogicalJoin struct {
	joinType       JoinType
	LeftTable      string
	RightTable     string
	joinConditions []*JoinCondition
	children       []LogicalPlan
}

// NewLogicalJoin 创建逻辑连接
func NewLogicalJoin(joinType JoinType, left, right LogicalPlan, conditions []*JoinCondition) *LogicalJoin {
	leftTable := ""
	if left != nil {
		if ds, ok := left.(*LogicalDataSource); ok {
			leftTable = ds.TableName
		}
	}

	rightTable := ""
	if right != nil {
		if ds, ok := right.(*LogicalDataSource); ok {
			rightTable = ds.TableName
		}
	}

	return &LogicalJoin{
		joinType:       joinType,
		LeftTable:      leftTable,
		RightTable:     rightTable,
		joinConditions: conditions,
		children:       []LogicalPlan{left, right},
	}
}

// Children 获取子节点
func (p *LogicalJoin) Children() []LogicalPlan {
	return p.children
}

// SetChildren 设置子节点
func (p *LogicalJoin) SetChildren(children ...LogicalPlan) {
	p.children = children
}

// Schema 返回输出列
func (p *LogicalJoin) Schema() []ColumnInfo {
	columns := []ColumnInfo{}
	if len(p.children) > 0 {
		columns = append(columns, p.children[0].Schema()...)
	}
	if len(p.children) > 1 {
		columns = append(columns, p.children[1].Schema()...)
	}
	return columns
}

// JoinType 返回连接类型
func (p *LogicalJoin) JoinType() JoinType {
	return p.joinType
}

// Conditions 返回连接条件
func (p *LogicalJoin) Conditions() []*JoinCondition {
	return p.joinConditions
}

// Explain 返回计划说明
func (p *LogicalJoin) Explain() string {
	return fmt.Sprintf("Join(%s, %s, type=%s)", p.LeftTable, p.RightTable, p.JoinType)
}

// LogicalAggregate 逻辑聚合
type LogicalAggregate struct {
	aggFuncs      []*AggregationItem
	groupByFields []string
	children      []LogicalPlan
}

// NewLogicalAggregate 创建逻辑聚合
func NewLogicalAggregate(aggFuncs []*AggregationItem, groupByCols []string, child LogicalPlan) *LogicalAggregate {
	return &LogicalAggregate{
		aggFuncs:      aggFuncs,
		groupByFields: groupByCols,
		children:      []LogicalPlan{child},
	}
}

// Children 获取子节点
func (p *LogicalAggregate) Children() []LogicalPlan {
	return p.children
}

// SetChildren 设置子节点
func (p *LogicalAggregate) SetChildren(children ...LogicalPlan) {
	p.children = children
}

// Schema 返回输出列
func (p *LogicalAggregate) Schema() []ColumnInfo {
	columns := []ColumnInfo{}

	// 添加 GROUP BY 列
	for _, col := range p.groupByFields {
		columns = append(columns, ColumnInfo{
			Name:     col,
			Type:     "unknown",
			Nullable: true,
		})
	}

	// 添加聚合函数列
	for _, agg := range p.aggFuncs {
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

// AggFuncs 返回聚合函数列表
func (p *LogicalAggregate) AggFuncs() []*AggregationItem {
	return p.aggFuncs
}

// GroupByCols 返回分组列列表
func (p *LogicalAggregate) GroupByCols() []string {
	return p.groupByFields
}

// Explain 返回计划说明
func (p *LogicalAggregate) Explain() string {
	aggStr := ""
	for i, agg := range p.AggFuncs {
		if i > 0 {
			aggStr += ", "
		}
		aggStr += fmt.Sprintf("%s(%v)", agg.Type, agg.Expr)
	}
	groupStr := ""
	if len(p.GroupByCols) > 0 {
		groupStr = fmt.Sprintf(" GROUP BY %v", p.GroupByCols)
	}
	return fmt.Sprintf("Aggregate(%s%s)", aggStr, groupStr)
}

// LogicalUnion 逻辑联合
type LogicalUnion struct {
	children []LogicalPlan
}

// NewLogicalUnion 创建逻辑联合
func NewLogicalUnion(children ...LogicalPlan) *LogicalUnion {
	return &LogicalUnion{
		children: children,
	}
}

// Children 获取子节点
func (p *LogicalUnion) Children() []LogicalPlan {
	return p.children
}

// SetChildren 设置子节点
func (p *LogicalUnion) SetChildren(children ...LogicalPlan) {
	p.children = children
}

// Schema 返回输出列
func (p *LogicalUnion) Schema() []ColumnInfo {
	if len(p.children) > 0 {
		return p.children[0].Schema()
	}
	return []ColumnInfo{}
}

// UnionType 返回UNION类型
func (p *LogicalUnion) UnionType() string {
	return p.UnionType
}

// All 返回是否包含重复行
func (p *LogicalUnion) All() bool {
	return p.All
}

// Explain 返回计划说明
func (p *LogicalUnion) Explain() string {
	return "Union"
}
