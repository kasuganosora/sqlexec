package optimizer

import (
	"fmt"

	"github.com/kasuganosora/sqlexec/service/parser"
	"github.com/kasuganosora/sqlexec/service/resource"
)

// LimitInfo Limit信息
type LimitInfo struct {
	Limit  int64
	Offset int64
}

// LogicalDataSource 逻辑数据源（表扫描）
type LogicalDataSource struct {
	TableName   string
	Columns     []ColumnInfo
	TableInfo   *resource.TableInfo
	Statistics  *Statistics
	children    []LogicalPlan
	pushedDownPredicates []*parser.Expression // 下推的谓词条�?
	pushedDownLimit     *LimitInfo           // 下推的Limit信息
}

// NewLogicalDataSource 创建逻辑数据�?
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

// Children 获取子节�?
func (p *LogicalDataSource) Children() []LogicalPlan {
	return p.children
}

// SetChildren 设置子节�?
func (p *LogicalDataSource) SetChildren(children ...LogicalPlan) {
	p.children = children
}

// Schema 返回输出�?
func (p *LogicalDataSource) Schema() []ColumnInfo {
	return p.Columns
}

// RowCount 返回预估行数
func (p *LogicalDataSource) RowCount() int64 {
	if p.Statistics != nil {
		return p.Statistics.RowCount
	}
	return 1000 // 默认估计
}

// Table 返回表名
func (p *LogicalDataSource) Table() string {
	return p.TableName
}

// Explain 返回计划说明
func (p *LogicalDataSource) Explain() string {
	return fmt.Sprintf("DataSource(%s)", p.TableName)
}

// PushDownPredicates 添加下推的谓词条�?
func (p *LogicalDataSource) PushDownPredicates(conditions []*parser.Expression) {
	p.pushedDownPredicates = append(p.pushedDownPredicates, conditions...)
}

// GetPushedDownPredicates 获取下推的谓词条�?
func (p *LogicalDataSource) GetPushedDownPredicates() []*parser.Expression {
	return p.pushedDownPredicates
}

// PushDownLimit 添加下推的Limit
func (p *LogicalDataSource) PushDownLimit(limit, offset int64) {
	p.pushedDownLimit = &LimitInfo{
		Limit:  limit,
		Offset: offset,
	}
}

// GetPushedDownLimit 获取下推的Limit
func (p *LogicalDataSource) GetPushedDownLimit() *LimitInfo {
	return p.pushedDownLimit
}

// LogicalSelection 逻辑过滤（选择�?
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

// Children 获取子节�?
func (p *LogicalSelection) Children() []LogicalPlan {
	return p.children
}

// SetChildren 设置子节�?
func (p *LogicalSelection) SetChildren(children ...LogicalPlan) {
	p.children = children
}

// Schema 返回输出�?
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

// GetConditions 返回过滤条件（用于避免与Conditions方法冲突�?
func (p *LogicalSelection) GetConditions() []*parser.Expression {
	return p.filterConditions
}

// Selectivity 返回选择�?
func (p *LogicalSelection) Selectivity() float64 {
	// 简化实现：默认0.1�?0%的选择率）
	return 0.1
}

// Explain 返回计划说明
func (p *LogicalSelection) Explain() string {
	condStr := ""
	conditions := p.GetConditions()
	if len(conditions) > 0 {
		condStr = fmt.Sprintf(" WHERE %v", conditions[0])
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

// Children 获取子节�?
func (p *LogicalProjection) Children() []LogicalPlan {
	return p.children
}

// SetChildren 设置子节�?
func (p *LogicalProjection) SetChildren(children ...LogicalPlan) {
	p.children = children
}

// Schema 返回输出�?
func (p *LogicalProjection) Schema() []ColumnInfo {
	return p.Columns
}

// Expressions 返回投影表达�?
func (p *LogicalProjection) Expressions() []*parser.Expression {
	return p.Exprs
}

// GetExprs 返回投影表达式（用于避免与Expressions方法冲突�?
func (p *LogicalProjection) GetExprs() []*parser.Expression {
	return p.Exprs
}

// Aliases 返回别名列表
func (p *LogicalProjection) Aliases() []string {
	return p.columnAliases
}

// GetAliases 返回别名列表（用于避免与Aliases方法冲突�?
func (p *LogicalProjection) GetAliases() []string {
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

// Children 获取子节�?
func (p *LogicalLimit) Children() []LogicalPlan {
	return p.children
}

// SetChildren 设置子节�?
func (p *LogicalLimit) SetChildren(children ...LogicalPlan) {
	p.children = children
}

// Schema 返回输出�?
func (p *LogicalLimit) Schema() []ColumnInfo {
	if len(p.children) > 0 {
		return p.children[0].Schema()
	}
	return []ColumnInfo{}
}

// Limit 返回LIMIT�?
func (p *LogicalLimit) Limit() int64 {
	return p.limitVal
}

// GetLimit 返回LIMIT值（用于避免与Limit方法冲突�?
func (p *LogicalLimit) GetLimit() int64 {
	return p.limitVal
}

// Offset 返回OFFSET�?
func (p *LogicalLimit) Offset() int64 {
	return p.offsetVal
}

// GetOffset 返回OFFSET值（用于避免与Offset方法冲突�?
func (p *LogicalLimit) GetOffset() int64 {
	return p.offsetVal
}

// Explain 返回计划说明
func (p *LogicalLimit) Explain() string {
	return fmt.Sprintf("Limit(offset=%d, limit=%d)", p.Offset(), p.Limit())
}

// LogicalSort 逻辑排序
type LogicalSort struct {
	OrderBy   []OrderByItem
	children  []LogicalPlan
}

// OrderByItem 排序�?
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

// Children 获取子节�?
func (p *LogicalSort) Children() []LogicalPlan {
	return p.children
}

// SetChildren 设置子节�?
func (p *LogicalSort) SetChildren(children ...LogicalPlan) {
	p.children = children
}

// Schema 返回输出�?
func (p *LogicalSort) Schema() []ColumnInfo {
	if len(p.children) > 0 {
		return p.children[0].Schema()
	}
	return []ColumnInfo{}
}

// GetOrderByItems 返回排序列表
func (p *LogicalSort) GetOrderByItems() []*OrderByItem {
	result := make([]*OrderByItem, 0, len(p.OrderBy))
	for i := range p.OrderBy {
		result = append(result, &p.OrderBy[i])
	}
	return result
}

// Explain 返回计划说明
func (p *LogicalSort) Explain() string {
	items := ""
	orderByItems := p.GetOrderByItems()
	for i, item := range orderByItems {
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

// Children 获取子节�?
func (p *LogicalJoin) Children() []LogicalPlan {
	return p.children
}

// SetChildren 设置子节�?
func (p *LogicalJoin) SetChildren(children ...LogicalPlan) {
	p.children = children
}

// Schema 返回输出�?
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

// GetJoinType 返回连接类型（用于避免与JoinType方法冲突�?
func (p *LogicalJoin) GetJoinType() JoinType {
	return p.joinType
}

// Conditions 返回连接条件
func (p *LogicalJoin) Conditions() []*JoinCondition {
	return p.joinConditions
}

// GetJoinConditions 返回连接条件（用于避免与Conditions方法冲突�?
func (p *LogicalJoin) GetJoinConditions() []*JoinCondition {
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

// Children 获取子节�?
func (p *LogicalAggregate) Children() []LogicalPlan {
	return p.children
}

// SetChildren 设置子节�?
func (p *LogicalAggregate) SetChildren(children ...LogicalPlan) {
	p.children = children
}

// Schema 返回输出�?
func (p *LogicalAggregate) Schema() []ColumnInfo {
	columns := []ColumnInfo{}

	// 添加 GROUP BY �?
	for _, col := range p.groupByFields {
		columns = append(columns, ColumnInfo{
			Name:     col,
			Type:     "unknown",
			Nullable: true,
		})
	}

	// 添加聚合函数�?
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

// GetAggFuncs 返回聚合函数列表（用于避免与方法名冲突）
func (p *LogicalAggregate) GetAggFuncs() []*AggregationItem {
	return p.aggFuncs
}

// GroupByCols 返回分组列列�?
func (p *LogicalAggregate) GroupByCols() []string {
	return p.groupByFields
}

// GetGroupByCols 返回分组列列表（用于避免与方法名冲突�?
func (p *LogicalAggregate) GetGroupByCols() []string {
	return p.groupByFields
}

// Explain 返回计划说明
func (p *LogicalAggregate) Explain() string {
	aggStr := ""
	aggFuncs := p.GetAggFuncs()
	for i, agg := range aggFuncs {
		if i > 0 {
			aggStr += ", "
		}
		aggStr += fmt.Sprintf("%s(%v)", agg.Type, agg.Expr)
	}
	groupStr := ""
	groupByCols := p.GetGroupByCols()
	if len(groupByCols) > 0 {
		groupStr = fmt.Sprintf(" GROUP BY %v", groupByCols)
	}
	return fmt.Sprintf("Aggregate(%s%s)", aggStr, groupStr)
}

// LogicalUnion 逻辑联合
type LogicalUnion struct {
	children    []LogicalPlan
	unionType   string
	all         bool
}

// NewLogicalUnion 创建逻辑联合
func NewLogicalUnion(children ...LogicalPlan) *LogicalUnion {
	return &LogicalUnion{
		children:    children,
		unionType:   "UNION",
		all:         false,
	}
}

// Children 获取子节�?
func (p *LogicalUnion) Children() []LogicalPlan {
	return p.children
}

// SetChildren 设置子节�?
func (p *LogicalUnion) SetChildren(children ...LogicalPlan) {
	p.children = children
}

// Schema 返回输出�?
func (p *LogicalUnion) Schema() []ColumnInfo {
	if len(p.children) > 0 {
		return p.children[0].Schema()
	}
	return []ColumnInfo{}
}

// GetUnionType 返回UNION类型
func (p *LogicalUnion) GetUnionType() string {
	return p.unionType
}

// GetAll 返回是否包含重复�?
func (p *LogicalUnion) GetAll() bool {
	return p.all
}

// Explain 返回计划说明
func (p *LogicalUnion) Explain() string {
	return "Union"
}
