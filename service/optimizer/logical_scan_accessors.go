package optimizer

// �?LogicalSelection 和其他算子添加访问器方法
// 用于修复方法名冲突问�?

import "github.com/kasuganosora/sqlexec/service/parser"

// SelectionConditions 过滤条件访问�?
func SelectionConditions(conditions []*parser.Expression) []*parser.Expression {
	return conditions
}

// SortOrderByItems 排序项访问器
func SortOrderByItems(orderBy []OrderByItem) []OrderByItem {
	result := make([]OrderByItem, 0, len(orderBy))
	for i := range orderBy {
		result[i] = OrderByItem{
			Column:    orderBy[i].Column,
			Direction: orderBy[i].Direction,
		}
	}
	return result
}

// AggregateAggFuncs 聚合函数访问�?
func AggregateAggFuncs(aggFuncs []*AggregationItem) []*AggregationItem {
	result := make([]*AggregationItem, 0, len(aggFuncs))
	for i := range aggFuncs {
		result[i] = &AggregationItem{
			Type:     aggFuncs[i].Type,
			Expr:     aggFuncs[i].Expr,
			Alias:    aggFuncs[i].Alias,
			Distinct: aggFuncs[i].Distinct,
		}
	}
	return result
}

// AggregateGroupByCols 分组列访问器
func AggregateGroupByCols(groupByCols []string) []string {
	result := make([]string, 0, len(groupByCols))
	copy(result, groupByCols)
	return result
}

// UnionChildren Union子节点访问器
func UnionChildren(children []LogicalPlan) []LogicalPlan {
	return children
}

// UnionUnionType Union类型访问�?
func UnionUnionType(unionType string) string {
	return unionType
}

// UnionAll Union all标志访问�?
func UnionAll(all bool) bool {
	return all
}
