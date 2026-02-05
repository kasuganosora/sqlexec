package optimizer

import (
	"fmt"
)

// LogicalAggregate 逻辑聚合
type LogicalAggregate struct {
	aggFuncs      []*AggregationItem
	groupByFields []string
	children       []LogicalPlan
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

// GetAggFuncs 返回聚合函数列表
func (p *LogicalAggregate) GetAggFuncs() []*AggregationItem {
	return p.aggFuncs
}

// GetGroupByCols 返回分组列列表
func (p *LogicalAggregate) GetGroupByCols() []string {
	return p.groupByFields
}

// GetGroupBy 返回分组列列表 (别名)
func (p *LogicalAggregate) GetGroupBy() []string {
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
		groupStr = " GROUP BY "
		for i, col := range groupByCols {
			if i > 0 {
				groupStr += ", "
			}
			groupStr += col
		}
	}
	return "Aggregate(" + aggStr + groupStr + ")"
}
