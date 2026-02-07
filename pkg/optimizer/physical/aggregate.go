package physical

import (
	"fmt"
	"strings"

	"github.com/kasuganosora/sqlexec/pkg/optimizer"
)

// PhysicalHashAggregate 物理哈希聚合算子
type PhysicalHashAggregate struct {
	AggFuncs    []*optimizer.AggregationItem
	GroupByCols []string
	cost        float64
	children    []PhysicalOperator
}

// NewPhysicalHashAggregate 创建物理哈希聚合算子
func NewPhysicalHashAggregate(aggFuncs []*optimizer.AggregationItem, groupByCols []string, child PhysicalOperator) *PhysicalHashAggregate {
	inputRows := int64(1000) // 假设

	// Hash Agg 成本 = 分组 + 聚合
	groupCost := float64(inputRows) * float64(len(groupByCols)) * 0.05
	aggCost := float64(inputRows) * float64(len(aggFuncs)) * 0.05
	cost := child.Cost() + groupCost + aggCost

	return &PhysicalHashAggregate{
		AggFuncs:    aggFuncs,
		GroupByCols: groupByCols,
		cost:        cost,
		children:    []PhysicalOperator{child},
	}
}

// Children 获取子节点
func (p *PhysicalHashAggregate) Children() []PhysicalOperator {
	return p.children
}

// SetChildren 设置子节点
func (p *PhysicalHashAggregate) SetChildren(children ...PhysicalOperator) {
	p.children = children
}

// Schema 返回输出列
func (p *PhysicalHashAggregate) Schema() []optimizer.ColumnInfo {
	columns := []optimizer.ColumnInfo{}

	// 添加 GROUP BY 列
	for _, col := range p.GroupByCols {
		columns = append(columns, optimizer.ColumnInfo{
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
		columns = append(columns, optimizer.ColumnInfo{
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

// Explain 返回计划说明
func (p *PhysicalHashAggregate) Explain() string {
	var aggFuncsBuilder strings.Builder
	for i, agg := range p.AggFuncs {
		if i > 0 {
			aggFuncsBuilder.WriteString(", ")
		}
		aggFuncsBuilder.WriteString(agg.Type.String())
	}
	aggFuncs := aggFuncsBuilder.String()

	groupBy := ""
	if len(p.GroupByCols) > 0 {
		groupBy = fmt.Sprintf(", GROUP BY(%s)", strings.Join(p.GroupByCols, ", "))
	}

	return fmt.Sprintf("HashAggregate(funcs=[%s]%s, cost=%.2f)", aggFuncs, groupBy, p.cost)
}

// GetAggFuncs 获取聚合函数（用于测试）
func (p *PhysicalHashAggregate) GetAggFuncs() []*optimizer.AggregationItem {
	return p.AggFuncs
}

// GetGroupByCols 获取GROUP BY列（用于测试）
func (p *PhysicalHashAggregate) GetGroupByCols() []string {
	return p.GroupByCols
}
