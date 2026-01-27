package optimizer

import (
	"fmt"

	"github.com/kasuganosora/sqlexec/pkg/parser"
)

// LogicalSelection 逻辑过滤（选择）
type LogicalSelection struct {
	filterConditions []*parser.Expression
	children         []LogicalPlan
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

// GetConditions 返回过滤条件（用于避免与Conditions方法冲突）
func (p *LogicalSelection) GetConditions() []*parser.Expression {
	return p.filterConditions
}

// Selectivity 返回选择率
func (p *LogicalSelection) Selectivity() float64 {
	// 简化实现：默认0.1（10%的选择率）
	return 0.1
}

// Explain 返回计划说明
func (p *LogicalSelection) Explain() string {
	conditions := p.GetConditions()
	if len(conditions) > 0 {
		return "Selection WHERE " + fmt.Sprintf("%v", conditions[0])
	}
	return "Selection"
}
