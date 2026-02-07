package physical

import (
	"fmt"

	"github.com/kasuganosora/sqlexec/pkg/optimizer"
	"github.com/kasuganosora/sqlexec/pkg/parser"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// PhysicalSelection 物理过滤算子
type PhysicalSelection struct {
	Conditions []*parser.Expression
	Filters    []domain.Filter
	cost       float64
	children   []PhysicalOperator
	dataSource domain.DataSource
}

// NewPhysicalSelection 创建物理过滤算子
func NewPhysicalSelection(conditions []*parser.Expression, filters []domain.Filter, child PhysicalOperator, dataSource domain.DataSource) *PhysicalSelection {
	inputCost := child.Cost()
	cost := inputCost*1.2 + 10 // 过滤成本

	return &PhysicalSelection{
		Conditions: conditions,
		Filters:    filters,
		cost:       cost,
		children:   []PhysicalOperator{child},
		dataSource: dataSource,
	}
}

// Children 获取子节点
func (p *PhysicalSelection) Children() []PhysicalOperator {
	return p.children
}

// SetChildren 设置子节点
func (p *PhysicalSelection) SetChildren(children ...PhysicalOperator) {
	p.children = children
}

// Schema 返回输出列
func (p *PhysicalSelection) Schema() []optimizer.ColumnInfo {
	if len(p.children) > 0 {
		return p.children[0].Schema()
	}
	return []optimizer.ColumnInfo{}
}

// Cost 返回执行成本
func (p *PhysicalSelection) Cost() float64 {
	return p.cost
}

// Explain 返回计划说明
func (p *PhysicalSelection) Explain() string {
	return fmt.Sprintf("Selection(cost=%.2f)", p.cost)
}

// GetConditions 获取条件表达式（用于测试）
func (p *PhysicalSelection) GetConditions() []*parser.Expression {
	return p.Conditions
}

// GetFilters 获取过滤器（用于测试）
func (p *PhysicalSelection) GetFilters() []domain.Filter {
	return p.Filters
}
