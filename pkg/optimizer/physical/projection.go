package physical

import (
	"fmt"

	"github.com/kasuganosora/sqlexec/pkg/optimizer"
	"github.com/kasuganosora/sqlexec/pkg/parser"
)

// PhysicalProjection 物理投影算子
type PhysicalProjection struct {
	Exprs    []*parser.Expression
	Aliases  []string
	Columns  []optimizer.ColumnInfo
	cost     float64
	children []PhysicalOperator
}

// NewPhysicalProjection 创建物理投影算子
func NewPhysicalProjection(exprs []*parser.Expression, aliases []string, child PhysicalOperator) *PhysicalProjection {
	inputCost := child.Cost()
	cost := inputCost*1.1 + float64(len(exprs))*5 // 投影成本

	columns := make([]optimizer.ColumnInfo, len(exprs))
	for i, expr := range exprs {
		name := ""
		if i < len(aliases) {
			name = aliases[i]
		}
		if name == "" {
			if expr.Type == parser.ExprTypeColumn {
				name = expr.Column
			} else {
				name = fmt.Sprintf("expr_%d", i)
			}
		}
		columns[i] = optimizer.ColumnInfo{
			Name:     name,
			Type:     "unknown",
			Nullable: true,
		}
	}

	return &PhysicalProjection{
		Exprs:    exprs,
		Aliases:  aliases,
		Columns:  columns,
		cost:     cost,
		children: []PhysicalOperator{child},
	}
}

// Children 获取子节点
func (p *PhysicalProjection) Children() []PhysicalOperator {
	return p.children
}

// SetChildren 设置子节点
func (p *PhysicalProjection) SetChildren(children ...PhysicalOperator) {
	p.children = children
}

// Schema 返回输出列
func (p *PhysicalProjection) Schema() []optimizer.ColumnInfo {
	return p.Columns
}

// Cost 返回执行成本
func (p *PhysicalProjection) Cost() float64 {
	return p.cost
}

// Explain 返回计划说明
func (p *PhysicalProjection) Explain() string {
	return fmt.Sprintf("Projection(cost=%.2f)", p.cost)
}

// GetExprs 获取表达式（用于测试）
func (p *PhysicalProjection) GetExprs() []*parser.Expression {
	return p.Exprs
}

// GetAliases 获取别名（用于测试）
func (p *PhysicalProjection) GetAliases() []string {
	return p.Aliases
}
