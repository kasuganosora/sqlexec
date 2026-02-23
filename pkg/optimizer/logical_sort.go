package optimizer

import (
	"github.com/kasuganosora/sqlexec/pkg/parser"
)

// LogicalSort represents a logical sort operation
type LogicalSort struct {
	orderBy  []*parser.OrderItem
	children []LogicalPlan
}

// NewLogicalSort creates a new LogicalSort node
func NewLogicalSort(orderBy []*parser.OrderItem, child LogicalPlan) *LogicalSort {
	return &LogicalSort{
		orderBy:  orderBy,
		children: []LogicalPlan{child},
	}
}

// Children returns the child plans
func (p *LogicalSort) Children() []LogicalPlan {
	return p.children
}

// SetChildren sets the child plans
func (p *LogicalSort) SetChildren(children ...LogicalPlan) {
	p.children = children
}

// Schema returns the output columns
func (p *LogicalSort) Schema() []ColumnInfo {
	if len(p.children) > 0 {
		return p.children[0].Schema()
	}
	return []ColumnInfo{}
}

// OrderBy returns the order by items
func (p *LogicalSort) OrderBy() []*parser.OrderItem {
	return p.orderBy
}

// GetOrderBy returns the order by items
func (p *LogicalSort) GetOrderBy() []*parser.OrderItem {
	return p.orderBy
}

// Explain returns the plan description
func (p *LogicalSort) Explain() string {
	return "LogicalSort"
}
