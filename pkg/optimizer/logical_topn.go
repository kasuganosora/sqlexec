package optimizer

import (
	"fmt"

	"github.com/kasuganosora/sqlexec/pkg/parser"
)

// LogicalTopN represents a TopN operation (Sort + Limit)
// It combines Sort and Limit into a single operator for optimization
type LogicalTopN struct {
	sortItems []*parser.OrderItem
	limit     int64
	offset    int64
	children  []LogicalPlan
}

// NewLogicalTopN creates a new LogicalTopN node
func NewLogicalTopN(items []*parser.OrderItem, limit, offset int64, child LogicalPlan) *LogicalTopN {
	return &LogicalTopN{
		sortItems: items,
		limit:     limit,
		offset:    offset,
		children:  []LogicalPlan{child},
	}
}

// Children returns the child plans
func (p *LogicalTopN) Children() []LogicalPlan {
	return p.children
}

// SetChildren sets the child plans
func (p *LogicalTopN) SetChildren(children ...LogicalPlan) {
	p.children = children
}

// Schema returns the output columns
func (p *LogicalTopN) Schema() []ColumnInfo {
	if len(p.children) > 0 {
		return p.children[0].Schema()
	}
	return []ColumnInfo{}
}

// SortItems returns the sort items
func (p *LogicalTopN) SortItems() []*parser.OrderItem {
	return p.sortItems
}

// GetLimit returns the limit count
func (p *LogicalTopN) GetLimit() int64 {
	return p.limit
}

// GetOffset returns the offset count
func (p *LogicalTopN) GetOffset() int64 {
	return p.offset
}

// SetLimit sets the limit count
func (p *LogicalTopN) SetLimit(limit int64) {
	p.limit = limit
}

// SetOffset sets the offset count
func (p *LogicalTopN) SetOffset(offset int64) {
	p.offset = offset
}

// Explain returns the plan description
func (p *LogicalTopN) Explain() string {
	return "TopN(Limit=" + fmt.Sprintf("%d", p.limit) + ", Offset=" + fmt.Sprintf("%d", p.offset) + ")"
}
