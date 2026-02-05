package optimizer

import (
	"fmt"

	"github.com/kasuganosora/sqlexec/pkg/parser"
)

// LogicalWindow represents a logical window function operation
type LogicalWindow struct {
	windowFuncs []*WindowFunctionItem
	children    []LogicalPlan
}

// WindowFunctionItem represents a window function with its specification
type WindowFunctionItem struct {
	Func       *parser.Expression
	Spec       *parser.WindowSpec
	OrderBy    []*parser.OrderItem
	PartitionBy []*parser.Expression
}

// NewLogicalWindow creates a new LogicalWindow node
func NewLogicalWindow(windowFuncs []*WindowFunctionItem, child LogicalPlan) *LogicalWindow {
	return &LogicalWindow{
		windowFuncs: windowFuncs,
		children:    []LogicalPlan{child},
	}
}

// Children returns the child plans
func (p *LogicalWindow) Children() []LogicalPlan {
	return p.children
}

// SetChildren sets the child plans
func (p *LogicalWindow) SetChildren(children ...LogicalPlan) {
	p.children = children
}

// Schema returns the output columns
func (p *LogicalWindow) Schema() []ColumnInfo {
	if len(p.children) > 0 {
		baseSchema := p.children[0].Schema()

		// Add window function result columns
		result := make([]ColumnInfo, len(baseSchema)+len(p.windowFuncs))
		copy(result, baseSchema)

		for i, wf := range p.windowFuncs {
			if wf.Func != nil {
				result[len(baseSchema)+i] = ColumnInfo{
					Name:     fmt.Sprintf("window_%d", i),
					Type:     "bigint", // Most window functions return numeric types
					Nullable: false,
				}
			}
		}

		return result
	}
	return []ColumnInfo{}
}

// WindowFuncs returns the window function items
func (p *LogicalWindow) WindowFuncs() []*WindowFunctionItem {
	return p.windowFuncs
}

// Explain returns the plan description
func (p *LogicalWindow) Explain() string {
	return "LogicalWindow"
}
