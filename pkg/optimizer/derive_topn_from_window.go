package optimizer

import (
	"context"

	"github.com/kasuganosora/sqlexec/pkg/parser"
)

// DeriveTopNFromWindowRule derives TopN from window functions
// Pattern: ROW_NUMBER() OVER (ORDER BY ...) LIMIT N -> TopN
type DeriveTopNFromWindowRule struct{}

// NewDeriveTopNFromWindowRule creates a new rule
func NewDeriveTopNFromWindowRule() *DeriveTopNFromWindowRule {
	return &DeriveTopNFromWindowRule{}
}

// Name returns the rule name
func (r *DeriveTopNFromWindowRule) Name() string {
	return "DeriveTopNFromWindow"
}

// Match checks if the plan has a window function that can be converted to TopN
func (r *DeriveTopNFromWindowRule) Match(plan LogicalPlan) bool {
	return r.hasTopNWindowPattern(plan)
}

// hasTopNWindowPattern checks for pattern: Window(ROW_NUMBER) + Limit
// Or: Projection containing ROW_NUMBER() + Limit
func (r *DeriveTopNFromWindowRule) hasTopNWindowPattern(plan LogicalPlan) bool {
	// Pattern 1: Limit -> Window (ROW_NUMBER with ORDER BY)
	if limit, ok := plan.(*LogicalLimit); ok {
		if len(limit.children) > 0 {
			child := limit.children[0]
			if r.isRowNumberWindow(child) {
				return true
			}

			// Pattern 2: Limit -> Projection -> Window
			if proj, ok := child.(*LogicalProjection); ok {
				if len(proj.children) > 0 {
					if r.isRowNumberWindow(proj.children[0]) {
						return true
					}
				}
			}
		}
	}

	// Pattern 3: Window -> Limit (reverse order)
	if window, ok := plan.(*LogicalWindow); ok {
		if len(window.children) > 0 {
			child := window.children[0]
			if _, ok := child.(*LogicalLimit); ok {
				return r.isRowNumberWindow(window)
			}
		}
	}

	return false
}

// isRowNumberWindow checks if a plan is a ROW_NUMBER window function with ORDER BY
func (r *DeriveTopNFromWindowRule) isRowNumberWindow(plan LogicalPlan) bool {
	window, ok := plan.(*LogicalWindow)
	if !ok {
		return false
	}

	// Check if any window function is ROW_NUMBER
	for _, wf := range window.WindowFuncs() {
		if wf.Func != nil && wf.Func.Function == "row_number" {
			// Check if there's an ORDER BY clause
			if len(wf.OrderBy) > 0 {
				return true
			}
		}
	}

	return false
}

// Apply converts window function to TopN
func (r *DeriveTopNFromWindowRule) Apply(ctx context.Context, plan LogicalPlan, optCtx *OptimizationContext) (LogicalPlan, error) {
	// Pattern 1: Limit -> Window (ROW_NUMBER)
	if limit, ok := plan.(*LogicalLimit); ok {
		if len(limit.children) > 0 {
			child := limit.children[0]

			// Case 1a: Limit -> Window directly
			if window, ok := child.(*LogicalWindow); ok {
				if r.isRowNumberWindow(window) {
					return r.convertWindowToTopN(window, limit.GetLimit(), limit.GetOffset())
				}
			}

			// Case 1b: Limit -> Projection -> Window
			if proj, ok := child.(*LogicalProjection); ok {
				if len(proj.children) > 0 {
					if window, ok := proj.children[0].(*LogicalWindow); ok {
						if r.isRowNumberWindow(window) {
							// Get ORDER BY from window
							sortItems := r.extractSortItems(window)

							// Create TopN
							if windowChildren := window.Children(); len(windowChildren) > 0 {
								topn := NewLogicalTopN(sortItems, limit.GetLimit(), limit.GetOffset(), windowChildren[0])

								// Add projection on top if needed (to preserve ROW_NUMBER column)
								if len(proj.Exprs) > 0 {
									return NewLogicalProjection(proj.Exprs, []string{}, topn), nil
								}
								return topn, nil
							}
						}
					}
				}
			}
		}
	}

	// Pattern 2: Window -> Limit (reverse order)
	if window, ok := plan.(*LogicalWindow); ok {
		if len(window.children) > 0 {
			if limit, ok := window.children[0].(*LogicalLimit); ok {
				if r.isRowNumberWindow(window) {
					return r.convertWindowToTopN(window, limit.GetLimit(), limit.GetOffset())
				}
			}
		}
	}

	return plan, nil
}

// extractSortItems extracts sort items from window function
func (r *DeriveTopNFromWindowRule) extractSortItems(window *LogicalWindow) []*parser.OrderItem {
	// Find ROW_NUMBER window function
	for _, wf := range window.WindowFuncs() {
		if wf.Func != nil && wf.Func.Function == "row_number" {
			// Return ORDER BY items directly (they are already OrderItem)
			return wf.OrderBy
		}
	}

	return []*parser.OrderItem{}
}

// convertWindowToTopN converts a window function to TopN
func (r *DeriveTopNFromWindowRule) convertWindowToTopN(window *LogicalWindow, limit, offset int64) (LogicalPlan, error) {
	// Extract ORDER BY items
	sortItems := r.extractSortItems(window)

	if len(sortItems) == 0 {
		return window, nil
	}

	// Get child of window
	if len(window.children) == 0 {
		return window, nil
	}

	child := window.children[0]

	// Create TopN
	topn := NewLogicalTopN(sortItems, limit, offset, child)

	return topn, nil
}
