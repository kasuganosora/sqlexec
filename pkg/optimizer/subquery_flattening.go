package optimizer

import (
	"context"
	"fmt"

	"github.com/kasuganosora/sqlexec/pkg/parser"
)

// SubqueryFlatteningRule flattens subqueries into joins
// Patterns: IN (SELECT ...) -> JOIN, EXISTS (SELECT ...) -> JOIN
type SubqueryFlatteningRule struct{}

// NewSubqueryFlatteningRule creates a new subquery flattening rule
func NewSubqueryFlatteningRule() *SubqueryFlatteningRule {
	return &SubqueryFlatteningRule{}
}

// Name returns rule name
func (r *SubqueryFlatteningRule) Name() string {
	return "SubqueryFlattening"
}

// Match checks if plan contains flatten-able subqueries
func (r *SubqueryFlatteningRule) Match(plan LogicalPlan) bool {
	return r.containsFlattenableSubquery(plan)
}

// containsFlattenableSubquery checks if plan contains subqueries that can be flattened
func (r *SubqueryFlatteningRule) containsFlattenableSubquery(plan LogicalPlan) bool {
	// Check if selection contains IN or EXISTS subqueries
	if sel, ok := plan.(*LogicalSelection); ok {
		for _, cond := range sel.Conditions() {
			if r.isFlattenableSubquery(cond) {
				return true
			}
		}
	}

	// Check if projection contains scalar subqueries
	if proj, ok := plan.(*LogicalProjection); ok {
		for _, expr := range proj.Exprs {
			if r.isFlattenableSubquery(expr) {
				return true
			}
		}
	}

	// Recursively check children
	for _, child := range plan.Children() {
		if r.containsFlattenableSubquery(child) {
			return true
		}
	}

	return false
}

// isFlattenableSubquery checks if expression contains a flatten-able subquery
func (r *SubqueryFlatteningRule) isFlattenableSubquery(expr *parser.Expression) bool {
	if expr == nil {
		return false
	}

	// Check for IN subquery: col IN (SELECT ...)
	// Note: We check for the operator, parser may not distinguish subqueries as a separate type
	if expr.Type == parser.ExprTypeOperator &&
		(expr.Operator == "in" || expr.Operator == "not in") {
		return true
	}

	// Check for EXISTS subquery: EXISTS (SELECT ...)
	if expr.Type == parser.ExprTypeOperator && expr.Operator == "exists" {
		return true
	}

	// Check for NOT EXISTS
	if expr.Type == parser.ExprTypeOperator && expr.Operator == "not" {
		if expr.Left != nil {
			if exprLeft := expr.Left; exprLeft.Type == parser.ExprTypeOperator && exprLeft.Operator == "exists" {
				return true
			}
		}
	}

	// Recursively check sub-expressions
	if r.isFlattenableSubquery(expr.Left) {
		return true
	}
	if r.isFlattenableSubquery(expr.Right) {
		return true
	}

	for _, arg := range expr.Args {
		if r.isFlattenableSubquery(&arg) {
			return true
		}
	}

	return false
}

// Apply applies the subquery flattening rule
func (r *SubqueryFlatteningRule) Apply(ctx context.Context, plan LogicalPlan, optCtx *OptimizationContext) (LogicalPlan, error) {
	// Recursively flatten subqueries
	return r.flatten(plan)
}

// flatten recursively flattens subqueries
func (r *SubqueryFlatteningRule) flatten(plan LogicalPlan) (LogicalPlan, error) {
	// First, recursively process children
	children := plan.Children()
	newChildren := make([]LogicalPlan, len(children))
	var err error

	for i, child := range children {
		newChildren[i], err = r.flatten(child)
		if err != nil {
			return nil, err
		}
	}

	plan.SetChildren(newChildren...)

	// Process current node
	switch p := plan.(type) {
	case *LogicalSelection:
		return r.flattenSelection(p)
	case *LogicalProjection:
		return r.flattenProjection(p)
	default:
		return plan, nil
	}
}

// flattenSelection flattens subqueries in selection
func (r *SubqueryFlatteningRule) flattenSelection(selection *LogicalSelection) (LogicalPlan, error) {
	child := selection.children[0]
	if child == nil {
		return selection, nil
	}

	// Check each condition for flattenable subqueries
	newConditions := []*parser.Expression{}
	for _, cond := range selection.Conditions() {
		flattened, err := r.flattenCondition(cond, child)
		if err != nil {
			return nil, err
		}

		if flattened == nil {
			// Condition was flattened, remove it
			continue
		}

		newConditions = append(newConditions, flattened)
	}

	// If no conditions remain, return child
	if len(newConditions) == 0 {
		return child, nil
	}

	// If conditions changed, create new selection
	if len(newConditions) != len(selection.Conditions()) {
		return NewLogicalSelection(newConditions, child), nil
	}

	return selection, nil
}

// flattenProjection flattens scalar subqueries in projection
func (r *SubqueryFlatteningRule) flattenProjection(projection *LogicalProjection) (LogicalPlan, error) {
	child := projection.children[0]
	if child == nil {
		return projection, nil
	}

	// For scalar subqueries in projection, we can convert to LEFT JOIN
	// This is more complex and may require multiple passes

	return projection, nil
}

// flattenCondition flattens a single condition containing subquery
// Returns nil if the condition was completely flattened into a join
func (r *SubqueryFlatteningRule) flattenCondition(cond *parser.Expression, child LogicalPlan) (*parser.Expression, error) {
	if cond == nil {
		return cond, nil
	}

	// Pattern 1: col IN (SELECT ...)
	if cond.Type == parser.ExprTypeOperator &&
		(cond.Operator == "in" || cond.Operator == "not in") {
		return r.flattenInSubquery(cond, child)
	}

	// Pattern 2: EXISTS (SELECT ...)
	if cond.Type == parser.ExprTypeOperator && cond.Operator == "exists" {
		return r.flattenExistsSubquery(cond, child)
	}

	// Pattern 3: NOT EXISTS (SELECT ...)
	if cond.Type == parser.ExprTypeOperator && cond.Operator == "not" {
		if cond.Left != nil && cond.Left.Type == parser.ExprTypeOperator && cond.Left.Operator == "exists" {
			return r.flattenNotExistsSubquery(cond, child)
		}
	}

	// Recursively process sub-expressions
	if cond.Left != nil {
		newLeft, err := r.flattenCondition(cond.Left, child)
		if err != nil {
			return nil, err
		}
		cond.Left = newLeft
	}

	if cond.Right != nil {
		newRight, err := r.flattenCondition(cond.Right, child)
		if err != nil {
			return nil, err
		}
		cond.Right = newRight
	}

	for i, arg := range cond.Args {
		newArg, err := r.flattenCondition(&arg, child)
		if err != nil {
			return nil, err
		}
		cond.Args[i] = *newArg
	}

	return cond, nil
}

// flattenInSubquery flattens IN (SELECT ...) to JOIN
func (r *SubqueryFlatteningRule) flattenInSubquery(cond *parser.Expression, child LogicalPlan) (*parser.Expression, error) {
	// IN (SELECT ...) -> SemiJoin (if IN) or AntiSemiJoin (if NOT IN)
	// Note: We don't check for ExprTypeSubquery type as it may not exist
	// Instead, we rely on the operator detection in isFlattenableSubquery

	// In a full implementation, we would:
	// 1. Extract the subquery plan from cond.Right
	// 2. Convert to SemiJoin (IN) or AntiSemiJoin (NOT IN)
	// 3. Return nil to indicate the condition was flattened

	// For now, just log the detection
	joinType := "SEMI JOIN"
	if cond.Operator == "not in" {
		joinType = "ANTI SEMI JOIN"
	}
	fmt.Printf("  [DEBUG] SubqueryFlattening: Detected %s subquery to be flattened\n", joinType)

	return cond, nil
}

// flattenExistsSubquery flattens EXISTS (SELECT ...) to SemiJoin
func (r *SubqueryFlatteningRule) flattenExistsSubquery(cond *parser.Expression, child LogicalPlan) (*parser.Expression, error) {
	// EXISTS (SELECT ...) -> SemiJoin
	// Note: We don't check for ExprTypeSubquery type

	// In a full implementation, we would:
	// 1. Extract the subquery plan from cond.Left
	// 2. Convert to SemiJoin
	// 3. Return nil to indicate the condition was flattened

	fmt.Printf("  [DEBUG] SubqueryFlattening: Detected EXISTS subquery to be flattened to SEMI JOIN\n")

	return cond, nil
}

// flattenNotExistsSubquery flattens NOT EXISTS (SELECT ...) to AntiSemiJoin
func (r *SubqueryFlatteningRule) flattenNotExistsSubquery(cond *parser.Expression, child LogicalPlan) (*parser.Expression, error) {
	// NOT EXISTS (SELECT ...) -> AntiSemiJoin
	if cond.Left == nil || cond.Left.Type != parser.ExprTypeOperator {
		return cond, nil
	}

	// Note: We don't check for ExprTypeSubquery type in the child

	// In a full implementation, we would:
	// 1. Extract the subquery plan from existsExpr.Left
	// 2. Convert to AntiSemiJoin
	// 3. Return nil to indicate the condition was flattened

	fmt.Printf("  [DEBUG] SubqueryFlattening: Detected NOT EXISTS subquery to be flattened to ANTI SEMI JOIN\n")

	return cond, nil
}
