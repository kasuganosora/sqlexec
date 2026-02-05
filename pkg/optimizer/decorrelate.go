package optimizer

import (
	"context"

	"github.com/kasuganosora/sqlexec/pkg/parser"
)

// DecorrelateRule eliminates Apply nodes by converting them to JOIN operations
// Implements 8 strategies for different subquery patterns
type DecorrelateRule struct {
	estimator CardinalityEstimator
}

// NewDecorrelateRule creates a new decorrelation rule
func NewDecorrelateRule(estimator CardinalityEstimator) *DecorrelateRule {
	return &DecorrelateRule{
		estimator: estimator,
	}
}

// Name returns the rule name
func (r *DecorrelateRule) Name() string {
	return "Decorrelate"
}

// Match checks if the rule matches an Apply node
func (r *DecorrelateRule) Match(plan LogicalPlan) bool {
	_, ok := plan.(*LogicalApply)
	return ok
}

// Apply applies the decorrelation rule
func (r *DecorrelateRule) Apply(ctx context.Context, plan LogicalPlan, optCtx *OptimizationContext) (LogicalPlan, error) {
	apply, ok := plan.(*LogicalApply)
	if !ok {
		return plan, nil
	}

	if len(apply.children) < 2 {
		return plan, nil
	}

	// Strategy 1: Prune redundant Apply (condition always true)
	if r.canPruneApply(apply) {
		return r.pruneApply(apply)
	}

	// Strategy 2: Uncorrelated subquery -> convert to JOIN
	if len(apply.CorrelatedColumns()) == 0 {
		return r.uncorrelatedToJoin(apply)
	}

	// Strategy 3: Selection subquery -> extract condition to JOIN ON
	if selection, ok := apply.children[1].(*LogicalSelection); ok {
		return r.selectionSubquery(apply, selection)
	}

	// Strategy 4: MaxOneRow subquery -> remove MaxOneRow if applicable
	if r.isMaxOneRow(apply.children[1]) {
		return r.maxOneRowSubquery(apply)
	}

	// Strategy 5: Projection subquery -> column replacement and decorrelation
	if projection, ok := apply.children[1].(*LogicalProjection); ok {
		return r.projectionSubquery(apply, projection)
	}

	// Strategy 6: Limit subquery -> for SemiJoin, remove Limit
	if limit, ok := apply.children[1].(*LogicalLimit); ok {
		return r.limitSubquery(apply, limit)
	}

	// Strategy 7: Aggregation subquery -> pull-up to LeftOuterJoin + Aggregation
	if agg, ok := apply.children[1].(*LogicalAggregate); ok {
		return r.aggregationSubquery(apply, agg)
	}

	// Strategy 8: Sort subquery -> remove top-level Sort
	if sort, ok := apply.children[1].(*LogicalSort); ok {
		return r.sortSubquery(apply, sort)
	}

	// No applicable strategy, return as-is
	return plan, nil
}

// canPruneApply checks if the Apply can be pruned (conditions are always true)
func (r *DecorrelateRule) canPruneApply(apply *LogicalApply) bool {
	if len(apply.conditions) == 0 {
		return false
	}

	// Check if all conditions are true constants
	for _, cond := range apply.conditions {
		if cond.Type != parser.ExprTypeValue {
			return false
		}
		if val, ok := cond.Value.(bool); !ok || !val {
			return false
		}
	}

	return true
}

// pruneApply removes a redundant Apply node
func (r *DecorrelateRule) pruneApply(apply *LogicalApply) (LogicalPlan, error) {
	// Return outer child only
	if len(apply.children) > 0 {
		return apply.children[0], nil
	}
	return apply, nil
}

// uncorrelatedToJoin converts an uncorrelated Apply to a JOIN
func (r *DecorrelateRule) uncorrelatedToJoin(apply *LogicalApply) (LogicalPlan, error) {
	// Convert Apply to regular Join
	outer := apply.children[0]
	inner := apply.children[1]

	// Use expression version of NewLogicalJoin
	join := NewLogicalJoinWithExprs(apply.joinType, outer, inner, apply.conditions)
	return join, nil
}

// selectionSubquery extracts Selection conditions to JOIN ON clause
func (r *DecorrelateRule) selectionSubquery(apply *LogicalApply, selection *LogicalSelection) (LogicalPlan, error) {
	// Merge Apply conditions and Selection conditions
	mergedConditions := append([]*parser.Expression{}, apply.conditions...)
	mergedConditions = append(mergedConditions, selection.Conditions()...)

	// Extract inner of Selection
	innerChild := selection.children[0]
	if innerChild == nil {
		return apply, nil
	}

	// Decorrelate conditions
	outerSchema := apply.children[0].Schema()
	newConditions := []*parser.Expression{}
	for _, cond := range mergedConditions {
		decorrelated, _ := Decorrelate(cond, outerSchema)
		newConditions = append(newConditions, decorrelated)
	}

	// Create Join instead of Apply (use expression version)
	join := NewLogicalJoinWithExprs(apply.joinType, apply.children[0], innerChild, newConditions)
	return join, nil
}

// isMaxOneRow checks if the node represents a MaxOneRow constraint
func (r *DecorrelateRule) isMaxOneRow(plan LogicalPlan) bool {
	// Check if this is an aggregation with GROUP BY empty or implicit MAX 1
	if agg, ok := plan.(*LogicalAggregate); ok {
		return len(agg.GetGroupBy()) == 0 && len(agg.GetAggFuncs()) > 0
	}
	return false
}

// maxOneRowSubquery removes MaxOneRow constraint for LeftOuterJoin
func (r *DecorrelateRule) maxOneRowSubquery(apply *LogicalApply) (LogicalPlan, error) {
	// For LeftOuterJoin, we can remove the MaxOneRow constraint
	if apply.joinType == LeftOuterJoin {
		// Convert to regular Join
		inner := apply.children[1]

		// Remove MaxOneRow (represented as Aggregate with no GROUP BY)
		if agg, ok := inner.(*LogicalAggregate); ok {
			innerChild := agg.children[0]
			if innerChild != nil {
				join := NewLogicalJoinWithExprs(apply.joinType, apply.children[0], innerChild, apply.conditions)
				return join, nil
			}
		}
	}

	return apply, nil
}

// projectionSubquery handles projection subquery with column replacement
func (r *DecorrelateRule) projectionSubquery(apply *LogicalApply, projection *LogicalProjection) (LogicalPlan, error) {
	// Decorrelate projection expressions
	outerSchema := apply.children[0].Schema()
	newExprs := []*parser.Expression{}
	mapping := make(map[string]string)

	for _, expr := range projection.Exprs {
		decorrelated, exprMapping := Decorrelate(expr, outerSchema)
		newExprs = append(newExprs, decorrelated)

		// Merge mappings
		for k, v := range exprMapping {
			mapping[k] = v
		}
	}

	// Get inner child of projection
	innerChild := projection.children[0]
	if innerChild == nil {
		return apply, nil
	}

	// Decorrelate conditions
	newConditions := []*parser.Expression{}
	for _, cond := range apply.conditions {
		decorrelated, _ := Decorrelate(cond, outerSchema)
		newConditions = append(newConditions, decorrelated)
	}

	// For scalar subquery, convert to LeftOuterJoin
	if apply.joinType == LeftOuterJoin {
		join := NewLogicalJoinWithExprs(LeftOuterJoin, apply.children[0], innerChild, newConditions)

	// Add projection on top
	newProjection := NewLogicalProjection(newExprs, []string{}, join)
	return newProjection, nil
	}

	// For other join types, create regular Join
	join := NewLogicalJoinWithExprs(apply.joinType, apply.children[0], innerChild, newConditions)
	return join, nil
}

// limitSubquery handles Limit subquery, removing Limit for SemiJoin
func (r *DecorrelateRule) limitSubquery(apply *LogicalApply, limit *LogicalLimit) (LogicalPlan, error) {
	// For SemiJoin or AntiSemiJoin, Limit can be removed
	// as they only check existence
	if apply.joinType == SemiJoin || apply.joinType == AntiSemiJoin {
		// Extract inner of Limit
		innerChild := limit.children[0]
		if innerChild == nil {
			return apply, nil
		}

		// Create Join without Limit
		join := NewLogicalJoinWithExprs(apply.joinType, apply.children[0], innerChild, apply.conditions)
		return join, nil
	}

	return apply, nil
}

// aggregationSubquery handles aggregation subquery by pulling it up
func (r *DecorrelateRule) aggregationSubquery(apply *LogicalApply, agg *LogicalAggregate) (LogicalPlan, error) {
	// Check if we can pull up aggregation
	// Condition: GROUP BY columns include all correlated columns
	correlatedCols := apply.CorrelatedColumns()
	groupByCols := agg.GetGroupBy()

	// Simple check: correlated columns must be in GROUP BY
	canPullUp := true
	for _, corrCol := range correlatedCols {
		found := false
		for _, gbCol := range groupByCols {
			if gbCol == corrCol.Column {
				found = true
				break
			}
		}
		if !found {
			canPullUp = false
			break
		}
	}

	if !canPullUp {
		// Cannot pull up, return as-is
		return apply, nil
	}

	// Pull up: convert to LeftOuterJoin + Aggregate
	innerChild := agg.children[0]
	if innerChild == nil {
		return apply, nil
	}

	// Decorrelate conditions
	outerSchema := apply.children[0].Schema()
	newConditions := []*parser.Expression{}
	for _, cond := range apply.conditions {
		decorrelated, _ := Decorrelate(cond, outerSchema)
		newConditions = append(newConditions, decorrelated)
	}

	// Create LeftOuterJoin (use expression version)
	join := NewLogicalJoinWithExprs(LeftOuterJoin, apply.children[0], innerChild, newConditions)

	// Add Aggregate on top (note: NewLogicalAggregate expects (aggFuncs, groupByCols, child))
	newAgg := NewLogicalAggregate(agg.GetAggFuncs(), groupByCols, join)
	return newAgg, nil
}

// sortSubquery handles Sort subquery by removing top-level Sort
func (r *DecorrelateRule) sortSubquery(apply *LogicalApply, sort *LogicalSort) (LogicalPlan, error) {
	// Top-level Sort in subquery doesn't affect result
	// Remove it and convert Apply to Join

	innerChild := sort.children[0]
	if innerChild == nil {
		return apply, nil
	}

	// Decorrelate conditions
	outerSchema := apply.children[0].Schema()
	newConditions := []*parser.Expression{}
	for _, cond := range apply.conditions {
		decorrelated, _ := Decorrelate(cond, outerSchema)
		newConditions = append(newConditions, decorrelated)
	}

	// Create Join (use expression version)
	join := NewLogicalJoinWithExprs(apply.joinType, apply.children[0], innerChild, newConditions)
	return join, nil
}
