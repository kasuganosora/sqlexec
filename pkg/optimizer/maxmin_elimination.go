package optimizer

import (
	"context"
	"fmt"

	"github.com/kasuganosora/sqlexec/pkg/parser"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// MaxMinEliminationRule eliminates MAX/MIN aggregations by converting them to TopN operations
type MaxMinEliminationRule struct {
	estimator CardinalityEstimator
}

// NewMaxMinEliminationRule creates a new MaxMinEliminationRule
func NewMaxMinEliminationRule(estimator CardinalityEstimator) *MaxMinEliminationRule {
	return &MaxMinEliminationRule{
		estimator: estimator,
	}
}

// Name returns the rule name
func (r *MaxMinEliminationRule) Name() string {
	return "MaxMinElimination"
}

// Match checks if the rule can be applied to the plan
func (r *MaxMinEliminationRule) Match(plan LogicalPlan) bool {
	agg, ok := plan.(*LogicalAggregate)
	if !ok {
		return false
	}

	return r.canEliminate(agg)
}

// canEliminate checks if MAX/MIN can be eliminated
func (r *MaxMinEliminationRule) canEliminate(agg *LogicalAggregate) bool {
	// 1. Must have no GROUP BY
	if len(agg.GetGroupByCols()) > 0 {
		return false
	}

	// 2. Check if all aggregation functions are MAX or MIN
	aggFuncs := agg.GetAggFuncs()
	if len(aggFuncs) == 0 {
		return false
	}

	for _, agg := range aggFuncs {
		if agg.Type != Max && agg.Type != Min {
			return false
		}
	}

	return true
}

// Apply applies the rule to the plan
func (r *MaxMinEliminationRule) Apply(ctx context.Context, plan LogicalPlan, optCtx *OptimizationContext) (LogicalPlan, error) {
	agg, ok := plan.(*LogicalAggregate)
	if !ok {
		return plan, nil
	}

	// Try to eliminate
	eliminated, err := r.tryEliminate(agg, optCtx)
	if err != nil {
		return plan, nil // Return original plan on error
	}

	if eliminated != nil {
		return eliminated, nil
	}

	return plan, nil
}

// tryEliminate tries to eliminate MAX/MIN
func (r *MaxMinEliminationRule) tryEliminate(agg *LogicalAggregate, optCtx *OptimizationContext) (LogicalPlan, error) {
	aggFuncs := agg.GetAggFuncs()

	// Single MAX/MIN function
	if len(aggFuncs) == 1 {
		return r.eliminateSingleMaxMin(agg, optCtx)
	}

	// Multiple MAX/MIN functions
	return r.eliminateMultipleMaxMin(agg, optCtx)
}

// eliminateSingleMaxMin eliminates a single MAX/MIN
func (r *MaxMinEliminationRule) eliminateSingleMaxMin(agg *LogicalAggregate, optCtx *OptimizationContext) (LogicalPlan, error) {
	aggFunc := agg.GetAggFuncs()[0]
	child := agg.Children()[0]

	if child == nil {
		return nil, nil
	}

	// Get column name from expression
	column := r.getColumnFromExpr(aggFunc.Expr)
	if column == "" {
		return nil, nil
	}

	// Get table name from child
	tableName := r.getTableName(child)
	if tableName == "" {
		return nil, nil
	}

	// Check if column has index
	hasIndex := r.checkColumnHasIndex(tableName, column, optCtx)
	if !hasIndex {
		// No index available, don't apply optimization
		return nil, nil
	}

	// Determine sort direction: MAX = DESC, MIN = ASC
	orderDirection := "ASC"
	if aggFunc.Type == Max {
		orderDirection = "DESC"
	}

	// Create inner query plan:
	// DataSource -> Selection (IS NOT NULL) -> Sort (ORDER BY) -> Limit(1)

	// Create DataSource node
	tableInfo, ok := optCtx.TableInfo[tableName]
	if !ok {
		return nil, nil
	}
	dataSource := NewLogicalDataSource(tableName, tableInfo)

	// Add IS NOT NULL filter
	notNullFilter := &parser.Expression{
		Type:     parser.ExprTypeOperator,
		Operator: "IS NOT",
		Left:     &parser.Expression{Type: parser.ExprTypeColumn, Column: column},
		Right:    &parser.Expression{Type: parser.ExprTypeValue, Value: nil},
	}
	selection := NewLogicalSelection([]*parser.Expression{notNullFilter}, dataSource)

	// Add sort - need to use OrderByItem
	sortItem := &parser.OrderItem{
		Expr: parser.Expression{
			Type:   parser.ExprTypeColumn,
			Column: column,
		},
		Direction: orderDirection,
	}
	sort := NewLogicalSort([]*parser.OrderItem{sortItem}, selection)

	// Add LIMIT 1
	limit := NewLogicalLimit(1, 0, sort)

	// Create outer aggregate, preserving original alias
	newAgg := NewLogicalAggregate(
		[]*AggregationItem{
			{Type: aggFunc.Type, Expr: aggFunc.Expr, Alias: aggFunc.Alias},
		},
		[]string{}, // No GROUP BY
		limit,
	)

	return newAgg, nil
}

// eliminateMultipleMaxMin eliminates multiple MAX/MIN functions
func (r *MaxMinEliminationRule) eliminateMultipleMaxMin(agg *LogicalAggregate, optCtx *OptimizationContext) (LogicalPlan, error) {
	aggFuncs := agg.GetAggFuncs()
	child := agg.Children()[0]

	if child == nil {
		return nil, nil
	}

	// Create independent subquery for each MAX/MIN
	subQueries := make([]LogicalPlan, 0, len(aggFuncs))

	for _, aggFunc := range aggFuncs {
		// Create single-table aggregation (before elimination)
		subAgg := NewLogicalAggregate(
			[]*AggregationItem{{Type: aggFunc.Type, Expr: aggFunc.Expr, Alias: aggFunc.Alias}},
			[]string{},
			child,
		)

		// Apply single MAX/MIN elimination
		eliminated, err := r.eliminateSingleMaxMin(subAgg, optCtx)
		if err != nil {
			return nil, err
		}

		if eliminated == nil {
			// If elimination failed, use original aggregation
			subQueries = append(subQueries, subAgg)
		} else {
			subQueries = append(subQueries, eliminated)
		}
	}

	// If only one subquery, return it directly
	if len(subQueries) == 1 {
		return subQueries[0], nil
	}

	// Multiple subqueries: use Cartesian Product (Cross Join)
	result := subQueries[0]
	for i := 1; i < len(subQueries); i++ {
		result = NewLogicalJoin(CrossJoin, result, subQueries[i], []*JoinCondition{})
	}

	// Add outer projection to restore original expressions
	projectionExprs := make([]*parser.Expression, 0, len(aggFuncs))
	aliases := make([]string, 0, len(aggFuncs))
	for i, aggFunc := range aggFuncs {
		alias := aggFunc.Alias
		if alias == "" {
			alias = fmt.Sprintf("agg%d", i)
		}
		projectionExprs = append(projectionExprs, &parser.Expression{
			Type:   parser.ExprTypeColumn,
			Column: alias,
		})
		aliases = append(aliases, alias)
	}

	projection := NewLogicalProjection(projectionExprs, aliases, result)
	return projection, nil
}

// getColumnFromExpr extracts column name from expression
func (r *MaxMinEliminationRule) getColumnFromExpr(expr *parser.Expression) string {
	if expr == nil {
		return ""
	}

	if expr.Type == parser.ExprTypeColumn {
		return expr.Column
	}

	// Handle nested expressions
	if expr.Left != nil {
		if col := r.getColumnFromExpr(expr.Left); col != "" {
			return col
		}
	}

	return ""
}

// getTableName extracts table name from logical plan
func (r *MaxMinEliminationRule) getTableName(plan LogicalPlan) string {
	if dataSource, ok := plan.(*LogicalDataSource); ok {
		return dataSource.TableName
	}

	// Check children recursively
	for _, child := range plan.Children() {
		if tableName := r.getTableName(child); tableName != "" {
			return tableName
		}
	}

	return ""
}

// checkColumnHasIndex checks if a column has an index by inspecting the
// table's column definitions (primary/unique) and any explicit indexes
// stored in the TableInfo.Atts map under the "__indexes__" key.
func (r *MaxMinEliminationRule) checkColumnHasIndex(tableName, column string, optCtx *OptimizationContext) bool {
	tableInfo, ok := optCtx.TableInfo[tableName]
	if !ok || tableInfo == nil {
		return false
	}

	// 1. Check column-level implicit indexes (primary key, unique constraint)
	for _, col := range tableInfo.Columns {
		if col.Name == column && (col.Primary || col.Unique) {
			return true
		}
	}

	// 2. Check explicit indexes stored in Atts["__indexes__"]
	if tableInfo.Atts != nil {
		if idxList, ok := tableInfo.Atts["__indexes__"]; ok {
			if indexes, ok := idxList.([]*domain.Index); ok {
				for _, idx := range indexes {
					for _, idxCol := range idx.Columns {
						if idxCol == column {
							return true
						}
					}
				}
			}
		}
	}

	return false
}
