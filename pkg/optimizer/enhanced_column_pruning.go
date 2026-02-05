package optimizer

import (
	"context"
	"fmt"

	"github.com/kasuganosora/sqlexec/pkg/parser"
)

// EnhancedColumnPruningRule enhances column pruning with cross-operator analysis
// Propagates required columns from top to bottom through the plan
type EnhancedColumnPruningRule struct {
	requiredCols map[string][]string // Plan explain -> required columns
}

// NewEnhancedColumnPruningRule creates a new enhanced column pruning rule
func NewEnhancedColumnPruningRule() *EnhancedColumnPruningRule {
	return &EnhancedColumnPruningRule{
		requiredCols: make(map[string][]string),
	}
}

// Name returns rule name
func (r *EnhancedColumnPruningRule) Name() string {
	return "EnhancedColumnPruning"
}

// Match always returns true for cross-operator analysis
func (r *EnhancedColumnPruningRule) Match(plan LogicalPlan) bool {
	return true
}

// Apply applies the enhanced column pruning rule
func (r *EnhancedColumnPruningRule) Apply(ctx context.Context, plan LogicalPlan, optCtx *OptimizationContext) (LogicalPlan, error) {
	// Reset required columns
	r.requiredCols = make(map[string][]string)

	// Analyze required columns from top to bottom
	r.analyzeRequiredColumns(plan)

	// Prune columns bottom-up
	pruned, err := r.pruneColumns(plan)
	if err != nil {
		return nil, err
	}

	return pruned, nil
}

// analyzeRequiredColumns analyzes which columns are required at each operator
// Propagates requirements from root to leaves
func (r *EnhancedColumnPruningRule) analyzeRequiredColumns(plan LogicalPlan) {
	planKey := plan.Explain()

	switch p := plan.(type) {
	case *LogicalProjection:
		// Projection requires columns from its expressions
		cols := r.extractColumnsFromExprs(p.Exprs)
		r.requiredCols[planKey] = cols
		fmt.Printf("  [DEBUG] EnhancedColumnPruning: Projection needs columns: %v\n", cols)

	case *LogicalSelection:
		// Selection requires columns from its conditions
		cols := r.extractColumnsFromExprs(p.Conditions())
		r.requiredCols[planKey] = cols
		fmt.Printf("  [DEBUG] EnhancedColumnPruning: Selection needs columns: %v\n", cols)

	case *LogicalJoin:
		// Join requires columns from both children and conditions
		leftCols := r.getChildRequiredColumns(p, 0)
		rightCols := r.getChildRequiredColumns(p, 1)
		conditionCols := r.extractColumnsFromExprs(p.GetConditions())

		allCols := append(leftCols, rightCols...)
		allCols = append(allCols, conditionCols...)

		// Deduplicate
		uniqueCols := r.deduplicateColumns(allCols)
		r.requiredCols[planKey] = uniqueCols
		fmt.Printf("  [DEBUG] EnhancedColumnPruning: Join needs columns: %v\n", uniqueCols)

	case *LogicalAggregate:
		// Aggregate requires GROUP BY columns and aggregation expressions
		groupByCols := p.GetGroupBy()
		aggFuncs := p.GetAggFuncs()
		aggCols := r.extractColumnsFromAggFuncs(aggFuncs)

		allCols := append(groupByCols, aggCols...)
		r.requiredCols[planKey] = allCols
		fmt.Printf("  [DEBUG] EnhancedColumnPruning: Aggregate needs columns: %v\n", allCols)

	case *LogicalSort:
		// Sort requires columns from order by items
		cols := r.extractColumnsFromSortItems(p.OrderBy())
		r.requiredCols[planKey] = cols
		fmt.Printf("  [DEBUG] EnhancedColumnPruning: Sort needs columns: %v\n", cols)

	case *LogicalTopN:
		// TopN requires columns from sort items
		cols := r.extractColumnsFromSortItems(p.SortItems())
		r.requiredCols[planKey] = cols
		fmt.Printf("  [DEBUG] EnhancedColumnPruning: TopN needs columns: %v\n", cols)

	case *LogicalApply:
		// Apply requires columns from conditions
		cols := r.extractColumnsFromExprs(p.GetConditions())
		r.requiredCols[planKey] = cols
		fmt.Printf("  [DEBUG] EnhancedColumnPruning: Apply needs columns: %v\n", cols)

	case *LogicalWindow:
		// Window requires columns from partition by, order by, and window functions
		cols := []string{}
		for _, wf := range p.WindowFuncs() {
			cols = append(cols, r.extractColumnsFromExprs([]*parser.Expression{wf.Func})...)
			cols = append(cols, r.extractColumnsFromExprs(wf.PartitionBy)...)
			for _, ob := range wf.OrderBy {
				expr := ob.Expr // Take address of the value
				cols = append(cols, r.extractColumnsFromExpr(&expr)...)
			}
		}
		r.requiredCols[planKey] = r.deduplicateColumns(cols)
		fmt.Printf("  [DEBUG] EnhancedColumnPruning: Window needs columns: %v\n", r.requiredCols[planKey])

	case *LogicalLimit:
		// Limit doesn't require any specific columns
		r.requiredCols[planKey] = []string{}

	case *LogicalDataSource:
		// DataSource will be pruned based on what's needed above
		// Default to all columns if not specified
		allCols := make([]string, len(p.Schema()))
		for i, col := range p.Schema() {
			allCols[i] = col.Name
		}
		r.requiredCols[planKey] = allCols
	}

	// Recursively analyze children
	children := plan.Children()
	for _, child := range children {
		r.analyzeRequiredColumns(child)
	}
}

// pruneColumns prunes columns bottom-up
func (r *EnhancedColumnPruningRule) pruneColumns(plan LogicalPlan) (LogicalPlan, error) {
	// First prune children
	children := plan.Children()
	newChildren := []LogicalPlan{}
	for i, child := range children {
		prunedChild, err := r.pruneColumns(child)
		if err != nil {
			return nil, err
		}

		// Propagate required columns to child
		childKey := child.Explain()
		requiredCols := r.getRequiredColumnsForChild(plan, i)

		// Merge with child's own required columns
		childRequired := r.requiredCols[childKey]
		merged := append(requiredCols, childRequired...)
		r.requiredCols[childKey] = r.deduplicateColumns(merged)

		newChildren = append(newChildren, prunedChild)
	}

	plan.SetChildren(newChildren...)

	// Prune current node
	return r.pruneNode(plan)
}

// pruneNode prunes columns for a specific node
func (r *EnhancedColumnPruningRule) pruneNode(plan LogicalPlan) (LogicalPlan, error) {
	switch p := plan.(type) {
	case *LogicalDataSource:
		return r.pruneDataSource(p)
	case *LogicalProjection:
		return r.pruneProjection(p)
	default:
		return plan, nil
	}
}

// pruneDataSource prunes columns in DataSource
func (r *EnhancedColumnPruningRule) pruneDataSource(dataSource *LogicalDataSource) (LogicalPlan, error) {
	planKey := dataSource.Explain()
	requiredCols := r.requiredCols[planKey]

	if len(requiredCols) == 0 || len(requiredCols) == len(dataSource.Schema()) {
		return dataSource, nil
	}

	// Filter columns to keep only required ones
	newColumns := []ColumnInfo{}
	for _, col := range dataSource.Schema() {
		for _, reqCol := range requiredCols {
			if col.Name == reqCol {
				newColumns = append(newColumns, col)
				break
			}
		}
	}

	if len(newColumns) == len(dataSource.Columns) {
		return dataSource, nil
	}

	// Create new DataSource with pruned columns
	fmt.Printf("  [DEBUG] EnhancedColumnPruning: DataSource pruning %s: %d -> %d columns\n",
		dataSource.TableName, len(dataSource.Columns), len(newColumns))

	newDataSource := NewLogicalDataSource(dataSource.TableName, dataSource.TableInfo)
	newDataSource.Columns = newColumns
	newDataSource.Statistics = dataSource.Statistics
	newDataSource.PushDownPredicates(dataSource.GetPushedDownPredicates())

	if limitInfo := dataSource.GetPushedDownLimit(); limitInfo != nil {
		newDataSource.PushDownLimit(limitInfo.Limit, limitInfo.Offset)
	}

	if topNInfo := dataSource.GetPushedDownTopN(); topNInfo != nil {
		newDataSource.SetPushDownTopN(topNInfo.SortItems, topNInfo.Limit, topNInfo.Offset)
	}

	return newDataSource, nil
}

// pruneProjection prunes columns in Projection
func (r *EnhancedColumnPruningRule) pruneProjection(projection *LogicalProjection) (LogicalPlan, error) {
	// For projection, we don't remove the projection itself
	// But we could optimize by removing unused expressions
	// For now, just return as-is
	return projection, nil
}

// getRequiredColumnsForChild gets required columns for a specific child
func (r *EnhancedColumnPruningRule) getRequiredColumnsForChild(plan LogicalPlan, childIndex int) []string {
	switch p := plan.(type) {
	case *LogicalJoin:
		if childIndex == 0 {
			// Left child needs columns from left side of conditions
			return r.extractColumnsFromExprs(p.GetConditions())
		} else {
			// Right child needs columns from right side of conditions
			return r.extractColumnsFromExprs(p.GetConditions())
		}
	default:
		return []string{}
	}
}

// extractColumnsFromExprs extracts column names from expressions
func (r *EnhancedColumnPruningRule) extractColumnsFromExprs(exprs []*parser.Expression) []string {
	cols := []string{}

	for _, expr := range exprs {
		cols = append(cols, r.extractColumnsFromExpr(expr)...)
	}
	return cols
}

// extractColumnsFromExpr extracts column names from a single expression
func (r *EnhancedColumnPruningRule) extractColumnsFromExpr(expr *parser.Expression) []string {
	if expr == nil {
		return []string{}
	}

	cols := []string{}

	if expr.Type == parser.ExprTypeColumn && expr.Column != "" {
		cols = append(cols, expr.Column)
	}

	// Recursively extract from sub-expressions
	cols = append(cols, r.extractColumnsFromExpr(expr.Left)...)
	cols = append(cols, r.extractColumnsFromExpr(expr.Right)...)

	// Check function arguments
	for _, arg := range expr.Args {
		cols = append(cols, r.extractColumnsFromExpr(&arg)...)
	}

	return cols
}

// extractColumnsFromAggFuncs extracts columns from aggregation functions
func (r *EnhancedColumnPruningRule) extractColumnsFromAggFuncs(aggFuncs []*AggregationItem) []string {
	cols := []string{}

	for _, agg := range aggFuncs {
		if agg.Expr != nil {
			cols = append(cols, r.extractColumnsFromExpr(agg.Expr)...)
		}
	}

	return cols
}

// extractColumnsFromSortItems extracts columns from sort items
func (r *EnhancedColumnPruningRule) extractColumnsFromSortItems(sortItems []*parser.OrderItem) []string {
	cols := []string{}

	for _, item := range sortItems {
		// item.Expr is of type parser.Expression (value), need to take address
		expr := item.Expr
		cols = append(cols, r.extractColumnsFromExpr(&expr)...)
	}

	return cols
}

// deduplicateColumns removes duplicate column names
func (r *EnhancedColumnPruningRule) deduplicateColumns(cols []string) []string {
	seen := make(map[string]bool)
	unique := []string{}

	for _, col := range cols {
		if !seen[col] {
			seen[col] = true
			unique = append(unique, col)
		}
	}

	return unique
}

// getChildRequiredColumns gets required columns from child schema
func (r *EnhancedColumnPruningRule) getChildRequiredColumns(join *LogicalJoin, childIndex int) []string {
	if childIndex >= len(join.children) {
		return []string{}
	}

	child := join.children[childIndex]
	schema := child.Schema()

	cols := make([]string, len(schema))
	for i, col := range schema {
		cols[i] = col.Name
	}

	return cols
}
