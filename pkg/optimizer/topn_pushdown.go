package optimizer

import (
	"context"

	"github.com/kasuganosora/sqlexec/pkg/parser"
)

// TopNPushDownRule pushes TopN down to data source
type TopNPushDownRule struct{}

// NewTopNPushDownRule creates a new TopN pushdown rule
func NewTopNPushDownRule() *TopNPushDownRule {
	return &TopNPushDownRule{}
}

// Name returns the rule name
func (r *TopNPushDownRule) Name() string {
	return "TopNPushDown"
}

// Match checks if the rule matches a TopN node
func (r *TopNPushDownRule) Match(plan LogicalPlan) bool {
	_, ok := plan.(*LogicalTopN)
	return ok
}

// Apply applies the TopN pushdown rule
func (r *TopNPushDownRule) Apply(ctx context.Context, plan LogicalPlan, optCtx *OptimizationContext) (LogicalPlan, error) {
	topn, ok := plan.(*LogicalTopN)
	if !ok {
		return plan, nil
	}

	if len(topn.children) == 0 {
		return plan, nil
	}

	child := topn.children[0]

	// Pattern 1: TopN -> DataSource -> push to DataSource
	if dataSource, ok := child.(*LogicalDataSource); ok {
		return r.pushToDataSource(topn, dataSource)
	}

	// Pattern 2: TopN -> Projection -> DataSource -> try to push through
	if projection, ok := child.(*LogicalProjection); ok {
		if len(projection.children) > 0 {
			dataSource, ok := projection.children[0].(*LogicalDataSource)
			if ok {
				return r.pushThroughProjection(topn, projection, dataSource)
			}
		}
	}

	// Pattern 3: TopN -> Selection -> DataSource -> try to push through
	if selection, ok := child.(*LogicalSelection); ok {
		if len(selection.children) > 0 {
			dataSource, ok := selection.children[0].(*LogicalDataSource)
			if ok {
				return r.pushThroughSelection(topn, selection, dataSource)
			}
		}
	}

	return plan, nil
}

// pushToDataSource pushes TopN directly to DataSource
func (r *TopNPushDownRule) pushToDataSource(topn *LogicalTopN, dataSource *LogicalDataSource) (LogicalPlan, error) {
	// Check if sort items reference columns from DataSource
	valid := true
	for _, item := range topn.sortItems {
		// item.Expr is of type parser.Expression (not pointer)

		// Check if expression references a column from DataSource
		if item.Expr.Type == parser.ExprTypeColumn {
			found := false
			for _, col := range dataSource.Schema() {
				if col.Name == item.Expr.Column {
					found = true
					break
				}
			}
			if !found {
				valid = false
				break
			}
		}
	}

	if !valid {
		return topn, nil
	}

	// Push TopN to DataSource
	dataSource.SetPushDownTopN(topn.sortItems, topn.limit, topn.offset)

	// Return DataSource without TopN
	return dataSource, nil
}

// pushThroughProjection pushes TopN through Projection
func (r *TopNPushDownRule) pushThroughProjection(topn *LogicalTopN, projection *LogicalProjection, dataSource *LogicalDataSource) (LogicalPlan, error) {
	// Check if we can push TopN through Projection
	// Condition: TopN sort items must reference columns that come from DataSource

	// Try to push TopN to DataSource
	dataSource.SetPushDownTopN(topn.sortItems, topn.limit, topn.offset)

	// Return Projection with DataSource without TopN
	projection.children[0] = dataSource
	return projection, nil
}

// pushThroughSelection pushes TopN through Selection
func (r *TopNPushDownRule) pushThroughSelection(topn *LogicalTopN, selection *LogicalSelection, dataSource *LogicalDataSource) (LogicalPlan, error) {
	// Check if we can push TopN through Selection
	// Condition: Selection predicates must not depend on sorted order

	// Push TopN to DataSource
	dataSource.SetPushDownTopN(topn.sortItems, topn.limit, topn.offset)

	// Return Selection with DataSource without TopN
	selection.children[0] = dataSource
	return selection, nil
}
