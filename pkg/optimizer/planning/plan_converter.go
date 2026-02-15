package planning

import (
	"context"
	"fmt"
	"strings"

	"github.com/kasuganosora/sqlexec/pkg/optimizer"
	"github.com/kasuganosora/sqlexec/pkg/optimizer/plan"
	"github.com/kasuganosora/sqlexec/pkg/parser"
	"github.com/kasuganosora/sqlexec/pkg/types"
)

// convertToPlan convert logical plan to serializable Plan
func (o *Optimizer) convertToPlan(ctx context.Context, logicalPlan optimizer.LogicalPlan, optCtx *OptimizationContext) (*plan.Plan, error) {
	switch p := logicalPlan.(type) {
	case *optimizer.LogicalDataSource:
		// Get pushed down predicates
		pushedDownPredicates := p.GetPushedDownPredicates()
		filters := o.convertConditionsToFilters(pushedDownPredicates)
		// Get pushed down Limit
		limitInfo := p.GetPushedDownLimit()

		// Build column info
		columns := make([]types.ColumnInfo, 0, len(p.TableInfo.Columns))
		for _, col := range p.TableInfo.Columns {
			columns = append(columns, types.ColumnInfo{
				Name:     col.Name,
				Type:     col.Type,
				Nullable: col.Nullable,
			})
		}

		return &plan.Plan{
			ID:          fmt.Sprintf("scan_%s", p.TableName),
			Type:        plan.TypeTableScan,
			OutputSchema: columns,
			Children:    []*plan.Plan{},
			Config: &plan.TableScanConfig{
				TableName:       p.TableName,
				Columns:         columns,
				Filters:         filters,
				LimitInfo:       convertToTypesLimitInfo(limitInfo),
				EnableParallel:  true,
				MinParallelRows: 100,
			},
		}, nil
	case *optimizer.LogicalSelection:
		child, err := o.convertToPlan(ctx, p.Children()[0], optCtx)
		if err != nil {
			return nil, err
		}
		return &plan.Plan{
			ID:          fmt.Sprintf("sel_%d", len(p.GetConditions())),
			Type:        plan.TypeSelection,
			OutputSchema: child.OutputSchema,
			Children:    []*plan.Plan{child},
			Config: &plan.SelectionConfig{
				Condition: p.GetConditions()[0],
			},
		}, nil
	case *optimizer.LogicalProjection:
		child, err := o.convertToPlan(ctx, p.Children()[0], optCtx)
		if err != nil {
			return nil, err
		}
		exprs := p.GetExprs()
		aliases := p.GetAliases()

		return &plan.Plan{
			ID:          fmt.Sprintf("proj_%d", len(exprs)),
			Type:        plan.TypeProjection,
			OutputSchema: child.OutputSchema,
			Children:    []*plan.Plan{child},
			Config: &plan.ProjectionConfig{
				Expressions: exprs,
				Aliases:     aliases,
			},
		}, nil
	case *optimizer.LogicalLimit:
		child, err := o.convertToPlan(ctx, p.Children()[0], optCtx)
		if err != nil {
			return nil, err
		}
		return &plan.Plan{
			ID:          fmt.Sprintf("limit_%d_%d", p.GetLimit(), p.GetOffset()),
			Type:        plan.TypeLimit,
			OutputSchema: child.OutputSchema,
			Children:    []*plan.Plan{child},
			Config: &plan.LimitConfig{
				Limit:  p.GetLimit(),
				Offset: p.GetOffset(),
			},
		}, nil
	case *optimizer.LogicalSort:
		// Simplified: temporarily don't implement sort, directly return child node
		return o.convertToPlan(ctx, p.Children()[0], optCtx)
	case *optimizer.LogicalJoin:
		left, err := o.convertToPlan(ctx, p.Children()[0], optCtx)
		if err != nil {
			return nil, err
		}
		right, err := o.convertToPlan(ctx, p.Children()[1], optCtx)
		if err != nil {
			return nil, err
		}
		joinConditions := p.GetJoinConditions()
		// Convert JoinCondition
		convertedConditions := make([]*types.JoinCondition, len(joinConditions))
		for i, cond := range joinConditions {
			convertedConditions[i] = &types.JoinCondition{
				Left:     convertParserExprToTypesExpr(cond.Left),
				Right:    convertParserExprToTypesExpr(cond.Right),
				Operator: cond.Operator,
			}
		}

		return &plan.Plan{
			ID:          fmt.Sprintf("join_%s", joinConditions[0].Operator),
			Type:        plan.TypeHashJoin,
			OutputSchema: left.OutputSchema,
			Children:    []*plan.Plan{left, right},
			Config: &plan.HashJoinConfig{
				JoinType:  types.JoinType(p.GetJoinType()),
				LeftCond:  convertedConditions[0],
				RightCond: convertedConditions[0],
				BuildSide: "left",
			},
		}, nil
	case *optimizer.LogicalAggregate:
		child, err := o.convertToPlan(ctx, p.Children()[0], optCtx)
		if err != nil {
			return nil, err
		}
		// Convert aggFuncs to types.AggregationItem
		aggFuncs := p.GetAggFuncs()
		convertedAggFuncs := make([]*types.AggregationItem, len(aggFuncs))
		for i, agg := range aggFuncs {
			convertedAggFuncs[i] = &types.AggregationItem{
				Type:     types.AggregationType(agg.Type),
				Expr:     convertParserExprToTypesExpr(agg.Expr),
				Alias:    agg.Alias,
				Distinct: agg.Distinct,
			}
		}

		return &plan.Plan{
			ID:          fmt.Sprintf("agg_%d", len(p.GetGroupByCols())),
			Type:        plan.TypeAggregate,
			OutputSchema: child.OutputSchema,
			Children:    []*plan.Plan{child},
			Config: &plan.AggregateConfig{
				AggFuncs:    convertedAggFuncs,
				GroupByCols: p.GetGroupByCols(),
			},
		}, nil
	default:
		return nil, fmt.Errorf("unsupported logical plan type: %T", p)
	}
}

// convertToTypesLimitInfo convert LimitInfo
func convertToTypesLimitInfo(limitInfo *optimizer.LimitInfo) *types.LimitInfo {
	if limitInfo == nil {
		return nil
	}
	return &types.LimitInfo{
		Limit:  limitInfo.Limit,
		Offset: limitInfo.Offset,
	}
}

// convertParserExprToTypesExpr convert parser.Expression to types.Expression
func convertParserExprToTypesExpr(expr *parser.Expression) *types.Expression {
	if expr == nil {
		return nil
	}
	return &types.Expression{
		Type:     string(expr.Type),
		Column:   expr.Column,
		Value:    expr.Value,
		Operator: expr.Operator,
		Left:     convertParserExprToTypesExpr(expr.Left),
		Right:    convertParserExprToTypesExpr(expr.Right),
	}
}

// ExplainPlan explain execution plan
func ExplainPlan(plan optimizer.PhysicalPlan) string {
	return explainPlan(plan, 0)
}

// ExplainPlanV2 explain new architecture execution plan (plan.Plan)
func ExplainPlanV2(plan *plan.Plan) string {
	return explainPlanV2(plan, 0)
}

// explainPlanV2 recursively explain new architecture plan
func explainPlanV2(plan *plan.Plan, depth int) string {
	var builder strings.Builder

	for i := 0; i < depth; i++ {
		builder.WriteString("  ")
	}

	builder.WriteString(plan.ID)
	builder.WriteString(" [")
	builder.WriteString(string(plan.Type))
	builder.WriteString("]")
	builder.WriteString("\n")

	for _, child := range plan.Children {
		builder.WriteString(explainPlanV2(child, depth+1))
	}

	return builder.String()
}

// explainPlan recursively explain plan
func explainPlan(plan optimizer.PhysicalPlan, depth int) string {
	var builder strings.Builder

	for i := 0; i < depth; i++ {
		builder.WriteString("  ")
	}
	builder.WriteString(plan.Explain())
	builder.WriteString("\n")

	for _, child := range plan.Children() {
		builder.WriteString(explainPlan(child, depth+1))
	}

	return builder.String()
}
