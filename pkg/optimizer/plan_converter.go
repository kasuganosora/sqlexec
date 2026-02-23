package optimizer

import (
	"context"
	"fmt"

	"github.com/kasuganosora/sqlexec/pkg/optimizer/cost"
	"github.com/kasuganosora/sqlexec/pkg/optimizer/index"
	"github.com/kasuganosora/sqlexec/pkg/optimizer/plan"
	"github.com/kasuganosora/sqlexec/pkg/parser"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/types"
)

// PlanConverter converts logical plans to physical plans
// This is shared between EnhancedOptimizer and EnhancedOptimizerV2
type PlanConverter struct {
	costModel     cost.CostModel
	indexSelector IndexSelector
}

// IndexSelector is the interface for index selection
type IndexSelector interface {
	SelectBestIndex(tableName string, filters []domain.Filter, requiredCols []string) *index.IndexSelection
}

// NewPlanConverter creates a new plan converter
func NewPlanConverter(costModel cost.CostModel, indexSelector IndexSelector) *PlanConverter {
	return &PlanConverter{
		costModel:     costModel,
		indexSelector: indexSelector,
	}
}

// ConvertToPlan converts a logical plan to a physical plan
func (pc *PlanConverter) ConvertToPlan(ctx context.Context, logicalPlan LogicalPlan, optCtx *OptimizationContext) (*plan.Plan, error) {
	switch p := logicalPlan.(type) {
	case *LogicalDataSource:
		return pc.convertDataSource(ctx, p)
	case *LogicalSelection:
		return pc.convertSelection(ctx, p, optCtx)
	case *LogicalProjection:
		return pc.convertProjection(ctx, p, optCtx)
	case *LogicalLimit:
		return pc.convertLimit(ctx, p, optCtx)
	case *LogicalSort:
		return pc.convertSort(ctx, p, optCtx)
	case *LogicalJoin:
		return pc.convertJoin(ctx, p, optCtx)
	case *LogicalAggregate:
		return pc.convertAggregate(ctx, p, optCtx)
	case *LogicalUnion:
		return pc.convertUnion(ctx, p, optCtx)
	case *LogicalInsert:
		return pc.convertInsert(ctx, p, optCtx)
	case *LogicalUpdate:
		return pc.convertUpdate(ctx, p, optCtx)
	case *LogicalDelete:
		return pc.convertDelete(ctx, p, optCtx)
	default:
		return nil, fmt.Errorf("unsupported logical plan type: %T", p)
	}
}

func (pc *PlanConverter) convertDataSource(ctx context.Context, p *LogicalDataSource) (*plan.Plan, error) {
	tableName := p.TableName

	// Apply index selection
	requiredCols := make([]string, 0, len(p.Columns))
	for _, col := range p.Columns {
		requiredCols = append(requiredCols, col.Name)
	}

	// Extract filters from predicates
	filters := convertPredicatesToFilters(p.GetPushedDownPredicates())

	// Select best index
	var indexSelection *index.IndexSelection
	if pc.indexSelector != nil {
		indexSelection = pc.indexSelector.SelectBestIndex(tableName, filters, requiredCols)
		debugf("  [CONVERTER] Index Selection: %s\n", indexSelection.String())
	}

	// Use index or full table scan
	useIndex := indexSelection != nil && indexSelection.SelectedIndex != nil

	// Build column info
	columns := make([]types.ColumnInfo, 0, len(p.TableInfo.Columns))
	for _, col := range p.TableInfo.Columns {
		columns = append(columns, types.ColumnInfo{
			Name:     col.Name,
			Type:     col.Type,
			Nullable: col.Nullable,
		})
	}

	// Apply column pruning
	if len(p.Columns) < len(p.TableInfo.Columns) {
		columns = make([]types.ColumnInfo, 0, len(p.Columns))
		for _, col := range p.Columns {
			columns = append(columns, types.ColumnInfo{
				Name:     col.Name,
				Type:     col.Type,
				Nullable: col.Nullable,
			})
		}
		debugf("  [CONVERTER] Applying column pruning: %d columns reduced to %d\n", len(p.TableInfo.Columns), len(p.Columns))
	}

	// Update cost
	scanCost := pc.costModel.ScanCost(tableName, 10000, useIndex)

	return &plan.Plan{
		ID:           fmt.Sprintf("scan_%s", tableName),
		Type:         plan.TypeTableScan,
		OutputSchema: columns,
		Children:     []*plan.Plan{},
		Config: &plan.TableScanConfig{
			TableName:       tableName,
			Columns:         columns,
			Filters:         filters,
			LimitInfo:       &types.LimitInfo{Limit: 0, Offset: 0},
			EnableParallel:  true,
			MinParallelRows: 100,
		},
		EstimatedCost: scanCost,
	}, nil
}

func (pc *PlanConverter) convertSelection(ctx context.Context, p *LogicalSelection, optCtx *OptimizationContext) (*plan.Plan, error) {
	if len(p.Children()) == 0 {
		return nil, fmt.Errorf("selection has no child")
	}

	// Convert child
	child, err := pc.ConvertToPlan(ctx, p.Children()[0], optCtx)
	if err != nil {
		return nil, err
	}

	// Calculate cost
	_ = pc.costModel.FilterCost(int64(10000), 0.1, nil)

	conditions := p.GetConditions()
	if len(conditions) == 0 {
		return nil, fmt.Errorf("selection has no conditions")
	}

	return &plan.Plan{
		ID:           fmt.Sprintf("sel_%d", len(conditions)),
		Type:         plan.TypeSelection,
		OutputSchema: child.OutputSchema,
		Children:     []*plan.Plan{child},
		Config: &plan.SelectionConfig{
			Condition: conditions[0],
		},
	}, nil
}

func (pc *PlanConverter) convertProjection(ctx context.Context, p *LogicalProjection, optCtx *OptimizationContext) (*plan.Plan, error) {
	if len(p.Children()) == 0 {
		return nil, fmt.Errorf("projection has no child")
	}

	child, err := pc.ConvertToPlan(ctx, p.Children()[0], optCtx)
	if err != nil {
		return nil, err
	}

	// Convert projection
	projExprs := p.GetExprs()
	aliases := p.GetAliases()

	// Calculate cost
	projCols := len(projExprs)
	_ = pc.costModel.ProjectCost(int64(10000), projCols)

	return &plan.Plan{
		ID:           fmt.Sprintf("proj_%d", len(projExprs)),
		Type:         plan.TypeProjection,
		OutputSchema: child.OutputSchema,
		Children:     []*plan.Plan{child},
		Config: &plan.ProjectionConfig{
			Expressions: projExprs,
			Aliases:     aliases,
		},
	}, nil
}

func (pc *PlanConverter) convertLimit(ctx context.Context, p *LogicalLimit, optCtx *OptimizationContext) (*plan.Plan, error) {
	if len(p.Children()) == 0 {
		return nil, fmt.Errorf("limit has no child")
	}

	child, err := pc.ConvertToPlan(ctx, p.Children()[0], optCtx)
	if err != nil {
		return nil, err
	}

	limit := p.GetLimit()
	offset := p.GetOffset()

	return &plan.Plan{
		ID:           fmt.Sprintf("limit_%d_%d", limit, offset),
		Type:         plan.TypeLimit,
		OutputSchema: child.OutputSchema,
		Children:     []*plan.Plan{child},
		Config: &plan.LimitConfig{
			Limit:  limit,
			Offset: offset,
		},
	}, nil
}

func (pc *PlanConverter) convertSort(ctx context.Context, p *LogicalSort, optCtx *OptimizationContext) (*plan.Plan, error) {
	if len(p.Children()) == 0 {
		return nil, fmt.Errorf("sort has no child")
	}

	child, err := pc.ConvertToPlan(ctx, p.Children()[0], optCtx)
	if err != nil {
		return nil, err
	}

	// Get sort items
	orderBy := p.GetOrderBy()

	return &plan.Plan{
		ID:           fmt.Sprintf("sort_%d", len(orderBy)),
		Type:         plan.TypeSort,
		OutputSchema: child.OutputSchema,
		Children:     []*plan.Plan{child},
		Config: &plan.SortConfig{
			OrderByItems: orderBy,
		},
	}, nil
}

func (pc *PlanConverter) convertJoin(ctx context.Context, p *LogicalJoin, optCtx *OptimizationContext) (*plan.Plan, error) {
	if len(p.Children()) != 2 {
		return nil, fmt.Errorf("join must have exactly 2 children")
	}

	// Convert left and right children
	left, err := pc.ConvertToPlan(ctx, p.Children()[0], optCtx)
	if err != nil {
		return nil, err
	}

	right, err := pc.ConvertToPlan(ctx, p.Children()[1], optCtx)
	if err != nil {
		return nil, err
	}

	// Calculate JOIN cost - convert JoinType to cost.JoinType
	var costJoinType cost.JoinType
	switch p.GetJoinType() {
	case InnerJoin:
		costJoinType = cost.InnerJoin
	case LeftOuterJoin:
		costJoinType = cost.LeftOuterJoin
	case RightOuterJoin:
		costJoinType = cost.RightOuterJoin
	case FullOuterJoin:
		costJoinType = cost.FullOuterJoin
	default:
		costJoinType = cost.InnerJoin
	}

	_ = pc.costModel.JoinCost(10000, 10000, costJoinType, convertJoinConditionsToExpressions(p.GetJoinConditions()))

	debugln("  [CONVERTER] Using original JOIN plan")

	// Build output schema
	outputSchema := make([]types.ColumnInfo, 0, len(left.OutputSchema)+len(right.OutputSchema))
	outputSchema = append(outputSchema, left.OutputSchema...)
	outputSchema = append(outputSchema, right.OutputSchema...)

	joinConditions := p.GetJoinConditions()
	joinPlan := &plan.Plan{
		ID:           fmt.Sprintf("join_%d", len(joinConditions)),
		Type:         plan.TypeHashJoin,
		OutputSchema: outputSchema,
		Children:     []*plan.Plan{left, right},
		Config: &plan.HashJoinConfig{
			JoinType:  types.JoinType(p.GetJoinType()),
			BuildSide: "left",
		},
	}
	if len(joinConditions) > 0 {
		cfg := joinPlan.Config.(*plan.HashJoinConfig)
		cfg.LeftCond = convertToTypesJoinCondition(joinConditions[0])
		cfg.RightCond = convertToTypesJoinCondition(joinConditions[0])
	}
	return joinPlan, nil
}

func (pc *PlanConverter) convertAggregate(ctx context.Context, p *LogicalAggregate, optCtx *OptimizationContext) (*plan.Plan, error) {
	if len(p.Children()) == 0 {
		return nil, fmt.Errorf("aggregate has no child")
	}

	child, err := pc.ConvertToPlan(ctx, p.Children()[0], optCtx)
	if err != nil {
		return nil, err
	}

	// Convert aggregation
	groupByCols := p.GetGroupByCols()
	aggFuncs := p.GetAggFuncs()

	// Calculate cost
	_ = pc.costModel.AggregateCost(int64(10000), len(groupByCols), len(aggFuncs))

	// Convert aggFuncs to types.AggregationItem
	convertedAggFuncs := make([]*types.AggregationItem, len(aggFuncs))
	for i, agg := range aggFuncs {
		convertedAggFuncs[i] = &types.AggregationItem{
			Type:     types.AggregationType(agg.Type),
			Expr:     convertToTypesExpr(agg.Expr),
			Alias:    agg.Alias,
			Distinct: agg.Distinct,
		}
	}

	return &plan.Plan{
		ID:           fmt.Sprintf("agg_%d_%d", len(groupByCols), len(aggFuncs)),
		Type:         plan.TypeAggregate,
		OutputSchema: child.OutputSchema,
		Children:     []*plan.Plan{child},
		Config: &plan.AggregateConfig{
			GroupByCols: groupByCols,
			AggFuncs:    convertedAggFuncs,
		},
	}, nil
}

func (pc *PlanConverter) convertUnion(ctx context.Context, p *LogicalUnion, optCtx *OptimizationContext) (*plan.Plan, error) {
	if len(p.Children()) == 0 {
		return nil, fmt.Errorf("union has no child")
	}

	// Convert all children
	children := make([]*plan.Plan, 0, len(p.Children()))
	for _, child := range p.Children() {
		converted, err := pc.ConvertToPlan(ctx, child, optCtx)
		if err != nil {
			return nil, err
		}
		children = append(children, converted)
	}

	// Use first child's schema as output schema
	outputSchema := children[0].OutputSchema

	return &plan.Plan{
		ID:           fmt.Sprintf("union_%d", len(children)),
		Type:         plan.TypeUnion,
		OutputSchema: outputSchema,
		Children:     children,
		Config: &plan.UnionConfig{
			Distinct: !p.IsAll(),
		},
	}, nil
}

func (pc *PlanConverter) convertInsert(ctx context.Context, p *LogicalInsert, optCtx *OptimizationContext) (*plan.Plan, error) {
	// Handle INSERT ... SELECT case
	if p.HasSelect() {
		selectPlan, err := pc.ConvertToPlan(ctx, p.GetSelectPlan(), optCtx)
		if err != nil {
			return nil, fmt.Errorf("failed to convert SELECT for INSERT: %v", err)
		}

		var onDuplicate *map[string]parser.Expression
		if p.OnDuplicate != nil {
			onDuplicateMap := p.OnDuplicate.GetSet()
			onDuplicate = &onDuplicateMap
		}

		return &plan.Plan{
			ID:   fmt.Sprintf("insert_%s_select", p.TableName),
			Type: plan.TypeInsert,
			OutputSchema: []types.ColumnInfo{
				{Name: "rows_affected", Type: "int", Nullable: false},
				{Name: "last_insert_id", Type: "int", Nullable: true},
			},
			Children: []*plan.Plan{selectPlan},
			Config: &plan.InsertConfig{
				TableName:   p.TableName,
				Columns:     p.Columns,
				Values:      p.Values,
				OnDuplicate: onDuplicate,
			},
		}, nil
	}

	// Handle direct value insert case
	var onDuplicate *map[string]parser.Expression
	if p.OnDuplicate != nil {
		onDuplicateMap := p.OnDuplicate.GetSet()
		onDuplicate = &onDuplicateMap
	}

	return &plan.Plan{
		ID:   fmt.Sprintf("insert_%s_values", p.TableName),
		Type: plan.TypeInsert,
		OutputSchema: []types.ColumnInfo{
			{Name: "rows_affected", Type: "int", Nullable: false},
			{Name: "last_insert_id", Type: "int", Nullable: true},
		},
		Children: []*plan.Plan{},
		Config: &plan.InsertConfig{
			TableName:   p.TableName,
			Columns:     p.Columns,
			Values:      p.Values,
			OnDuplicate: onDuplicate,
		},
	}, nil
}

func (pc *PlanConverter) convertUpdate(ctx context.Context, p *LogicalUpdate, optCtx *OptimizationContext) (*plan.Plan, error) {
	return &plan.Plan{
		ID:   fmt.Sprintf("update_%s", p.TableName),
		Type: plan.TypeUpdate,
		OutputSchema: []types.ColumnInfo{
			{Name: "rows_affected", Type: "int", Nullable: false},
		},
		Children: []*plan.Plan{},
		Config: &plan.UpdateConfig{
			TableName: p.TableName,
			Set:       p.GetSet(),
			Where:     p.GetWhere(),
			OrderBy:   p.GetOrderBy(),
			Limit:     p.GetLimit(),
		},
	}, nil
}

func (pc *PlanConverter) convertDelete(ctx context.Context, p *LogicalDelete, optCtx *OptimizationContext) (*plan.Plan, error) {
	return &plan.Plan{
		ID:   fmt.Sprintf("delete_%s", p.TableName),
		Type: plan.TypeDelete,
		OutputSchema: []types.ColumnInfo{
			{Name: "rows_affected", Type: "int", Nullable: false},
		},
		Children: []*plan.Plan{},
		Config: &plan.DeleteConfig{
			TableName: p.TableName,
			Where:     p.GetWhere(),
			OrderBy:   p.GetOrderBy(),
			Limit:     p.GetLimit(),
		},
	}, nil
}
