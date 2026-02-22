package optimizer

import (
	"context"
	"fmt"

	"github.com/kasuganosora/sqlexec/pkg/optimizer/plan"
	"github.com/kasuganosora/sqlexec/pkg/parser"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/types"
)

// Optimizer 优化器
type Optimizer struct {
	rules      RuleSet
	costModel  CostModel
	dataSource domain.DataSource
	planCache  *PlanCache
}

// NewOptimizer 创建优化器
func NewOptimizer(dataSource domain.DataSource) *Optimizer {
	return &Optimizer{
		rules:      DefaultRuleSet(),
		costModel:  NewDefaultCostModel(),
		dataSource: dataSource,
		planCache:  NewPlanCache(1024),
	}
}

// Optimize 优化查询计划（返回可序列化的Plan）
func (o *Optimizer) Optimize(ctx context.Context, stmt *parser.SQLStatement) (*plan.Plan, error) {
	fp := SQLFingerprint(stmt)
	if cached, ok := o.planCache.Get(fp); ok {
		return cached, nil
	}

	debugln("  [DEBUG] Optimize: 步骤1 - 转换为逻辑计划")
	// 1. 转换为逻辑计划
	logicalPlan, err := o.convertToLogicalPlan(stmt)
	if err != nil {
		return nil, fmt.Errorf("convert to logical plan failed: %w", err)
	}
	debugln("  [DEBUG] Optimize: 逻辑计划转换完成, 类型:", logicalPlan.Explain())

	// 2. 应用优化规则
	debugln("  [DEBUG] Optimize: 步骤2 - 应用优化规则")
	optCtx := &OptimizationContext{
		DataSource: o.dataSource,
		TableInfo: make(map[string]*domain.TableInfo),
		Stats:      make(map[string]*Statistics),
		CostModel:  o.costModel,
	}

	optimizedPlan, err := o.rules.Apply(ctx, logicalPlan, optCtx)
	if err != nil {
		return nil, fmt.Errorf("apply optimization rules failed: %w", err)
	}
	debugln("  [DEBUG] Optimize: 优化规则应用完成")

	// 3. 转换为可序列化的Plan
	debugln("  [DEBUG] Optimize: 步骤3 - 转换为可序列化的Plan")
	resultPlan, err := o.convertToPlan(ctx, optimizedPlan, optCtx)
	if err != nil {
		return nil, fmt.Errorf("convert to plan failed: %w", err)
	}
	debugln("  [DEBUG] Optimize: Plan转换完成")

	o.planCache.Put(fp, resultPlan)
	return resultPlan, nil
}

// convertToLogicalPlan 将 SQL 语句转换为逻辑计划
func (o *Optimizer) convertToLogicalPlan(stmt *parser.SQLStatement) (LogicalPlan, error) {
	switch stmt.Type {
	case parser.SQLTypeSelect:
		return o.convertSelect(stmt.Select)
	case parser.SQLTypeInsert:
		return o.convertInsert(stmt.Insert)
	case parser.SQLTypeUpdate:
		return o.convertUpdate(stmt.Update)
	case parser.SQLTypeDelete:
		return o.convertDelete(stmt.Delete)
	default:
		return nil, fmt.Errorf("unsupported SQL type: %s", stmt.Type)
	}
}

// convertSelect 转换 SELECT 语句
func (o *Optimizer) convertSelect(stmt *parser.SelectStatement) (LogicalPlan, error) {
	debugln("  [DEBUG] convertSelect: 开始转换, 表名:", stmt.From)

	// 处理没有 FROM 子句的查询（如 SELECT DATABASE()）
	if stmt.From == "" {
		debugln("  [DEBUG] convertSelect: 无 FROM 子句，使用常量数据源")
		// 创建一个虚拟表，只有一行一列
		virtualTableInfo := &domain.TableInfo{
			Name:    "dual",
			Columns: []domain.ColumnInfo{},
		}

		var logicalPlan LogicalPlan = NewLogicalDataSource("dual", virtualTableInfo)

		// 应用 WHERE 条件（Selection）
		if stmt.Where != nil {
			conditions := o.extractConditions(stmt.Where)
			logicalPlan = NewLogicalSelection(conditions, logicalPlan)
		}

		// 应用 GROUP BY（Aggregate）
		if len(stmt.GroupBy) > 0 {
			aggFuncs := o.extractAggFuncs(stmt.Columns)
			logicalPlan = NewLogicalAggregate(aggFuncs, stmt.GroupBy, logicalPlan)
		}

		// Apply ORDER BY (Sort)
		if len(stmt.OrderBy) > 0 {
			sortItems := make([]parser.OrderItem, len(stmt.OrderBy))
			for i, item := range stmt.OrderBy {
				sortItems[i] = parser.OrderItem{
					Expr: parser.Expression{
						Type: parser.ExprTypeColumn,
						Column: item.Column,
					},
					Direction: item.Direction,
				}
			}
			// Create pointer slice for NewLogicalSort
			sortItemsPtr := make([]*parser.OrderItem, len(sortItems))
			for i := range sortItems {
				sortItemsPtr[i] = &sortItems[i]
			}
			logicalPlan = NewLogicalSort(sortItemsPtr, logicalPlan)
		}

		// 应用 LIMIT（Limit）
		if stmt.Limit != nil {
			limit := *stmt.Limit
			offset := int64(0)
			if stmt.Offset != nil {
				offset = *stmt.Offset
			}
			logicalPlan = NewLogicalLimit(limit, offset, logicalPlan)
		}

		return logicalPlan, nil
	}

	// 1. 创建 DataSource
	tableInfo, err := o.dataSource.GetTableInfo(context.Background(), stmt.From)
	if err != nil {
		debugln("  [DEBUG] convertSelect: GetTableInfo 失败:", err)
		return nil, fmt.Errorf("get table info failed: %w", err)
	}
	debugln("  [DEBUG] convertSelect: GetTableInfo 成功, 列数:", len(tableInfo.Columns))

	var logicalPlan LogicalPlan = NewLogicalDataSource(stmt.From, tableInfo)
	debugln("  [DEBUG] convertSelect: LogicalDataSource 创建完成")

	// 2. 应用 WHERE 条件（Selection）
	if stmt.Where != nil {
		conditions := o.extractConditions(stmt.Where)
		logicalPlan = NewLogicalSelection(conditions, logicalPlan)
	}

	// 3. 应用 GROUP BY 或独立聚合函数（Aggregate）
	// 没有 GROUP BY 但存在聚合函数（如 SELECT count(*) FROM t）时也需要 AggregateOperator
	aggFuncs := o.extractAggFuncs(stmt.Columns)
	if len(stmt.GroupBy) > 0 || len(aggFuncs) > 0 {
		logicalPlan = NewLogicalAggregate(aggFuncs, stmt.GroupBy, logicalPlan)
	}

	// 4. 应用 ORDER BY（Sort）
	if len(stmt.OrderBy) > 0 {
		sortItems := make([]parser.OrderItem, len(stmt.OrderBy))
		for i, item := range stmt.OrderBy {
			sortItems[i] = parser.OrderItem{
				Expr: parser.Expression{
					Type: parser.ExprTypeColumn,
					Column: item.Column,
				},
				Direction: item.Direction,
			}
		}
		// Create pointer slice for NewLogicalSort
		sortItemsPtr := make([]*parser.OrderItem, len(sortItems))
		for i := range sortItems {
			sortItemsPtr[i] = &sortItems[i]
		}
		logicalPlan = NewLogicalSort(sortItemsPtr, logicalPlan)
	}

	// 5. 应用 LIMIT（Limit）
	if stmt.Limit != nil {
		limit := *stmt.Limit
		offset := int64(0)
		if stmt.Offset != nil {
			offset = *stmt.Offset
		}
		logicalPlan = NewLogicalLimit(limit, offset, logicalPlan)
	}

	// 6. 应用 SELECT 列（Projection）
	// 当存在聚合函数时跳过 Projection：AggregateOperator 已经产生了正确的输出列。
	// 额外的 Projection 会尝试投影不存在的列名（如 "COUNT_*"），导致空结果。
	debugf("  [DEBUG] convertSelect: SELECT列数量: %d, IsWildcard=%v\n", len(stmt.Columns), isWildcard(stmt.Columns))
	if len(stmt.Columns) > 0 {
		debugf("  [DEBUG] convertSelect: cols[0].Name='%s'\n", stmt.Columns[0].Name)
	}
	if len(aggFuncs) == 0 && len(stmt.Columns) > 0 && !isWildcard(stmt.Columns) {
		debugln("  [DEBUG] convertSelect: 创建Projection")
		exprs := make([]*parser.Expression, len(stmt.Columns))
		aliases := make([]string, len(stmt.Columns))
		for i, col := range stmt.Columns {
			debugf("  [DEBUG] convertSelect: 列%d: Name='%s', Alias='%s'\n", i, col.Name, col.Alias)
			exprs[i] = &parser.Expression{
				Type:   parser.ExprTypeColumn,
				Column: col.Name,
			}
			if col.Alias != "" {
				aliases[i] = col.Alias
			} else {
				aliases[i] = col.Name
			}
		}
		logicalPlan = NewLogicalProjection(exprs, aliases, logicalPlan)
	}

	return logicalPlan, nil
}

// convertInsert 转换 INSERT 语句
func (o *Optimizer) convertInsert(stmt *parser.InsertStatement) (LogicalPlan, error) {
	// 验证表存在
	_, err := o.dataSource.GetTableInfo(context.Background(), stmt.Table)
	if err != nil {
		return nil, fmt.Errorf("table '%s' does not exist: %v", stmt.Table, err)
	}

	// 转换 Values 中的值为 parser.Expression
	values := make([][]parser.Expression, len(stmt.Values))
	for i, row := range stmt.Values {
		values[i] = make([]parser.Expression, len(row))
		for j, val := range row {
			values[i][j] = o.valueToExpression(val)
		}
	}

	// 创建 LogicalInsert
	insertPlan := NewLogicalInsert(stmt.Table, stmt.Columns, values)

	// 处理 ON DUPLICATE KEY UPDATE
	if stmt.OnDuplicate != nil {
		updatePlan, err := o.convertUpdate(stmt.OnDuplicate)
		if err != nil {
			return nil, fmt.Errorf("failed to convert ON DUPLICATE KEY UPDATE: %v", err)
		}
		if logicalUpdate, ok := updatePlan.(*LogicalUpdate); ok {
			insertPlan.SetOnDuplicate(logicalUpdate)
		}
	}

	debugf("  [DEBUG] convertInsert: INSERT into %s with %d rows\n", stmt.Table, len(values))
	return insertPlan, nil
}

// convertUpdate 转换 UPDATE 语句
func (o *Optimizer) convertUpdate(stmt *parser.UpdateStatement) (LogicalPlan, error) {
	// 验证表存在
	_, err := o.dataSource.GetTableInfo(context.Background(), stmt.Table)
	if err != nil {
		return nil, fmt.Errorf("table '%s' does not exist: %v", stmt.Table, err)
	}

	// 转换 SET 子句
	set := make(map[string]parser.Expression)
	for col, val := range stmt.Set {
		set[col] = o.valueToExpression(val)
	}

	// 创建 LogicalUpdate
	updatePlan := NewLogicalUpdate(stmt.Table, set)

	// 设置 WHERE 条件
	updatePlan.SetWhere(stmt.Where)

	// 设置 ORDER BY
	if len(stmt.OrderBy) > 0 {
		orderItems := make([]*parser.OrderItem, len(stmt.OrderBy))
		for i, item := range stmt.OrderBy {
			orderItems[i] = &parser.OrderItem{
				Expr: parser.Expression{
					Type:   parser.ExprTypeColumn,
					Column: item.Column,
				},
				Direction: item.Direction,
			}
		}
		updatePlan.SetOrderBy(orderItems)
	}

	// 设置 LIMIT
	updatePlan.SetLimit(stmt.Limit)

	debugf("  [DEBUG] convertUpdate: UPDATE %s with %d columns\n", stmt.Table, len(set))
	return updatePlan, nil
}

// convertDelete 转换 DELETE 语句
func (o *Optimizer) convertDelete(stmt *parser.DeleteStatement) (LogicalPlan, error) {
	// 验证表存在
	_, err := o.dataSource.GetTableInfo(context.Background(), stmt.Table)
	if err != nil {
		return nil, fmt.Errorf("table '%s' does not exist: %v", stmt.Table, err)
	}

	// 创建 LogicalDelete
	deletePlan := NewLogicalDelete(stmt.Table)

	// 设置 WHERE 条件
	deletePlan.SetWhere(stmt.Where)

	// 设置 ORDER BY
	if len(stmt.OrderBy) > 0 {
		orderItems := make([]*parser.OrderItem, len(stmt.OrderBy))
		for i, item := range stmt.OrderBy {
			orderItems[i] = &parser.OrderItem{
				Expr: parser.Expression{
					Type:   parser.ExprTypeColumn,
					Column: item.Column,
				},
				Direction: item.Direction,
			}
		}
		deletePlan.SetOrderBy(orderItems)
	}

	// 设置 LIMIT
	deletePlan.SetLimit(stmt.Limit)

	debugf("  [DEBUG] convertDelete: DELETE from %s\n", stmt.Table)
	return deletePlan, nil
}

// convertToPlan 将逻辑计划转换为可序列化的Plan
func (o *Optimizer) convertToPlan(ctx context.Context, logicalPlan LogicalPlan, optCtx *OptimizationContext) (*plan.Plan, error) {
	switch p := logicalPlan.(type) {
	case *LogicalDataSource:
		// 获取下推的谓词条件
		pushedDownPredicates := p.GetPushedDownPredicates()
		filters := o.convertConditionsToFilters(pushedDownPredicates)
		// 获取下推的Limit
		limitInfo := p.GetPushedDownLimit()
		debugf("  [DEBUG] convertToPlan: DataSource(%s), 下推谓词数量: %d, 下推Limit: %v\n", p.TableName, len(filters), limitInfo != nil)
		
		// 构建列信息
		columns := make([]types.ColumnInfo, 0, len(p.TableInfo.Columns))
		for _, col := range p.TableInfo.Columns {
			columns = append(columns, types.ColumnInfo{
				Name:     col.Name,
				Type:     col.Type,
				Nullable: col.Nullable,
			})
		}
		
		return &plan.Plan{
			ID:   fmt.Sprintf("scan_%s", p.TableName),
			Type: plan.TypeTableScan,
			OutputSchema: columns,
			Children: []*plan.Plan{},
			Config: &plan.TableScanConfig{
				TableName:       p.TableName,
				Columns:         columns,
				Filters:         filters,
				LimitInfo:       convertToTypesLimitInfo(limitInfo),
				EnableParallel:  true,
				MinParallelRows: 100,
			},
		}, nil
	case *LogicalSelection:
		child, err := o.convertToPlan(ctx, p.Children()[0], optCtx)
		if err != nil {
			return nil, err
		}
		debugln("  [DEBUG] convertToPlan: Selection")
		conditions := p.GetConditions()
		if len(conditions) == 0 {
			return nil, fmt.Errorf("selection has no conditions")
		}
		return &plan.Plan{
			ID:   fmt.Sprintf("sel_%d", len(conditions)),
			Type: plan.TypeSelection,
			OutputSchema: child.OutputSchema,
			Children: []*plan.Plan{child},
			Config: &plan.SelectionConfig{
				Condition: conditions[0],
			},
		}, nil
	case *LogicalProjection:
		child, err := o.convertToPlan(ctx, p.Children()[0], optCtx)
		if err != nil {
			return nil, err
		}
		exprs := p.GetExprs()
		aliases := p.GetAliases()
		debugf("  [DEBUG] convertToPlan: Projection, 表达式数量: %d, 别名数量: %d\n", len(exprs), len(aliases))
		
		return &plan.Plan{
			ID:   fmt.Sprintf("proj_%d", len(exprs)),
			Type: plan.TypeProjection,
			OutputSchema: child.OutputSchema,
			Children: []*plan.Plan{child},
			Config: &plan.ProjectionConfig{
				Expressions: exprs,
				Aliases:     aliases,
			},
		}, nil
	case *LogicalLimit:
		child, err := o.convertToPlan(ctx, p.Children()[0], optCtx)
		if err != nil {
			return nil, err
		}
		return &plan.Plan{
			ID:   fmt.Sprintf("limit_%d_%d", p.GetLimit(), p.GetOffset()),
			Type: plan.TypeLimit,
			OutputSchema: child.OutputSchema,
			Children: []*plan.Plan{child},
			Config: &plan.LimitConfig{
				Limit:  p.GetLimit(),
				Offset: p.GetOffset(),
			},
		}, nil
	case *LogicalSort:
		if len(p.Children()) == 0 {
			return nil, fmt.Errorf("sort has no child")
		}
		child, err := o.convertToPlan(ctx, p.Children()[0], optCtx)
		if err != nil {
			return nil, err
		}
		
		// 直接使用 parser.OrderItem
		orderItems := p.GetOrderBy()
		
		return &plan.Plan{
			ID:           fmt.Sprintf("sort_%d", len(orderItems)),
			Type:         plan.TypeSort,
			OutputSchema: child.OutputSchema,
			Children:     []*plan.Plan{child},
			Config: &plan.SortConfig{
				OrderByItems: orderItems,
			},
		}, nil
	case *LogicalJoin:
		left, err := o.convertToPlan(ctx, p.Children()[0], optCtx)
		if err != nil {
			return nil, err
		}
		right, err := o.convertToPlan(ctx, p.Children()[1], optCtx)
		if err != nil {
			return nil, err
		}
		joinConditions := p.GetJoinConditions()
	// 转换JoinCondition
	convertedConditions := make([]*types.JoinCondition, len(joinConditions))
	for i, cond := range joinConditions {
		convertedConditions[i] = &types.JoinCondition{
			Left:     convertToTypesExpr(cond.Left),
			Right:    convertToTypesExpr(cond.Right),
			Operator: cond.Operator,
		}
	}

		if len(joinConditions) == 0 {
			return nil, fmt.Errorf("join has no conditions")
		}
		return &plan.Plan{
			ID:   fmt.Sprintf("join_%s", joinConditions[0].Operator),
			Type: plan.TypeHashJoin,
			OutputSchema: left.OutputSchema,
			Children: []*plan.Plan{left, right},
			Config: &plan.HashJoinConfig{
				JoinType:  types.JoinType(p.GetJoinType()),
				LeftCond:  convertedConditions[0],
				RightCond: convertedConditions[0],
				BuildSide: "left",
			},
		}, nil
	case *LogicalAggregate:
		child, err := o.convertToPlan(ctx, p.Children()[0], optCtx)
		if err != nil {
			return nil, err
		}
		// 转换aggFuncs到types.AggregationItem
	aggFuncs := p.GetAggFuncs()
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
		ID:   fmt.Sprintf("agg_%d", len(p.GetGroupByCols())),
		Type: plan.TypeAggregate,
		OutputSchema: child.OutputSchema,
		Children: []*plan.Plan{child},
		Config: &plan.AggregateConfig{
			AggFuncs:   convertedAggFuncs,
			GroupByCols: p.GetGroupByCols(),
		},
	}, nil
default:
		return nil, fmt.Errorf("unsupported logical plan type: %T", p)
	}
}

// convertToTypesLimitInfo 转换 LimitInfo
func convertToTypesLimitInfo(limitInfo *LimitInfo) *types.LimitInfo {
	if limitInfo == nil {
		return nil
	}
	return &types.LimitInfo{
		Limit:  limitInfo.Limit,
		Offset: limitInfo.Offset,
	}
}


