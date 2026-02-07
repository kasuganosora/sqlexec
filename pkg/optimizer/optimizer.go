package optimizer

import (
	"context"
	"fmt"
	"strings"

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
}

// NewOptimizer 创建优化器
func NewOptimizer(dataSource domain.DataSource) *Optimizer {
	return &Optimizer{
		rules:     DefaultRuleSet(),
		costModel:  NewDefaultCostModel(),
		dataSource: dataSource,
	}
}

// Optimize 优化查询计划（返回可序列化的Plan）
func (o *Optimizer) Optimize(ctx context.Context, stmt *parser.SQLStatement) (*plan.Plan, error) {
	fmt.Println("  [DEBUG] Optimize: 步骤1 - 转换为逻辑计划")
	// 1. 转换为逻辑计划
	logicalPlan, err := o.convertToLogicalPlan(stmt)
	if err != nil {
		return nil, fmt.Errorf("convert to logical plan failed: %w", err)
	}
	fmt.Println("  [DEBUG] Optimize: 逻辑计划转换完成, 类型:", logicalPlan.Explain())

	// 2. 应用优化规则
	fmt.Println("  [DEBUG] Optimize: 步骤2 - 应用优化规则")
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
	fmt.Println("  [DEBUG] Optimize: 优化规则应用完成")

	// 3. 转换为可序列化的Plan
	fmt.Println("  [DEBUG] Optimize: 步骤3 - 转换为可序列化的Plan")
	plan, err := o.convertToPlan(ctx, optimizedPlan, optCtx)
	if err != nil {
		return nil, fmt.Errorf("convert to plan failed: %w", err)
	}
	fmt.Println("  [DEBUG] Optimize: Plan转换完成")

	return plan, nil
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
	fmt.Println("  [DEBUG] convertSelect: 开始转换, 表名:", stmt.From)

	// 处理没有 FROM 子句的查询（如 SELECT DATABASE()）
	if stmt.From == "" {
		fmt.Println("  [DEBUG] convertSelect: 无 FROM 子句，使用常量数据源")
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
		fmt.Println("  [DEBUG] convertSelect: GetTableInfo 失败:", err)
		return nil, fmt.Errorf("get table info failed: %w", err)
	}
	fmt.Println("  [DEBUG] convertSelect: GetTableInfo 成功, 列数:", len(tableInfo.Columns))

	var logicalPlan LogicalPlan = NewLogicalDataSource(stmt.From, tableInfo)
	fmt.Println("  [DEBUG] convertSelect: LogicalDataSource 创建完成")

	// 2. 应用 WHERE 条件（Selection）
	if stmt.Where != nil {
		conditions := o.extractConditions(stmt.Where)
		logicalPlan = NewLogicalSelection(conditions, logicalPlan)
	}

	// 3. 应用 GROUP BY（Aggregate）
	if len(stmt.GroupBy) > 0 {
		aggFuncs := o.extractAggFuncs(stmt.Columns)
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
	fmt.Printf("  [DEBUG] convertSelect: SELECT列数量: %d, IsWildcard=%v\n", len(stmt.Columns), isWildcard(stmt.Columns))
	if len(stmt.Columns) > 0 {
		fmt.Printf("  [DEBUG] convertSelect: cols[0].Name='%s'\n", stmt.Columns[0].Name)
	}
	if len(stmt.Columns) > 0 && !isWildcard(stmt.Columns) {
		fmt.Println("  [DEBUG] convertSelect: 创建Projection")
		exprs := make([]*parser.Expression, len(stmt.Columns))
		aliases := make([]string, len(stmt.Columns))
		for i, col := range stmt.Columns {
			fmt.Printf("  [DEBUG] convertSelect: 列%d: Name='%s', Alias='%s'\n", i, col.Name, col.Alias)
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

	fmt.Printf("  [DEBUG] convertInsert: INSERT into %s with %d rows\n", stmt.Table, len(values))
	return insertPlan, nil
}

// valueToExpression 将 interface{} 值转换为 parser.Expression
func (o *Optimizer) valueToExpression(val interface{}) parser.Expression {
	if val == nil {
		return parser.Expression{
			Type:  parser.ExprTypeValue,
			Value: nil,
		}
	}

	switch v := val.(type) {
	case int, int32, int64:
		return parser.Expression{
			Type:  parser.ExprTypeValue,
			Value: v,
		}
	case float32, float64:
		return parser.Expression{
			Type:  parser.ExprTypeValue,
			Value: v,
		}
	case string:
		return parser.Expression{
			Type:  parser.ExprTypeValue,
			Value: v,
		}
	case bool:
		return parser.Expression{
			Type:  parser.ExprTypeValue,
			Value: v,
		}
	default:
		// 对于复杂类型，尝试序列化为字符串
		return parser.Expression{
			Type:  parser.ExprTypeValue,
			Value: fmt.Sprintf("%v", val),
		}
	}
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

	fmt.Printf("  [DEBUG] convertUpdate: UPDATE %s with %d columns\n", stmt.Table, len(set))
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

	fmt.Printf("  [DEBUG] convertDelete: DELETE from %s\n", stmt.Table)
	return deletePlan, nil
}

// extractConditions 从表达式中提取条件列表
func (o *Optimizer) extractConditions(expr *parser.Expression) []*parser.Expression {
	conditions := []*parser.Expression{expr}
	// 简化实现，不处理复杂表达式
	return conditions
}

// extractAggFuncs 提取聚合函数
// 从 SELECT 列中识别并提取聚合函数（如 COUNT, SUM, AVG, MAX, MIN）
func (o *Optimizer) extractAggFuncs(cols []parser.SelectColumn) []*AggregationItem {
	aggFuncs := []*AggregationItem{}

	for _, col := range cols {
		// 跳过通配符
		if col.IsWildcard {
			continue
		}

		// 检查表达式类型
		if col.Expr == nil {
			continue
		}

		// 解析聚合函数
		if aggItem := o.parseAggregationFunction(col.Expr); aggItem != nil {
			// 如果有别名，使用别名；否则使用聚合函数名称
			if col.Alias != "" {
				aggItem.Alias = col.Alias
			} else {
				// 生成默认别名（如 "COUNT_id", "SUM_amount"）
				aggItem.Alias = fmt.Sprintf("%s_%s", aggItem.Type.String(),
					expressionToString(col.Expr))
			}
			aggFuncs = append(aggFuncs, aggItem)
		}
	}

	return aggFuncs
}

// parseAggregationFunction 解析单个聚合函数
func (o *Optimizer) parseAggregationFunction(expr *parser.Expression) *AggregationItem {
	if expr == nil {
		return nil
	}

	// 检查是否是函数调用（Type == ExprTypeFunction 或函数名）
	funcName := ""
	var funcExpr *parser.Expression

	// 尝试从表达式提取函数名和参数
	if expr.Type == parser.ExprTypeFunction {
		// 假设表达式中有 FunctionName 和 Args 字段
		if name, ok := expr.Value.(string); ok {
			funcName = name
		}
		funcExpr = expr
	} else if expr.Type == parser.ExprTypeColumn {
		// 可能是列名，也可能包含函数调用
		colName := expr.Column
		// 解析函数名（如 "COUNT(id)" -> "COUNT"）
		if idx := strings.Index(colName, "("); idx > 0 {
			funcName = strings.ToUpper(colName[:idx])
		}
	}

	// 匹配聚合函数类型
	var aggType AggregationType
	isDistinct := false

	// 检查 DISTINCT 关键字
	if strings.Contains(strings.ToUpper(o.expressionToString(expr)), "DISTINCT") {
		isDistinct = true
	}

	switch strings.ToUpper(funcName) {
	case "COUNT":
		aggType = Count
	case "SUM":
		aggType = Sum
	case "AVG":
		aggType = Avg
	case "MAX":
		aggType = Max
	case "MIN":
		aggType = Min
	default:
		// 不是聚合函数
		return nil
	}

	// 构建聚合项
	return &AggregationItem{
		Type:     aggType,
		Expr:     funcExpr,
		Alias:    "",
		Distinct: isDistinct,
	}
}

// expressionToString 将表达式转换为字符串
func (o *Optimizer) expressionToString(expr *parser.Expression) string {
	if expr == nil {
		return ""
	}

	if expr.Type == parser.ExprTypeColumn {
		return expr.Column
	}

	if expr.Type == parser.ExprTypeValue {
		return fmt.Sprintf("%v", expr.Value)
	}

	if expr.Type == parser.ExprTypeOperator {
		left := o.expressionToString(expr.Left)
		right := o.expressionToString(expr.Right)
		if left != "" && right != "" {
			return fmt.Sprintf("%s %s %s", left, expr.Operator, right)
		}
		if left != "" {
			return fmt.Sprintf("%s %s", expr.Operator, left)
		}
	}

	return fmt.Sprintf("%v", expr.Value)
}

// isWildcard 检查是否是通配符
func isWildcard(cols []parser.SelectColumn) bool {
	if len(cols) == 1 && cols[0].IsWildcard {
		return true
	}
	return false
}

// convertConditionsToFilters 将条件表达式转换为过滤器
func (o *Optimizer) convertConditionsToFilters(conditions []*parser.Expression) []domain.Filter {
	filters := []domain.Filter{}

	for _, cond := range conditions {
		if cond == nil {
			continue
		}

		// 提取 AND 条件中的所有独立条件
		conditionFilters := o.extractFiltersFromCondition(cond)
		filters = append(filters, conditionFilters...)
	}

	fmt.Println("  [DEBUG] convertConditionsToFilters: 生成的过滤器数量:", len(filters))
	return filters
}

// extractFiltersFromCondition 从条件中提取所有过滤器（处理 AND 表达式）
func (o *Optimizer) extractFiltersFromCondition(expr *parser.Expression) []domain.Filter {
	filters := []domain.Filter{}
	
	if expr == nil {
		return filters
	}

	// 如果是 AND 操作符，递归处理两边
	if expr.Type == parser.ExprTypeOperator && expr.Operator == "and" {
		if expr.Left != nil {
			filters = append(filters, o.extractFiltersFromCondition(expr.Left)...)
		}
		if expr.Right != nil {
			filters = append(filters, o.extractFiltersFromCondition(expr.Right)...)
		}
		return filters
	}

	// 否则，转换为单个过滤器
	filter := o.convertExpressionToFilter(expr)
	if filter != nil {
		filters = append(filters, *filter)
	}

	return filters
}

// convertExpressionToFilter 将表达式转换为过滤器
func (o *Optimizer) convertExpressionToFilter(expr *parser.Expression) *domain.Filter {
	if expr == nil || expr.Type != parser.ExprTypeOperator {
		return nil
	}

		// 处理二元比较表达式 (e.g., age > 30, name = 'Alice')
		if expr.Left != nil && expr.Right != nil && expr.Operator != "" {
			// 左边是列名
			if expr.Left.Type == parser.ExprTypeColumn && expr.Left.Column != "" {
				// 右边是常量值
				if expr.Right.Type == parser.ExprTypeValue {
					// 映射操作符
					operator := o.mapOperator(expr.Operator)
					return &domain.Filter{
						Field:    expr.Left.Column,
						Operator:  operator,
						Value:     expr.Right.Value,
					}
				}
			}
		}

		// 处理 AND 逻辑表达式
		if expr.Operator == "and" && expr.Left != nil && expr.Right != nil {
			leftFilter := o.convertExpressionToFilter(expr.Left)
			rightFilter := o.convertExpressionToFilter(expr.Right)
			if leftFilter != nil {
				return leftFilter
			}
			if rightFilter != nil {
				return rightFilter
			}
		}

	return nil
}

// mapOperator 映射parser操作符到domain.Filter操作符
func (o *Optimizer) mapOperator(parserOp string) string {
	// 转换parser操作符到domain.Filter操作符
	switch parserOp {
	case "gt":
		return ">"
	case "gte":
		return ">="
	case "lt":
		return "<"
	case "lte":
		return "<="
	case "eq", "===":
		return "="
	case "ne", "!=":
		return "!="
	default:
		return parserOp
	}
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
		fmt.Printf("  [DEBUG] convertToPlan: DataSource(%s), 下推谓词数量: %d, 下推Limit: %v\n", p.TableName, len(filters), limitInfo != nil)
		
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
		fmt.Println("  [DEBUG] convertToPlan: Selection")
		return &plan.Plan{
			ID:   fmt.Sprintf("sel_%d", len(p.GetConditions())),
			Type: plan.TypeSelection,
			OutputSchema: child.OutputSchema,
			Children: []*plan.Plan{child},
			Config: &plan.SelectionConfig{
				Condition: p.GetConditions()[0],
			},
		}, nil
	case *LogicalProjection:
		child, err := o.convertToPlan(ctx, p.Children()[0], optCtx)
		if err != nil {
			return nil, err
		}
		exprs := p.GetExprs()
		aliases := p.GetAliases()
		fmt.Printf("  [DEBUG] convertToPlan: Projection, 表达式数量: %d, 别名数量: %d\n", len(exprs), len(aliases))
		
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
		// 简化：暂时不实现排序，直接返回子节点
		return o.convertToPlan(ctx, p.Children()[0], optCtx)
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

// ExplainPlan 解释执行计划
func ExplainPlan(plan PhysicalPlan) string {
	return explainPlan(plan, 0)
}

// ExplainPlanV2 解释新架构的执行计划（plan.Plan）
func ExplainPlanV2(plan *plan.Plan) string {
	return explainPlanV2(plan, 0)
}

// explainPlanV2 递归解释新架构的计划
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

// explainPlan 递归解释计划
func explainPlan(plan PhysicalPlan, depth int) string {
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
