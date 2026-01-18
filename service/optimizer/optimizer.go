package optimizer

import (
	"context"
	"fmt"

	"github.com/kasuganosora/sqlexec/service/parser"
	"github.com/kasuganosora/sqlexec/service/resource"
)

// Optimizer 优化�?
type Optimizer struct {
	rules      RuleSet
	costModel  CostModel
	dataSource resource.DataSource
}

// NewOptimizer 创建优化�?
func NewOptimizer(dataSource resource.DataSource) *Optimizer {
	return &Optimizer{
		rules:     DefaultRuleSet(),
		costModel:  NewDefaultCostModel(),
		dataSource: dataSource,
	}
}

// Optimize 优化查询计划
func (o *Optimizer) Optimize(ctx context.Context, stmt *parser.SQLStatement) (PhysicalPlan, error) {
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
		TableInfo: make(map[string]*resource.TableInfo),
		Stats:      make(map[string]*Statistics),
		CostModel:  o.costModel,
	}

	optimizedPlan, err := o.rules.Apply(ctx, logicalPlan, optCtx)
	if err != nil {
		return nil, fmt.Errorf("apply optimization rules failed: %w", err)
	}
	fmt.Println("  [DEBUG] Optimize: 优化规则应用完成")

	// 3. 转换为物理计�?
	fmt.Println("  [DEBUG] Optimize: 步骤3 - 转换为物理计�?)
	physicalPlan, err := o.convertToPhysicalPlan(ctx, optimizedPlan, optCtx)
	if err != nil {
		return nil, fmt.Errorf("convert to physical plan failed: %w", err)
	}
	fmt.Println("  [DEBUG] Optimize: 物理计划转换完成")

	return physicalPlan, nil
}

// convertToLogicalPlan �?SQL 语句转换为逻辑计划
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
	fmt.Println("  [DEBUG] convertSelect: 开始转�? 表名:", stmt.From)
	// 1. 创建 DataSource
	tableInfo, err := o.dataSource.GetTableInfo(context.Background(), stmt.From)
	if err != nil {
		fmt.Println("  [DEBUG] convertSelect: GetTableInfo 失败:", err)
		return nil, fmt.Errorf("get table info failed: %w", err)
	}
	fmt.Println("  [DEBUG] convertSelect: GetTableInfo 成功, 列数:", len(tableInfo.Columns))

	var logicalPlan LogicalPlan = NewLogicalDataSource(stmt.From, tableInfo)
	fmt.Println("  [DEBUG] convertSelect: LogicalDataSource 创建完成")

	// 2. 应用 WHERE 条件（Selection�?
	if stmt.Where != nil {
		conditions := o.extractConditions(stmt.Where)
		logicalPlan = NewLogicalSelection(conditions, logicalPlan)
	}

	// 3. 应用 GROUP BY（Aggregate�?
	if len(stmt.GroupBy) > 0 {
		aggFuncs := o.extractAggFuncs(stmt.Columns)
		logicalPlan = NewLogicalAggregate(aggFuncs, stmt.GroupBy, logicalPlan)
	}

	// 4. 应用 ORDER BY（Sort�?
	if len(stmt.OrderBy) > 0 {
		orderItems := make([]OrderByItem, len(stmt.OrderBy))
		for i, item := range stmt.OrderBy {
			orderItems[i] = OrderByItem{
				Column:    item.Column,
				Direction: item.Direction,
			}
		}
		logicalPlan = NewLogicalSort(orderItems, logicalPlan)
	}

	// 5. 应用 LIMIT（Limit�?
	if stmt.Limit != nil {
		limit := *stmt.Limit
		offset := int64(0)
		if stmt.Offset != nil {
			offset = *stmt.Offset
		}
		logicalPlan = NewLogicalLimit(limit, offset, logicalPlan)
	}

	// 6. 应用 SELECT 列（Projection�?
	fmt.Printf("  [DEBUG] convertSelect: SELECT列数�? %d, IsWildcard=%v\n", len(stmt.Columns), isWildcard(stmt.Columns))
	if len(stmt.Columns) > 0 {
		fmt.Printf("  [DEBUG] convertSelect: cols[0].Name='%s'\n", stmt.Columns[0].Name)
	}
	if len(stmt.Columns) > 0 && !isWildcard(stmt.Columns) {
		fmt.Println("  [DEBUG] convertSelect: 创建Projection")
		exprs := make([]*parser.Expression, len(stmt.Columns))
		aliases := make([]string, len(stmt.Columns))
		for i, col := range stmt.Columns {
			fmt.Printf("  [DEBUG] convertSelect: �?d: Name='%s', Alias='%s'\n", i, col.Name, col.Alias)
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
	return nil, fmt.Errorf("INSERT statement not supported in optimizer yet")
}

// convertUpdate 转换 UPDATE 语句
func (o *Optimizer) convertUpdate(stmt *parser.UpdateStatement) (LogicalPlan, error) {
	return nil, fmt.Errorf("UPDATE statement not supported in optimizer yet")
}

// convertDelete 转换 DELETE 语句
func (o *Optimizer) convertDelete(stmt *parser.DeleteStatement) (LogicalPlan, error) {
	return nil, fmt.Errorf("DELETE statement not supported in optimizer yet")
}

// extractConditions 从表达式中提取条件列�?
func (o *Optimizer) extractConditions(expr *parser.Expression) []*parser.Expression {
	conditions := []*parser.Expression{expr}
	// 简化实现，不处理复杂表达式
	return conditions
}

// extractAggFuncs 提取聚合函数
func (o *Optimizer) extractAggFuncs(cols []parser.SelectColumn) []*AggregationItem {
	aggFuncs := []*AggregationItem{}
	// TODO: 解析 SELECT 列中的聚合函�?
	return aggFuncs
}

// isWildcard 检查是否是通配�?
func isWildcard(cols []parser.SelectColumn) bool {
	if len(cols) == 1 && cols[0].IsWildcard {
		return true
	}
	return false
}

// convertConditionsToFilters 将条件表达式转换为过滤器
func (o *Optimizer) convertConditionsToFilters(conditions []*parser.Expression) []resource.Filter {
	filters := []resource.Filter{}

	for _, cond := range conditions {
		if cond == nil {
			continue
		}

		filter := o.convertExpressionToFilter(cond)
		if filter != nil {
			filters = append(filters, *filter)
		}
	}

	fmt.Println("  [DEBUG] convertConditionsToFilters: 生成的过滤器数量:", len(filters))
	return filters
}

// convertExpressionToFilter 将表达式转换为过滤器
func (o *Optimizer) convertExpressionToFilter(expr *parser.Expression) *resource.Filter {
	if expr == nil || expr.Type != parser.ExprTypeOperator {
		return nil
	}

		// 处理二元比较表达�?(e.g., age > 30, name = 'Alice')
		if expr.Left != nil && expr.Right != nil && expr.Operator != "" {
			// 左边是列�?
			if expr.Left.Type == parser.ExprTypeColumn && expr.Left.Column != "" {
				// 右边是常量�?
				if expr.Right.Type == parser.ExprTypeValue {
					// 映射操作�?
					operator := o.mapOperator(expr.Operator)
					return &resource.Filter{
						Field:    expr.Left.Column,
						Operator:  operator,
						Value:     expr.Right.Value,
					}
				}
			}
		}

		// 处理 AND 逻辑表达�?
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

// mapOperator 映射parser操作符到resource.Filter操作�?
func (o *Optimizer) mapOperator(parserOp string) string {
	// 转换parser操作符到resource.Filter操作�?
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

// convertToPhysicalPlan 将逻辑计划转换为物理计�?
func (o *Optimizer) convertToPhysicalPlan(ctx context.Context, logicalPlan LogicalPlan, optCtx *OptimizationContext) (PhysicalPlan, error) {
	switch p := logicalPlan.(type) {
	case *LogicalDataSource:
		// 获取下推的谓词条�?
		pushedDownPredicates := p.GetPushedDownPredicates()
		filters := o.convertConditionsToFilters(pushedDownPredicates)
		// 获取下推的Limit
		limitInfo := p.GetPushedDownLimit()
		fmt.Printf("  [DEBUG] convertToPhysicalPlan: DataSource(%s), 下推谓词数量: %d, 下推Limit: %v\n", p.TableName, len(filters), limitInfo != nil)
		return NewPhysicalTableScan(p.TableName, p.TableInfo, o.dataSource, filters, limitInfo), nil
	case *LogicalSelection:
		child, err := o.convertToPhysicalPlan(ctx, p.Children()[0], optCtx)
		if err != nil {
			return nil, err
		}
		// 转换条件为过滤器
		filters := o.convertConditionsToFilters(p.GetConditions())
		fmt.Println("  [DEBUG] convertToPhysicalPlan: Selection, 过滤器数�?", len(filters))
		return NewPhysicalSelection(p.GetConditions(), filters, child, o.dataSource), nil
	case *LogicalProjection:
		child, err := o.convertToPhysicalPlan(ctx, p.Children()[0], optCtx)
		if err != nil {
			return nil, err
		}
		exprs := p.GetExprs()
		aliases := p.GetAliases()
		fmt.Printf("  [DEBUG] convertToPhysicalPlan: Projection, 表达式数�? %d, 别名数量: %d\n", len(exprs), len(aliases))
		for i, expr := range exprs {
			fmt.Printf("  [DEBUG] convertToPhysicalPlan: 表达�?d: Type=%d, Column='%s'\n", i, expr.Type, expr.Column)
			if i < len(aliases) {
				fmt.Printf("  [DEBUG] convertToPhysicalPlan: 别名%d: '%s'\n", i, aliases[i])
			}
		}
		return NewPhysicalProjection(exprs, aliases, child), nil
	case *LogicalLimit:
		child, err := o.convertToPhysicalPlan(ctx, p.Children()[0], optCtx)
		if err != nil {
			return nil, err
		}
		return NewPhysicalLimit(p.GetLimit(), p.GetOffset(), child), nil
	case *LogicalSort:
		// 简化：暂时不实现排�?
		return o.convertToPhysicalPlan(ctx, p.Children()[0], optCtx)
	case *LogicalJoin:
		left, err := o.convertToPhysicalPlan(ctx, p.Children()[0], optCtx)
		if err != nil {
			return nil, err
		}
		right, err := o.convertToPhysicalPlan(ctx, p.Children()[1], optCtx)
		if err != nil {
			return nil, err
		}
		return NewPhysicalHashJoin(p.GetJoinType(), left, right, p.GetJoinConditions()), nil
	case *LogicalAggregate:
		child, err := o.convertToPhysicalPlan(ctx, p.Children()[0], optCtx)
		if err != nil {
			return nil, err
		}
		return NewPhysicalHashAggregate(p.GetAggFuncs(), p.GetGroupByCols(), child), nil
	default:
		return nil, fmt.Errorf("unsupported logical plan type: %T", p)
	}
}

// ExplainPlan 解释执行计划
func ExplainPlan(plan PhysicalPlan) string {
	return explainPlan(plan, 0)
}

// explainPlan 递归解释计划
func explainPlan(plan PhysicalPlan, depth int) string {
	indent := ""
	for i := 0; i < depth; i++ {
		indent += "  "
	}

	result := indent + plan.Explain() + "\n"

	for _, child := range plan.Children() {
		result += explainPlan(child, depth+1)
	}

	return result
}
