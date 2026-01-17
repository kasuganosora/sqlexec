package optimizer

import (
	"context"
	"fmt"

	"mysql-proxy/mysql/parser"
	"mysql-proxy/mysql/resource"
)

// Optimizer 优化器
type Optimizer struct {
	rules      RuleSet
	costModel  CostModel
	dataSource resource.DataSource
}

// NewOptimizer 创建优化器
func NewOptimizer(dataSource resource.DataSource) *Optimizer {
	return &Optimizer{
		rules:     DefaultRuleSet(),
		costModel:  NewDefaultCostModel(),
		dataSource: dataSource,
	}
}

// Optimize 优化查询计划
func (o *Optimizer) Optimize(ctx context.Context, stmt *parser.SQLStatement) (PhysicalPlan, error) {
	// 1. 转换为逻辑计划
	logicalPlan, err := o.convertToLogicalPlan(stmt)
	if err != nil {
		return nil, fmt.Errorf("convert to logical plan failed: %w", err)
	}

	// 2. 应用优化规则
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

	// 3. 转换为物理计划
	physicalPlan, err := o.convertToPhysicalPlan(ctx, optimizedPlan, optCtx)
	if err != nil {
		return nil, fmt.Errorf("convert to physical plan failed: %w", err)
	}

	return physicalPlan, nil
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
	// 1. 创建 DataSource
	tableInfo, err := o.dataSource.GetTableInfo(context.Background(), stmt.From)
	if err != nil {
		return nil, fmt.Errorf("get table info failed: %w", err)
	}

	dataSource := NewLogicalDataSource(stmt.From, tableInfo)

	// 2. 应用 WHERE 条件（Selection）
	if stmt.Where != nil {
		conditions := o.extractConditions(stmt.Where)
		dataSource = NewLogicalSelection(conditions, dataSource)
	}

	// 3. 应用 GROUP BY（Aggregate）
	if len(stmt.GroupBy) > 0 {
		aggFuncs := o.extractAggFuncs(stmt.Columns)
		dataSource = NewLogicalAggregate(aggFuncs, stmt.GroupBy, dataSource)
	}

	// 4. 应用 ORDER BY（Sort）
	if len(stmt.OrderBy) > 0 {
		orderItems := make([]OrderByItem, len(stmt.OrderBy))
		for i, item := range stmt.OrderBy {
			orderItems[i] = OrderByItem{
				Column:    item.Column,
				Direction: item.Direction,
			}
		}
		dataSource = NewLogicalSort(orderItems, dataSource)
	}

	// 5. 应用 LIMIT（Limit）
	if stmt.Limit != nil {
		limit := *stmt.Limit
		offset := int64(0)
		if stmt.Offset != nil {
			offset = *stmt.Offset
		}
		dataSource = NewLogicalLimit(limit, offset, dataSource)
	}

	// 6. 应用 SELECT 列（Projection）
	if len(stmt.Columns) > 0 && !isWildcard(stmt.Columns) {
		exprs := make([]*parser.Expression, len(stmt.Columns))
		aliases := make([]string, len(stmt.Columns))
		for i, col := range stmt.Columns {
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
		dataSource = NewLogicalProjection(exprs, aliases, dataSource)
	}

	return dataSource, nil
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

// extractConditions 从表达式中提取条件列表
func (o *Optimizer) extractConditions(expr *parser.Expression) []*parser.Expression {
	conditions := []*parser.Expression{expr}
	// 简化实现，不处理复杂表达式
	return conditions
}

// extractAggFuncs 提取聚合函数
func (o *Optimizer) extractAggFuncs(cols []parser.SelectColumn) []*AggregationItem {
	aggFuncs := []*AggregationItem{}
	// TODO: 解析 SELECT 列中的聚合函数
	return aggFuncs
}

// isWildcard 检查是否是通配符
func isWildcard(cols []parser.SelectColumn) bool {
	if len(cols) == 1 && cols[0].IsWildcard {
		return true
	}
	return false
}

// convertToPhysicalPlan 将逻辑计划转换为物理计划
func (o *Optimizer) convertToPhysicalPlan(ctx context.Context, logicalPlan LogicalPlan, optCtx *OptimizationContext) (PhysicalPlan, error) {
	switch p := logicalPlan.(type) {
	case *LogicalDataSource:
		return NewPhysicalTableScan(p.TableName, p.TableInfo, o.dataSource)
	case *LogicalSelection:
		child, err := o.convertToPhysicalPlan(ctx, p.Children()[0], optCtx)
		if err != nil {
			return nil, err
		}
		// 简化：不转换条件为过滤器
		return NewPhysicalSelection(p.Conditions, []resource.Filter{}, child, o.dataSource)
	case *LogicalProjection:
		child, err := o.convertToPhysicalPlan(ctx, p.Children()[0], optCtx)
		if err != nil {
			return nil, err
		}
		return NewPhysicalProjection(p.Exprs, p.Aliases, child)
	case *LogicalLimit:
		child, err := o.convertToPhysicalPlan(ctx, p.Children()[0], optCtx)
		if err != nil {
			return nil, err
		}
		return NewPhysicalLimit(p.Limit, p.Offset, child)
	case *LogicalSort:
		// 简化：暂时不实现排序
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
		return NewPhysicalHashJoin(p.JoinType, left, right, p.Conditions)
	case *LogicalAggregate:
		child, err := o.convertToPhysicalPlan(ctx, p.Children()[0], optCtx)
		if err != nil {
			return nil, err
		}
		return NewPhysicalHashAggregate(p.AggFuncs, p.GroupByCols, child)
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
