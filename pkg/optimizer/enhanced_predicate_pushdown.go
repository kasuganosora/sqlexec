package optimizer

import (
	"context"
	"fmt"
	"strings"

	"github.com/kasuganosora/sqlexec/pkg/parser"
)

// EnhancedPredicatePushdownRule 增强的谓词下推规则
// 支持跨JOIN谓词下推、OR条件处理
type EnhancedPredicatePushdownRule struct {
	cardinalityEstimator CardinalityEstimator
}

// NewEnhancedPredicatePushdownRule 创建增强谓词下推规则
func NewEnhancedPredicatePushdownRule(estimator CardinalityEstimator) *EnhancedPredicatePushdownRule {
	return &EnhancedPredicatePushdownRule{
		cardinalityEstimator: estimator,
	}
}

// Name 返回规则名称
func (r *EnhancedPredicatePushdownRule) Name() string {
	return "EnhancedPredicatePushdown"
}

// Match 检查规则是否匹配
func (r *EnhancedPredicatePushdownRule) Match(plan LogicalPlan) bool {
	_, ok := plan.(*LogicalSelection)
	return ok
}

// Apply 应用规则
func (r *EnhancedPredicatePushdownRule) Apply(ctx context.Context, plan LogicalPlan, optCtx *OptimizationContext) (LogicalPlan, error) {
	selection, ok := plan.(*LogicalSelection)
	if !ok {
		return plan, nil
	}

	if len(selection.Children()) == 0 {
		return plan, nil
	}

	child := selection.Children()[0]

	// 1. 尝试下推到DataSource
	if dataSource, ok := child.(*LogicalDataSource); ok {
		pushedDown := r.tryPushDownToDataSource(selection, dataSource)
		if pushedDown {
			debugln("  [PUSHDOWN] Predicates pushed down to DataSource")
			// 如果所有条件都已下推，返回DataSource
			if len(selection.Conditions()) == 0 {
				return dataSource, nil
			}
		}
	}

	// 2. 尝试跨JOIN下推
	if join, ok := child.(*LogicalJoin); ok {
		pushedDown := r.tryPushDownAcrossJoin(selection, join)
		if pushedDown {
			debugln("  [PUSHDOWN] Predicates pushed down across JOIN")
		}
	}

	// 3. 处理OR条件，转换为UNION
	if orPlan := r.tryConvertORToUnion(selection); orPlan != nil {
		debugln("  [PUSHDOWN] Converted OR to UNION")
		return orPlan, nil
	}

	// 4. 合并相邻Selection
	merged := r.mergeAdjacentSelections(selection)
	if merged != selection {
		return merged, nil
	}

	return plan, nil
}

// tryPushDownToDataSource 尝试下推到DataSource
func (r *EnhancedPredicatePushdownRule) tryPushDownToDataSource(selection *LogicalSelection, dataSource *LogicalDataSource) bool {
	conditions := selection.Conditions()
	if len(conditions) == 0 {
		return false
	}

	// 分析哪些条件可以下推
	pushableConditions := make([]*parser.Expression, 0)
	remainingConditions := make([]*parser.Expression, 0)

	for _, cond := range conditions {
		if r.canPushDownToDataSource(cond, dataSource) {
			pushableConditions = append(pushableConditions, cond)
		} else {
			remainingConditions = append(remainingConditions, cond)
		}
	}

	// 如果所有条件都可以下推，更新DataSource
	if len(remainingConditions) == 0 {
		// 将条件标记到DataSource
		for _, cond := range pushableConditions {
			dataSource.PushDownPredicates([]*parser.Expression{cond})
		}
		// 清除已下推的条件
		selection.filterConditions = nil
		return true
	}

	// 部分下推：创建新的Selection
	if len(pushableConditions) > 0 {
		for _, cond := range pushableConditions {
			dataSource.PushDownPredicates([]*parser.Expression{cond})
		}
		// 更新剩余条件
		selection.filterConditions = remainingConditions
	}

	return len(pushableConditions) > 0
}

// canPushDownToDataSource 检查条件是否可以下推到DataSource
// ALL referenced columns must be present in the DataSource schema
func (r *EnhancedPredicatePushdownRule) canPushDownToDataSource(cond *parser.Expression, dataSource *LogicalDataSource) bool {
	schema := dataSource.Schema()
	if len(schema) == 0 {
		return false
	}

	// 检查所有引用的列是否都在DataSource中
	cols := r.extractColumnsFromExpression(cond)
	if len(cols) == 0 {
		return false
	}

	schemaSet := make(map[string]bool, len(schema))
	for _, schemaCol := range schema {
		schemaSet[schemaCol.Name] = true
	}

	for _, col := range cols {
		// Strip table qualifier (e.g., "accounts.deleted_at" → "deleted_at")
		colName := col
		if idx := strings.LastIndex(col, "."); idx >= 0 {
			colName = col[idx+1:]
		}
		if !schemaSet[colName] {
			return false
		}
	}

	return true
}

// tryPushDownAcrossJoin 尝试跨JOIN下推谓词
func (r *EnhancedPredicatePushdownRule) tryPushDownAcrossJoin(selection *LogicalSelection, join *LogicalJoin) bool {
	conditions := selection.Conditions()
	if len(conditions) == 0 {
		return false
	}

	// 分析JOIN条件，确定哪些表参与
	joinTables := r.extractTablesFromJoin(join)
	joinConditions := r.extractJoinConditions(join)

	// 分类条件
	leftPushable := make([]*parser.Expression, 0)
	rightPushable := make([]*parser.Expression, 0)
	bothPushable := make([]*parser.Expression, 0)
	remaining := make([]*parser.Expression, 0)

	for _, cond := range conditions {
		// 确定条件引用的表
		condTables := r.extractTablesFromCondition(cond)

		// 检查是否可以下推
		pushDecision := r.analyzePushability(cond, condTables, joinTables, joinConditions)

		switch pushDecision {
		case PushLeft:
			leftPushable = append(leftPushable, cond)
		case PushRight:
			rightPushable = append(rightPushable, cond)
		case PushBoth:
			bothPushable = append(bothPushable, cond)
		default:
			remaining = append(remaining, cond)
		}
	}

	// 应用下推
	children := join.Children()
	if len(children) != 2 {
		return false
	}

	leftChild := children[0]
	rightChild := children[1]
	pushed := false

	// 为左子节点创建或更新Selection
	if len(leftPushable) > 0 {
		leftSelection := r.createOrMergeSelection(leftChild, leftPushable)
		join.SetChildren(leftSelection, rightChild)
		pushed = true
	}

	// 为右子节点创建或更新Selection
	if len(rightPushable) > 0 {
		children = join.Children()
		leftChild = children[0]
		rightChild = children[1]

		rightSelection := r.createOrMergeSelection(rightChild, rightPushable)
		join.SetChildren(leftChild, rightSelection)
		pushed = true
	}

	return pushed
}

// createOrMergeSelection 创建或合并Selection节点
func (r *EnhancedPredicatePushdownRule) createOrMergeSelection(child LogicalPlan, conditions []*parser.Expression) LogicalPlan {
	if existingSelection, ok := child.(*LogicalSelection); ok {
		// 合并条件 - 创建新切片避免修改原始切片
		existingConds := existingSelection.Conditions()
		mergedConditions := make([]*parser.Expression, 0, len(existingConds)+len(conditions))
		mergedConditions = append(mergedConditions, existingConds...)
		mergedConditions = append(mergedConditions, conditions...)
		return NewLogicalSelection(mergedConditions, existingSelection.Children()[0])
	}
	// 创建新的Selection
	return NewLogicalSelection(conditions, child)
}

// tryConvertORToUnion 尝试将OR条件转换为UNION
func (r *EnhancedPredicatePushdownRule) tryConvertORToUnion(selection *LogicalSelection) LogicalPlan {
	conditions := selection.Conditions()
	if len(conditions) == 0 {
		return nil
	}

	// 查找OR条件
	orConditions := r.findORConditions(conditions)
	if len(orConditions) == 0 {
		return nil
	}

	// 转换OR为UNION
	child := selection.Children()[0]
	unionPlans := make([]LogicalPlan, 0, len(orConditions))

	for _, orCond := range orConditions {
		// 为OR的每个分支创建Selection
		unionPlans = append(unionPlans, NewLogicalSelection([]*parser.Expression{orCond}, child))
	}

	// 创建UNION节点
	return NewLogicalUnion(unionPlans)
}

// mergeAdjacentSelections 合并相邻的Selection节点
func (r *EnhancedPredicatePushdownRule) mergeAdjacentSelections(selection *LogicalSelection) LogicalPlan {
	child := selection.Children()[0]
	if child == nil {
		return selection
	}

	// 检查子节点是否也是Selection
	if childSelection, ok := child.(*LogicalSelection); ok {
		// 合并两个Selection的条件 - 创建新切片避免修改原始切片
		selConds := selection.Conditions()
		childConds := childSelection.Conditions()
		mergedConditions := make([]*parser.Expression, 0, len(selConds)+len(childConds))
		mergedConditions = append(mergedConditions, selConds...)
		mergedConditions = append(mergedConditions, childConds...)
		return NewLogicalSelection(mergedConditions, childSelection.Children()[0])
	}

	return selection
}

// PushDecision 下推决策
type PushDecision int

const (
	PushNone PushDecision = iota
	PushLeft
	PushRight
	PushBoth
)

// extractColumnsFromExpression 从表达式提取列名
func (r *EnhancedPredicatePushdownRule) extractColumnsFromExpression(expr *parser.Expression) []string {
	cols := make(map[string]bool)
	r.collectColumns(expr, cols)

	result := make([]string, 0, len(cols))
	for col := range cols {
		result = append(result, col)
	}
	return result
}

// collectColumns 递归收集表达式中的列
func (r *EnhancedPredicatePushdownRule) collectColumns(expr *parser.Expression, cols map[string]bool) {
	if expr == nil {
		return
	}

	if expr.Column != "" {
		cols[expr.Column] = true
	}

	r.collectColumns(expr.Left, cols)
	r.collectColumns(expr.Right, cols)

	// 处理函数参数
	for _, arg := range expr.Args {
		if arg.Column != "" {
			cols[arg.Column] = true
		}
	}
}

// extractTablesFromJoin 从JOIN节点提取表名
func (r *EnhancedPredicatePushdownRule) extractTablesFromJoin(join *LogicalJoin) []string {
	tables := make(map[string]bool)
	r.collectTablesFromPlan(join, tables)

	result := make([]string, 0, len(tables))
	for table := range tables {
		result = append(result, table)
	}
	return result
}

// collectTablesFromPlan 从计划收集表名
func (r *EnhancedPredicatePushdownRule) collectTablesFromPlan(plan LogicalPlan, tables map[string]bool) {
	if dataSource, ok := plan.(*LogicalDataSource); ok {
		tables[dataSource.TableName] = true
		return
	}

	for _, child := range plan.Children() {
		r.collectTablesFromPlan(child, tables)
	}
}

// extractTablesFromCondition 从条件提取表名
func (r *EnhancedPredicatePushdownRule) extractTablesFromCondition(expr *parser.Expression) []string {
	tables := make(map[string]bool)
	r.collectColumns(expr, tables) // 简化：假设列名和表名一致

	result := make([]string, 0, len(tables))
	for table := range tables {
		result = append(result, table)
	}
	return result
}

// extractJoinConditions 提取JOIN条件
func (r *EnhancedPredicatePushdownRule) extractJoinConditions(join *LogicalJoin) []*parser.Expression {
	// 将JoinCondition转换为Expression
	joinConds := join.GetJoinConditions()
	result := make([]*parser.Expression, 0, len(joinConds))

	for _, jc := range joinConds {
		// 将JoinCondition (Left, Right, Operator) 转换为Expression
		result = append(result, &parser.Expression{
			Type:     parser.ExprTypeOperator,
			Operator: jc.Operator,
			Left:     jc.Left,
			Right:    jc.Right,
		})
	}

	return result
}

// analyzePushability 分析条件是否可以下推
func (r *EnhancedPredicatePushdownRule) analyzePushability(
	cond *parser.Expression,
	condTables []string,
	joinTables []string,
	joinConditions []*parser.Expression,
) PushDecision {
	// 检查条件是否只引用左表
	if r.referencesOnlyTables(cond, joinTables[:1]) {
		return PushLeft
	}

	// 检查条件是否只引用右表
	if len(joinTables) > 1 && r.referencesOnlyTables(cond, joinTables[1:]) {
		return PushRight
	}

	// 检查条件是否是等值连接且引用所有表
	if r.isEquijoinCondition(cond, joinConditions) {
		return PushBoth
	}

	// 否则无法下推
	return PushNone
}

// referencesOnlyTables 检查表达式是否只引用指定的表
func (r *EnhancedPredicatePushdownRule) referencesOnlyTables(expr *parser.Expression, tables []string) bool {
	if len(tables) == 0 {
		return false
	}

	exprTables := r.extractTablesFromCondition(expr)

	// 检查exprTables是否是tables的子集
	tablesMap := make(map[string]bool)
	for _, t := range tables {
		tablesMap[t] = true
	}

	for _, et := range exprTables {
		if !tablesMap[et] {
			return false
		}
	}

	return true
}

// isEquijoinCondition 检查是否是等值连接条件
func (r *EnhancedPredicatePushdownRule) isEquijoinCondition(expr *parser.Expression, joinConditions []*parser.Expression) bool {
	if expr == nil || len(joinConditions) == 0 {
		return false
	}

	// 简化：检查是否是单列等值连接
	// 实际应该更复杂的分析
	return expr.Operator == "=" && expr.Column != ""
}

// findORConditions 查找OR条件
func (r *EnhancedPredicatePushdownRule) findORConditions(conditions []*parser.Expression) []*parser.Expression {
	orConds := []*parser.Expression{}
	for _, cond := range conditions {
		if r.isORExpression(cond) {
			orConds = append(orConds, cond)
		}
	}
	return orConds
}

// isORExpression 判断是否是OR表达式
func (r *EnhancedPredicatePushdownRule) isORExpression(expr *parser.Expression) bool {
	if expr == nil {
		return false
	}

	// 检查顶层运算符是否是OR
	if expr.Type == parser.ExprTypeOperator && expr.Operator == "or" {
		return true
	}

	// 递归检查子表达式
	if r.isORExpression(expr.Left) || r.isORExpression(expr.Right) {
		return true
	}

	return false
}

// Explain 解释规则应用
func (r *EnhancedPredicatePushdownRule) Explain(result LogicalPlan) string {
	return fmt.Sprintf(
		"EnhancedPredicatePushdown: Applied, result type: %T",
		result,
	)
}
