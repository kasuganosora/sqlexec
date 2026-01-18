package optimizer

import (
	"context"
	"fmt"

	"github.com/kasuganosora/sqlexec/service/parser"
	"github.com/kasuganosora/sqlexec/service/resource"
)

// JoinEliminationRule JOIN消除规则
// 消除冗余的JOIN操作，如1:1的外键主键JOIN
type JoinEliminationRule struct {
	cardinalityEstimator CardinalityEstimator
}

// Name 返回规则名称
func (r *JoinEliminationRule) Name() string {
	return "JoinElimination"
}

// Match 检查规则是否匹�?
func (r *JoinEliminationRule) Match(plan LogicalPlan) bool {
	// 检查是否包含JOIN节点
	return containsJoin(plan)
}

// Apply 应用规则：消除冗余JOIN
func (r *JoinEliminationRule) Apply(ctx context.Context, plan LogicalPlan, optCtx *OptimizationContext) (LogicalPlan, error) {
	// 尝试消除每个JOIN节点
	return r.eliminateJoins(plan), nil
}

// eliminateJoins 递归消除JOIN节点
func (r *JoinEliminationRule) eliminateJoins(plan LogicalPlan) LogicalPlan {
	if join, ok := plan.(*LogicalJoin); ok {
		// 检查是否可以消除这个JOIN
		if r.canEliminate(join) {
			// 消除JOIN：返回子节点
			if len(join.Children()) > 0 {
				return join.Children()[0]
			}
			return plan
		}
	}

	// 递归处理子节�?
	for i, child := range plan.Children() {
		newChild := r.eliminateJoins(child)
		if newChild != child {
			children := plan.Children()
			children[i] = newChild
			plan.SetChildren(children...)
			return plan
		}
	}

	return plan
}

// canEliminate 检查是否可以消除JOIN
func (r *JoinEliminationRule) canEliminate(join *LogicalJoin) bool {
	// 简化实现：检查以下情�?
	// 1. 1:1的JOIN（外键主键关系）
	// 2. 连接条件包含等式
	// 3. 右表（或左表）可以被推导

	conditions := join.Conditions()
	if len(conditions) == 0 {
		return false // 没有连接条件，不能消�?
	}

	// 检查连接条件是否为等�?
	for i := range conditions {
		if !isEqualityCondition(conditions[i]) {
			return false // 不是等值条件，不能消除
		}
	}

	// 检查是否为1:1关系
	leftCardinality := r.cardinalityEstimator.EstimateFilter(getTableName(join.Children()[0]), []resource.Filter{})
	rightCardinality := r.cardinalityEstimator.EstimateFilter(getTableName(join.Children()[1]), []resource.Filter{})

	// 如果一边表很小（如1行），可以考虑消除
	if leftCardinality <= 1 || rightCardinality <= 1 {
		return true
	}

	// 检查是否为外键-主键关系（简化版�?
	// 实际应该从schema中提取外键信�?
	if r.isForeignKeyPrimaryKeyJoin(join) {
		return true
	}

	return false
}

// isEqualityCondition 检查条件是否为等�?
func isEqualityCondition(cond *JoinCondition) bool {
	// 简化：检查连接条件的结构
	// 实际应该检查表达式类型
	return cond.Left != nil && cond.Right != nil && cond.Operator == "="
}

// isForeignKeyPrimaryKeyJoin 检查是否为外键-主键JOIN（简化版�?
func (r *JoinEliminationRule) isForeignKeyPrimaryKeyJoin(join *LogicalJoin) bool {
	// 简化实现：假设表名包含外键信息
	// 实际应该从schema中读取外键定�?

	leftTables := extractTableNames(join.Children()[0])
	rightTables := extractTableNames(join.Children()[1])

	if len(leftTables) != 1 || len(rightTables) != 1 {
		return false
	}

	// 简化判断：如果表名包含_id或以_id结尾，可能是主键
	leftTable := leftTables[0]
	rightTable := rightTables[0]

	// 检查连接条�?
	conditions := join.Conditions()
	for _, cond := range conditions {
		// 如果连接条件�?id = other_id，可能是外键主键关系
		leftExpr := expressionToString(cond.Left)
		rightExpr := expressionToString(cond.Right)
		if (leftExpr == "id" || leftExpr == "id_"+leftTable) &&
			(rightExpr == rightTable+"_id" || rightExpr == "id") {
			return true
		}
	}

	return false
}

// expressionToString 将表达式转换为字符串（简化版�?
func expressionToString(expr *parser.Expression) string {
	if expr == nil {
		return ""
	}
	// 简化实现：直接返回字面量值或列名
	// 实际应该遍历表达式树
	if expr.Type == parser.ExprTypeValue {
		return fmt.Sprintf("%v", expr.Value)
	}
	if expr.Type == parser.ExprTypeColumn {
		return expr.Column
	}
	return ""
}

// extractTableNames 从计划中提取表名
func extractTableNames(plan LogicalPlan) []string {
	if dataSource, ok := plan.(*LogicalDataSource); ok {
		return []string{dataSource.TableName}
	}

	tables := make(map[string]bool)
	for _, child := range plan.Children() {
		childTables := extractTableNames(child)
		for _, t := range childTables {
			tables[t] = true
		}
	}

	result := make([]string, 0, len(tables))
	for table := range tables {
		result = append(result, table)
	}

	return result
}

// NewJoinEliminationRule 创建JOIN消除规则
func NewJoinEliminationRule(estimator CardinalityEstimator) *JoinEliminationRule {
	return &JoinEliminationRule{
		cardinalityEstimator: estimator,
	}
}
