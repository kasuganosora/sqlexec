package optimizer

import (
	"fmt"
	"strings"

	"github.com/kasuganosora/sqlexec/pkg/parser"
	"github.com/kasuganosora/sqlexec/pkg/utils"
)

// extractConditions 从表达式中提取条件列表
// 支持递归拆解 AND 条件，将复合条件拆分为独立条件列表
// 例如: WHERE a=1 AND b=2 → [a=1, b=2]
// 例如: WHERE a=1 OR b=2 → [OR(a=1, b=2)] (OR 条件保持整体)
func (o *Optimizer) extractConditions(expr *parser.Expression) []*parser.Expression {
	if expr == nil {
		return []*parser.Expression{}
	}

	// 处理 AND 操作符：递归拆解左右两边
	if expr.Type == parser.ExprTypeOperator && expr.Operator == "and" {
		leftConditions := o.extractConditions(expr.Left)
		rightConditions := o.extractConditions(expr.Right)
		return append(leftConditions, rightConditions...)
	}

	// 其他条件（包括 OR）保持为单个条件
	return []*parser.Expression{expr}
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
					o.expressionToString(aggItem.Expr))
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

// expressionToString 将表达式转换为字符串（Optimizer 方法版本）
func (o *Optimizer) expressionToString(expr *parser.Expression) string {
	if expr == nil {
		return ""
	}

	if expr.Type == parser.ExprTypeColumn {
		return expr.Column
	}

	if expr.Type == parser.ExprTypeValue {
		return utils.ToString(expr.Value)
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

	return utils.ToString(expr.Value)
}

// isWildcard 检查是否是通配符
func isWildcard(cols []parser.SelectColumn) bool {
	if len(cols) == 1 && cols[0].IsWildcard {
		return true
	}
	return false
}

// valueToExpression 将 interface{} 值转换为 parser.Expression（Optimizer 方法版本）
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
			Value: utils.ToString(val),
		}
	}
}
