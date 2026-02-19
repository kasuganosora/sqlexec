package optimizer

import (
	"strings"

	"github.com/kasuganosora/sqlexec/pkg/parser"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/utils"
)

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

	debugln("  [DEBUG] convertConditionsToFilters: 生成的过滤器数量:", len(filters))
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

	// 处理一元操作符 (IS NULL / IS NOT NULL)
	if expr.Left != nil && expr.Right == nil {
		op := strings.ToLower(expr.Operator)
		if op == "is null" || op == "isnull" || op == "is not null" || op == "isnotnull" {
			// 左边必须是列名
			if expr.Left.Type == parser.ExprTypeColumn && expr.Left.Column != "" {
				return &domain.Filter{
					Field:    expr.Left.Column,
					Operator: utils.MapOperator(expr.Operator),
					Value:    nil,
				}
			}
		}
	}

	// 处理二元比较表达式 (e.g., age > 30, name = 'Alice')
	if expr.Left != nil && expr.Right != nil && expr.Operator != "" {
		// 左边是列名
		if expr.Left.Type == parser.ExprTypeColumn && expr.Left.Column != "" {
			// 右边是常量值
			if expr.Right.Type == parser.ExprTypeValue {
				// 映射操作符
				operator := utils.MapOperator(expr.Operator)
				return &domain.Filter{
					Field:    expr.Left.Column,
					Operator: operator,
					Value:    expr.Right.Value,
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
