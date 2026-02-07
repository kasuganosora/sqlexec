package planning

import (
	"fmt"
	"strings"

	"github.com/kasuganosora/sqlexec/pkg/optimizer"
	"github.com/kasuganosora/sqlexec/pkg/parser"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// extractConditions from expression to condition list
func (o *Optimizer) extractConditions(expr *parser.Expression) []*parser.Expression {
	conditions := []*parser.Expression{expr}
	// Simplified implementation, not handling complex expressions
	return conditions
}

// extractAggFuncs extract aggregation functions from SELECT columns
// Identify and extract aggregation functions (COUNT, SUM, AVG, MAX, MIN)
func (o *Optimizer) extractAggFuncs(cols []parser.SelectColumn) []*optimizer.AggregationItem {
	aggFuncs := []*optimizer.AggregationItem{}

	for _, col := range cols {
		// Skip wildcard
		if col.IsWildcard {
			continue
		}

		// Check expression type
		if col.Expr == nil {
			continue
		}

		// Parse aggregation function
		if aggItem := o.parseAggregationFunction(col.Expr); aggItem != nil {
			// Use alias if provided, otherwise use aggregation function name
			if col.Alias != "" {
				aggItem.Alias = col.Alias
			} else {
				// Generate default alias (e.g., "COUNT_id", "SUM_amount")
				aggItem.Alias = fmt.Sprintf("%s_%s", aggItem.Type.String(),
					expressionToString(col.Expr))
			}
			aggFuncs = append(aggFuncs, aggItem)
		}
	}

	return aggFuncs
}

// parseAggregationFunction parse a single aggregation function
func (o *Optimizer) parseAggregationFunction(expr *parser.Expression) *optimizer.AggregationItem {
	if expr == nil {
		return nil
	}

	// Check if it's a function call (Type == ExprTypeFunction or function name)
	funcName := ""
	var funcExpr *parser.Expression

	// Try to extract function name and arguments from expression
	if expr.Type == parser.ExprTypeFunction {
		// Assume expression has FunctionName and Args fields
		if name, ok := expr.Value.(string); ok {
			funcName = name
		}
		funcExpr = expr
	} else if expr.Type == parser.ExprTypeColumn {
		// May be column name or function call
		colName := expr.Column
		// Parse function name (e.g., "COUNT(id)" -> "COUNT")
		if idx := strings.Index(colName, "("); idx > 0 {
			funcName = strings.ToUpper(colName[:idx])
		}
	}

	// Match aggregation function type
	var aggType optimizer.AggregationType
	isDistinct := false

	// Check DISTINCT keyword
	if strings.Contains(strings.ToUpper(expressionToString(expr)), "DISTINCT") {
		isDistinct = true
	}

	switch strings.ToUpper(funcName) {
	case "COUNT":
		aggType = optimizer.Count
	case "SUM":
		aggType = optimizer.Sum
	case "AVG":
		aggType = optimizer.Avg
	case "MAX":
		aggType = optimizer.Max
	case "MIN":
		aggType = optimizer.Min
	default:
		// Not an aggregation function
		return nil
	}

	// Build aggregation item
	return &optimizer.AggregationItem{
		Type:     aggType,
		Expr:     funcExpr,
		Alias:    "",
		Distinct: isDistinct,
	}
}

// expressionToString convert expression to string
func expressionToString(expr *parser.Expression) string {
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
		left := expressionToString(expr.Left)
		right := expressionToString(expr.Right)
		if left != "" && right != "" {
			return fmt.Sprintf("%s %s %s", left, expr.Operator, right)
		}
		if left != "" {
			return fmt.Sprintf("%s %s", expr.Operator, left)
		}
	}

	return fmt.Sprintf("%v", expr.Value)
}

// convertConditionsToFilters convert condition expressions to filters
func (o *Optimizer) convertConditionsToFilters(conditions []*parser.Expression) []domain.Filter {
	filters := []domain.Filter{}

	for _, cond := range conditions {
		if cond == nil {
			continue
		}

		// Extract all independent conditions from AND expressions
		conditionFilters := o.extractFiltersFromCondition(cond)
		filters = append(filters, conditionFilters...)
	}

	return filters
}

// extractFiltersFromCondition extract all filters from condition (handle AND expressions)
func (o *Optimizer) extractFiltersFromCondition(expr *parser.Expression) []domain.Filter {
	filters := []domain.Filter{}

	if expr == nil {
		return filters
	}

	// If it's AND operator, recursively process both sides
	if expr.Type == parser.ExprTypeOperator && expr.Operator == "and" {
		if expr.Left != nil {
			filters = append(filters, o.extractFiltersFromCondition(expr.Left)...)
		}
		if expr.Right != nil {
			filters = append(filters, o.extractFiltersFromCondition(expr.Right)...)
		}
		return filters
	}

	// Otherwise, convert to single filter
	filter := o.convertExpressionToFilter(expr)
	if filter != nil {
		filters = append(filters, *filter)
	}

	return filters
}

// convertExpressionToFilter convert expression to filter
func (o *Optimizer) convertExpressionToFilter(expr *parser.Expression) *domain.Filter {
	if expr == nil || expr.Type != parser.ExprTypeOperator {
		return nil
	}

	// Handle binary comparison expressions (e.g., age > 30, name = 'Alice')
	if expr.Left != nil && expr.Right != nil && expr.Operator != "" {
		// Left side is column name
		if expr.Left.Type == parser.ExprTypeColumn && expr.Left.Column != "" {
			// Right side is constant value
			if expr.Right.Type == parser.ExprTypeValue {
				// Map operator
				operator := o.mapOperator(expr.Operator)
				return &domain.Filter{
					Field:    expr.Left.Column,
					Operator: operator,
					Value:    expr.Right.Value,
				}
			}
		}
	}

	// Handle AND logical expressions
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

// mapOperator map parser operator to domain.Filter operator
func (o *Optimizer) mapOperator(parserOp string) string {
	// Convert parser operator to domain.Filter operator
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

// valueToExpression convert interface{} value to parser.Expression
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
		// For complex types, try to serialize as string
		return parser.Expression{
			Type:  parser.ExprTypeValue,
			Value: fmt.Sprintf("%v", val),
		}
	}
}
