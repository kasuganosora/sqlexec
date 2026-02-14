package operators

import (
	"context"
	"fmt"
	"strconv"

	"github.com/kasuganosora/sqlexec/pkg/dataaccess"
	"github.com/kasuganosora/sqlexec/pkg/optimizer/plan"
	"github.com/kasuganosora/sqlexec/pkg/parser"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// SelectionOperator 选择算子
type SelectionOperator struct {
	*BaseOperator
	config *plan.SelectionConfig
}

// NewSelectionOperator 创建选择算子
func NewSelectionOperator(p *plan.Plan, das dataaccess.Service) (*SelectionOperator, error) {
	config, ok := p.Config.(*plan.SelectionConfig)
	if !ok {
		return nil, fmt.Errorf("invalid config type for Selection: %T", p.Config)
	}

	base := NewBaseOperator(p, das)
	
	// 构建子算子
	buildFn := func(childPlan *plan.Plan) (Operator, error) {
		return buildOperator(childPlan, das)
	}
	if err := base.BuildChildOperators(buildFn); err != nil {
		return nil, err
	}

	return &SelectionOperator{
		BaseOperator: base,
		config:       config,
	}, nil
}

// Execute 执行选择
func (op *SelectionOperator) Execute(ctx context.Context) (*domain.QueryResult, error) {
	// 执行子算子
	if len(op.children) == 0 {
		return nil, fmt.Errorf("SelectionOperator requires at least 1 child")
	}

	childResult, err := op.children[0].Execute(ctx)
	if err != nil {
		return nil, fmt.Errorf("execute child failed: %w", err)
	}

	// 应用过滤条件
	filteredRows := make([]domain.Row, 0)
	for _, row := range childResult.Rows {
		if op.evaluateCondition(row, op.config.Condition) {
			filteredRows = append(filteredRows, row)
		}
	}

	return &domain.QueryResult{
		Columns: childResult.Columns,
		Rows:    filteredRows,
	}, nil
}

// evaluateCondition 评估条件
func (op *SelectionOperator) evaluateCondition(row domain.Row, cond *parser.Expression) bool {
	if cond == nil {
		return true
	}

	switch cond.Type {
	case parser.ExprTypeOperator:
		return op.evaluateOperator(row, cond)
	case parser.ExprTypeColumn:
		if val, ok := row[cond.Column]; ok && val != nil {
			switch v := val.(type) {
			case bool:
				return v
			case int:
				return v != 0
			case string:
				return v != ""
			}
		}
		return false
	default:
		return true
	}
}

// evaluateOperator 评估操作符表达式
func (op *SelectionOperator) evaluateOperator(row domain.Row, expr *parser.Expression) bool {
	leftVal := op.getExpressionValue(row, expr.Left)
	rightVal := op.getExpressionValue(row, expr.Right)

	switch expr.Operator {
	case "eq", "===":
		return op.compareValues(leftVal, rightVal) == 0
	case "ne", "!=":
		return op.compareValues(leftVal, rightVal) != 0
	case "gt":
		return op.compareValues(leftVal, rightVal) > 0
	case "gte":
		return op.compareValues(leftVal, rightVal) >= 0
	case "lt":
		return op.compareValues(leftVal, rightVal) < 0
	case "lte":
		return op.compareValues(leftVal, rightVal) <= 0
	default:
		return false
	}
}

// getExpressionValue 获取表达式值
func (op *SelectionOperator) getExpressionValue(row domain.Row, expr *parser.Expression) interface{} {
	if expr == nil {
		return nil
	}

	switch expr.Type {
	case parser.ExprTypeColumn:
		return row[expr.Column]
	case parser.ExprTypeValue:
		return expr.Value
	case parser.ExprTypeOperator:
		if op.evaluateOperator(row, expr) {
			return 1
		}
		return 0
	default:
		return nil
	}
}

// compareValues 比较两个值
func (op *SelectionOperator) compareValues(a, b interface{}) int {
	if a == nil || b == nil {
		return 0
	}

	// 尝试转换为int比较
	aInt, aOk := toInt(a)
	bInt, bOk := toInt(b)
	if aOk && bOk {
		if aInt < bInt {
			return -1
		} else if aInt > bInt {
			return 1
		}
		return 0
	}

	// 尝试转换为float64比较
	aFloat, aOk := a.(float64)
	bFloat, bOk := b.(float64)
	if aOk && bOk {
		if aFloat < bFloat {
			return -1
		} else if aFloat > bFloat {
			return 1
		}
		return 0
	}

	// 字符串比较
	aStr, aOk := a.(string)
	bStr, bOk := b.(string)
	if aOk && bOk {
		if aStr < bStr {
			return -1
		} else if aStr > bStr {
			return 1
		}
		return 0
	}

	return 0
}

// toInt 尝试转换为int
func toInt(v interface{}) (int, bool) {
	switch val := v.(type) {
	case int:
		return val, true
	case int64:
		return int(val), true
	case float64:
		return int(val), true
	case string:
		if i, err := strconv.Atoi(val); err == nil {
			return i, true
		}
	}
	return 0, false
}
