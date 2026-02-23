package operators

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/kasuganosora/sqlexec/pkg/dataaccess"
	"github.com/kasuganosora/sqlexec/pkg/optimizer/feedback"
	"github.com/kasuganosora/sqlexec/pkg/optimizer/plan"
	"github.com/kasuganosora/sqlexec/pkg/parser"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/utils"
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

	// 应用过滤条件（预分配容量，减少 append 扩容）
	filteredRows := make([]domain.Row, 0, len(childResult.Rows)/2+1)
	for _, row := range childResult.Rows {
		if op.evaluateCondition(row, op.config.Condition) {
			filteredRows = append(filteredRows, row)
		}
	}

	// DQ feedback: record observed selectivity for cost model calibration
	if op.config.Condition != nil && op.config.Condition.Left != nil &&
		op.config.Condition.Left.Type == parser.ExprTypeColumn {
		feedback.GetGlobalFeedback().RecordSelectivity(
			op.config.Condition.Left.Column,
			int64(len(childResult.Rows)),
			int64(len(filteredRows)),
		)
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
		col := cond.Column
		val, ok := row[col]
		if !ok {
			// Try stripping table qualifier
			if idx := strings.LastIndex(col, "."); idx >= 0 {
				val, ok = row[col[idx+1:]]
			}
		}
		if ok && val != nil {
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
	// Handle unary and logical operators first (no need for both left/right values)
	switch expr.Operator {
	case "IS NULL", "is null":
		leftVal := op.getExpressionValue(row, expr.Left)
		return leftVal == nil
	case "IS NOT NULL", "is not null":
		leftVal := op.getExpressionValue(row, expr.Left)
		return leftVal != nil
	case "AND", "and":
		return op.evaluateCondition(row, expr.Left) && op.evaluateCondition(row, expr.Right)
	case "OR", "or":
		return op.evaluateCondition(row, expr.Left) || op.evaluateCondition(row, expr.Right)
	}

	leftVal := op.getExpressionValue(row, expr.Left)
	rightVal := op.getExpressionValue(row, expr.Right)

	switch expr.Operator {
	case "eq", "===", "=":
		return op.compareValues(leftVal, rightVal) == 0
	case "ne", "!=", "<>":
		return op.compareValues(leftVal, rightVal) != 0
	case "gt", ">":
		return op.compareValues(leftVal, rightVal) > 0
	case "gte", ">=":
		return op.compareValues(leftVal, rightVal) >= 0
	case "lt", "<":
		return op.compareValues(leftVal, rightVal) < 0
	case "lte", "<=":
		return op.compareValues(leftVal, rightVal) <= 0
	case "like":
		return op.likeValues(leftVal, rightVal)
	case "not like":
		return !op.likeValues(leftVal, rightVal)
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
		if val, ok := row[expr.Column]; ok {
			return val
		}
		// Try stripping table qualifier (e.g., "accounts.deleted_at" → "deleted_at")
		if idx := strings.LastIndex(expr.Column, "."); idx >= 0 {
			return row[expr.Column[idx+1:]]
		}
		return nil
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

	// 尝试转换为int64比较（保持大整数精度）
	aInt, aOk := toInt64(a)
	bInt, bOk := toInt64(b)
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

// toInt64 尝试转换为int64（仅限整数类型，不转换float64以避免精度丢失）
func toInt64(v interface{}) (int64, bool) {
	switch val := v.(type) {
	case int:
		return int64(val), true
	case int64:
		return val, true
	case int32:
		return int64(val), true
	case uint:
		return int64(val), true
	case uint32:
		return int64(val), true
	case uint64:
		return int64(val), true
	case string:
		if i, err := strconv.ParseInt(val, 10, 64); err == nil {
			return i, true
		}
	}
	return 0, false
}

// likeValues implements SQL LIKE pattern matching
func (op *SelectionOperator) likeValues(value, pattern interface{}) bool {
	valStr, ok := value.(string)
	if !ok {
		valStr = utils.ToString(value)
	}
	patStr, ok := pattern.(string)
	if !ok {
		patStr = utils.ToString(pattern)
	}
	return utils.MatchesLike(valStr, patStr)
}
