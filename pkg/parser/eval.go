package parser

import (
	"fmt"
	"strings"

	"github.com/kasuganosora/sqlexec/pkg/utils"
)

// EvalExpr evaluates an expression against a row, resolving column references
// and computing arithmetic. This is used for SET clauses like "gold = gold - 300".
func EvalExpr(expr *Expression, row Row) (interface{}, error) {
	if expr == nil {
		return nil, nil
	}

	switch expr.Type {
	case ExprTypeValue:
		return expr.Value, nil

	case ExprTypeColumn:
		col := expr.Column
		// Strip table qualifier (e.g. "t.gold" → "gold")
		if idx := strings.LastIndex(col, "."); idx >= 0 {
			col = col[idx+1:]
		}
		if val, ok := row[col]; ok {
			return val, nil
		}
		return nil, fmt.Errorf("column %q not found in row", col)

	case ExprTypeOperator:
		return evalOperator(expr, row)

	default:
		return nil, fmt.Errorf("unsupported expression type in SET: %v", expr.Type)
	}
}

func evalOperator(expr *Expression, row Row) (interface{}, error) {
	if expr.Left == nil {
		return nil, fmt.Errorf("operator %q: missing left operand", expr.Operator)
	}

	left, err := EvalExpr(expr.Left, row)
	if err != nil {
		return nil, err
	}

	// Unary operator
	if expr.Right == nil {
		return evalUnary(expr.Operator, left)
	}

	right, err := EvalExpr(expr.Right, row)
	if err != nil {
		return nil, err
	}

	op := strings.ToLower(expr.Operator)
	switch op {
	case "+", "plus":
		return addValues(left, right)
	case "-", "minus":
		return subValues(left, right)
	case "*", "mul":
		return mulValues(left, right)
	case "/", "div":
		return divValues(left, right)
	case "%", "mod":
		return modValues(left, right)
	default:
		return nil, fmt.Errorf("unsupported arithmetic operator in SET: %s", expr.Operator)
	}
}

func evalUnary(op string, val interface{}) (interface{}, error) {
	switch strings.ToLower(op) {
	case "-", "minus":
		f, err := utils.ToFloat64(val)
		if err != nil {
			return nil, err
		}
		if i, ok := utils.ToInt64(val); ok == nil {
			return -i, nil
		}
		return -f, nil
	case "+", "plus":
		return val, nil
	default:
		return nil, fmt.Errorf("unsupported unary operator in SET: %s", op)
	}
}

func addValues(a, b interface{}) (interface{}, error) {
	ai, aErr := utils.ToInt64(a)
	bi, bErr := utils.ToInt64(b)
	if aErr == nil && bErr == nil {
		return ai + bi, nil
	}
	af, aErr := utils.ToFloat64(a)
	bf, bErr := utils.ToFloat64(b)
	if aErr == nil && bErr == nil {
		return af + bf, nil
	}
	return nil, fmt.Errorf("cannot add %T and %T", a, b)
}

func subValues(a, b interface{}) (interface{}, error) {
	ai, aErr := utils.ToInt64(a)
	bi, bErr := utils.ToInt64(b)
	if aErr == nil && bErr == nil {
		return ai - bi, nil
	}
	af, aErr := utils.ToFloat64(a)
	bf, bErr := utils.ToFloat64(b)
	if aErr == nil && bErr == nil {
		return af - bf, nil
	}
	return nil, fmt.Errorf("cannot subtract %T and %T", a, b)
}

func mulValues(a, b interface{}) (interface{}, error) {
	ai, aErr := utils.ToInt64(a)
	bi, bErr := utils.ToInt64(b)
	if aErr == nil && bErr == nil {
		return ai * bi, nil
	}
	af, aErr := utils.ToFloat64(a)
	bf, bErr := utils.ToFloat64(b)
	if aErr == nil && bErr == nil {
		return af * bf, nil
	}
	return nil, fmt.Errorf("cannot multiply %T and %T", a, b)
}

func divValues(a, b interface{}) (interface{}, error) {
	af, aErr := utils.ToFloat64(a)
	bf, bErr := utils.ToFloat64(b)
	if aErr == nil && bErr == nil {
		if bf == 0 {
			return nil, fmt.Errorf("division by zero")
		}
		return af / bf, nil
	}
	return nil, fmt.Errorf("cannot divide %T by %T", a, b)
}

func modValues(a, b interface{}) (interface{}, error) {
	ai, aErr := utils.ToInt64(a)
	bi, bErr := utils.ToInt64(b)
	if aErr == nil && bErr == nil {
		if bi == 0 {
			return nil, fmt.Errorf("division by zero")
		}
		return ai % bi, nil
	}
	return nil, fmt.Errorf("cannot modulo %T and %T", a, b)
}
