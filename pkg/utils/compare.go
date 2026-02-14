package utils

import (
	"fmt"
	"strings"
)

// CompareValues compares two values with given operator
// Returns true if comparison matches, false otherwise
func CompareValues(a, b interface{}, operator string) (bool, error) {
	// Normalize operator
	op := strings.ToUpper(operator)

	// Handle special operators
	switch op {
	case "IN":
		return compareIn(a, b)
	case "NOT IN":
		result, err := compareIn(a, b)
		return !result, err
	case "BETWEEN":
		return compareBetween(a, b)
	case "NOT BETWEEN":
		result, err := compareBetween(a, b)
		return !result, err
	case "LIKE":
		return compareLike(a, b)
	case "NOT LIKE":
		result, err := compareLike(a, b)
		return !result, err
	}

	// Handle nil values
	if a == nil || b == nil {
		switch op {
		case "=", "EQ":
			return a == nil && b == nil, nil
		case "!=", "NEQ":
			return !(a == nil && b == nil), nil
		default:
			return false, nil
		}
	}

	// Try numeric comparison
	aNum, aErr := ToFloat64(a)
	bNum, bErr := ToFloat64(b)

	if aErr == nil && bErr == nil {
		switch op {
		case "=", "EQ":
			return aNum == bNum, nil
		case "!=", "NEQ":
			return aNum != bNum, nil
		case ">", "GT":
			return aNum > bNum, nil
		case "<", "LT":
			return aNum < bNum, nil
		case ">=", "GE":
			return aNum >= bNum, nil
		case "<=", "LE":
			return aNum <= bNum, nil
		default:
			return false, fmt.Errorf("unsupported operator: %s", operator)
		}
	}

	// String comparison
	aStr, aOk := a.(string)
	bStr, bOk := b.(string)

	if aOk && bOk {
		switch op {
		case "=", "EQ":
			return aStr == bStr, nil
		case "!=", "NEQ":
			return aStr != bStr, nil
		case ">", "GT":
			return aStr > bStr, nil
		case "<", "LT":
			return aStr < bStr, nil
		case ">=", "GE":
			return aStr >= bStr, nil
		case "<=", "LE":
			return aStr <= bStr, nil
		default:
			return false, fmt.Errorf("unsupported operator: %s", operator)
		}
	}

	return false, fmt.Errorf("cannot compare %T with %T", a, b)
}

// CompareValuesForSort compares two values for sorting
// Returns -1: a < b, 0: a == b, 1: a > b
func CompareValuesForSort(a, b interface{}) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	// Try numeric comparison
	aNum, aErr := ToFloat64(a)
	bNum, bErr := ToFloat64(b)

	if aErr == nil && bErr == nil {
		if aNum < bNum {
			return -1
		} else if aNum > bNum {
			return 1
		}
		return 0
	}

	// String comparison
	aStr := fmt.Sprintf("%v", a)
	bStr := fmt.Sprintf("%v", b)

	if aStr < bStr {
		return -1
	} else if aStr > bStr {
		return 1
	}
	return 0
}

// compareIn checks if value is in list
func compareIn(a, b interface{}) (bool, error) {
	values, ok := b.([]interface{})
	if !ok {
		return false, fmt.Errorf("IN operator requires array value")
	}

	for _, v := range values {
		if result, err := CompareValues(a, v, "="); err == nil && result {
			return true, nil
		}
	}
	return false, nil
}

// compareBetween checks if value is between min and max
func compareBetween(a, b interface{}) (bool, error) {
	slice, ok := b.([]interface{})
	if !ok || len(slice) < 2 {
		return false, fmt.Errorf("BETWEEN operator requires array with 2 elements")
	}

	min := slice[0]
	max := slice[1]

	// Check lower bound
	lowerOK, err := CompareValues(a, min, ">=")
	if err != nil || !lowerOK {
		return false, err
	}

	// Check upper bound
	upperOK, err := CompareValues(a, max, "<=")
	if err != nil || !upperOK {
		return false, err
	}

	return true, nil
}

// MapOperator 映射parser操作符到标准操作符
// 支持的parser操作符: gt, gte, lt, lte, eq, ne, ===, !=
// 返回标准SQL操作符: >, >=, <, <=, =, !=
func MapOperator(parserOp string) string {
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

// compareLike checks if value matches pattern
// Supports % (any chars) and * (any chars - glob style)
func compareLike(a, b interface{}) (bool, error) {
	aStr, ok := a.(string)
	if !ok {
		aStr = ToString(a)
	}
	bStr, ok := b.(string)
	if !ok {
		bStr = ToString(b)
	}

	// Check for * wildcard (glob style)
	if strings.Contains(bStr, "*") {
		normalizedPattern := strings.ReplaceAll(bStr, "*", "%")
		return MatchesLike(aStr, normalizedPattern), nil
	}

	return MatchesLike(aStr, bStr), nil
}
