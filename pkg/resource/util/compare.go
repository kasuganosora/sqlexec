package util

import (
	"fmt"
	"reflect"
	"strconv"
)

// CompareEqual 比较两个值是否相等
func CompareEqual(a, b interface{}) bool {
	if a == nil || b == nil {
		return a == b
	}

	// 尝试数值比较
	if cmp, ok := CompareNumeric(a, b); ok {
		return cmp == 0
	}

	// 尝试字符串比较
	aStr := fmt.Sprintf("%v", a)
	bStr := fmt.Sprintf("%v", b)
	return aStr == bStr
}

// CompareNumeric 数值比较，返回 -1 (a<b), 0 (a==b), 1 (a>b) 和成功标志
func CompareNumeric(a, b interface{}) (int, bool) {
	aFloat, okA := ConvertToFloat64(a)
	bFloat, okB := ConvertToFloat64(b)
	if !okA || !okB {
		return 0, false
	}

	if aFloat < bFloat {
		return -1, true
	} else if aFloat > bFloat {
		return 1, true
	}
	return 0, true
}

// CompareGreater 比较a是否大于b
func CompareGreater(a, b interface{}) bool {
	// 尝试数值比较
	if cmp, ok := CompareNumeric(a, b); ok {
		return cmp > 0
	}
	// 降级到字符串比较
	aStr := fmt.Sprintf("%v", a)
	bStr := fmt.Sprintf("%v", b)
	return aStr > bStr
}

// CompareLike 模糊匹配（支持 * 和 % 通配符）
func CompareLike(a, b interface{}) bool {
	aStr := fmt.Sprintf("%v", a)
	bStr := fmt.Sprintf("%v", b)

	// 简化实现：只支持 * 通配符
	pattern := ""
	for _, ch := range bStr {
		if ch == '*' || ch == '%' {
			pattern += ".*"
		} else if ch == '_' {
			pattern += "."
		} else {
			pattern += string(ch)
		}
	}

	// 使用 contains 进行简化匹配
	return Contains(aStr, bStr)
}

// CompareIn 检查a是否在b的值列表中
func CompareIn(a, b interface{}) bool {
	values, ok := b.([]interface{})
	if !ok {
		return false
	}

	for _, v := range values {
		if CompareEqual(a, v) {
			return true
		}
	}
	return false
}

// CompareBetween 检查值是否在范围内
func CompareBetween(a, b interface{}) bool {
	// b 应该是一个包含两个元素的数组 [min, max]
	slice, ok := b.([]interface{})
	if !ok || len(slice) < 2 {
		return false
	}

	min := slice[0]
	max := slice[1]

	// 对于字符串，使用字符串比较
	aStr := fmt.Sprintf("%v", a)
	minStr := fmt.Sprintf("%v", min)
	maxStr := fmt.Sprintf("%v", max)

	// 对于数值，使用数值比较
	if cmp, ok := CompareNumeric(a, min); ok && cmp >= 0 {
		if cmpMax, okMax := CompareNumeric(a, max); okMax && cmpMax <= 0 {
			return true
		}
	}

	// 降级到字符串比较：min <= a <= max
	return (aStr >= minStr) && (aStr <= maxStr)
}

// CompareValues 比较两个值（用于索引排序）
func CompareValues(a, b interface{}) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	// 尝试数值比较
	aFloat, aOk := ConvertToFloat64(a)
	bFloat, bOk := ConvertToFloat64(b)
	if aOk && bOk {
		if aFloat < bFloat {
			return -1
		} else if aFloat > bFloat {
			return 1
		}
		return 0
	}

	// 字符串比较
	aStr := fmt.Sprintf("%v", a)
	bStr := fmt.Sprintf("%v", b)
	if aStr < bStr {
		return -1
	} else if aStr > bStr {
		return 1
	}
	return 0
}

// ConvertToFloat64 将值转换为 float64 进行数值比较
func ConvertToFloat64(v interface{}) (float64, bool) {
	switch val := v.(type) {
	case int:
		return float64(val), true
	case int8:
		return float64(val), true
	case int16:
		return float64(val), true
	case int32:
		return float64(val), true
	case int64:
		return float64(val), true
	case uint:
		return float64(val), true
	case uint8:
		return float64(val), true
	case uint16:
		return float64(val), true
	case uint32:
		return float64(val), true
	case uint64:
		return float64(val), true
	case float32:
		return float64(val), true
	case float64:
		return val, true
	case string:
		f, err := strconv.ParseFloat(val, 64)
		return f, err == nil
	default:
		// 尝试通过反射获取数值
		rv := reflect.ValueOf(v)
		switch rv.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return float64(rv.Int()), true
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return float64(rv.Uint()), true
		case reflect.Float32, reflect.Float64:
			return rv.Float(), true
		}
		return 0, false
	}
}
