package optimizer

import (
	"fmt"
	"reflect"
	"strconv"
)

// toFloat64 转换值为float64
// 这个函数用于数值类型转换
func toFloat64(val interface{}) (float64, bool) {
	if val == nil {
		return 0, false
	}

	switch v := val.(type) {
	case int, int8, int16, int32, int64:
		return float64(reflect.ValueOf(v).Int()), true
	case uint, uint8, uint16, uint32, uint64:
		return float64(reflect.ValueOf(v).Uint()), true
	case float32, float64:
		return float64(reflect.ValueOf(v).Float()), true
	case string:
		// 尝试从字符串解析数字
		f, err := strconv.ParseFloat(v, 64)
		if err == nil {
			return f, true
		}
		return 0, false
	default:
		return 0, false
	}
}

// toNumber 转换值为数值
// 这个函数用于比较和排序
func toNumber(val interface{}) (float64, bool) {
	return toFloat64(val)
}

// compareValues 比较两个值
// 返回 -1: a < b, 0: a == b, 1: a > b
func compareValues(a, b interface{}) int {
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
	aNum, aOk := toFloat64(a)
	bNum, bOk := toFloat64(b)
	if aOk && bOk {
		if aNum < bNum {
			return -1
		} else if aNum > bNum {
			return 1
		}
		return 0
	}

	// 降级到字符串比较
	aStr := fmt.Sprintf("%v", a)
	bStr := fmt.Sprintf("%v", b)
	if aStr < bStr {
		return -1
	} else if aStr > bStr {
		return 1
	}
	return 0
}

// compareValuesEqual 比较两个值是否相等
func compareValuesEqual(v1, v2 interface{}) bool {
	if v1 == nil && v2 == nil {
		return true
	}
	if v1 == nil || v2 == nil {
		return false
	}

	// 尝试数值比较
	if n1, ok1 := toFloat64(v1); ok1 {
		if n2, ok2 := toFloat64(v2); ok2 {
			return n1 == n2
		}
	}

	// 降级到字符串比较
	return fmt.Sprintf("%v", v1) == fmt.Sprintf("%v", v2)
}
