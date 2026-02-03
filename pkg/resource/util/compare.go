package util

import (
	"github.com/kasuganosora/sqlexec/pkg/utils"
)

// CompareEqual 比较两个值是否相等
func CompareEqual(a, b interface{}) bool {
	result, _ := utils.CompareValues(a, b, "=")
	return result
}

// CompareNumeric 数值比较，返回 -1 (a<b), 0 (a==b), 1 (a>b) 和成功标志
func CompareNumeric(a, b interface{}) (int, bool) {
	// 检查是否都可以转换为数字
	aFloat, aOk := ConvertToFloat64(a)
	bFloat, bOk := ConvertToFloat64(b)

	if !aOk || !bOk {
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
	result, _ := utils.CompareValues(a, b, ">")
	return result
}

// CompareLike 模糊匹配（支持 * 和 % 通配符）
func CompareLike(a, b interface{}) bool {
	result, _ := utils.CompareValues(a, b, "LIKE")
	return result
}

// CompareIn 检查a是否在b的值列表中
func CompareIn(a, b interface{}) bool {
	result, _ := utils.CompareValues(a, b, "IN")
	return result
}

// CompareBetween 检查值是否在范围内
func CompareBetween(a, b interface{}) bool {
	result, _ := utils.CompareValues(a, b, "BETWEEN")
	return result
}

// CompareValues 比较两个值（用于索引排序）
func CompareValues(a, b interface{}) int {
	return utils.CompareValuesForSort(a, b)
}

// ConvertToFloat64 将值转换为 float64 进行数值比较（兼容包装）
func ConvertToFloat64(v interface{}) (float64, bool) {
	f, err := utils.ToFloat64(v)
	return f, err == nil
}
