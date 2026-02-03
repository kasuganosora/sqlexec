package optimizer

import (
	"github.com/kasuganosora/sqlexec/pkg/utils"
)

// toFloat64 转换值为float64（兼容包装）
// 这个函数用于数值类型转换
func toFloat64(val interface{}) (float64, bool) {
	f, err := utils.ToFloat64(val)
	return f, err == nil
}

// toNumber 转换值为数值（兼容包装）
// 这个函数用于比较和排序
func toNumber(val interface{}) (float64, bool) {
	return toFloat64(val)
}

// compareValues 比较两个值
// 返回 -1: a < b, 0: a == b, 1: a > b
func compareValues(a, b interface{}) int {
	// 使用 utils 包的 CompareValuesForSort 函数
	return utils.CompareValuesForSort(a, b)
}

// compareValuesEqual 比较两个值是否相等
func compareValuesEqual(v1, v2 interface{}) bool {
	return utils.CompareValuesForSort(v1, v2) == 0
}
