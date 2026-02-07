package optimizer

import (
	"github.com/kasuganosora/sqlexec/pkg/utils"
)

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

