package util

import (
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"sort"
)

// ApplyOrder 应用排序
func ApplyOrder(rows []domain.Row, options *domain.QueryOptions) []domain.Row {
	if options == nil || options.OrderBy == "" {
		return rows
	}

	result := make([]domain.Row, len(rows))
	copy(result, rows)

	// 获取排序列
	column := options.OrderBy

	// 获取排序方向
	order := options.Order
	if order == "" {
		order = "ASC"
	}

	// 排序
	sort.Slice(result, func(i, j int) bool {
		valI, existsI := result[i][column]
		valJ, existsJ := result[j][column]

		if !existsI && !existsJ {
			return false
		}
		if !existsI {
			return order != "ASC"
		}
		if !existsJ {
			return order == "ASC"
		}

		cmp := CompareValues(valI, valJ)
		if order == "DESC" {
			return cmp > 0
		}
		return cmp < 0
	})

	return result
}
