package util

import (
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// ApplyPagination 应用分页（通用实现）

// ApplyPagination 应用分页（通用实现）
func ApplyPagination(rows []domain.Row, offset, limit int) []domain.Row {
	if limit <= 0 {
		return rows
	}

	start := offset
	if start < 0 {
		start = 0
	}
	if start >= len(rows) {
		return []domain.Row{}
	}

	end := start + limit
	if end > len(rows) {
		end = len(rows)
	}

	return rows[start:end:end]
}

// PruneRows 裁剪行，只保留指定的列
func PruneRows(rows []domain.Row, columns []string) []domain.Row {
	if len(columns) == 0 {
		return rows
	}

	numCols := len(columns)
	result := make([]domain.Row, len(rows))
	for i, row := range rows {
		pruned := make(domain.Row, numCols)
		for _, col := range columns {
			if val, ok := row[col]; ok {
				pruned[col] = val
			}
		}
		result[i] = pruned
	}

	return result
}

// ApplyQueryOperations 应用查询操作（过滤器、排序、分页）
func ApplyQueryOperations(rows []domain.Row, options *domain.QueryOptions, columns *[]domain.ColumnInfo) []domain.Row {
	if options == nil {
		return rows
	}

	// 应用过滤器
	filteredRows := ApplyFilters(rows, options)

	// 应用排序
	sortedRows := ApplyOrder(filteredRows, options)

	// 应用分页
	pagedRows := ApplyPagination(sortedRows, options.Offset, options.Limit)

	// 如果需要列裁剪
	if len(options.SelectColumns) > 0 && columns != nil {
		pagedRows = PruneRows(pagedRows, options.SelectColumns)
	}

	return pagedRows
}

// GetNeededColumns 获取需要读取的列（通用方法）
func GetNeededColumns(options *domain.QueryOptions) []string {
	if options == nil {
		return nil
	}

	needed := make(map[string]bool)
	for _, filter := range options.Filters {
		needed[filter.Field] = true
	}

	if options.OrderBy != "" {
		needed[options.OrderBy] = true
	}

	if len(options.SelectColumns) > 0 {
		for _, col := range options.SelectColumns {
			needed[col] = true
		}
	}

	if len(needed) == 0 {
		return nil
	}

	result := make([]string, 0, len(needed))
	for col := range needed {
		result = append(result, col)
	}

	return result
}
