package utils

import (
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// ApplyFilters applies filters to result rows
func ApplyFilters(rows []domain.Row, filters []domain.Filter) ([]domain.Row, error) {
	for _, filter := range filters {
		var filteredRows []domain.Row

		for _, row := range rows {
			matches, err := MatchesFilter(row, filter)
			if err != nil {
				return nil, err
			}
			if matches {
				filteredRows = append(filteredRows, row)
			}
		}

		rows = filteredRows
	}

	return rows, nil
}

// MatchesFilter checks if a row matches a filter
func MatchesFilter(row domain.Row, filter domain.Filter) (bool, error) {
	// 处理逻辑运算符（AND/OR）
	if filter.LogicOp == "OR" || filter.LogicOp == "or" {
		return MatchesAnySubFilter(row, filter.SubFilters), nil
	}
	if filter.LogicOp == "AND" || filter.LogicOp == "and" {
		return MatchesAllSubFilters(row, filter.SubFilters), nil
	}

	// 处理普通字段比较
	value, exists := row[filter.Field]
	if !exists {
		return false, nil
	}

	return CompareValues(value, filter.Value, filter.Operator)
}

// MatchesAnySubFilter 检查行是否匹配任意子过滤器（OR 逻辑）
func MatchesAnySubFilter(row domain.Row, subFilters []domain.Filter) bool {
	if len(subFilters) == 0 {
		return true
	}
	for _, subFilter := range subFilters {
		if matched, _ := MatchesFilter(row, subFilter); matched {
			return true
		}
	}
	return false
}

// MatchesAllSubFilters 检查行是否匹配所有子过滤器（AND 逻辑）
func MatchesAllSubFilters(row domain.Row, subFilters []domain.Filter) bool {
	if len(subFilters) == 0 {
		return true
	}
	for _, subFilter := range subFilters {
		if matched, _ := MatchesFilter(row, subFilter); !matched {
			return false
		}
	}
	return true
}

// MatchesLike implements simple LIKE pattern matching
func MatchesLike(value, pattern string) bool {
	// Simple implementation - can be enhanced for full LIKE support
	if pattern == "%" {
		return true
	}
	if pattern == value {
		return true
	}
	if len(pattern) > 0 && pattern[0] == '%' && len(pattern) > 1 {
		suffix := pattern[1:]
		return len(value) >= len(suffix) && value[len(value)-len(suffix):] == suffix
	}
	if len(pattern) > 0 && pattern[len(pattern)-1] == '%' && len(pattern) > 1 {
		prefix := pattern[:len(pattern)-1]
		return len(value) >= len(prefix) && value[:len(prefix)] == prefix
	}
	return false
}
