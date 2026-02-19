package util

import (
	"strings"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/utils"
)

// ApplyFilters 应用过滤器（通用实现）
func ApplyFilters(rows []domain.Row, options *domain.QueryOptions) []domain.Row {
	if options == nil || len(options.Filters) == 0 {
		return rows
	}

	result := make([]domain.Row, 0, len(rows)/2+1)
	for _, row := range rows {
		if MatchesFilters(row, options.Filters) {
			result = append(result, row)
		}
	}
	return result
}

// MatchesFilters 检查行是否匹配过滤器列表
func MatchesFilters(row domain.Row, filters []domain.Filter) bool {
	// 如果没有过滤器，所有行都匹配
	if len(filters) == 0 {
		return true
	}

	// 检查第一个过滤器
	filter := filters[0]

	// 如果有逻辑操作符
	if filter.LogicOp == "OR" || filter.LogicOp == "or" {
		// OR 操作：行的任何子过滤器匹配即可
		return MatchesAnySubFilter(row, filter.SubFilters)
	}

	// 如果有 AND 逻辑操作符
	if filter.LogicOp == "AND" || filter.LogicOp == "and" {
		// AND 操作：行的所有子过滤器都必须匹配
		return MatchesAllSubFilters(row, filter.SubFilters)
	}

	// 默认（AND 逻辑）：所有过滤器都必须匹配
	for _, f := range filters {
		if !MatchFilter(row, f) {
			return false
		}
	}
	return true
}

// MatchesAnySubFilter 检查行是否匹配任意子过滤器（OR 逻辑）
func MatchesAnySubFilter(row domain.Row, subFilters []domain.Filter) bool {
	// 如果没有子过滤器，返回 true
	if len(subFilters) == 0 {
		return true
	}
	// 检查是否有子过滤器匹配
	for _, subFilter := range subFilters {
		if MatchFilter(row, subFilter) {
			return true
		}
	}
	return false
}

// MatchesAllSubFilters 检查行是否匹配所有子过滤器（AND 逻辑）
func MatchesAllSubFilters(row domain.Row, subFilters []domain.Filter) bool {
	// 如果没有子过滤器，返回 true
	if len(subFilters) == 0 {
		return true
	}
	// 检查是否所有子过滤器都匹配
	for _, subFilter := range subFilters {
		if !MatchFilter(row, subFilter) {
			return false
		}
	}
	return true
}

// MatchFilter 匹配单个过滤器
func MatchFilter(row domain.Row, filter domain.Filter) bool {
	// 处理逻辑运算符（AND/OR）
	if filter.LogicOp == "OR" || filter.LogicOp == "or" {
		return MatchesAnySubFilter(row, filter.SubFilters)
	}
	if filter.LogicOp == "AND" || filter.LogicOp == "and" {
		return MatchesAllSubFilters(row, filter.SubFilters)
	}

	// 处理 IS NULL 和 IS NOT NULL 操作符
	op := strings.ToUpper(filter.Operator)
	if op == "IS NULL" || op == "ISNULL" {
		value, exists := row[filter.Field]
		if !exists {
			return true // 不存在的字段视为 NULL
		}
		return value == nil
	}
	if op == "IS NOT NULL" || op == "ISNOTNULL" {
		value, exists := row[filter.Field]
		if !exists {
			return false // 不存在的字段视为 NULL
		}
		return value != nil
	}

	// 处理普通字段比较
	value, exists := row[filter.Field]
	if !exists {
		return false
	}

	result, err := utils.CompareValues(value, filter.Value, filter.Operator)
	if err != nil {
		return false
	}
	return result
}
