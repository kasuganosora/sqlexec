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
	// Handle logical operators (AND/OR)
	if filter.LogicOp == "OR" || filter.LogicOp == "or" {
		return MatchesAnySubFilter(row, filter.SubFilters)
	}
	if filter.LogicOp == "AND" || filter.LogicOp == "and" {
		return MatchesAllSubFilters(row, filter.SubFilters)
	}

	// Handle regular field comparison
	value, exists := row[filter.Field]
	if !exists {
		return false, nil
	}

	return CompareValues(value, filter.Value, filter.Operator)
}

// MatchesAnySubFilter checks if a row matches any sub-filter (OR logic)
// Returns the first error encountered, but continues matching
func MatchesAnySubFilter(row domain.Row, subFilters []domain.Filter) (bool, error) {
	if len(subFilters) == 0 {
		return true, nil
	}
	for _, subFilter := range subFilters {
		matched, err := MatchesFilter(row, subFilter)
		if err != nil {
			// Log the error but continue checking other filters
			// Return false only if all filters fail
			continue
		}
		if matched {
			return true, nil
		}
	}
	return false, nil
}

// MatchesAllSubFilters checks if a row matches all sub-filters (AND logic)
// Returns error immediately if any filter fails with an error
func MatchesAllSubFilters(row domain.Row, subFilters []domain.Filter) (bool, error) {
	if len(subFilters) == 0 {
		return true, nil
	}
	for _, subFilter := range subFilters {
		matched, err := MatchesFilter(row, subFilter)
		if err != nil {
			return false, err
		}
		if !matched {
			return false, nil
		}
	}
	return true, nil
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
