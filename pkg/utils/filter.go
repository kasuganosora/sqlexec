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
// Supports: % (any chars), _ (single char)
func MatchesLike(value, pattern string) bool {
	// Empty pattern only matches empty value
	if pattern == "" {
		return value == ""
	}

	// Single wildcard matches everything
	if pattern == "%" {
		return true
	}

	// Exact match
	if pattern == value {
		return true
	}

	// Count wildcards to determine complexity
	percentCount := 0
	for _, c := range pattern {
		if c == '%' {
			percentCount++
		}
	}

	// For complex patterns with multiple %, use recursive matching
	if percentCount > 2 {
		return matchesLikeRecursive(value, pattern)
	}

	// Check for middle wildcard: %xxx%
	if len(pattern) >= 2 && pattern[0] == '%' && pattern[len(pattern)-1] == '%' {
		middle := pattern[1 : len(pattern)-1]
		if middle == "" {
			return true
		}
		// Use simple substring check
		return containsSubstring(value, middle)
	}

	// Check for suffix wildcard: xxx%
	if len(pattern) > 1 && pattern[len(pattern)-1] == '%' {
		prefix := pattern[:len(pattern)-1]
		return len(value) >= len(prefix) && value[:len(prefix)] == prefix
	}

	// Check for prefix wildcard: %xxx
	if len(pattern) > 1 && pattern[0] == '%' {
		suffix := pattern[1:]
		return len(value) >= len(suffix) && value[len(value)-len(suffix):] == suffix
	}

	return false
}

// matchesLikeRecursive handles complex LIKE patterns with multiple wildcards
func matchesLikeRecursive(value, pattern string) bool {
	// Base cases
	if pattern == "" {
		return value == ""
	}
	if pattern == "%" {
		return true
	}
	if value == "" {
		return pattern == ""
	}

	// Handle leading %
	if pattern[0] == '%' {
		// Try matching % with zero chars, then recurse
		for i := 0; i <= len(value); i++ {
			if matchesLikeRecursive(value[i:], pattern[1:]) {
				return true
			}
		}
		return false
	}

	// Handle trailing % (optimization)
	if pattern[len(pattern)-1] == '%' {
		prefix := pattern[:len(pattern)-1]
		return len(value) >= len(prefix) && value[:len(prefix)] == prefix || 
			containsSubstring(value, prefix)
	}

	// Handle literal character at start
	if value[0] == pattern[0] || pattern[0] == '_' {
		return matchesLikeRecursive(value[1:], pattern[1:])
	}

	return false
}

// containsSubstring checks if substr exists in s (simple implementation)
func containsSubstring(s, substr string) bool {
	if len(substr) == 0 {
		return true
	}
	if len(substr) > len(s) {
		return false
	}
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
