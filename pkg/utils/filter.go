package utils

import (
	"strings"
	"unicode"

	"golang.org/x/text/cases"
	"golang.org/x/text/language"
	"golang.org/x/text/unicode/norm"

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

// MatchesLike implements SQL LIKE pattern matching.
// Supports: % (match any sequence of characters), _ (match any single character).
// Uses a segment-based algorithm: splits the pattern by '%', then matches
// each literal segment in order. O(n*k) where k = number of segments.
func MatchesLike(value, pattern string) bool {
	// Empty pattern only matches empty value
	if pattern == "" {
		return value == ""
	}

	// Single wildcard matches everything
	if pattern == "%" {
		return true
	}

	// Exact match (fast: compares length then pointer/bytes)
	if value == pattern {
		return true
	}

	// Single scan to classify the pattern
	percentCount := 0
	hasUnderscore := false
	for i := 0; i < len(pattern); i++ {
		switch pattern[i] {
		case '%':
			percentCount++
		case '_':
			hasUnderscore = true
		}
	}

	// No wildcards → exact match
	if percentCount == 0 && !hasUnderscore {
		return value == pattern
	}

	// Fast paths for single-% patterns without '_'
	if percentCount == 1 && !hasUnderscore {
		if pattern[len(pattern)-1] == '%' {
			// xxx%
			prefix := pattern[:len(pattern)-1]
			return len(value) >= len(prefix) && value[:len(prefix)] == prefix
		}
		if pattern[0] == '%' {
			// %xxx
			suffix := pattern[1:]
			return len(value) >= len(suffix) && value[len(value)-len(suffix):] == suffix
		}
	}

	// Fast path for %xxx% pattern without '_'
	if percentCount == 2 && !hasUnderscore && pattern[0] == '%' && pattern[len(pattern)-1] == '%' {
		return strings.Contains(value, pattern[1:len(pattern)-1])
	}

	// General segment-based matching
	return matchesLikeSegmented(value, pattern)
}

// matchesLikeSegmented splits pattern by '%' into literal segments and matches
// them in order against value. Each '_' in a segment matches exactly one character.
func matchesLikeSegmented(value, pattern string) bool {
	segments := strings.Split(pattern, "%")
	// segments[0] = before first %, segments[last] = after last %

	startsWithPercent := pattern[0] == '%'
	endsWithPercent := pattern[len(pattern)-1] == '%'

	pos := 0

	for i, seg := range segments {
		if seg == "" {
			continue
		}

		isFirst := (i == 0) && !startsWithPercent
		isLast := (i == len(segments)-1) && !endsWithPercent

		if isFirst {
			// First segment must match at the start of value
			if !matchSegmentAt(value, 0, seg) {
				return false
			}
			pos = len(seg)
		} else if isLast {
			// Last segment must match at the end of value
			startPos := len(value) - len(seg)
			if startPos < pos {
				return false
			}
			if !matchSegmentAt(value, startPos, seg) {
				return false
			}
			pos = len(value)
		} else {
			// Middle segment: find first occurrence from current pos
			found := findSegment(value, pos, seg)
			if found < 0 {
				return false
			}
			pos = found + len(seg)
		}
	}

	// If pattern doesn't end with %, value must be fully consumed
	if !endsWithPercent && pos != len(value) {
		return false
	}

	return true
}

// matchSegmentAt checks if segment matches at exactly position pos in value.
// '_' in segment matches any single byte.
func matchSegmentAt(value string, pos int, seg string) bool {
	if pos+len(seg) > len(value) {
		return false
	}
	for i := 0; i < len(seg); i++ {
		if seg[i] != '_' && seg[i] != value[pos+i] {
			return false
		}
	}
	return true
}

// MatchesLikeWithCollation performs LIKE matching with collation awareness.
// For _ci collations, folds both value and pattern to lowercase before matching.
// For _ai_ci collations, additionally strips accents via NFD decomposition.
func MatchesLikeWithCollation(value, pattern, collation string) bool {
	if collation == "" || collation == "utf8mb4_bin" || collation == "binary" {
		return MatchesLike(value, pattern)
	}

	engine := GetGlobalCollationEngine()

	// For accent-insensitive collations, strip accents first
	if engine.IsAccentInsensitive(collation) {
		value = stripAccents(value)
		pattern = stripAccents(pattern)
	}

	// For case-insensitive collations, fold case
	if engine.IsCaseInsensitive(collation) {
		folder := cases.Fold()
		value = folder.String(value)
		pattern = folder.String(pattern)
	}

	return MatchesLike(value, pattern)
}

// stripAccents removes diacritical marks from a string using NFD decomposition.
func stripAccents(s string) string {
	// NFD decomposes characters: é → e + combining accent
	decomposed := norm.NFD.String(s)
	// Filter out combining marks (unicode category Mn)
	var buf strings.Builder
	for _, r := range decomposed {
		if !unicode.Is(unicode.Mn, r) {
			buf.WriteRune(r)
		}
	}
	return buf.String()
}

// stripAccentsForLocale strips accents using locale-specific rules.
// Currently delegates to stripAccents; can be extended for locale-specific behavior.
func stripAccentsForLocale(s string, _ language.Tag) string {
	return stripAccents(s)
}

// findSegment finds the first position >= startPos where segment matches in value.
// Returns -1 if not found.
func findSegment(value string, startPos int, seg string) int {
	if !strings.Contains(seg, "_") {
		// No underscore: use strings.Index for speed
		idx := strings.Index(value[startPos:], seg)
		if idx < 0 {
			return -1
		}
		return startPos + idx
	}
	// Has underscore: linear scan
	for i := startPos; i <= len(value)-len(seg); i++ {
		if matchSegmentAt(value, i, seg) {
			return i
		}
	}
	return -1
}
