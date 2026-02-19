package api

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// bindParams binds parameters to SQL query placeholders
// Supports ? placeholders, including slice expansion for IN clauses
func bindParams(sql string, params []interface{}) (string, error) {
	if len(params) == 0 {
		return sql, nil
	}

	// First, expand slices and count total values needed
	expandedParams := make([]interface{}, 0, len(params))
	for _, p := range params {
		if isSlice(p) {
			// Expand slice into individual elements
			slice := reflect.ValueOf(p)
			for i := 0; i < slice.Len(); i++ {
				expandedParams = append(expandedParams, slice.Index(i).Interface())
			}
		} else {
			expandedParams = append(expandedParams, p)
		}
	}

	// Count placeholders
	placeholderCount := strings.Count(sql, "?")
	if placeholderCount != len(params) {
		// Check if this might be due to slice expansion
		// For "id in (?,?,?)" pattern, we need to handle slice specially
		if placeholderCount == len(expandedParams) {
			// Continue with expanded params
		} else if placeholderCount != len(params) {
			return "", fmt.Errorf("parameter count mismatch: expected %d placeholders, got %d params",
				placeholderCount, len(params))
		}
	}

	result := make([]byte, 0, len(sql)*2)
	paramIndex := 0
	expandedIndex := 0

	for i := 0; i < len(sql); i++ {
		if sql[i] == '?' && paramIndex < len(params) {
			p := params[paramIndex]

			// Check if this is a slice and we need to expand it for IN clause
			if isSlice(p) && isINClause(sql, i) {
				slice := reflect.ValueOf(p)
				if slice.Len() == 0 {
					// Empty slice -> (NULL) to avoid SQL syntax error
					result = append(result, "(NULL)"...)
				} else {
					result = append(result, '(')
					for j := 0; j < slice.Len(); j++ {
						if j > 0 {
							result = append(result, ',')
						}
						value, err := paramToSQLLiteral(slice.Index(j).Interface())
						if err != nil {
							return "", fmt.Errorf("error binding slice element %d: %w", j+1, err)
						}
						result = append(result, value...)
					}
					result = append(result, ')')
				}
				paramIndex++
				expandedIndex += slice.Len()
			} else {
				// Single value
				value, err := paramToSQLLiteral(p)
				if err != nil {
					return "", fmt.Errorf("error binding parameter %d: %w", paramIndex+1, err)
				}
				result = append(result, value...)
				paramIndex++
				expandedIndex++
			}
		} else {
			result = append(result, sql[i])
		}
	}

	return string(result), nil
}

// isSlice checks if a value is a slice (but not []byte which is handled specially)
func isSlice(v interface{}) bool {
	if v == nil {
		return false
	}
	typ := reflect.TypeOf(v)
	if typ.Kind() == reflect.Slice {
		// []byte is handled as a single value (hex)
		if typ.Elem().Kind() == reflect.Uint8 {
			return false
		}
		return true
	}
	return false
}

// isINClause checks if the ? is part of an IN clause
func isINClause(sql string, questionIndex int) bool {
	// Look back for "IN" keyword (case insensitive)
	lookback := 20
	start := questionIndex - lookback
	if start < 0 {
		start = 0
	}
	before := strings.ToUpper(sql[start:questionIndex])
	return strings.HasSuffix(before, "IN ") ||
		strings.HasSuffix(before, "IN(") ||
		strings.Contains(before, "IN ") ||
		strings.Contains(before, "IN(")
}

// paramToSQLLiteral converts a Go value to SQL literal string
func paramToSQLLiteral(v interface{}) ([]byte, error) {
	if v == nil {
		return []byte("NULL"), nil
	}

	switch val := v.(type) {
	case string:
		// Escape single quotes by doubling them
		escaped := strings.ReplaceAll(val, "'", "''")
		return []byte(fmt.Sprintf("'%s'", escaped)), nil

	case int, int8, int16, int32, int64:
		return []byte(fmt.Sprintf("%d", val)), nil

	case uint, uint8, uint16, uint32, uint64:
		return []byte(fmt.Sprintf("%d", val)), nil

	case float32, float64:
		return []byte(fmt.Sprintf("%v", val)), nil

	case bool:
		if val {
			return []byte("TRUE"), nil
		}
		return []byte("FALSE"), nil

	case []byte:
		// Escape hex representation
		hex := "0x" + fmt.Sprintf("%x", val)
		return []byte(hex), nil

	case time.Time:
		// Format time as standard datetime string for SQL
		return []byte(fmt.Sprintf("'%s'", val.Format("2006-01-02 15:04:05.999999999"))), nil

	default:
		// Try to format as string (fallback)
		str := fmt.Sprintf("%v", val)
		escaped := strings.ReplaceAll(str, "'", "''")
		return []byte(fmt.Sprintf("'%s'", escaped)), nil
	}
}

// ParamToString converts a parameter to string representation
func ParamToString(v interface{}) string {
	if v == nil {
		return "NULL"
	}

	switch val := v.(type) {
	case string:
		return fmt.Sprintf("'%s'", strings.ReplaceAll(val, "'", "''"))
	case int, int8, int16, int32, int64:
		return fmt.Sprintf("%d", val)
	case uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", val)
	case float32, float64:
		return fmt.Sprintf("%v", val)
	case bool:
		if val {
			return "TRUE"
		}
		return "FALSE"
	case []byte:
		return "0x" + fmt.Sprintf("%x", val)
	case time.Time:
		return fmt.Sprintf("'%s'", val.Format("2006-01-02 15:04:05.999999999"))
	default:
		str := fmt.Sprintf("%v", val)
		return fmt.Sprintf("'%s'", strings.ReplaceAll(str, "'", "''"))
	}
}

// ParseInt parses a string to int with error handling
func ParseInt(s string) (int, error) {
	return strconv.Atoi(s)
}

// ParseInt64 parses a string to int64 with error handling
func ParseInt64(s string) (int64, error) {
	return strconv.ParseInt(s, 10, 64)
}

// ParseFloat parses a string to float64 with error handling
func ParseFloat(s string) (float64, error) {
	return strconv.ParseFloat(s, 64)
}

// ParseBool parses a string to bool with error handling
func ParseBool(s string) (bool, error) {
	return strconv.ParseBool(s)
}
