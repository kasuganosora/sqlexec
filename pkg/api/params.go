package api

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// bindParams binds parameters to SQL query placeholders
// Supports ? placeholders
func bindParams(sql string, params []interface{}) (string, error) {
	if len(params) == 0 {
		return sql, nil
	}

	// Count placeholders
	placeholderCount := strings.Count(sql, "?")
	if placeholderCount != len(params) {
		return "", fmt.Errorf("parameter count mismatch: expected %d placeholders, got %d params",
			placeholderCount, len(params))
	}

	result := make([]byte, 0, len(sql)*2)
	paramIndex := 0

	for i := 0; i < len(sql); i++ {
		if sql[i] == '?' && paramIndex < len(params) {
			// Convert parameter to SQL literal
			value, err := paramToSQLLiteral(params[paramIndex])
			if err != nil {
				return "", fmt.Errorf("error binding parameter %d: %w", paramIndex+1, err)
			}
			result = append(result, value...)
			paramIndex++
		} else {
			result = append(result, sql[i])
		}
	}

	return string(result), nil
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
