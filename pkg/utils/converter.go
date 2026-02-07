package utils

import (
	"fmt"
	"strconv"
)

// ToString converts any value to string
func ToString(v interface{}) string {
	if v == nil {
		return ""
	}

	switch val := v.(type) {
	case string:
		return val
	case []byte:
		return string(val)
	case bool:
		if val {
			return "true"
		}
		return "false"
	case int:
		return fmt.Sprintf("%d", val)
	case int8:
		return fmt.Sprintf("%d", val)
	case int16:
		return fmt.Sprintf("%d", val)
	case int32:
		return fmt.Sprintf("%d", val)
	case int64:
		return fmt.Sprintf("%d", val)
	case uint:
		return fmt.Sprintf("%d", val)
	case uint8:
		return fmt.Sprintf("%d", val)
	case uint16:
		return fmt.Sprintf("%d", val)
	case uint32:
		return fmt.Sprintf("%d", val)
	case uint64:
		return fmt.Sprintf("%d", val)
	case float32:
		// Use %g for cleaner representation without trailing zeros
		return fmt.Sprintf("%g", val)
	case float64:
		// Use %g for cleaner representation without trailing zeros
		return fmt.Sprintf("%g", val)
	default:
		return fmt.Sprintf("%v", val)
	}
}

// ToInt converts a value to int
func ToInt(arg interface{}) (int, error) {
	if arg == nil {
		return 0, fmt.Errorf("cannot convert nil to int")
	}

	switch v := arg.(type) {
	case int:
		return v, nil
	case int8:
		return int(v), nil
	case int16:
		return int(v), nil
	case int32:
		return int(v), nil
	case int64:
		return int(v), nil
	case uint:
		return int(v), nil
	case uint8:
		return int(v), nil
	case uint16:
		return int(v), nil
	case uint32:
		return int(v), nil
	case uint64:
		return int(v), nil
	case float64:
		return int(v), nil
	case float32:
		return int(v), nil
	case string:
		i, err := strconv.Atoi(v)
		if err != nil {
			return 0, fmt.Errorf("cannot convert string '%s' to int: %v", v, err)
		}
		return i, nil
	default:
		return 0, fmt.Errorf("cannot convert %T to int", arg)
	}
}

// ToInt64 converts a value to int64
func ToInt64(arg interface{}) (int64, error) {
	if arg == nil {
		return 0, fmt.Errorf("cannot convert nil to int64")
	}

	switch v := arg.(type) {
	case int:
		return int64(v), nil
	case int8:
		return int64(v), nil
	case int16:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case int64:
		return v, nil
	case uint:
		return int64(v), nil
	case uint8:
		return int64(v), nil
	case uint16:
		return int64(v), nil
	case uint32:
		return int64(v), nil
	case uint64:
		return int64(v), nil
	case float64:
		return int64(v), nil
	case float32:
		return int64(v), nil
	default:
		return 0, fmt.Errorf("cannot convert %T to int64", arg)
	}
}

// ToFloat64 converts a value to float64
func ToFloat64(arg interface{}) (float64, error) {
	if arg == nil {
		return 0, fmt.Errorf("cannot convert nil to float64")
	}

	switch v := arg.(type) {
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	case int:
		return float64(v), nil
	case int8:
		return float64(v), nil
	case int16:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case uint:
		return float64(v), nil
	case uint8:
		return float64(v), nil
	case uint16:
		return float64(v), nil
	case uint32:
		return float64(v), nil
	case uint64:
		return float64(v), nil
	case string:
		// Try parsing string to float64
		f, err := strconv.ParseFloat(v, 64)
		if err == nil {
			return f, nil
		}
		return 0, fmt.Errorf("cannot convert %T to float64", arg)
	default:
		return 0, fmt.Errorf("cannot convert %T to float64", arg)
	}
}
