package utils

import (
	"fmt"
	"strconv"
)

// ParseInt parses a string to int, returns default value on error
func ParseInt(s string, defaultValue int) int {
	if s == "" {
		return defaultValue
	}
	val, err := strconv.Atoi(s)
	if err != nil {
		return defaultValue
	}
	return val
}

// ParseInt64 parses a string to int64, returns default value on error
func ParseInt64(s string, defaultValue int64) int64 {
	if s == "" {
		return defaultValue
	}
	val, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return defaultValue
	}
	return val
}

// ParseFloat64 parses a string to float64, returns default value on error
func ParseFloat64(s string, defaultValue float64) float64 {
	if s == "" {
		return defaultValue
	}
	val, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return defaultValue
	}
	return val
}

// ParseBool parses a string to bool, returns default value on error
// Accepts "1", "t", "T", "true", "TRUE", "True" as true
// Accepts "0", "f", "F", "false", "FALSE", "False" as false
func ParseBool(s string, defaultValue bool) bool {
	if s == "" {
		return defaultValue
	}
	val, err := strconv.ParseBool(s)
	if err != nil {
		return defaultValue
	}
	return val
}

// ParseIntStrict parses a string to int, returns error on failure
func ParseIntStrict(s string) (int, error) {
	return strconv.Atoi(s)
}

// ParseInt64Strict parses a string to int64, returns error on failure
func ParseInt64Strict(s string) (int64, error) {
	return strconv.ParseInt(s, 10, 64)
}

// ParseFloat64Strict parses a string to float64, returns error on failure
func ParseFloat64Strict(s string) (float64, error) {
	return strconv.ParseFloat(s, 64)
}

// ParseBoolStrict parses a string to bool, returns error on failure
func ParseBoolStrict(s string) (bool, error) {
	return strconv.ParseBool(s)
}

// ParseUint parses a string to uint, returns default value on error
func ParseUint(s string, defaultValue uint) uint {
	if s == "" {
		return defaultValue
	}
	val, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return defaultValue
	}
	return uint(val)
}

// ParseUint64 parses a string to uint64, returns default value on error
func ParseUint64(s string, defaultValue uint64) uint64 {
	if s == "" {
		return defaultValue
	}
	val, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return defaultValue
	}
	return val
}

// MustParseInt parses a string to int, panics on error
func MustParseInt(s string) int {
	val, err := strconv.Atoi(s)
	if err != nil {
		panic(fmt.Sprintf("failed to parse int: %s", s))
	}
	return val
}

// MustParseInt64 parses a string to int64, panics on error
func MustParseInt64(s string) int64 {
	val, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		panic(fmt.Sprintf("failed to parse int64: %s", s))
	}
	return val
}

// MustParseFloat64 parses a string to float64, panics on error
func MustParseFloat64(s string) float64 {
	val, err := strconv.ParseFloat(s, 64)
	if err != nil {
		panic(fmt.Sprintf("failed to parse float64: %s", s))
	}
	return val
}
