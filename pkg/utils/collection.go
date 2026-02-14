package utils

import (
	"sort"
)

// Ordered is a constraint that permits any ordered type.
type Ordered interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~uintptr |
		~float32 | ~float64 |
		~string
}

// Signed is a constraint that permits any signed integer type.
type Signed interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64
}

// SortedKeys returns the keys of a map sorted in ascending order
func SortedKeys[K Ordered, V any](m map[K]V) []K {
	keys := make([]K, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})
	return keys
}

// SortedStringKeys returns the string keys of a map sorted in ascending order
func SortedStringKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// UniqueStrings removes duplicate strings from a slice
func UniqueStrings(slice []string) []string {
	if len(slice) == 0 {
		return slice
	}
	seen := make(map[string]bool, len(slice))
	result := make([]string, 0, len(slice))
	for _, v := range slice {
		if !seen[v] {
			seen[v] = true
			result = append(result, v)
		}
	}
	return result
}

// UniqueInts removes duplicate integers from a slice
func UniqueInts(slice []int) []int {
	if len(slice) == 0 {
		return slice
	}
	seen := make(map[int]bool, len(slice))
	result := make([]int, 0, len(slice))
	for _, v := range slice {
		if !seen[v] {
			seen[v] = true
			result = append(result, v)
		}
	}
	return result
}

// UniqueInt64s removes duplicate int64 from a slice
func UniqueInt64s(slice []int64) []int64 {
	if len(slice) == 0 {
		return slice
	}
	seen := make(map[int64]bool, len(slice))
	result := make([]int64, 0, len(slice))
	for _, v := range slice {
		if !seen[v] {
			seen[v] = true
			result = append(result, v)
		}
	}
	return result
}

// ContainsSlice checks if a slice contains a specific element
func ContainsSlice[T comparable](slice []T, item T) bool {
	for _, v := range slice {
		if v == item {
			return true
		}
	}
	return false
}

// DeepCopy creates a deep copy of basic types (primitive values, slices, maps)
// For complex types, use json.Marshal/Unmarshal or implement custom copying
func DeepCopy(value interface{}) interface{} {
	if value == nil {
		return nil
	}

	switch v := value.(type) {
	case bool, int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64, string:
		return v

	case []interface{}:
		result := make([]interface{}, len(v))
		for i, item := range v {
			result[i] = DeepCopy(item)
		}
		return result

	case map[string]interface{}:
		result := make(map[string]interface{}, len(v))
		for key, val := range v {
			result[key] = DeepCopy(val)
		}
		return result

	case map[interface{}]interface{}:
		result := make(map[interface{}]interface{}, len(v))
		for key, val := range v {
			result[DeepCopy(key)] = DeepCopy(val)
		}
		return result

	default:
		// For other types, return as-is (shallow copy)
		return value
	}
}

// MapKeys returns all keys of a map as a slice
func MapKeys[K comparable, V any](m map[K]V) []K {
	keys := make([]K, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// MapValues returns all values of a map as a slice
func MapValues[K comparable, V any](m map[K]V) []V {
	values := make([]V, 0, len(m))
	for _, v := range m {
		values = append(values, v)
	}
	return values
}

// ReverseSlice reverses a slice in place
func ReverseSlice[T any](slice []T) {
	for i, j := 0, len(slice)-1; i < j; i, j = i+1, j-1 {
		slice[i], slice[j] = slice[j], slice[i]
	}
}

// FilterSlice filters a slice based on a predicate function
func FilterSlice[T any](slice []T, predicate func(T) bool) []T {
	result := make([]T, 0, len(slice))
	for _, v := range slice {
		if predicate(v) {
			result = append(result, v)
		}
	}
	return result
}

// MapSlice transforms each element of a slice using a function
func MapSlice[T, U any](slice []T, transform func(T) U) []U {
	result := make([]U, len(slice))
	for i, v := range slice {
		result[i] = transform(v)
	}
	return result
}
