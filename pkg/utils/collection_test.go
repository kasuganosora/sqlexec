package utils

import (
	"reflect"
	"sort"
	"testing"
)

func TestSortedStringKeys(t *testing.T) {
	m := map[string]int{
		"c": 3,
		"a": 1,
		"b": 2,
	}

	keys := SortedStringKeys(m)
	expected := []string{"a", "b", "c"}

	if !reflect.DeepEqual(keys, expected) {
		t.Errorf("SortedStringKeys() = %v, want %v", keys, expected)
	}
}

func TestUniqueStrings(t *testing.T) {
	tests := []struct {
		input    []string
		expected []string
	}{
		{[]string{"a", "b", "a", "c", "b"}, []string{"a", "b", "c"}},
		{[]string{"a", "a", "a"}, []string{"a"}},
		{[]string{}, []string{}},
		{[]string{"x"}, []string{"x"}},
	}

	for _, tt := range tests {
		result := UniqueStrings(tt.input)
		if !reflect.DeepEqual(result, tt.expected) {
			t.Errorf("UniqueStrings(%v) = %v, want %v", tt.input, result, tt.expected)
		}
	}
}

func TestUniqueInts(t *testing.T) {
	tests := []struct {
		input    []int
		expected []int
	}{
		{[]int{1, 2, 1, 3, 2}, []int{1, 2, 3}},
		{[]int{5, 5, 5}, []int{5}},
		{[]int{}, []int{}},
	}

	for _, tt := range tests {
		result := UniqueInts(tt.input)
		if !reflect.DeepEqual(result, tt.expected) {
			t.Errorf("UniqueInts(%v) = %v, want %v", tt.input, result, tt.expected)
		}
	}
}

func TestContainsSlice(t *testing.T) {
	tests := []struct {
		slice    []int
		item     int
		expected bool
	}{
		{[]int{1, 2, 3}, 2, true},
		{[]int{1, 2, 3}, 4, false},
		{[]int{}, 1, false},
		{[]int{5}, 5, true},
	}

	for _, tt := range tests {
		result := ContainsSlice(tt.slice, tt.item)
		if result != tt.expected {
			t.Errorf("ContainsSlice(%v, %d) = %v, want %v", tt.slice, tt.item, result, tt.expected)
		}
	}
}

func TestDeepCopy(t *testing.T) {
	// Test primitive types
	if DeepCopy(42) != 42 {
		t.Error("DeepCopy failed for int")
	}
	if DeepCopy("hello") != "hello" {
		t.Error("DeepCopy failed for string")
	}

	// Test slice
	slice := []interface{}{1, "two", 3.0}
	copied := DeepCopy(slice).([]interface{})
	if !reflect.DeepEqual(slice, copied) {
		t.Error("DeepCopy failed for slice")
	}

	// Test map
	m := map[string]interface{}{"a": 1, "b": "two"}
	copiedMap := DeepCopy(m).(map[string]interface{})
	if !reflect.DeepEqual(m, copiedMap) {
		t.Error("DeepCopy failed for map")
	}
}

func TestMapKeys(t *testing.T) {
	m := map[string]int{"a": 1, "b": 2, "c": 3}
	keys := MapKeys(m)

	if len(keys) != 3 {
		t.Errorf("MapKeys() returned %d keys, want 3", len(keys))
	}

	sort.Strings(keys)
	expected := []string{"a", "b", "c"}
	if !reflect.DeepEqual(keys, expected) {
		t.Errorf("MapKeys() = %v, want %v", keys, expected)
	}
}

func TestMapValues(t *testing.T) {
	m := map[string]int{"a": 1, "b": 2}
	values := MapValues(m)

	if len(values) != 2 {
		t.Errorf("MapValues() returned %d values, want 2", len(values))
	}

	sort.Ints(values)
	expected := []int{1, 2}
	if !reflect.DeepEqual(values, expected) {
		t.Errorf("MapValues() = %v, want %v", values, expected)
	}
}

func TestReverseSlice(t *testing.T) {
	tests := []struct {
		input    []int
		expected []int
	}{
		{[]int{1, 2, 3}, []int{3, 2, 1}},
		{[]int{1}, []int{1}},
		{[]int{}, []int{}},
	}

	for _, tt := range tests {
		// Make a copy since ReverseSlice modifies in place
		slice := make([]int, len(tt.input))
		copy(slice, tt.input)
		ReverseSlice(slice)

		if !reflect.DeepEqual(slice, tt.expected) {
			t.Errorf("ReverseSlice(%v) = %v, want %v", tt.input, slice, tt.expected)
		}
	}
}

func TestFilterSlice(t *testing.T) {
	input := []int{1, 2, 3, 4, 5}
	result := FilterSlice(input, func(v int) bool {
		return v%2 == 0
	})
	expected := []int{2, 4}

	if !reflect.DeepEqual(result, expected) {
		t.Errorf("FilterSlice() = %v, want %v", result, expected)
	}
}

func TestMapSlice(t *testing.T) {
	input := []int{1, 2, 3}
	result := MapSlice(input, func(v int) int {
		return v * 2
	})
	expected := []int{2, 4, 6}

	if !reflect.DeepEqual(result, expected) {
		t.Errorf("MapSlice() = %v, want %v", result, expected)
	}
}
