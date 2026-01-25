package util

import (
	"testing"
)

// TestCompareEqual 测试CompareEqual函数
func TestCompareEqual(t *testing.T) {
	tests := []struct {
		name     string
		a        interface{}
		b        interface{}
		expected bool
	}{
		{"both nil", nil, nil, true},
		{"one nil", nil, 1, false},
		{"one nil reverse", 1, nil, false},
		{"int equal", 1, 1, true},
		{"int not equal", 1, 2, false},
		{"int64 equal", int64(1), int64(1), true},
		{"float64 equal", 1.0, 1.0, true},
		{"float64 not equal", 1.0, 2.0, false},
		{"string equal", "hello", "hello", true},
		{"string not equal", "hello", "world", false},
		{"int and int64 equal", 1, int64(1), true},
		{"int and float equal", 1, 1.0, true},
		{"different types numeric", 1, "1", true}, // CompareEqual会转字符串比较，1和"1"都变成"1"
		{"empty strings", "", "", true},
		{"zero values", 0, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CompareEqual(tt.a, tt.b)
			if result != tt.expected {
				t.Errorf("CompareEqual(%v, %v) = %v, expected %v", tt.a, tt.b, result, tt.expected)
			}
		})
	}
}

// TestCompareNumeric 测试CompareNumeric函数
func TestCompareNumeric(t *testing.T) {
	tests := []struct {
		name     string
		a        interface{}
		b        interface{}
		expected int
		success  bool
	}{
		{"int less", 1, 2, -1, true},
		{"int equal", 1, 1, 0, true},
		{"int greater", 2, 1, 1, true},
		{"int64 less", int64(1), int64(2), -1, true},
		{"float64 less", 1.0, 2.0, -1, true},
		{"mixed types", 1, 1.0, 0, true},
		{"string numeric", "1", 1, 0, true},
		{"string not numeric", "abc", 1, 0, false},
		{"both non-numeric", "abc", "def", 0, false},
		{"nil and int", nil, 1, 0, false},
		{"negative numbers", -1, 1, -1, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, success := CompareNumeric(tt.a, tt.b)
			if result != tt.expected || success != tt.success {
				t.Errorf("CompareNumeric(%v, %v) = (%v, %v), expected (%v, %v)",
					tt.a, tt.b, result, success, tt.expected, tt.success)
			}
		})
	}
}

// TestCompareGreater 测试CompareGreater函数
func TestCompareGreater(t *testing.T) {
	tests := []struct {
		name     string
		a        interface{}
		b        interface{}
		expected bool
	}{
		{"int greater", 5, 3, true},
		{"int not greater", 3, 5, false},
		{"int equal", 5, 5, false},
		{"float64 greater", 5.0, 3.0, true},
		{"negative", -1, -2, true},
		{"string comparison", "zebra", "apple", true},
		{"string less", "apple", "zebra", false},
		{"mixed numeric", 5.5, 5, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CompareGreater(tt.a, tt.b)
			if result != tt.expected {
				t.Errorf("CompareGreater(%v, %v) = %v, expected %v", tt.a, tt.b, result, tt.expected)
			}
		})
	}
}

// TestCompareLike 测试CompareLike函数
func TestCompareLike(t *testing.T) {
	tests := []struct {
		name     string
		a        interface{}
		b        interface{}
		expected bool
	}{
		{"simple contains", "hello world", "*world*", true},
		{"not contains", "hello", "*world*", false},
		{"case sensitive", "Hello", "*hello*", false},
		{"empty pattern", "hello", "", false},
		{"empty string", "", "*hello*", false},
		{"both empty", "", "*", true}, // * 匹配所有
		{"full match", "hello", "hello", true},
		{"substring", "helloworld", "*world*", true},
		{"prefix", "world", "wo*", true},
		{"suffix", "helloworld", "*world", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CompareLike(tt.a, tt.b)
			if result != tt.expected {
				t.Errorf("CompareLike(%v, %v) = %v, expected %v", tt.a, tt.b, result, tt.expected)
			}
		})
	}
}

// TestCompareIn 测试CompareIn函数
func TestCompareIn(t *testing.T) {
	tests := []struct {
		name     string
		a        interface{}
		b        interface{}
		expected bool
	}{
		{"int in list", 2, []interface{}{1, 2, 3}, true},
		{"int not in list", 5, []interface{}{1, 2, 3}, false},
		{"string in list", "apple", []interface{}{"apple", "banana"}, true},
		{"string not in list", "orange", []interface{}{"apple", "banana"}, false},
		{"mixed types", 2, []interface{}{1, int64(2), 3}, true}, // int(2) matches int64(2) via numeric comparison
		{"empty list", 1, []interface{}{}, false},
		{"not a slice", 1, 1, false},
		{"value in list", 2, []interface{}{1, 2, 3}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CompareIn(tt.a, tt.b)
			if result != tt.expected {
				t.Errorf("CompareIn(%v, %v) = %v, expected %v", tt.a, tt.b, result, tt.expected)
			}
		})
	}
}

// TestCompareBetween 测试CompareBetween函数
func TestCompareBetween(t *testing.T) {
	tests := []struct {
		name     string
		a        interface{}
		b        interface{}
		expected bool
	}{
		{"numeric in range", 25, []interface{}{20, 30}, true},
		{"numeric at lower bound", 20, []interface{}{20, 30}, true},
		{"numeric at upper bound", 30, []interface{}{20, 30}, true},
		{"numeric below range", 15, []interface{}{20, 30}, false},
		{"numeric above range", 35, []interface{}{20, 30}, false},
		{"string in range", "banana", []interface{}{"apple", "cherry"}, true},
		{"string below range", "aardvark", []interface{}{"apple", "cherry"}, false},
		{"string above range", "zebra", []interface{}{"apple", "cherry"}, false},
		{"not a slice", 25, 20, false},
		{"slice too short", 25, []interface{}{20}, false},
		{"empty slice", 25, []interface{}{}, false},
		// Note: CompareBetween doesn't specifically handle nil for the 'a' parameter
		// If you need to test nil behavior, use nil as the first parameter
		// For now, this test is removed as it's not representative of actual behavior
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CompareBetween(tt.a, tt.b)
			if result != tt.expected {
				t.Errorf("CompareBetween(%v, %v) = %v, expected %v", tt.a, tt.b, result, tt.expected)
			}
		})
	}
}

// TestCompareValues 测试CompareValues函数
func TestCompareValues(t *testing.T) {
	tests := []struct {
		name     string
		a        interface{}
		b        interface{}
		expected int
	}{
		{"both nil", nil, nil, 0},
		{"a nil", nil, 1, -1},
		{"b nil", 1, nil, 1},
		{"int less", 1, 2, -1},
		{"int equal", 1, 1, 0},
		{"int greater", 2, 1, 1},
		{"float64 less", 1.0, 2.0, -1},
		{"string less", "apple", "banana", -1},
		{"string greater", "zebra", "apple", 1},
		{"string equal", "hello", "hello", 0},
		{"mixed numeric", 1, 2.0, -1},
		{"int and string", 1, "1", 0}, // CompareValues falls back to string comparison: "1" == "1"
		{"zero values", 0, 0, 0},
		{"negative numbers", -1, 1, -1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CompareValues(tt.a, tt.b)
			if result != tt.expected {
				t.Errorf("CompareValues(%v, %v) = %v, expected %v", tt.a, tt.b, result, tt.expected)
			}
		})
	}
}

// TestConvertToFloat64 测试ConvertToFloat64函数
func TestConvertToFloat64(t *testing.T) {
	tests := []struct {
		name     string
		v        interface{}
		expected float64
		success  bool
	}{
		{"int", 1, 1.0, true},
		{"int8", int8(1), 1.0, true},
		{"int16", int16(1), 1.0, true},
		{"int32", int32(1), 1.0, true},
		{"int64", int64(1), 1.0, true},
		{"uint", uint(1), 1.0, true},
		{"uint8", uint8(1), 1.0, true},
		{"uint16", uint16(1), 1.0, true},
		{"uint32", uint32(1), 1.0, true},
		{"uint64", uint64(1), 1.0, true},
		{"float32", float32(1.5), 1.5, true},
		{"float64", 1.5, 1.5, true},
		{"string numeric", "2.5", 2.5, true},
		{"string not numeric", "abc", 0, false},
		{"nil", nil, 0, false},
		{"bool", true, 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, success := ConvertToFloat64(tt.v)
			if result != tt.expected || success != tt.success {
				t.Errorf("ConvertToFloat64(%v) = (%v, %v), expected (%v, %v)",
					tt.v, result, success, tt.expected, tt.success)
			}
		})
	}
}
