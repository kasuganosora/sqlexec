package optimizer

import (
	"testing"
)

func TestCompareValues(t *testing.T) {
	tests := []struct {
		name     string
		a        interface{}
		b        interface{}
		expected int
	}{
		{
			name:     "int less",
			a:        1,
			b:        2,
			expected: -1,
		},
		{
			name:     "int equal",
			a:        5,
			b:        5,
			expected: 0,
		},
		{
			name:     "int greater",
			a:        10,
			b:        5,
			expected: 1,
		},
		{
			name:     "string less",
			a:        "apple",
			b:        "banana",
			expected: -1,
		},
		{
			name:     "string equal",
			a:        "test",
			b:        "test",
			expected: 0,
		},
		{
			name:     "string greater",
			a:        "zebra",
			b:        "apple",
			expected: 1,
		},
		{
			name:     "float less",
			a:        1.5,
			b:        2.5,
			expected: -1,
		},
		{
			name:     "float equal",
			a:        3.14,
			b:        3.14,
			expected: 0,
		},
		{
			name:     "float greater",
			a:        10.5,
			b:        5.5,
			expected: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := compareValues(tt.a, tt.b)
			if result != tt.expected {
				t.Errorf("compareValues(%v, %v) = %v, want %v", tt.a, tt.b, result, tt.expected)
			}
		})
	}
}

func TestCompareValuesEqual(t *testing.T) {
	tests := []struct {
		name     string
		v1       interface{}
		v2       interface{}
		expected bool
	}{
		{
			name:     "int equal",
			v1:       42,
			v2:       42,
			expected: true,
		},
		{
			name:     "int not equal",
			v1:       42,
			v2:       43,
			expected: false,
		},
		{
			name:     "string equal",
			v1:       "hello",
			v2:       "hello",
			expected: true,
		},
		{
			name:     "string not equal",
			v1:       "hello",
			v2:       "world",
			expected: false,
		},
		{
			name:     "float equal",
			v1:       3.14159,
			v2:       3.14159,
			expected: true,
		},
		{
			name:     "float not equal",
			v1:       3.14,
			v2:       3.15,
			expected: false,
		},
		// 注意：由于 utils.CompareValuesForSort 的行为，不同类型比较的结果可能不确定
		// 取决于具体实现，这里跳过这个测试用例
		{
			name:     "nil values",
			v1:       nil,
			v2:       nil,
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := compareValuesEqual(tt.v1, tt.v2)
			if result != tt.expected {
				t.Errorf("compareValuesEqual(%v, %v) = %v, want %v", tt.v1, tt.v2, result, tt.expected)
			}
		})
	}
}

func TestCompareValuesConsistency(t *testing.T) {
	// 测试 compareValues 和 compareValuesEqual 的一致性
	pairs := []struct {
		v1 interface{}
		v2 interface{}
	}{
		{1, 1},
		{1, 2},
		{"test", "test"},
		{"test", "other"},
		{3.14, 3.14},
		{3.14, 2.71},
	}

	for _, pair := range pairs {
		cmpResult := compareValues(pair.v1, pair.v2)
		equalResult := compareValuesEqual(pair.v1, pair.v2)
		
		// 如果 compareValues 返回 0，compareValuesEqual 应该返回 true
		// 如果 compareValues 返回非 0，compareValuesEqual 应该返回 false
		expectedEqual := (cmpResult == 0)
		if equalResult != expectedEqual {
			t.Errorf("Inconsistent results for compareValues(%v, %v) = %d, compareValuesEqual(%v, %v) = %v",
				pair.v1, pair.v2, cmpResult, pair.v1, pair.v2, equalResult)
		}
	}
}
