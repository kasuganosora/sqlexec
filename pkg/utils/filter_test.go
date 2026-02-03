package utils

import (
	"fmt"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"testing"
)

func TestApplyFilters(t *testing.T) {
	tests := []struct {
		name     string
		rows     []domain.Row
		filters  []domain.Filter
		expected int
		wantErr  bool
	}{
		{
			name: "单个过滤器",
			rows: []domain.Row{
				{"name": "Alice", "age": 25},
				{"name": "Bob", "age": 30},
				{"name": "Charlie", "age": 35},
			},
			filters: []domain.Filter{
				{Field: "age", Value: 30, Operator: "="},
			},
			expected: 1,
			wantErr:  false,
		},
		{
			name: "多个过滤器AND",
			rows: []domain.Row{
				{"name": "Alice", "age": 25},
				{"name": "Bob", "age": 30},
				{"name": "Charlie", "age": 25},
			},
			filters: []domain.Filter{
				{Field: "age", Value: 25, Operator: "="},
				{Field: "name", Value: "Alice", Operator: "="},
			},
			expected: 1,
			wantErr:  false,
		},
		{
			name: "无匹配结果",
			rows: []domain.Row{
				{"name": "Alice", "age": 25},
				{"name": "Bob", "age": 30},
			},
			filters: []domain.Filter{
				{Field: "age", Value: 40, Operator: "="},
			},
			expected: 0,
			wantErr:  false,
		},
		{
			name:    "空行",
			rows:    []domain.Row{},
			filters: []domain.Filter{{Field: "age", Value: 30, Operator: "="}},
			expected: 0,
			wantErr:  false,
		},
		{
			name: "空过滤器",
			rows: []domain.Row{
				{"name": "Alice", "age": 25},
			},
			filters:  []domain.Filter{},
			expected: 1,
			wantErr:  false,
		},
		{
			name: "多个过滤器OR",
			rows: []domain.Row{
				{"name": "Alice", "age": 25},
				{"name": "Bob", "age": 30},
				{"name": "Charlie", "age": 35},
			},
			filters: []domain.Filter{
				{
					LogicOp: "OR",
					SubFilters: []domain.Filter{
						{Field: "age", Value: 25, Operator: "="},
						{Field: "age", Value: 35, Operator: "="},
					},
				},
			},
			expected: 2,
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ApplyFilters(tt.rows, tt.filters)
			if (err != nil) != tt.wantErr {
				t.Errorf("ApplyFilters() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if len(result) != tt.expected {
				t.Errorf("ApplyFilters() result length = %d, want %d", len(result), tt.expected)
			}
		})
	}
}

func TestMatchesFilter(t *testing.T) {
	tests := []struct {
		name     string
		row      domain.Row
		filter   domain.Filter
		expected bool
		wantErr  bool
	}{
		{
			name:     "简单等值匹配",
			row:      domain.Row{"name": "Alice", "age": 25},
			filter:   domain.Filter{Field: "name", Value: "Alice", Operator: "="},
			expected: true,
			wantErr:  false,
		},
		{
			name:     "简单等值不匹配",
			row:      domain.Row{"name": "Alice", "age": 25},
			filter:   domain.Filter{Field: "name", Value: "Bob", Operator: "="},
			expected: false,
			wantErr:  false,
		},
		{
			name:     "字段不存在",
			row:      domain.Row{"name": "Alice"},
			filter:   domain.Filter{Field: "age", Value: 25, Operator: "="},
			expected: false,
			wantErr:  false,
		},
		{
			name: "OR逻辑匹配",
			row:  domain.Row{"name": "Alice", "age": 25},
			filter: domain.Filter{
				LogicOp: "OR",
				SubFilters: []domain.Filter{
					{Field: "name", Value: "Alice", Operator: "="},
					{Field: "age", Value: 30, Operator: "="},
				},
			},
			expected: true,
			wantErr:  false,
		},
		{
			name: "OR逻辑不匹配",
			row:  domain.Row{"name": "Charlie", "age": 25},
			filter: domain.Filter{
				LogicOp: "OR",
				SubFilters: []domain.Filter{
					{Field: "name", Value: "Alice", Operator: "="},
					{Field: "age", Value: 30, Operator: "="},
				},
			},
			expected: false,
			wantErr:  false,
		},
		{
			name: "AND逻辑匹配",
			row:  domain.Row{"name": "Alice", "age": 25},
			filter: domain.Filter{
				LogicOp: "AND",
				SubFilters: []domain.Filter{
					{Field: "name", Value: "Alice", Operator: "="},
					{Field: "age", Value: 25, Operator: "="},
				},
			},
			expected: true,
			wantErr:  false,
		},
		{
			name: "AND逻辑不匹配",
			row:  domain.Row{"name": "Alice", "age": 25},
			filter: domain.Filter{
				LogicOp: "AND",
				SubFilters: []domain.Filter{
					{Field: "name", Value: "Alice", Operator: "="},
					{Field: "age", Value: 30, Operator: "="},
				},
			},
			expected: false,
			wantErr:  false,
		},
		{
			name:     "大于比较",
			row:      domain.Row{"age": 30},
			filter:   domain.Filter{Field: "age", Value: 25, Operator: ">"},
			expected: true,
			wantErr:  false,
		},
		{
			name:     "小于比较",
			row:      domain.Row{"age": 20},
			filter:   domain.Filter{Field: "age", Value: 25, Operator: "<"},
			expected: true,
			wantErr:  false,
		},
		{
			name:     "LIKE匹配",
			row:      domain.Row{"name": "Alice"},
			filter:   domain.Filter{Field: "name", Value: "Ali%", Operator: "LIKE"},
			expected: true,
			wantErr:  false,
		},
		{
			name:     "IN操作",
			row:      domain.Row{"age": 25},
			filter:   domain.Filter{Field: "age", Value: []interface{}{20, 25, 30}, Operator: "IN"},
			expected: true,
			wantErr:  false,
		},
		{
			name:     "BETWEEN操作",
			row:      domain.Row{"age": 25},
			filter:   domain.Filter{Field: "age", Value: []interface{}{20, 30}, Operator: "BETWEEN"},
			expected: true,
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := MatchesFilter(tt.row, tt.filter)
			if (err != nil) != tt.wantErr {
				t.Errorf("MatchesFilter() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if result != tt.expected {
				t.Errorf("MatchesFilter() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestMatchesAnySubFilter(t *testing.T) {
	tests := []struct {
		name       string
		row        domain.Row
		subFilters []domain.Filter
		expected   bool
	}{
		{
			name:       "空子过滤器",
			row:        domain.Row{"name": "Alice"},
			subFilters: []domain.Filter{},
			expected:   true,
		},
		{
			name: "匹配第一个",
			row:  domain.Row{"name": "Alice"},
			subFilters: []domain.Filter{
				{Field: "name", Value: "Alice", Operator: "="},
				{Field: "name", Value: "Bob", Operator: "="},
			},
			expected: true,
		},
		{
			name: "匹配第二个",
			row:  domain.Row{"name": "Bob"},
			subFilters: []domain.Filter{
				{Field: "name", Value: "Alice", Operator: "="},
				{Field: "name", Value: "Bob", Operator: "="},
			},
			expected: true,
		},
		{
			name: "不匹配任何",
			row:  domain.Row{"name": "Charlie"},
			subFilters: []domain.Filter{
				{Field: "name", Value: "Alice", Operator: "="},
				{Field: "name", Value: "Bob", Operator: "="},
			},
			expected: false,
		},
		{
			name: "多个匹配",
			row:  domain.Row{"name": "Alice", "age": 25},
			subFilters: []domain.Filter{
				{Field: "name", Value: "Alice", Operator: "="},
				{Field: "age", Value: 25, Operator: "="},
			},
			expected: true,
		},
		{
			name: "嵌套OR",
			row:  domain.Row{"name": "Alice", "age": 25},
			subFilters: []domain.Filter{
				{
					LogicOp: "OR",
					SubFilters: []domain.Filter{
						{Field: "name", Value: "Alice", Operator: "="},
						{Field: "name", Value: "Bob", Operator: "="},
					},
				},
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := MatchesAnySubFilter(tt.row, tt.subFilters)
			if result != tt.expected {
				t.Errorf("MatchesAnySubFilter() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestMatchesAllSubFilters(t *testing.T) {
	tests := []struct {
		name       string
		row        domain.Row
		subFilters []domain.Filter
		expected   bool
	}{
		{
			name:       "空子过滤器",
			row:        domain.Row{"name": "Alice"},
			subFilters: []domain.Filter{},
			expected:   true,
		},
		{
			name: "全部匹配",
			row:  domain.Row{"name": "Alice", "age": 25},
			subFilters: []domain.Filter{
				{Field: "name", Value: "Alice", Operator: "="},
				{Field: "age", Value: 25, Operator: "="},
			},
			expected: true,
		},
		{
			name: "部分不匹配",
			row:  domain.Row{"name": "Alice", "age": 25},
			subFilters: []domain.Filter{
				{Field: "name", Value: "Alice", Operator: "="},
				{Field: "age", Value: 30, Operator: "="},
			},
			expected: false,
		},
		{
			name: "全部不匹配",
			row:  domain.Row{"name": "Charlie", "age": 25},
			subFilters: []domain.Filter{
				{Field: "name", Value: "Alice", Operator: "="},
				{Field: "name", Value: "Bob", Operator: "="},
			},
			expected: false,
		},
		{
			name: "嵌套AND",
			row:  domain.Row{"name": "Alice", "age": 25, "city": "NYC"},
			subFilters: []domain.Filter{
				{
					LogicOp: "AND",
					SubFilters: []domain.Filter{
						{Field: "name", Value: "Alice", Operator: "="},
						{Field: "age", Value: 25, Operator: "="},
					},
				},
				{Field: "city", Value: "NYC", Operator: "="},
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := MatchesAllSubFilters(tt.row, tt.subFilters)
			if result != tt.expected {
				t.Errorf("MatchesAllSubFilters() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestMatchesLike(t *testing.T) {
	tests := []struct {
		name     string
		value    string
		pattern  string
		expected bool
	}{
		{"匹配全部", "hello world", "%", true},
		{"精确匹配", "hello", "hello", true},
		{"前缀通配符", "hello world", "%world", true},
		{"后缀通配符", "hello world", "hello%", true},
		{"中间通配符", "hello world", "hel%rld", false},
		{"不匹配", "hello", "world", false},
		{"空值", "", "", true},
		{"空模式", "hello", "", false},
		{"单字符通配符", "hello", "he_lo", false},
		{"单字符前缀", "hello", "_ello", false},
		{"单字符后缀", "hello", "hell_", false},
		{"多个%", "hello world", "%ell%", true},
		{"匹配空字符串", "", "%", true},
		{"空字符串匹配", "", "", true},
		{"长模式", "hello world this is a long string", "hello%", true},
		{"特殊字符", "!@#$%", "!@#", false},
		{"Unicode", "你好世界", "你%", true},
		{"数字", "12345", "12%", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := MatchesLike(tt.value, tt.pattern)
			if result != tt.expected {
				t.Errorf("MatchesLike(%q, %q) = %v, want %v", tt.value, tt.pattern, result, tt.expected)
			}
		})
	}
}

func TestFilterEdgeCases(t *testing.T) {
	// 测试边界情况
	tests := []struct {
		name     string
		row      domain.Row
		filter   domain.Filter
		expected bool
	}{
		{
			name:     "nil值字段",
			row:      domain.Row{"age": nil},
			filter:   domain.Filter{Field: "age", Value: 25, Operator: "="},
			expected: false,
		},
		{
			name:     "nil值比较",
			row:      domain.Row{"age": nil},
			filter:   domain.Filter{Field: "age", Value: nil, Operator: "="},
			expected: true,
		},
		{
			name:     "字符串nil值",
			row:      domain.Row{"name": nil},
			filter:   domain.Filter{Field: "name", Value: "Alice", Operator: "="},
			expected: false,
		},
		{
			name:     "空字符串匹配",
			row:      domain.Row{"name": ""},
			filter:   domain.Filter{Field: "name", Value: "", Operator: "="},
			expected: true,
		},
		{
			name:     "零值匹配",
			row:      domain.Row{"age": 0},
			filter:   domain.Filter{Field: "age", Value: 0, Operator: "="},
			expected: true,
		},
		{
			name:     "负值匹配",
			row:      domain.Row{"age": -10},
			filter:   domain.Filter{Field: "age", Value: -10, Operator: "="},
			expected: true,
		},
		{
			name:     "大数值匹配",
			row:      domain.Row{"age": 9223372036854775807},
			filter:   domain.Filter{Field: "age", Value: 9223372036854775807, Operator: "="},
			expected: true,
		},
		{
			name:     "浮点数匹配",
			row:      domain.Row{"price": 10.5},
			filter:   domain.Filter{Field: "price", Value: 10.5, Operator: "="},
			expected: true,
		},
		{
			name:     "布尔值匹配",
			row:      domain.Row{"active": true},
			filter:   domain.Filter{Field: "active", Value: true, Operator: "="},
			expected: true,
		},
		{
			name:     "不支持的类型",
			row:      domain.Row{"data": map[string]int{}},
			filter:   domain.Filter{Field: "data", Value: "test", Operator: "="},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, _ := MatchesFilter(tt.row, tt.filter)
			if result != tt.expected {
				t.Errorf("MatchesFilter() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestFilterComplexScenarios(t *testing.T) {
	// 复杂场景测试
	tests := []struct {
		name     string
		rows     []domain.Row
		filters  []domain.Filter
		expected int
	}{
		{
			name: "OR和AND混合",
			rows: []domain.Row{
				{"name": "Alice", "age": 25, "city": "NYC"},
				{"name": "Bob", "age": 30, "city": "LA"},
				{"name": "Charlie", "age": 25, "city": "LA"},
			},
			filters: []domain.Filter{
				{
					LogicOp: "AND",
					SubFilters: []domain.Filter{
						{
							LogicOp: "OR",
							SubFilters: []domain.Filter{
								{Field: "name", Value: "Alice", Operator: "="},
								{Field: "name", Value: "Bob", Operator: "="},
							},
						},
						{Field: "age", Value: 25, Operator: ">"},
					},
				},
			},
			expected: 1,
		},
		{
			name: "多层嵌套",
			rows: []domain.Row{
				{"name": "Alice", "age": 25, "city": "NYC"},
				{"name": "Bob", "age": 30, "city": "LA"},
				{"name": "Charlie", "age": 25, "city": "NYC"},
			},
			filters: []domain.Filter{
				{
					LogicOp: "OR",
					SubFilters: []domain.Filter{
						{
							LogicOp: "AND",
							SubFilters: []domain.Filter{
								{Field: "name", Value: "Alice", Operator: "="},
								{Field: "city", Value: "NYC", Operator: "="},
							},
						},
						{
							LogicOp: "AND",
							SubFilters: []domain.Filter{
								{Field: "age", Value: 30, Operator: "="},
								{Field: "city", Value: "LA", Operator: "="},
							},
						},
					},
				},
			},
			expected: 2,
		},
		{
			name: "多个过滤器依次应用",
			rows: []domain.Row{
				{"name": "Alice", "age": 25, "city": "NYC"},
				{"name": "Bob", "age": 30, "city": "NYC"},
				{"name": "Charlie", "age": 35, "city": "NYC"},
			},
			filters: []domain.Filter{
				{Field: "city", Value: "NYC", Operator: "="},
				{Field: "age", Value: 30, Operator: "<="},
			},
			expected: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, _ := ApplyFilters(tt.rows, tt.filters)
			if len(result) != tt.expected {
				t.Errorf("ApplyFilters() result length = %d, want %d", len(result), tt.expected)
			}
		})
	}
}

func BenchmarkApplyFilters(b *testing.B) {
	rows := make([]domain.Row, 100)
	for i := 0; i < 100; i++ {
		rows[i] = domain.Row{"id": i, "name": "user"}
	}
	filters := []domain.Filter{{Field: "id", Value: 50, Operator: ">"}}

	for i := 0; i < b.N; i++ {
		ApplyFilters(rows, filters)
	}
}

func BenchmarkMatchesFilter(b *testing.B) {
	row := domain.Row{"name": "Alice", "age": 25}
	filter := domain.Filter{Field: "name", Value: "Alice", Operator: "="}

	for i := 0; i < b.N; i++ {
		MatchesFilter(row, filter)
	}
}

func BenchmarkMatchesLike(b *testing.B) {
	for i := 0; i < b.N; i++ {
		MatchesLike("hello world", "hel%")
	}
}

func ExampleApplyFilters() {
	rows := []domain.Row{
		{"name": "Alice", "age": 25},
		{"name": "Bob", "age": 30},
		{"name": "Charlie", "age": 35},
	}

	filters := []domain.Filter{
		{Field: "age", Value: 30, Operator: ">="},
	}

	result, _ := ApplyFilters(rows, filters)
	fmt.Println(len(result))
	// Output: 2
}

func ExampleMatchesLike() {
	result := MatchesLike("hello world", "%world")
	fmt.Println(result)

	result2 := MatchesLike("hello", "hel%")
	fmt.Println(result2)

	result3 := MatchesLike("hello", "%ll%")
	fmt.Println(result3)
	// Output:
	// true
	// true
	// false
}
