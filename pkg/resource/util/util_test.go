package util

import (
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// TestApplyFilters 测试ApplyFilters函数
func TestApplyFilters(t *testing.T) {
	tests := []struct {
		name     string
		rows     []domain.Row
		options  *domain.QueryOptions
		expected int
	}{
		{
			name: "nil options",
			rows: []domain.Row{
				{"id": 1, "name": "Alice"},
				{"id": 2, "name": "Bob"},
			},
			options:  nil,
			expected: 2,
		},
		{
			name: "empty filters",
			rows: []domain.Row{
				{"id": 1, "name": "Alice"},
				{"id": 2, "name": "Bob"},
			},
			options:  &domain.QueryOptions{},
			expected: 2,
		},
		{
			name: "with filters",
			rows: []domain.Row{
				{"id": 1, "name": "Alice", "age": int64(20)},
				{"id": 2, "name": "Bob", "age": int64(30)},
			},
			options: &domain.QueryOptions{
				Filters: []domain.Filter{
					{Field: "age", Operator: ">", Value: int64(25)},
				},
			},
			expected: 1,
		},
		{
			name: "no match",
			rows: []domain.Row{
				{"id": 1, "name": "Alice", "age": int64(20)},
				{"id": 2, "name": "Bob", "age": int64(30)},
			},
			options: &domain.QueryOptions{
				Filters: []domain.Filter{
					{Field: "age", Operator: "=", Value: int64(25)},
				},
			},
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ApplyFilters(tt.rows, tt.options)
			if len(result) != tt.expected {
				t.Errorf("ApplyFilters() returned %d rows, expected %d", len(result), tt.expected)
			}
		})
	}
}

// TestMatchesFilters 测试MatchesFilters函数
func TestMatchesFilters(t *testing.T) {
	tests := []struct {
		name     string
		row      domain.Row
		filters  []domain.Filter
		expected bool
	}{
		{
			name:     "empty filters",
			row:      domain.Row{"id": 1, "name": "Alice"},
			filters:  []domain.Filter{},
			expected: true,
		},
		{
			name:     "single filter match",
			row:      domain.Row{"id": 1, "name": "Alice", "age": int64(20)},
			filters:  []domain.Filter{{Field: "age", Operator: "=", Value: int64(20)}},
			expected: true,
		},
		{
			name:     "single filter no match",
			row:      domain.Row{"id": 1, "name": "Alice", "age": int64(20)},
			filters:  []domain.Filter{{Field: "age", Operator: "=", Value: int64(30)}},
			expected: false,
		},
		{
			name: "multiple filters all match",
			row:  domain.Row{"id": 1, "name": "Alice", "age": int64(20), "status": "active"},
			filters: []domain.Filter{
				{Field: "age", Operator: "=", Value: int64(20)},
				{Field: "status", Operator: "=", Value: "active"},
			},
			expected: true,
		},
		{
			name: "multiple filters partial match",
			row:  domain.Row{"id": 1, "name": "Alice", "age": int64(20), "status": "active"},
			filters: []domain.Filter{
				{Field: "age", Operator: "=", Value: int64(30)},
				{Field: "status", Operator: "=", Value: "active"},
			},
			expected: false,
		},
		{
			name: "OR logic filter",
			row:  domain.Row{"id": 1, "name": "Alice", "age": int64(20)},
			filters: []domain.Filter{
				{
					LogicOp: "OR",
					SubFilters: []domain.Filter{
						{Field: "age", Operator: "=", Value: int64(20)},
						{Field: "age", Operator: "=", Value: int64(30)},
					},
				},
			},
			expected: true,
		},
		{
			name: "AND logic filter",
			row:  domain.Row{"id": 1, "name": "Alice", "age": int64(20), "status": "active"},
			filters: []domain.Filter{
				{
					LogicOp: "AND",
					SubFilters: []domain.Filter{
						{Field: "age", Operator: ">", Value: int64(18)},
						{Field: "status", Operator: "=", Value: "active"},
					},
				},
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := MatchesFilters(tt.row, tt.filters)
			if result != tt.expected {
				t.Errorf("MatchesFilters() returned %v, expected %v", result, tt.expected)
			}
		})
	}
}

// TestMatchFilter 测试MatchFilter函数
func TestMatchFilter(t *testing.T) {
	tests := []struct {
		name     string
		row      domain.Row
		filter   domain.Filter
		expected bool
	}{
		{
			name:     "field not exists",
			row:      domain.Row{"id": 1, "name": "Alice"},
			filter:   domain.Filter{Field: "age", Operator: "=", Value: int64(20)},
			expected: false,
		},
		{
			name:     "equal operator match",
			row:      domain.Row{"age": int64(20)},
			filter:   domain.Filter{Field: "age", Operator: "=", Value: int64(20)},
			expected: true,
		},
		{
			name:     "equal operator no match",
			row:      domain.Row{"age": int64(20)},
			filter:   domain.Filter{Field: "age", Operator: "=", Value: int64(30)},
			expected: false,
		},
		{
			name:     "not equal operator",
			row:      domain.Row{"age": int64(20)},
			filter:   domain.Filter{Field: "age", Operator: "!=", Value: int64(30)},
			expected: true,
		},
		{
			name:     "greater than operator",
			row:      domain.Row{"age": int64(30)},
			filter:   domain.Filter{Field: "age", Operator: ">", Value: int64(20)},
			expected: true,
		},
		{
			name:     "less than operator",
			row:      domain.Row{"age": int64(20)},
			filter:   domain.Filter{Field: "age", Operator: "<", Value: int64(30)},
			expected: true,
		},
		{
			name:     "greater or equal operator",
			row:      domain.Row{"age": int64(20)},
			filter:   domain.Filter{Field: "age", Operator: ">=", Value: int64(20)},
			expected: true,
		},
		{
			name:     "less or equal operator",
			row:      domain.Row{"age": int64(20)},
			filter:   domain.Filter{Field: "age", Operator: "<=", Value: int64(20)},
			expected: true,
		},
		{
			name:     "LIKE operator match",
			row:      domain.Row{"name": "Alice"},
			filter:   domain.Filter{Field: "name", Operator: "LIKE", Value: "*Ali*"},
			expected: true,
		},
		{
			name:     "LIKE operator no match",
			row:      domain.Row{"name": "Bob"},
			filter:   domain.Filter{Field: "name", Operator: "LIKE", Value: "Ali"},
			expected: false,
		},
		{
			name:     "IN operator match",
			row:      domain.Row{"id": int64(2)},
			filter:   domain.Filter{Field: "id", Operator: "IN", Value: []interface{}{int64(1), int64(2), int64(3)}},
			expected: true,
		},
		{
			name:     "IN operator no match",
			row:      domain.Row{"id": int64(5)},
			filter:   domain.Filter{Field: "id", Operator: "IN", Value: []interface{}{int64(1), int64(2), int64(3)}},
			expected: false,
		},
		{
			name:     "BETWEEN operator match",
			row:      domain.Row{"age": int64(25)},
			filter:   domain.Filter{Field: "age", Operator: "BETWEEN", Value: []interface{}{int64(20), int64(30)}},
			expected: true,
		},
		{
			name:     "BETWEEN operator no match",
			row:      domain.Row{"age": int64(35)},
			filter:   domain.Filter{Field: "age", Operator: "BETWEEN", Value: []interface{}{int64(20), int64(30)}},
			expected: false,
		},
		{
			name:     "unknown operator",
			row:      domain.Row{"age": int64(20)},
			filter:   domain.Filter{Field: "age", Operator: "UNKNOWN", Value: int64(20)},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := MatchFilter(tt.row, tt.filter)
			if result != tt.expected {
				t.Errorf("MatchFilter() returned %v, expected %v", result, tt.expected)
			}
		})
	}
}

// TestApplyOrder 测试ApplyOrder函数
func TestApplyOrder(t *testing.T) {
	tests := []struct {
		name     string
		rows     []domain.Row
		options  *domain.QueryOptions
		validate func([]domain.Row)
	}{
		{
			name: "nil options",
			rows: []domain.Row{
				{"id": 2, "name": "Bob"},
				{"id": 1, "name": "Alice"},
			},
			options: nil,
			validate: func(result []domain.Row) {
				if len(result) != 2 {
					t.Errorf("Expected 2 rows, got %d", len(result))
				}
			},
		},
		{
			name: "empty order by",
			rows: []domain.Row{
				{"id": 2, "name": "Bob"},
				{"id": 1, "name": "Alice"},
			},
			options: &domain.QueryOptions{},
			validate: func(result []domain.Row) {
				if len(result) != 2 {
					t.Errorf("Expected 2 rows, got %d", len(result))
				}
			},
		},
		{
			name: "ASC order",
			rows: []domain.Row{
				{"id": int64(3), "name": "Charlie"},
				{"id": int64(1), "name": "Alice"},
				{"id": int64(2), "name": "Bob"},
			},
			options: &domain.QueryOptions{
				OrderBy: "id",
				Order:   "ASC",
			},
			validate: func(result []domain.Row) {
				if len(result) != 3 {
					t.Errorf("Expected 3 rows, got %d", len(result))
					return
				}
				if id, ok := result[0]["id"].(int64); !ok || id != 1 {
					t.Errorf("First row id should be 1, got %v", result[0]["id"])
				}
				if id, ok := result[2]["id"].(int64); !ok || id != 3 {
					t.Errorf("Last row id should be 3, got %v", result[2]["id"])
				}
			},
		},
		{
			name: "DESC order",
			rows: []domain.Row{
				{"id": int64(1), "name": "Alice"},
				{"id": int64(3), "name": "Charlie"},
				{"id": int64(2), "name": "Bob"},
			},
			options: &domain.QueryOptions{
				OrderBy: "id",
				Order:   "DESC",
			},
			validate: func(result []domain.Row) {
				if len(result) != 3 {
					t.Errorf("Expected 3 rows, got %d", len(result))
					return
				}
				if id, ok := result[0]["id"].(int64); !ok || id != 3 {
					t.Errorf("First row id should be 3, got %v", result[0]["id"])
				}
				if id, ok := result[2]["id"].(int64); !ok || id != 1 {
					t.Errorf("Last row id should be 1, got %v", result[2]["id"])
				}
			},
		},
		{
			name: "default ASC order",
			rows: []domain.Row{
				{"id": int64(3), "name": "Charlie"},
				{"id": int64(1), "name": "Alice"},
				{"id": int64(2), "name": "Bob"},
			},
			options: &domain.QueryOptions{
				OrderBy: "id",
			},
			validate: func(result []domain.Row) {
				if len(result) != 3 {
					t.Errorf("Expected 3 rows, got %d", len(result))
					return
				}
				if id, ok := result[0]["id"].(int64); !ok || id != 1 {
					t.Errorf("First row id should be 1 (default ASC), got %v", result[0]["id"])
				}
			},
		},
		{
			name: "string order",
			rows: []domain.Row{
				{"name": "Charlie", "id": int64(3)},
				{"name": "Alice", "id": int64(1)},
				{"name": "Bob", "id": int64(2)},
			},
			options: &domain.QueryOptions{
				OrderBy: "name",
				Order:   "ASC",
			},
			validate: func(result []domain.Row) {
				if len(result) != 3 {
					t.Errorf("Expected 3 rows, got %d", len(result))
					return
				}
				if name, ok := result[0]["name"].(string); !ok || name != "Alice" {
					t.Errorf("First row name should be Alice, got %v", result[0]["name"])
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ApplyOrder(tt.rows, tt.options)
			tt.validate(result)
		})
	}
}

// TestApplyPagination 测试ApplyPagination函数
func TestApplyPagination(t *testing.T) {
	tests := []struct {
		name     string
		rows     []domain.Row
		offset   int
		limit    int
		expected int
	}{
		{
			name:     "no pagination (limit 0)",
			rows:     []domain.Row{{"id": 1}, {"id": 2}, {"id": 3}},
			offset:   0,
			limit:    0,
			expected: 3,
		},
		{
			name:     "first page",
			rows:     []domain.Row{{"id": 1}, {"id": 2}, {"id": 3}, {"id": 4}, {"id": 5}},
			offset:   0,
			limit:    2,
			expected: 2,
		},
		{
			name:     "middle page",
			rows:     []domain.Row{{"id": 1}, {"id": 2}, {"id": 3}, {"id": 4}, {"id": 5}},
			offset:   2,
			limit:    2,
			expected: 2,
		},
		{
			name:     "last page partial",
			rows:     []domain.Row{{"id": 1}, {"id": 2}, {"id": 3}, {"id": 4}, {"id": 5}},
			offset:   4,
			limit:    10,
			expected: 1,
		},
		{
			name:     "offset beyond range",
			rows:     []domain.Row{{"id": 1}, {"id": 2}, {"id": 3}},
			offset:   10,
			limit:    10,
			expected: 0,
		},
		{
			name:     "negative offset",
			rows:     []domain.Row{{"id": 1}, {"id": 2}, {"id": 3}},
			offset:   -1,
			limit:    2,
			expected: 2,
		},
		{
			name:     "empty rows",
			rows:     []domain.Row{},
			offset:   0,
			limit:    10,
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ApplyPagination(tt.rows, tt.offset, tt.limit)
			if len(result) != tt.expected {
				t.Errorf("ApplyPagination() returned %d rows, expected %d", len(result), tt.expected)
			}
		})
	}
}

// TestPruneRows 测试PruneRows函数
func TestPruneRows(t *testing.T) {
	tests := []struct {
		name     string
		rows     []domain.Row
		columns  []string
		expected []domain.Row
	}{
		{
			name: "empty columns",
			rows: []domain.Row{
				{"id": 1, "name": "Alice", "age": int64(20)},
				{"id": 2, "name": "Bob", "age": int64(30)},
			},
			columns:  []string{},
			expected: []domain.Row{
				{"id": 1, "name": "Alice", "age": int64(20)},
				{"id": 2, "name": "Bob", "age": int64(30)},
			},
		},
		{
			name: "select one column",
			rows: []domain.Row{
				{"id": 1, "name": "Alice", "age": int64(20)},
				{"id": 2, "name": "Bob", "age": int64(30)},
			},
			columns: []string{"name"},
			expected: []domain.Row{
				{"name": "Alice"},
				{"name": "Bob"},
			},
		},
		{
			name: "select multiple columns",
			rows: []domain.Row{
				{"id": 1, "name": "Alice", "age": int64(20)},
				{"id": 2, "name": "Bob", "age": int64(30)},
			},
			columns: []string{"id", "name"},
			expected: []domain.Row{
				{"id": 1, "name": "Alice"},
				{"id": 2, "name": "Bob"},
			},
		},
		{
			name: "column not exists",
			rows: []domain.Row{
				{"id": 1, "name": "Alice"},
			},
			columns: []string{"id", "nonexistent"},
			expected: []domain.Row{
				{"id": 1},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := PruneRows(tt.rows, tt.columns)
			if len(result) != len(tt.expected) {
				t.Errorf("PruneRows() returned %d rows, expected %d", len(result), len(tt.expected))
				return
			}
			for i, row := range result {
				for col, val := range tt.expected[i] {
					if row[col] != val {
						t.Errorf("Row %d: column %s = %v, expected %v", i, col, row[col], val)
					}
				}
			}
		})
	}
}

// TestApplyQueryOperations 测试ApplyQueryOperations函数
func TestApplyQueryOperations(t *testing.T) {
	rows := []domain.Row{
		{"id": int64(1), "name": "Alice", "age": int64(20)},
		{"id": int64(2), "name": "Bob", "age": int64(30)},
		{"id": int64(3), "name": "Charlie", "age": int64(25)},
	}

	options := &domain.QueryOptions{
		Filters: []domain.Filter{
			{Field: "age", Operator: ">=", Value: int64(25)},
		},
		OrderBy: "age",
		Order:   "ASC",
		Limit:   1,
		Offset:  0,
	}

	result := ApplyQueryOperations(rows, options, nil)

	if len(result) != 1 {
		t.Errorf("ApplyQueryOperations() returned %d rows, expected 1", len(result))
	}

	if age, ok := result[0]["age"].(int64); !ok || age != 25 {
		t.Errorf("Expected age 25, got %v", result[0]["age"])
	}
}

// TestGetNeededColumns 测试GetNeededColumns函数
func TestGetNeededColumns(t *testing.T) {
	tests := []struct {
		name     string
		options  *domain.QueryOptions
		expected []string
	}{
		{
			name:     "nil options",
			options:  nil,
			expected: []string{},
		},
		{
			name:     "empty options",
			options:  &domain.QueryOptions{},
			expected: []string{},
		},
		{
			name: "with filters",
			options: &domain.QueryOptions{
				Filters: []domain.Filter{
					{Field: "age", Operator: ">", Value: int64(20)},
					{Field: "name", Operator: "=", Value: "Alice"},
				},
			},
			expected: []string{"age", "name"},
		},
		{
			name: "with order by",
			options: &domain.QueryOptions{
				OrderBy: "age",
			},
			expected: []string{"age"},
		},
		{
			name: "with select columns",
			options: &domain.QueryOptions{
				SelectColumns: []string{"id", "name"},
			},
			expected: []string{"id", "name"},
		},
		{
			name: "combined options",
			options: &domain.QueryOptions{
				Filters: []domain.Filter{
					{Field: "age", Operator: ">", Value: int64(20)},
				},
				OrderBy:       "name",
				SelectColumns: []string{"id", "name", "age"},
			},
			expected: []string{"age", "name", "id"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GetNeededColumns(tt.options)
			if len(result) != len(tt.expected) {
				t.Errorf("GetNeededColumns() returned %d columns, expected %d", len(result), len(tt.expected))
				return
			}
			for _, exp := range tt.expected {
				found := false
				for _, r := range result {
					if r == exp {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("Expected column %s not found in result", exp)
				}
			}
		})
	}
}

// TestMatchesAnySubFilter 测试MatchesAnySubFilter函数
func TestMatchesAnySubFilter(t *testing.T) {
	tests := []struct {
		name       string
		row        domain.Row
		subFilters []domain.Filter
		expected   bool
	}{
		{
			name:       "empty sub filters",
			row:        domain.Row{"id": 1},
			subFilters: []domain.Filter{},
			expected:   true,
		},
		{
			name: "one matches",
			row:  domain.Row{"age": int64(20)},
			subFilters: []domain.Filter{
				{Field: "age", Operator: "=", Value: int64(20)},
				{Field: "age", Operator: "=", Value: int64(30)},
			},
			expected: true,
		},
		{
			name: "none matches",
			row:  domain.Row{"age": int64(25)},
			subFilters: []domain.Filter{
				{Field: "age", Operator: "=", Value: int64(20)},
				{Field: "age", Operator: "=", Value: int64(30)},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := MatchesAnySubFilter(tt.row, tt.subFilters)
			if result != tt.expected {
				t.Errorf("MatchesAnySubFilter() returned %v, expected %v", result, tt.expected)
			}
		})
	}
}

// TestMatchesAllSubFilters 测试MatchesAllSubFilters函数
func TestMatchesAllSubFilters(t *testing.T) {
	tests := []struct {
		name       string
		row        domain.Row
		subFilters []domain.Filter
		expected   bool
	}{
		{
			name:       "empty sub filters",
			row:        domain.Row{"id": 1},
			subFilters: []domain.Filter{},
			expected:   true,
		},
		{
			name: "all match",
			row:  domain.Row{"age": int64(20), "name": "Alice"},
			subFilters: []domain.Filter{
				{Field: "age", Operator: "=", Value: int64(20)},
				{Field: "name", Operator: "=", Value: "Alice"},
			},
			expected: true,
		},
		{
			name: "one does not match",
			row:  domain.Row{"age": int64(20), "name": "Bob"},
			subFilters: []domain.Filter{
				{Field: "age", Operator: "=", Value: int64(20)},
				{Field: "name", Operator: "=", Value: "Alice"},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := MatchesAllSubFilters(tt.row, tt.subFilters)
			if result != tt.expected {
				t.Errorf("MatchesAllSubFilters() returned %v, expected %v", result, tt.expected)
			}
		})
	}
}
