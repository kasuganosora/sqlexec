package domain

import (
	"context"
	"testing"
)

// MockFilterableDataSource 模拟实现FilterableDataSource接口的数据源
type MockFilterableDataSourceForTest struct {
	DataSource
	supportsFiltering bool
}

func (m *MockFilterableDataSourceForTest) SupportsFiltering(tableName string) bool {
	return m.supportsFiltering
}

func (m *MockFilterableDataSourceForTest) Filter(
	ctx context.Context,
	tableName string,
	filter Filter,
	offset, limit int,
) ([]Row, int64, error) {
	return []Row{}, 0, nil
}

// TestFilterableDataSource_Interface 测试FilterableDataSource接口定义
func TestFilterableDataSource_Interface(t *testing.T) {
	// 编译时检查：确保接口定义正确
	var _ FilterableDataSource = (*MockFilterableDataSourceForTest)(nil)

	// 测试接口方法可以被调用
	mockDS := &MockFilterableDataSourceForTest{supportsFiltering: true}

	// 测试 SupportsFiltering 方法
	if !mockDS.SupportsFiltering("test_table") {
		t.Errorf("SupportsFiltering should return true")
	}

	// 测试 Filter 方法
	ctx := context.Background()
	filter := Filter{
		Field:    "age",
		Operator: ">",
		Value:    18,
	}
	rows, total, err := mockDS.Filter(ctx, "users", filter, 0, 10)
	if err != nil {
		t.Errorf("Filter() returned unexpected error: %v", err)
	}
	if rows == nil {
		t.Errorf("Filter() should return non-nil rows slice")
	}
	if total < 0 {
		t.Errorf("Filter() should return non-negative total")
	}
}

// TestFilterableDataSource_SupportsFiltering 测试SupportsFiltering方法的各种场景
func TestFilterableDataSource_SupportsFiltering(t *testing.T) {
	tests := []struct {
		name             string
		supportsFiltering bool
		tableName        string
		expected         bool
	}{
		{
			name:             "支持过滤的表",
			supportsFiltering: true,
			tableName:        "users",
			expected:         true,
		},
		{
			name:             "不支持过滤的表",
			supportsFiltering: false,
			tableName:        "logs",
			expected:         false,
		},
		{
			name:             "空表名",
			supportsFiltering: true,
			tableName:        "",
			expected:         true, // 由实现决定，这里测试可以调用
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockDS := &MockFilterableDataSourceForTest{
				supportsFiltering: tt.supportsFiltering,
			}

			got := mockDS.SupportsFiltering(tt.tableName)
			if got != tt.expected {
				t.Errorf("SupportsFiltering() = %v, want %v", got, tt.expected)
			}
		})
	}
}

// TestFilterableDataSource_Filter_Simple 测试简单过滤
func TestFilterableDataSource_Filter_Simple(t *testing.T) {
	ctx := context.Background()
	mockDS := &MockFilterableDataSourceForTest{supportsFiltering: true}

	filter := Filter{
		Field:    "age",
		Operator: ">",
		Value:    18,
	}

	rows, total, err := mockDS.Filter(ctx, "users", filter, 0, 10)
	if err != nil {
		t.Errorf("Filter() returned unexpected error: %v", err)
	}
	if rows == nil {
		t.Errorf("Filter() should return non-nil rows")
	}
	if total < 0 {
		t.Errorf("Filter() should return non-negative total, got %d", total)
	}
}

// TestFilterableDataSource_Filter_Nested 测试嵌套过滤
func TestFilterableDataSource_Filter_Nested(t *testing.T) {
	ctx := context.Background()
	mockDS := &MockFilterableDataSourceForTest{supportsFiltering: true}

	// 创建嵌套过滤器 (AND)
	filter := Filter{
		Logic: "AND",
		Value: []Filter{
			{Field: "age", Operator: ">", Value: 18},
			{Field: "age", Operator: "<", Value: 65},
		},
	}

	rows, total, err := mockDS.Filter(ctx, "users", filter, 0, 0)
	if err != nil {
		t.Errorf("Filter() with nested filters returned unexpected error: %v", err)
	}
	if rows == nil {
		t.Errorf("Filter() with nested filters should return non-nil rows")
	}
	if total < 0 {
		t.Errorf("Filter() with nested filters should return non-negative total, got %d", total)
	}
}

// TestFilterableDataSource_Filter_Pagination 测试分页参数
func TestFilterableDataSource_Filter_Pagination(t *testing.T) {
	ctx := context.Background()
	mockDS := &MockFilterableDataSourceForTest{supportsFiltering: true}

	filter := Filter{}

	tests := []struct {
		name   string
		offset int
		limit  int
	}{
		{
			name:   "no pagination",
			offset: 0,
			limit:  0,
		},
		{
			name:   "with offset",
			offset: 10,
			limit:  0,
		},
		{
			name:   "with limit",
			offset: 0,
			limit:  10,
		},
		{
			name:   "with both offset and limit",
			offset: 20,
			limit:  10,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows, total, err := mockDS.Filter(ctx, "users", filter, tt.offset, tt.limit)
			if err != nil {
				t.Errorf("Filter() with offset=%d, limit=%d returned unexpected error: %v", tt.offset, tt.limit, err)
			}
			if rows == nil {
				t.Errorf("Filter() should return non-nil rows")
			}
			if total < 0 {
				t.Errorf("Filter() should return non-negative total, got %d", total)
			}
		})
	}
}

// TestFilterableDataSource_Filter_LogicTypes 测试不同的逻辑类型
func TestFilterableDataSource_Filter_LogicTypes(t *testing.T) {
	ctx := context.Background()
	mockDS := &MockFilterableDataSourceForTest{supportsFiltering: true}

	tests := []struct {
		name  string
		logic string
	}{
		{
			name:  "AND logic",
			logic: "AND",
		},
		{
			name:  "OR logic",
			logic: "OR",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter := Filter{
				Logic: tt.logic,
				Value: []Filter{
					{Field: "status", Operator: "=", Value: "active"},
					{Field: "verified", Operator: "=", Value: true},
				},
			}

			rows, total, err := mockDS.Filter(ctx, "users", filter, 0, 10)
			if err != nil {
				t.Errorf("Filter() with logic=%s returned unexpected error: %v", tt.logic, err)
			}
			if rows == nil {
				t.Errorf("Filter() should return non-nil rows")
			}
			if total < 0 {
				t.Errorf("Filter() should return non-negative total, got %d", total)
			}
		})
	}
}

// TestFilterableDataSource_Filter_Operators 测试不同的操作符
func TestFilterableDataSource_Filter_Operators(t *testing.T) {
	ctx := context.Background()
	mockDS := &MockFilterableDataSourceForTest{supportsFiltering: true}

	operators := []string{"=", "!=", ">", ">=", "<", "<=", "LIKE", "IN"}

	for _, op := range operators {
		t.Run("operator_"+op, func(t *testing.T) {
			filter := Filter{
				Field:    "value",
				Operator: op,
				Value:    100,
			}

			rows, total, err := mockDS.Filter(ctx, "items", filter, 0, 10)
			if err != nil {
				t.Errorf("Filter() with operator=%s returned unexpected error: %v", op, err)
			}
			if rows == nil {
				t.Errorf("Filter() should return non-nil rows")
			}
			if total < 0 {
				t.Errorf("Filter() should return non-negative total, got %d", total)
			}
		})
	}
}
