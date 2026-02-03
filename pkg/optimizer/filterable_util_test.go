package optimizer

import (
	"context"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// MockFilterableDataSource 模拟实现FilterableDataSource接口的数据源
type MockFilterableDataSource struct {
	domain.DataSource
	implFilterable bool
}

func (m *MockFilterableDataSource) SupportsFiltering(tableName string) bool {
	return m.implFilterable
}

func (m *MockFilterableDataSource) Filter(
	ctx context.Context,
	tableName string,
	filter domain.Filter,
	offset, limit int,
) ([]domain.Row, int64, error) {
	return []domain.Row{}, 0, nil
}

// MockBasicDataSource 只实现基础DataSource接口
type MockBasicDataSource struct {
	domain.DataSource
}

// TestIsFilterable 测试IsFilterable函数
func TestIsFilterable(t *testing.T) {
	tests := []struct {
		name     string
		ds       domain.DataSource
		expected bool
	}{
		{
			name:     "数据源支持过滤",
			ds:       &MockFilterableDataSource{implFilterable: true},
			expected: true,
		},
		{
			name:     "数据源不支持过滤",
			ds:       &MockBasicDataSource{},
			expected: false,
		},
		{
			name:     "nil数据源",
			ds:       nil,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsFilterable(tt.ds)
			if got != tt.expected {
				t.Errorf("IsFilterable() = %v, want %v", got, tt.expected)
			}
		})
	}
}

// TestFilterableDataSource_InterfaceCompliance 测试MockFilterableDataSource接口合规性
func TestFilterableDataSource_InterfaceCompliance(t *testing.T) {
	// 编译时检查：确保MockFilterableDataSource实现了FilterableDataSource接口
	var _ domain.FilterableDataSource = (*MockFilterableDataSource)(nil)

	// 运行时检查
	mockDS := &MockFilterableDataSource{implFilterable: true}

	// 测试 SupportsFiltering
	if !mockDS.SupportsFiltering("any_table") {
		t.Errorf("SupportsFiltering should return true")
	}

	// 测试 Filter 方法可以正确调用（不验证具体行为，只验证不会panic）
	ctx := context.Background()
	filter := domain.Filter{Field: "test", Operator: "=", Value: 1}
	_, _, err := mockDS.Filter(ctx, "test_table", filter, 0, 10)
	if err != nil {
		t.Errorf("Filter() returned unexpected error: %v", err)
	}
}

// BenchmarkIsFilterable 性能测试IsFilterable函数
func BenchmarkIsFilterable(b *testing.B) {
	filterableDS := &MockFilterableDataSource{implFilterable: true}
	basicDS := &MockBasicDataSource{}

	b.Run("filterable", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			IsFilterable(filterableDS)
		}
	})

	b.Run("not-filterable", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			IsFilterable(basicDS)
		}
	})
}
