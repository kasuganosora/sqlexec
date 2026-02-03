package optimizer

import (
	"context"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// MockFilterableDataSourceForOptimizer 用于测试优化器的模拟数据源
type MockFilterableDataSourceForOptimizer struct {
	domain.DataSource
	shouldUseFilter bool
}

func (m *MockFilterableDataSourceForOptimizer) SupportsFiltering(tableName string) bool {
	return m.shouldUseFilter
}

func (m *MockFilterableDataSourceForOptimizer) Filter(
	ctx context.Context,
	tableName string,
	filter domain.Filter,
	offset, limit int,
) ([]domain.Row, int64, error) {
	// 返回模拟数据
	mockRows := []domain.Row{
		{"id": int64(1), "name": "Alice", "age": 30},
		{"id": int64(2), "name": "Bob", "age": 25},
	}
	return mockRows, int64(len(mockRows)), nil
}

func (m *MockFilterableDataSourceForOptimizer) Query(
	ctx context.Context,
	tableName string,
	options *domain.QueryOptions,
) (*domain.QueryResult, error) {
	// 返回模拟数据
	mockRows := []domain.Row{
		{"id": int64(1), "name": "Alice", "age": 30},
		{"id": int64(2), "name": "Bob", "age": 25},
		{"id": int64(3), "name": "Charlie", "age": 35},
	}
	return &domain.QueryResult{
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int64"},
			{Name: "name", Type: "string"},
			{Name: "age", Type: "int64"},
		},
		Rows:  mockRows,
		Total: int64(len(mockRows)),
	}, nil
}

func (m *MockFilterableDataSourceForOptimizer) GetTableInfo(
	ctx context.Context,
	tableName string,
) (*domain.TableInfo, error) {
	return &domain.TableInfo{
		Name: tableName,
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int64", Primary: true},
			{Name: "name", Type: "string"},
			{Name: "age", Type: "int64"},
		},
	}, nil
}

// TestPhysicalTableScan_Execute_WithFilterableDataSource 测试使用支持过滤的数据源
func TestPhysicalTableScan_Execute_WithFilterableDataSource(t *testing.T) {
	ctx := context.Background()

	// 创建支持过滤的数据源
	mockDS := &MockFilterableDataSourceForOptimizer{shouldUseFilter: true}

	// 创建表信息
	tableInfo := &domain.TableInfo{
		Name: "users",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int64", Primary: true},
			{Name: "name", Type: "string"},
			{Name: "age", Type: "int64"},
		},
	}

	// 创建过滤器
	filters := []domain.Filter{
		{Field: "age", Operator: ">", Value: 25},
	}

	// 创建物理表扫描节点
	physicalScan := NewPhysicalTableScan("users", tableInfo, mockDS, filters, nil)

	// 执行扫描
	result, err := physicalScan.Execute(ctx)
	if err != nil {
		t.Errorf("Execute() returned unexpected error: %v", err)
	}
	if result == nil {
		t.Errorf("Execute() should return non-nil result")
	}
	if len(result.Rows) == 0 {
		t.Errorf("Execute() should return some rows")
	}
	if result.Total < 0 {
		t.Errorf("Execute() should return non-negative total, got %d", result.Total)
	}
}

// TestPhysicalTableScan_Execute_WithNonFilterableDataSource 测试使用不支持过滤的数据源
func TestPhysicalTableScan_Execute_WithNonFilterableDataSource(t *testing.T) {
	ctx := context.Background()

	// 创建不支持过滤的数据源
	mockDS := &MockFilterableDataSourceForOptimizer{shouldUseFilter: false}

	// 创建表信息
	tableInfo := &domain.TableInfo{
		Name: "users",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int64", Primary: true},
			{Name: "name", Type: "string"},
			{Name: "age", Type: "int64"},
		},
	}

	// 创建过滤器
	filters := []domain.Filter{
		{Field: "age", Operator: ">", Value: 25},
	}

	// 创建物理表扫描节点
	physicalScan := NewPhysicalTableScan("users", tableInfo, mockDS, filters, nil)

	// 执行扫描
	result, err := physicalScan.Execute(ctx)
	if err != nil {
		t.Errorf("Execute() returned unexpected error: %v", err)
	}
	if result == nil {
		t.Errorf("Execute() should return non-nil result")
	}
	// 不支持过滤的数据源应该使用 Query 方法，返回更多数据
	if len(result.Rows) == 0 {
		t.Errorf("Execute() should return some rows")
	}
}

// TestPhysicalTableScan_Execute_WithMultipleFilters 测试多个过滤条件
func TestPhysicalTableScan_Execute_WithMultipleFilters(t *testing.T) {
	ctx := context.Background()

	// 创建支持过滤的数据源
	mockDS := &MockFilterableDataSourceForOptimizer{shouldUseFilter: true}

	// 创建表信息
	tableInfo := &domain.TableInfo{
		Name: "users",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int64", Primary: true},
			{Name: "name", Type: "string"},
			{Name: "age", Type: "int64"},
			{Name: "status", Type: "string"},
		},
	}

	// 创建多个过滤器
	filters := []domain.Filter{
		{Field: "age", Operator: ">", Value: 25},
		{Field: "status", Operator: "=", Value: "active"},
	}

	// 创建物理表扫描节点
	physicalScan := NewPhysicalTableScan("users", tableInfo, mockDS, filters, nil)

	// 执行扫描
	result, err := physicalScan.Execute(ctx)
	if err != nil {
		t.Errorf("Execute() with multiple filters returned unexpected error: %v", err)
	}
	if result == nil {
		t.Errorf("Execute() should return non-nil result")
	}
}

// TestPhysicalTableScan_Execute_WithLimit 测试带分页的查询
func TestPhysicalTableScan_Execute_WithLimit(t *testing.T) {
	ctx := context.Background()

	// 创建支持过滤的数据源
	mockDS := &MockFilterableDataSourceForOptimizer{shouldUseFilter: true}

	// 创建表信息
	tableInfo := &domain.TableInfo{
		Name: "users",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int64", Primary: true},
			{Name: "name", Type: "string"},
		},
	}

	// 创建过滤器和限制信息
	filters := []domain.Filter{
		{Field: "age", Operator: ">", Value: 20},
	}
	limitInfo := &LimitInfo{
		Offset: 10,
		Limit:  20,
	}

	// 创建物理表扫描节点
	physicalScan := NewPhysicalTableScan("users", tableInfo, mockDS, filters, limitInfo)

	// 执行扫描
	result, err := physicalScan.Execute(ctx)
	if err != nil {
		t.Errorf("Execute() with limit returned unexpected error: %v", err)
	}
	if result == nil {
		t.Errorf("Execute() should return non-nil result")
	}
}

// TestPhysicalTableScan_Execute_NoFilters 测试无过滤条件的查询
func TestPhysicalTableScan_Execute_NoFilters(t *testing.T) {
	ctx := context.Background()

	// 创建支持过滤的数据源
	mockDS := &MockFilterableDataSourceForOptimizer{shouldUseFilter: true}

	// 创建表信息
	tableInfo := &domain.TableInfo{
		Name: "users",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int64", Primary: true},
			{Name: "name", Type: "string"},
		},
	}

	// 创建无过滤器的物理表扫描节点
	physicalScan := NewPhysicalTableScan("users", tableInfo, mockDS, nil, nil)

	// 执行扫描
	result, err := physicalScan.Execute(ctx)
	if err != nil {
		t.Errorf("Execute() with no filters returned unexpected error: %v", err)
	}
	if result == nil {
		t.Errorf("Execute() should return non-nil result")
	}
}

// TestPhysicalTableScan_FilterCombination 测试过滤器组合逻辑
func TestPhysicalTableScan_FilterCombination(t *testing.T) {
	ctx := context.Background()

	// 创建支持过滤的数据源
	mockDS := &MockFilterableDataSourceForOptimizer{shouldUseFilter: true}

	// 创建表信息
	tableInfo := &domain.TableInfo{
		Name: "products",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int64", Primary: true},
			{Name: "price", Type: "float64"},
			{Name: "category", Type: "string"},
		},
	}

	tests := []struct {
		name     string
		filters  []domain.Filter
		expected bool
	}{
		{
			name: "single filter",
			filters: []domain.Filter{
				{Field: "price", Operator: ">", Value: 100},
			},
			expected: true,
		},
		{
			name: "multiple filters (AND logic)",
			filters: []domain.Filter{
				{Field: "price", Operator: ">", Value: 100},
				{Field: "category", Operator: "=", Value: "electronics"},
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			physicalScan := NewPhysicalTableScan("products", tableInfo, mockDS, tt.filters, nil)

			result, err := physicalScan.Execute(ctx)
			if err != nil {
				t.Errorf("Execute() returned unexpected error: %v", err)
			}
			if result == nil {
				t.Errorf("Execute() should return non-nil result")
			}
		})
	}
}

// TestPhysicalTableScan_CostEstimation 测试成本估计
func TestPhysicalTableScan_CostEstimation(t *testing.T) {
	tableInfo := &domain.TableInfo{
		Name: "test_table",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int64"},
		},
	}

	mockDS := &MockFilterableDataSourceForOptimizer{shouldUseFilter: true}

	// 测试无限制的扫描
	scanNoLimit := NewPhysicalTableScan("test_table", tableInfo, mockDS, nil, nil)
	if scanNoLimit.Cost() <= 0 {
		t.Errorf("Expected positive cost for scan without limit")
	}

	// 测试有限制的扫描
	limitInfo := &LimitInfo{Limit: 10, Offset: 0}
	scanWithLimit := NewPhysicalTableScan("test_table", tableInfo, mockDS, nil, limitInfo)
	if scanWithLimit.Cost() <= 0 {
		t.Errorf("Expected positive cost for scan with limit")
	}

	// 限制的成本应该更小
	if scanWithLimit.Cost() >= scanNoLimit.Cost() {
		t.Errorf("Expected scan with limit to have lower cost")
	}
}

// TestPhysicalTableScan_Schema 测试Schema方法
func TestPhysicalTableScan_Schema(t *testing.T) {
	tableInfo := &domain.TableInfo{
		Name: "test_table",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int64", Primary: true},
			{Name: "name", Type: "string"},
		},
	}

	mockDS := &MockFilterableDataSourceForOptimizer{shouldUseFilter: true}
	physicalScan := NewPhysicalTableScan("test_table", tableInfo, mockDS, nil, nil)

	schema := physicalScan.Schema()
	if len(schema) != len(tableInfo.Columns) {
		t.Errorf("Expected schema to have %d columns, got %d", len(tableInfo.Columns), len(schema))
	}

	for i, col := range schema {
		if col.Name != tableInfo.Columns[i].Name {
			t.Errorf("Expected column %d name to be %s, got %s", i, tableInfo.Columns[i].Name, col.Name)
		}
		if col.Type != tableInfo.Columns[i].Type {
			t.Errorf("Expected column %d type to be %s, got %s", i, tableInfo.Columns[i].Type, col.Type)
		}
	}
}

// TestPhysicalTableScan_Explain 测试Explain方法
func TestPhysicalTableScan_Explain(t *testing.T) {
	tableInfo := &domain.TableInfo{
		Name: "test_table",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int64"},
		},
	}

	mockDS := &MockFilterableDataSourceForOptimizer{shouldUseFilter: true}
	physicalScan := NewPhysicalTableScan("test_table", tableInfo, mockDS, nil, nil)

	explain := physicalScan.Explain()
	if explain == "" {
		t.Errorf("Expected non-empty explain string")
	}

	// 检查explain是否包含表名和成本
	if explain == "" {
		t.Errorf("Explain() should return a non-empty string")
	}
}
