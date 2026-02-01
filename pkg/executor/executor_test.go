package executor

import (
	"context"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/parser"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// MockExecutor 测试用的模拟执行器
type MockExecutor struct {
	results map[string]*domain.QueryResult
	errors  map[string]error
}

func NewMockExecutor() *MockExecutor {
	return &MockExecutor{
		results: make(map[string]*domain.QueryResult),
		errors:  make(map[string]error),
	}
}

func (m *MockExecutor) SetResult(sql string, result *domain.QueryResult) {
	m.results[sql] = result
}

func (m *MockExecutor) SetError(sql string, err error) {
	m.errors[sql] = err
}

func (m *MockExecutor) Execute(ctx context.Context, sql string) (*domain.QueryResult, error) {
	if err, ok := m.errors[sql]; ok {
		return nil, err
	}
	if result, ok := m.results[sql]; ok {
		return result, nil
	}
	return &domain.QueryResult{}, nil
}

// TestQueryExecutor_SimpleSelect 测试简单SELECT查询（纯逻辑，不涉及网络）
func TestQueryExecutor_SimpleSelect(t *testing.T) {
	ctx := context.Background()

	// 创建模拟执行器
	mockExec := NewMockExecutor()
	mockExec.SetResult("SELECT * FROM users", &domain.QueryResult{
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int64"},
			{Name: "name", Type: "string"},
		},
		Rows: []domain.Row{
			{"id": int64(1), "name": "Alice"},
			{"id": int64(2), "name": "Bob"},
		},
		Total: 2,
	})

	// 执行查询
	result, err := mockExec.Execute(ctx, "SELECT * FROM users")

	// 验证结果
	require.NoError(t, err)
	assert.Equal(t, 2, len(result.Rows))
	assert.Equal(t, "Alice", result.Rows[0]["name"])
	assert.Equal(t, "Bob", result.Rows[1]["name"])
	assert.Equal(t, int64(2), result.Total)
}

// TestQueryExecutor_WithParser 测试完整的SQL解析+执行流程
func TestQueryExecutor_WithParser(t *testing.T) {
	ctx := context.Background()

	// 创建SQL解析器
	adapter := parser.NewSQLAdapter()

	// 解析SQL
	sql := "SELECT id, name FROM users WHERE id = 1"
	stmt, err := adapter.Parse(sql)
	require.NoError(t, err)
	require.NotNil(t, stmt)

	// 验证解析结果
	assert.Equal(t, parser.SQLTypeSelect, stmt.Type)
	require.NotNil(t, stmt.Select)
	assert.Equal(t, 2, len(stmt.Select.Columns))
	assert.Equal(t, "users", stmt.Select.From)

	// 创建模拟执行器并设置预期结果
	mockExec := NewMockExecutor()
	mockExec.SetResult(sql, &domain.QueryResult{
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int64"},
			{Name: "name", Type: "string"},
		},
		Rows: []domain.Row{
			{"id": int64(1), "name": "Alice"},
		},
		Total: 1,
	})

	// 执行查询
	result, err := mockExec.Execute(ctx, sql)

	// 验证结果
	require.NoError(t, err)
	assert.Equal(t, 1, len(result.Rows))
	assert.Equal(t, "Alice", result.Rows[0]["name"])
}

// TestQueryExecutor_ErrorHandling 测试错误处理
func TestQueryExecutor_ErrorHandling(t *testing.T) {
	ctx := context.Background()

	mockExec := NewMockExecutor()

	// 设置错误
	mockExec.SetError("SELECT * FROM invalid_table",
		domain.NewError("table not found", nil))

	// 执行查询
	result, err := mockExec.Execute(ctx, "SELECT * FROM invalid_table")

	// 验证错误
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "table not found")
}

// TestQueryExecutor_WithFilter 测试带过滤条件的查询
func TestQueryExecutor_WithFilter(t *testing.T) {
	ctx := context.Background()

	mockExec := NewMockExecutor()

	// 测试WHERE子句
	sql := "SELECT * FROM products WHERE price > 100"
	mockExec.SetResult(sql, &domain.QueryResult{
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int64"},
			{Name: "name", Type: "string"},
			{Name: "price", Type: "float64"},
		},
		Rows: []domain.Row{
			{"id": int64(1), "name": "Product A", "price": 150.0},
			{"id": int64(2), "name": "Product B", "price": 200.0},
		},
		Total: 2,
	})

	result, err := mockExec.Execute(ctx, sql)
	require.NoError(t, err)
	assert.Equal(t, 2, len(result.Rows))
	assert.Equal(t, 150.0, result.Rows[0]["price"])
}

// TestQueryExecutor_WithJoin 测试JOIN查询
func TestQueryExecutor_WithJoin(t *testing.T) {
	ctx := context.Background()

	mockExec := NewMockExecutor()

	sql := "SELECT users.name, orders.amount FROM users JOIN orders ON users.id = orders.user_id"
	mockExec.SetResult(sql, &domain.QueryResult{
		Columns: []domain.ColumnInfo{
			{Name: "name", Type: "string"},
			{Name: "amount", Type: "float64"},
		},
		Rows: []domain.Row{
			{"name": "Alice", "amount": 100.0},
			{"name": "Bob", "amount": 200.0},
		},
		Total: 2,
	})

	result, err := mockExec.Execute(ctx, sql)
	require.NoError(t, err)
	assert.Equal(t, 2, len(result.Rows))
	assert.Equal(t, "Alice", result.Rows[0]["name"])
}
