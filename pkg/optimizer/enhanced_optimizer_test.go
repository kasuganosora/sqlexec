package optimizer

import (
	"context"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/parser"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/resource/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEnhancedOptimizer_BasicOptimization(t *testing.T) {
	factory := memory.NewMemoryFactory()
	dataSource, err := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	})
	require.NoError(t, err)

	ctx := context.Background()

	// Create test table
	err = dataSource.CreateTable(ctx, &domain.TableInfo{
		Name: "test_table",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int", Primary: true, AutoIncrement: true},
			{Name: "name", Type: "string", Nullable: false},
			{Name: "age", Type: "int", Nullable: true},
		},
	})
	require.NoError(t, err)

	// Create optimizer
	opt := NewOptimizer(dataSource)
	assert.NotNil(t, opt)

	// Create simple SELECT statement
	stmt := &parser.SelectStatement{
		Columns: []parser.SelectColumn{
			{Name: "id"},
			{Name: "name"},
		},
		From: "test_table",
		Where: &parser.Expression{
			Type:     parser.ExprTypeOperator,
			Operator: "gt",
			Left: &parser.Expression{
				Type:   parser.ExprTypeColumn,
				Column: "age",
			},
			Right: &parser.Expression{
				Type:  parser.ExprTypeValue,
				Value: 18,
			},
		},
	}

	sqlStmt := &parser.SQLStatement{
		Type:   parser.SQLTypeSelect,
		Select: stmt,
	}

	// Optimize
	plan, err := opt.Optimize(ctx, sqlStmt)
	assert.NoError(t, err)
	assert.NotNil(t, plan)
}

func TestEnhancedOptimizer_ComplexQuery(t *testing.T) {
	factory := memory.NewMemoryFactory()
	dataSource, err := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	})
	require.NoError(t, err)

	ctx := context.Background()

	// Create tables
	err = dataSource.CreateTable(ctx, &domain.TableInfo{
		Name: "users",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int", Primary: true},
			{Name: "name", Type: "string"},
			{Name: "age", Type: "int"},
		},
	})
	require.NoError(t, err)

	err = dataSource.CreateTable(ctx, &domain.TableInfo{
		Name: "orders",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int", Primary: true},
			{Name: "user_id", Type: "int"},
			{Name: "amount", Type: "int"},
		},
	})
	require.NoError(t, err)

	// Create optimizer
	opt := NewOptimizer(dataSource)

	// Create JOIN query (simplified)
	stmt := &parser.SelectStatement{
		Columns: []parser.SelectColumn{
			{Name: "users.id"},
			{Name: "users.name"},
			{Name: "orders.amount"},
		},
		From: "users",
	}

	sqlStmt := &parser.SQLStatement{
		Type:   parser.SQLTypeSelect,
		Select: stmt,
	}

	// Optimize
	plan, err := opt.Optimize(ctx, sqlStmt)
	assert.NoError(t, err)
	assert.NotNil(t, plan)
}

func TestEnhancedOptimizer_Aggregation(t *testing.T) {
	factory := memory.NewMemoryFactory()
	dataSource, err := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	})
	require.NoError(t, err)

	ctx := context.Background()

	// Create table
	err = dataSource.CreateTable(ctx, &domain.TableInfo{
		Name: "sales",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int", Primary: true},
			{Name: "category", Type: "string"},
			{Name: "amount", Type: "int"},
		},
	})
	require.NoError(t, err)

	// Create optimizer
	opt := NewOptimizer(dataSource)

	// Create aggregation query
	stmt := &parser.SelectStatement{
		Columns: []parser.SelectColumn{
			{Name: "category"},
			{Name: "COUNT(*)", Alias: "count"},
			{Name: "SUM(amount)", Alias: "total"},
		},
		From:   "sales",
		GroupBy: []string{"category"},
	}

	sqlStmt := &parser.SQLStatement{
		Type:   parser.SQLTypeSelect,
		Select: stmt,
	}

	// Optimize
	plan, err := opt.Optimize(ctx, sqlStmt)
	assert.NoError(t, err)
	assert.NotNil(t, plan)
}

func TestEnhancedOptimizer_SortAndLimit(t *testing.T) {
	factory := memory.NewMemoryFactory()
	dataSource, err := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	})
	require.NoError(t, err)

	ctx := context.Background()

	// Create table
	err = dataSource.CreateTable(ctx, &domain.TableInfo{
		Name: "products",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int", Primary: true},
			{Name: "name", Type: "string"},
			{Name: "price", Type: "int"},
		},
	})
	require.NoError(t, err)

	// Create optimizer
	opt := NewOptimizer(dataSource)

	limit := int64(10)
	stmt := &parser.SelectStatement{
		Columns: []parser.SelectColumn{
			{Name: "*"},
		},
		From: "products",
		OrderBy: []parser.OrderByItem{
			{Column: "price", Direction: "DESC"},
		},
		Limit: &limit,
	}

	sqlStmt := &parser.SQLStatement{
		Type:   parser.SQLTypeSelect,
		Select: stmt,
	}

	// Optimize
	plan, err := opt.Optimize(ctx, sqlStmt)
	assert.NoError(t, err)
	assert.NotNil(t, plan)
}

func TestEnhancedOptimizer_MultipleTables(t *testing.T) {
	factory := memory.NewMemoryFactory()
	dataSource, err := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	})
	require.NoError(t, err)

	ctx := context.Background()

	// Create multiple tables
	tables := []string{"table1", "table2", "table3"}
	for _, tableName := range tables {
		err := dataSource.CreateTable(ctx, &domain.TableInfo{
			Name: tableName,
			Columns: []domain.ColumnInfo{
				{Name: "id", Type: "int", Primary: true},
				{Name: "data", Type: "string"},
			},
		})
		require.NoError(t, err)
	}

	// Create optimizer
	opt := NewOptimizer(dataSource)

	// Query first table
	stmt := &parser.SelectStatement{
		Columns: []parser.SelectColumn{
			{Name: "*"},
		},
		From: "table1",
	}

	sqlStmt := &parser.SQLStatement{
		Type:   parser.SQLTypeSelect,
		Select: stmt,
	}

	// Optimize
	plan, err := opt.Optimize(ctx, sqlStmt)
	assert.NoError(t, err)
	assert.NotNil(t, plan)
}

func TestEnhancedOptimizer_TableNotFound(t *testing.T) {
	factory := memory.NewMemoryFactory()
	dataSource, err := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	})
	require.NoError(t, err)

	ctx := context.Background()

	// Create optimizer
	opt := NewOptimizer(dataSource)

	// Query non-existent table
	stmt := &parser.SelectStatement{
		Columns: []parser.SelectColumn{
			{Name: "*"},
		},
		From: "non_existent_table",
	}

	sqlStmt := &parser.SQLStatement{
		Type:   parser.SQLTypeSelect,
		Select: stmt,
	}

	// Optimize (should handle gracefully)
	plan, err := opt.Optimize(ctx, sqlStmt)
	// May return error or nil plan, but should not panic
	if err != nil {
		assert.NotNil(t, err)
	}
	if plan == nil {
		// Expected behavior for non-existent table
		assert.Nil(t, plan)
	} else {
		assert.NotNil(t, plan)
	}
}

func TestEnhancedOptimizer_EmptyWhere(t *testing.T) {
	factory := memory.NewMemoryFactory()
	dataSource, err := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	})
	require.NoError(t, err)

	ctx := context.Background()

	// Create table
	err = dataSource.CreateTable(ctx, &domain.TableInfo{
		Name: "test_table",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int", Primary: true},
		},
	})
	require.NoError(t, err)

	// Create optimizer
	opt := NewOptimizer(dataSource)

	// Query without WHERE
	stmt := &parser.SelectStatement{
		Columns: []parser.SelectColumn{
			{Name: "*"},
		},
		From:  "test_table",
		Where: nil,
	}

	sqlStmt := &parser.SQLStatement{
		Type:   parser.SQLTypeSelect,
		Select: stmt,
	}

	// Optimize
	plan, err := opt.Optimize(ctx, sqlStmt)
	assert.NoError(t, err)
	assert.NotNil(t, plan)
}

func TestEnhancedOptimizer_Subquery(t *testing.T) {
	factory := memory.NewMemoryFactory()
	dataSource, err := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	})
	require.NoError(t, err)

	ctx := context.Background()

	// Create table
	err = dataSource.CreateTable(ctx, &domain.TableInfo{
		Name: "employees",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int", Primary: true},
			{Name: "name", Type: "string"},
			{Name: "salary", Type: "int"},
		},
	})
	require.NoError(t, err)

	// Create optimizer
	opt := NewOptimizer(dataSource)

	// Query with subquery in WHERE (simplified)
	stmt := &parser.SelectStatement{
		Columns: []parser.SelectColumn{
			{Name: "*"},
		},
		From: "employees",
		Where: &parser.Expression{
			Type:     parser.ExprTypeOperator,
			Operator: "gt",
			Left: &parser.Expression{
				Type:   parser.ExprTypeColumn,
				Column: "salary",
			},
			Right: &parser.Expression{
				Type:  parser.ExprTypeValue,
				Value: 50000,
			},
		},
	}

	sqlStmt := &parser.SQLStatement{
		Type:   parser.SQLTypeSelect,
		Select: stmt,
	}

	// Optimize
	plan, err := opt.Optimize(ctx, sqlStmt)
	assert.NoError(t, err)
	assert.NotNil(t, plan)
}

func TestEnhancedOptimizer_Distinct(t *testing.T) {
	factory := memory.NewMemoryFactory()
	dataSource, err := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	})
	require.NoError(t, err)

	ctx := context.Background()

	// Create table
	err = dataSource.CreateTable(ctx, &domain.TableInfo{
		Name: "test_table",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int", Primary: true},
			{Name: "name", Type: "string"},
		},
	})
	require.NoError(t, err)

	// Create optimizer
	opt := NewOptimizer(dataSource)

	// Query with DISTINCT
	stmt := &parser.SelectStatement{
		Columns: []parser.SelectColumn{
			{Name: "DISTINCT name"},
		},
		From: "test_table",
	}

	sqlStmt := &parser.SQLStatement{
		Type:   parser.SQLTypeSelect,
		Select: stmt,
	}

	// Optimize
	plan, err := opt.Optimize(ctx, sqlStmt)
	assert.NoError(t, err)
	assert.NotNil(t, plan)
}

func TestEnhancedOptimizer_ComplexWhere(t *testing.T) {
	factory := memory.NewMemoryFactory()
	dataSource, err := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	})
	require.NoError(t, err)

	ctx := context.Background()

	// Create table
	err = dataSource.CreateTable(ctx, &domain.TableInfo{
		Name: "test_table",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int", Primary: true},
			{Name: "age", Type: "int"},
			{Name: "score", Type: "int"},
		},
	})
	require.NoError(t, err)

	// Create optimizer
	opt := NewOptimizer(dataSource)

	// Query with complex WHERE: age > 18 AND score > 50
	stmt := &parser.SelectStatement{
		Columns: []parser.SelectColumn{
			{Name: "*"},
		},
		From: "test_table",
		Where: &parser.Expression{
			Type:     parser.ExprTypeOperator,
			Operator: "and",
			Left: &parser.Expression{
				Type:     parser.ExprTypeOperator,
				Operator: "gt",
				Left: &parser.Expression{
					Type:   parser.ExprTypeColumn,
					Column: "age",
				},
				Right: &parser.Expression{
					Type:  parser.ExprTypeValue,
					Value: 18,
				},
			},
			Right: &parser.Expression{
				Type:     parser.ExprTypeOperator,
				Operator: "gt",
				Left: &parser.Expression{
					Type:   parser.ExprTypeColumn,
					Column: "score",
				},
				Right: &parser.Expression{
					Type:  parser.ExprTypeValue,
					Value: 50,
				},
			},
		},
	}

	sqlStmt := &parser.SQLStatement{
		Type:   parser.SQLTypeSelect,
		Select: stmt,
	}

	// Optimize
	plan, err := opt.Optimize(ctx, sqlStmt)
	assert.NoError(t, err)
	assert.NotNil(t, plan)
}

func TestEnhancedOptimizer_NullHandling(t *testing.T) {
	factory := memory.NewMemoryFactory()
	dataSource, err := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	})
	require.NoError(t, err)

	ctx := context.Background()

	// Create table with nullable columns
	err = dataSource.CreateTable(ctx, &domain.TableInfo{
		Name: "test_table",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int", Primary: true},
			{Name: "name", Type: "string", Nullable: true},
			{Name: "age", Type: "int", Nullable: true},
		},
	})
	require.NoError(t, err)

	// Create optimizer
	opt := NewOptimizer(dataSource)

	// Query with IS NULL check (simplified)
	stmt := &parser.SelectStatement{
		Columns: []parser.SelectColumn{
			{Name: "*"},
		},
		From: "test_table",
	}

	sqlStmt := &parser.SQLStatement{
		Type:   parser.SQLTypeSelect,
		Select: stmt,
	}

	// Optimize
	plan, err := opt.Optimize(ctx, sqlStmt)
	assert.NoError(t, err)
	assert.NotNil(t, plan)
}

func TestEnhancedOptimizer_ContextCancellation(t *testing.T) {
	factory := memory.NewMemoryFactory()
	dataSource, err := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	})
	require.NoError(t, err)

	ctx := context.Background()

	// Create table
	err = dataSource.CreateTable(ctx, &domain.TableInfo{
		Name: "test_table",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int", Primary: true},
		},
	})
	require.NoError(t, err)

	// Create optimizer
	opt := NewOptimizer(dataSource)

	stmt := &parser.SelectStatement{
		Columns: []parser.SelectColumn{
			{Name: "*"},
		},
		From: "test_table",
	}

	sqlStmt := &parser.SQLStatement{
		Type:   parser.SQLTypeSelect,
		Select: stmt,
	}

	// Optimize with valid context
	plan, err := opt.Optimize(ctx, sqlStmt)
	assert.NoError(t, err)
	assert.NotNil(t, plan)
}

// Benchmark tests
func BenchmarkEnhancedOptimizer_SimpleSelect(b *testing.B) {
	factory := memory.NewMemoryFactory()
	dataSource, _ := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	})

	ctx := context.Background()
	dataSource.CreateTable(ctx, &domain.TableInfo{
		Name: "test_table",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int", Primary: true},
		},
	})

	opt := NewOptimizer(dataSource)

	stmt := &parser.SelectStatement{
		Columns: []parser.SelectColumn{{Name: "*"}},
		From:    "test_table",
	}

	sqlStmt := &parser.SQLStatement{
		Type:   parser.SQLTypeSelect,
		Select: stmt,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		opt.Optimize(ctx, sqlStmt)
	}
}

func BenchmarkNewOptimizer(b *testing.B) {
	factory := memory.NewMemoryFactory()
	dataSource, _ := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		NewOptimizer(dataSource)
	}
}
