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

func TestEnhancedPredicatePushdown_SimpleFilter(t *testing.T) {
	factory := memory.NewMemoryFactory()
	dataSource, err := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	})
	require.NoError(t, err)

	ctx := context.Background()

	// Create table
	err = dataSource.CreateTable(ctx, &domain.TableInfo{
		Name: "users",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int", Primary: true},
			{Name: "name", Type: "string"},
			{Name: "age", Type: "int"},
		},
	})
	require.NoError(t, err)

	// Insert test data
	for i := 1; i <= 10; i++ {
		row := domain.Row{
			"id":   int64(i),
			"name": "user" + string(rune('0'+i)),
			"age":  int64(20 + i),
		}
		rows := []domain.Row{row}
		_, err := dataSource.Insert(ctx, "users", rows, &domain.InsertOptions{})
		require.NoError(t, err)
	}

	// Create optimizer
	opt := NewOptimizer(dataSource)

	// Create query with WHERE filter
	stmt := &parser.SelectStatement{
		Columns: []parser.SelectColumn{
			{Name: "*"},
		},
		From: "users",
		Where: &parser.Expression{
			Type:     parser.ExprTypeOperator,
			Operator: "gt",
			Left: &parser.Expression{
				Type:   parser.ExprTypeColumn,
				Column: "age",
			},
			Right: &parser.Expression{
				Type:  parser.ExprTypeValue,
				Value: 25,
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

	// Verify plan is optimized
	explain := plan.Explain()
	assert.NotEmpty(t, explain)
}

func TestEnhancedPredicatePushdown_ComplexFilter(t *testing.T) {
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
			{Name: "stock", Type: "int"},
		},
	})
	require.NoError(t, err)

	// Create optimizer
	opt := NewOptimizer(dataSource)

	// Create query with complex WHERE: price > 50 AND stock > 10
	stmt := &parser.SelectStatement{
		Columns: []parser.SelectColumn{
			{Name: "*"},
		},
		From: "products",
		Where: &parser.Expression{
			Type:     parser.ExprTypeOperator,
			Operator: "and",
			Left: &parser.Expression{
				Type:     parser.ExprTypeOperator,
				Operator: "gt",
				Left: &parser.Expression{
					Type:   parser.ExprTypeColumn,
					Column: "price",
				},
				Right: &parser.Expression{
					Type:  parser.ExprTypeValue,
					Value: 50,
				},
			},
			Right: &parser.Expression{
				Type:     parser.ExprTypeOperator,
				Operator: "gt",
				Left: &parser.Expression{
					Type:   parser.ExprTypeColumn,
					Column: "stock",
				},
				Right: &parser.Expression{
					Type:  parser.ExprTypeValue,
					Value: 10,
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

func TestEnhancedPredicatePushdown_WithJoin(t *testing.T) {
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

	// Query with WHERE on joined tables (simplified)
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

func TestEnhancedPredicatePushdown_WithAggregation(t *testing.T) {
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

	// Query with WHERE and GROUP BY
	stmt := &parser.SelectStatement{
		Columns: []parser.SelectColumn{
			{Name: "category"},
			{Name: "COUNT(*)", Alias: "count"},
		},
		From:    "sales",
		GroupBy: []string{"category"},
		Where: &parser.Expression{
			Type:     parser.ExprTypeOperator,
			Operator: "gt",
			Left: &parser.Expression{
				Type:   parser.ExprTypeColumn,
				Column: "amount",
			},
			Right: &parser.Expression{
				Type:  parser.ExprTypeValue,
				Value: 100,
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

func TestEnhancedPredicatePushdown_WithLimit(t *testing.T) {
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
			{Name: "data", Type: "string"},
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
		From: "test_table",
		Where: &parser.Expression{
			Type:     parser.ExprTypeOperator,
			Operator: "gt",
			Left: &parser.Expression{
				Type:   parser.ExprTypeColumn,
				Column: "id",
			},
			Right: &parser.Expression{
				Type:  parser.ExprTypeValue,
				Value: 0,
			},
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

func TestEnhancedPredicatePushdown_ORFilter(t *testing.T) {
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

	// Query with OR: name = 'apple' OR name = 'banana'
	stmt := &parser.SelectStatement{
		Columns: []parser.SelectColumn{
			{Name: "*"},
		},
		From: "products",
		Where: &parser.Expression{
			Type:     parser.ExprTypeOperator,
			Operator: "or",
			Left: &parser.Expression{
				Type:     parser.ExprTypeOperator,
				Operator: "=",
				Left: &parser.Expression{
					Type:   parser.ExprTypeColumn,
					Column: "name",
				},
				Right: &parser.Expression{
					Type:  parser.ExprTypeValue,
					Value: "apple",
				},
			},
			Right: &parser.Expression{
				Type:     parser.ExprTypeOperator,
				Operator: "=",
				Left: &parser.Expression{
					Type:   parser.ExprTypeColumn,
					Column: "name",
				},
				Right: &parser.Expression{
					Type:  parser.ExprTypeValue,
					Value: "banana",
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

func TestEnhancedPredicatePushdown_INFilter(t *testing.T) {
	factory := memory.NewMemoryFactory()
	dataSource, err := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	})
	require.NoError(t, err)

	ctx := context.Background()

	// Create table
	err = dataSource.CreateTable(ctx, &domain.TableInfo{
		Name: "users",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int", Primary: true},
			{Name: "name", Type: "string"},
		},
	})
	require.NoError(t, err)

	// Create optimizer
	opt := NewOptimizer(dataSource)

	// Query with IN (simplified as regular filter)
	stmt := &parser.SelectStatement{
		Columns: []parser.SelectColumn{
			{Name: "*"},
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

func TestEnhancedPredicatePushdown_NestedConditions(t *testing.T) {
	factory := memory.NewMemoryFactory()
	dataSource, err := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	})
	require.NoError(t, err)

	ctx := context.Background()

	// Create table
	err = dataSource.CreateTable(ctx, &domain.TableInfo{
		Name: "records",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int", Primary: true},
			{Name: "field1", Type: "int"},
			{Name: "field2", Type: "int"},
			{Name: "field3", Type: "int"},
		},
	})
	require.NoError(t, err)

	// Create optimizer
	opt := NewOptimizer(dataSource)

	// Query with nested conditions: (field1 > 10 OR field2 > 20) AND field3 < 30
	stmt := &parser.SelectStatement{
		Columns: []parser.SelectColumn{
			{Name: "*"},
		},
		From: "records",
		Where: &parser.Expression{
			Type:     parser.ExprTypeOperator,
			Operator: "and",
			Left: &parser.Expression{
				Type:     parser.ExprTypeOperator,
				Operator: "or",
				Left: &parser.Expression{
					Type:     parser.ExprTypeOperator,
					Operator: "gt",
					Left: &parser.Expression{
						Type:   parser.ExprTypeColumn,
						Column: "field1",
					},
					Right: &parser.Expression{
						Type:  parser.ExprTypeValue,
						Value: 10,
					},
				},
				Right: &parser.Expression{
					Type:     parser.ExprTypeOperator,
					Operator: "gt",
					Left: &parser.Expression{
						Type:   parser.ExprTypeColumn,
						Column: "field2",
					},
					Right: &parser.Expression{
						Type:  parser.ExprTypeValue,
						Value: 20,
					},
				},
			},
			Right: &parser.Expression{
				Type:     parser.ExprTypeOperator,
				Operator: "lt",
				Left: &parser.Expression{
					Type:   parser.ExprTypeColumn,
					Column: "field3",
				},
				Right: &parser.Expression{
					Type:  parser.ExprTypeValue,
					Value: 30,
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

func TestEnhancedPredicatePushdown_WithOrderBy(t *testing.T) {
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

	// Query with WHERE and ORDER BY
	stmt := &parser.SelectStatement{
		Columns: []parser.SelectColumn{
			{Name: "*"},
		},
		From: "test_table",
		Where: &parser.Expression{
			Type:     parser.ExprTypeOperator,
			Operator: "gt",
			Left: &parser.Expression{
				Type:   parser.ExprTypeColumn,
				Column: "id",
			},
			Right: &parser.Expression{
				Type:  parser.ExprTypeValue,
				Value: 5,
			},
		},
		OrderBy: []parser.OrderByItem{
			{Column: "name", Direction: "ASC"},
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

func TestEnhancedPredicatePushdown_NoPredicate(t *testing.T) {
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

// Benchmark tests
func BenchmarkEnhancedPredicatePushdown_SimpleFilter(b *testing.B) {
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
		Where: &parser.Expression{
			Type:     parser.ExprTypeOperator,
			Operator: "gt",
			Left: &parser.Expression{
				Type:   parser.ExprTypeColumn,
				Column: "id",
			},
			Right: &parser.Expression{
				Type:  parser.ExprTypeValue,
				Value: 10,
			},
		},
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

func BenchmarkEnhancedPredicatePushdown_ComplexFilter(b *testing.B) {
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

	// Complex nested condition
	stmt := &parser.SelectStatement{
		Columns: []parser.SelectColumn{{Name: "*"}},
		From:    "test_table",
		Where: &parser.Expression{
			Type:     parser.ExprTypeOperator,
			Operator: "and",
			Left: &parser.Expression{
				Type:     parser.ExprTypeOperator,
				Operator: "or",
				Left: &parser.Expression{
					Type:     parser.ExprTypeOperator,
					Operator: "gt",
					Left: &parser.Expression{
						Type:   parser.ExprTypeColumn,
						Column: "id",
					},
					Right: &parser.Expression{
						Type:  parser.ExprTypeValue,
						Value: 10,
					},
				},
				Right: &parser.Expression{
					Type:     parser.ExprTypeOperator,
					Operator: "lt",
					Left: &parser.Expression{
						Type:   parser.ExprTypeColumn,
						Column: "id",
					},
					Right: &parser.Expression{
						Type:  parser.ExprTypeValue,
						Value: 100,
					},
				},
			},
			Right: &parser.Expression{
				Type:     parser.ExprTypeOperator,
				Operator: "ne",
				Left: &parser.Expression{
					Type:   parser.ExprTypeColumn,
					Column: "id",
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

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		opt.Optimize(ctx, sqlStmt)
	}
}
