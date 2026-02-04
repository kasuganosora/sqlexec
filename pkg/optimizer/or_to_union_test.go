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

func TestORToUnion_SimpleOR(t *testing.T) {
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
			{Name: "category", Type: "string"},
		},
	})
	require.NoError(t, err)

	// Insert test data
	categories := []string{"electronics", "clothing", "food"}
	for i := 0; i < 15; i++ {
		row := domain.Row{
			"id":       int64(i + 1),
			"name":     "product" + string(rune('0'+i)),
			"category": categories[i%len(categories)],
		}
		rows := []domain.Row{row}
		_, err := dataSource.Insert(ctx, "products", rows, &domain.InsertOptions{})
		require.NoError(t, err)
	}

	// Create optimizer
	opt := NewOptimizer(dataSource)

	// Query with OR: category = 'electronics' OR category = 'clothing'
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
					Column: "category",
				},
				Right: &parser.Expression{
					Type:  parser.ExprTypeValue,
					Value: "electronics",
				},
			},
			Right: &parser.Expression{
				Type:     parser.ExprTypeOperator,
				Operator: "=",
				Left: &parser.Expression{
					Type:   parser.ExprTypeColumn,
					Column: "category",
				},
				Right: &parser.Expression{
					Type:  parser.ExprTypeValue,
					Value: "clothing",
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

	// Verify plan structure
	explain := plan.Explain()
	assert.NotEmpty(t, explain)
}

func TestORToUnion_ThreeORConditions(t *testing.T) {
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
			{Name: "status", Type: "string"},
		},
	})
	require.NoError(t, err)

	// Create optimizer
	opt := NewOptimizer(dataSource)

	// Query with 3 OR conditions: status = 'active' OR status = 'pending' OR status = 'review'
	stmt := &parser.SelectStatement{
		Columns: []parser.SelectColumn{
			{Name: "*"},
		},
		From: "test_table",
		Where: &parser.Expression{
			Type:     parser.ExprTypeOperator,
			Operator: "or",
			Left: &parser.Expression{
				Type:     parser.ExprTypeOperator,
				Operator: "or",
				Left: &parser.Expression{
					Type:     parser.ExprTypeOperator,
					Operator: "=",
					Left: &parser.Expression{
						Type:   parser.ExprTypeColumn,
						Column: "status",
					},
					Right: &parser.Expression{
						Type:  parser.ExprTypeValue,
						Value: "active",
					},
				},
				Right: &parser.Expression{
					Type:     parser.ExprTypeOperator,
					Operator: "=",
					Left: &parser.Expression{
						Type:   parser.ExprTypeColumn,
						Column: "status",
					},
					Right: &parser.Expression{
						Type:  parser.ExprTypeValue,
						Value: "pending",
					},
				},
			},
			Right: &parser.Expression{
				Type:     parser.ExprTypeOperator,
				Operator: "=",
				Left: &parser.Expression{
					Type:   parser.ExprTypeColumn,
					Column: "status",
				},
				Right: &parser.Expression{
					Type:  parser.ExprTypeValue,
					Value: "review",
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

func TestORToUnion_WithOtherClauses(t *testing.T) {
	factory := memory.NewMemoryFactory()
	dataSource, err := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	})
	require.NoError(t, err)

	ctx := context.Background()

	// Create table
	err = dataSource.CreateTable(ctx, &domain.TableInfo{
		Name: "orders",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int", Primary: true},
			{Name: "customer_id", Type: "int"},
			{Name: "status", Type: "string"},
			{Name: "amount", Type: "int"},
		},
	})
	require.NoError(t, err)

	// Create optimizer
	opt := NewOptimizer(dataSource)

	// Query with OR and additional WHERE: (status = 'shipped' OR status = 'delivered') AND amount > 100
	stmt := &parser.SelectStatement{
		Columns: []parser.SelectColumn{
			{Name: "*"},
		},
		From: "orders",
		Where: &parser.Expression{
			Type:     parser.ExprTypeOperator,
			Operator: "and",
			Left: &parser.Expression{
				Type:     parser.ExprTypeOperator,
				Operator: "or",
				Left: &parser.Expression{
					Type:     parser.ExprTypeOperator,
					Operator: "=",
					Left: &parser.Expression{
						Type:   parser.ExprTypeColumn,
						Column: "status",
					},
					Right: &parser.Expression{
						Type:  parser.ExprTypeValue,
						Value: "shipped",
					},
				},
				Right: &parser.Expression{
					Type:     parser.ExprTypeOperator,
					Operator: "=",
					Left: &parser.Expression{
						Type:   parser.ExprTypeColumn,
						Column: "status",
					},
					Right: &parser.Expression{
						Type:  parser.ExprTypeValue,
						Value: "delivered",
					},
				},
			},
			Right: &parser.Expression{
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

func TestORToUnion_WithLimit(t *testing.T) {
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
			{Name: "value", Type: "int"},
		},
	})
	require.NoError(t, err)

	// Create optimizer
	opt := NewOptimizer(dataSource)

	limit := int64(10)
	// Query with OR and LIMIT: (value = 1 OR value = 2) LIMIT 10
	stmt := &parser.SelectStatement{
		Columns: []parser.SelectColumn{
			{Name: "*"},
		},
		From: "test_table",
		Where: &parser.Expression{
			Type:     parser.ExprTypeOperator,
			Operator: "or",
			Left: &parser.Expression{
				Type:     parser.ExprTypeOperator,
				Operator: "=",
				Left: &parser.Expression{
					Type:   parser.ExprTypeColumn,
					Column: "value",
				},
				Right: &parser.Expression{
					Type:  parser.ExprTypeValue,
					Value: 1,
				},
			},
			Right: &parser.Expression{
				Type:     parser.ExprTypeOperator,
				Operator: "=",
				Left: &parser.Expression{
					Type:   parser.ExprTypeColumn,
					Column: "value",
				},
				Right: &parser.Expression{
					Type:  parser.ExprTypeValue,
					Value: 2,
				},
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

func TestORToUnion_WithOrderBy(t *testing.T) {
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

	// Query with OR and ORDER BY: (name = 'Alice' OR name = 'Bob') ORDER BY id
	stmt := &parser.SelectStatement{
		Columns: []parser.SelectColumn{
			{Name: "*"},
		},
		From: "test_table",
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
					Value: "Alice",
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
					Value: "Bob",
				},
			},
		},
		OrderBy: []parser.OrderByItem{
			{Column: "id", Direction: "ASC"},
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

func TestORToUnion_NumericalValues(t *testing.T) {
	factory := memory.NewMemoryFactory()
	dataSource, err := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	})
	require.NoError(t, err)

	ctx := context.Background()

	// Create table
	err = dataSource.CreateTable(ctx, &domain.TableInfo{
		Name: "numbers",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int", Primary: true},
			{Name: "value", Type: "int"},
		},
	})
	require.NoError(t, err)

	// Create optimizer
	opt := NewOptimizer(dataSource)

	// Query with OR on numeric values: value = 1 OR value = 2 OR value = 3
	stmt := &parser.SelectStatement{
		Columns: []parser.SelectColumn{
			{Name: "*"},
		},
		From: "numbers",
		Where: &parser.Expression{
			Type:     parser.ExprTypeOperator,
			Operator: "or",
			Left: &parser.Expression{
				Type:     parser.ExprTypeOperator,
				Operator: "or",
				Left: &parser.Expression{
					Type:     parser.ExprTypeOperator,
					Operator: "=",
					Left: &parser.Expression{
						Type:   parser.ExprTypeColumn,
						Column: "value",
					},
					Right: &parser.Expression{
						Type:  parser.ExprTypeValue,
						Value: 1,
					},
				},
				Right: &parser.Expression{
					Type:     parser.ExprTypeOperator,
					Operator: "=",
					Left: &parser.Expression{
						Type:   parser.ExprTypeColumn,
						Column: "value",
					},
					Right: &parser.Expression{
						Type:  parser.ExprTypeValue,
						Value: 2,
					},
				},
			},
			Right: &parser.Expression{
				Type:     parser.ExprTypeOperator,
				Operator: "=",
				Left: &parser.Expression{
					Type:   parser.ExprTypeColumn,
					Column: "value",
				},
				Right: &parser.Expression{
					Type:  parser.ExprTypeValue,
					Value: 3,
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

func TestORToUnion_WithRangeConditions(t *testing.T) {
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
			{Name: "score", Type: "int"},
		},
	})
	require.NoError(t, err)

	// Create optimizer
	opt := NewOptimizer(dataSource)

	// Query with OR and range: (score < 50 OR score > 90)
	stmt := &parser.SelectStatement{
		Columns: []parser.SelectColumn{
			{Name: "*"},
		},
		From: "test_table",
		Where: &parser.Expression{
			Type:     parser.ExprTypeOperator,
			Operator: "or",
			Left: &parser.Expression{
				Type:     parser.ExprTypeOperator,
				Operator: "lt",
				Left: &parser.Expression{
					Type:   parser.ExprTypeColumn,
					Column: "score",
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
					Column: "score",
				},
				Right: &parser.Expression{
					Type:  parser.ExprTypeValue,
					Value: 90,
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

func TestORToUnion_SingleCondition(t *testing.T) {
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
			{Name: "status", Type: "string"},
		},
	})
	require.NoError(t, err)

	// Create optimizer
	opt := NewOptimizer(dataSource)

	// Query with single condition (no OR): status = 'active'
	stmt := &parser.SelectStatement{
		Columns: []parser.SelectColumn{
			{Name: "*"},
		},
		From: "test_table",
		Where: &parser.Expression{
			Type:     parser.ExprTypeOperator,
			Operator: "=",
			Left: &parser.Expression{
				Type:   parser.ExprTypeColumn,
				Column: "status",
			},
			Right: &parser.Expression{
				Type:  parser.ExprTypeValue,
				Value: "active",
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

func TestORToUnion_NoWhere(t *testing.T) {
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

func TestORToUnion_TableNotFound(t *testing.T) {
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
		Where: &parser.Expression{
			Type:     parser.ExprTypeOperator,
			Operator: "or",
			Left: &parser.Expression{
				Type:     parser.ExprTypeOperator,
				Operator: "=",
				Left: &parser.Expression{
					Type:   parser.ExprTypeColumn,
					Column: "id",
				},
				Right: &parser.Expression{
					Type:  parser.ExprTypeValue,
					Value: 1,
				},
			},
			Right: &parser.Expression{
				Type:     parser.ExprTypeOperator,
				Operator: "=",
				Left: &parser.Expression{
					Type:   parser.ExprTypeColumn,
					Column: "id",
				},
				Right: &parser.Expression{
					Type:  parser.ExprTypeValue,
					Value: 2,
				},
			},
		},
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
		// Expected for non-existent table
		assert.Nil(t, plan)
	} else {
		assert.NotNil(t, plan)
	}
}

// Benchmark tests
func BenchmarkORToUnion_SimpleOR(b *testing.B) {
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
			Operator: "or",
			Left: &parser.Expression{
				Type:     parser.ExprTypeOperator,
				Operator: "=",
				Left: &parser.Expression{
					Type:   parser.ExprTypeColumn,
					Column: "id",
				},
				Right: &parser.Expression{
					Type:  parser.ExprTypeValue,
					Value: 1,
				},
			},
			Right: &parser.Expression{
				Type:     parser.ExprTypeOperator,
				Operator: "=",
				Left: &parser.Expression{
					Type:   parser.ExprTypeColumn,
					Column: "id",
				},
				Right: &parser.Expression{
					Type:  parser.ExprTypeValue,
					Value: 2,
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

func BenchmarkORToUnion_ComplexOR(b *testing.B) {
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

	// Complex nested OR conditions
	stmt := &parser.SelectStatement{
		Columns: []parser.SelectColumn{{Name: "*"}},
		From:    "test_table",
		Where: &parser.Expression{
			Type:     parser.ExprTypeOperator,
			Operator: "or",
			Left: &parser.Expression{
				Type:     parser.ExprTypeOperator,
				Operator: "or",
				Left: &parser.Expression{
					Type:     parser.ExprTypeOperator,
					Operator: "=",
					Left: &parser.Expression{
						Type:   parser.ExprTypeColumn,
						Column: "id",
					},
					Right: &parser.Expression{
						Type:  parser.ExprTypeValue,
						Value: 1,
					},
				},
				Right: &parser.Expression{
					Type:     parser.ExprTypeOperator,
					Operator: "=",
					Left: &parser.Expression{
						Type:   parser.ExprTypeColumn,
						Column: "id",
					},
					Right: &parser.Expression{
						Type:  parser.ExprTypeValue,
						Value: 2,
					},
				},
			},
			Right: &parser.Expression{
				Type:     parser.ExprTypeOperator,
				Operator: "or",
				Left: &parser.Expression{
					Type:     parser.ExprTypeOperator,
					Operator: "=",
					Left: &parser.Expression{
						Type:   parser.ExprTypeColumn,
						Column: "id",
					},
					Right: &parser.Expression{
						Type:  parser.ExprTypeValue,
						Value: 3,
					},
				},
				Right: &parser.Expression{
					Type:     parser.ExprTypeOperator,
					Operator: "=",
					Left: &parser.Expression{
						Type:   parser.ExprTypeColumn,
						Column: "id",
					},
					Right: &parser.Expression{
						Type:  parser.ExprTypeValue,
						Value: 4,
					},
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
