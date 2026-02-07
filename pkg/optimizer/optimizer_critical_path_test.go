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

// TestOptimizerCriticalPath_Select tests the critical path for SELECT optimization
func TestOptimizerCriticalPath_Select(t *testing.T) {
	factory := memory.NewMemoryFactory()
	dataSource, err := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	})
	require.NoError(t, err)

	ctx := context.Background()

	// Create test table
	err = dataSource.CreateTable(ctx, &domain.TableInfo{
		Name: "users",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int", Primary: true, AutoIncrement: true},
			{Name: "name", Type: "string", Nullable: false},
			{Name: "age", Type: "int", Nullable: true},
			{Name: "email", Type: "string", Nullable: true},
		},
	})
	require.NoError(t, err)

	// Create optimizer
	opt := NewOptimizer(dataSource)
	assert.NotNil(t, opt)

	// Create SELECT statement
	stmt := &parser.SQLStatement{
		Type: parser.SQLTypeSelect,
		Select: &parser.SelectStatement{
			Columns: []parser.SelectColumn{
				{Name: "id"},
				{Name: "name"},
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
					Value: 18,
				},
			},
		},
	}

	// Optimize
	plan, err := opt.Optimize(ctx, stmt)
	assert.NoError(t, err)
	assert.NotNil(t, plan)

	// Verify plan
	explain := plan.Explain()
	assert.NotEmpty(t, explain)
	t.Logf("Plan explanation:\n%s", explain)
}

// TestOptimizerCriticalPath_Aggregation tests optimization with aggregation
func TestOptimizerCriticalPath_Aggregation(t *testing.T) {
	factory := memory.NewMemoryFactory()
	dataSource, err := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	})
	require.NoError(t, err)

	ctx := context.Background()

	// Create test table
	err = dataSource.CreateTable(ctx, &domain.TableInfo{
		Name: "sales",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int", Primary: true},
			{Name: "product", Type: "string", Nullable: false},
			{Name: "category", Type: "string", Nullable: false},
			{Name: "amount", Type: "int", Nullable: false},
		},
	})
	require.NoError(t, err)

	// Create optimizer
	opt := NewOptimizer(dataSource)
	assert.NotNil(t, opt)

	// Create SELECT with GROUP BY and aggregation
	stmt := &parser.SQLStatement{
		Type: parser.SQLTypeSelect,
		Select: &parser.SelectStatement{
			Columns: []parser.SelectColumn{
				{Name: "category"},
				{Name: "COUNT(*)", Alias: "count"},
			},
			From: "sales",
			GroupBy: []string{"category"},
		},
	}

	// Optimize
	plan, err := opt.Optimize(ctx, stmt)
	assert.NoError(t, err)
	assert.NotNil(t, plan)

	// Verify plan
	explain := plan.Explain()
	assert.NotEmpty(t, explain)
	t.Logf("Aggregation Plan explanation:\n%s", explain)
}

// TestOptimizerCriticalPath_ErrorHandling tests error handling in critical paths
func TestOptimizerCriticalPath_ErrorHandling(t *testing.T) {
	factory := memory.NewMemoryFactory()
	dataSource, err := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	})
	require.NoError(t, err)

	ctx := context.Background()

	// Create optimizer
	opt := NewOptimizer(dataSource)
	assert.NotNil(t, opt)

	// Test: Non-existent table
	stmt := &parser.SQLStatement{
		Type: parser.SQLTypeSelect,
		Select: &parser.SelectStatement{
			Columns: []parser.SelectColumn{{Name: "*"}},
			From:    "non_existent_table",
		},
	}

	_, err = opt.Optimize(ctx, stmt)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "get table info failed")

	// Test: Unsupported SQL type
	stmt2 := &parser.SQLStatement{
		Type: parser.SQLType("UNKNOWN_TYPE"),
	}

	_, err = opt.Optimize(ctx, stmt2)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported SQL type")
}

// TestOptimizerCriticalPath_NoFromClause tests SELECT without FROM clause
func TestOptimizerCriticalPath_NoFromClause(t *testing.T) {
	factory := memory.NewMemoryFactory()
	dataSource, err := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	})
	require.NoError(t, err)

	ctx := context.Background()

	// Create optimizer
	opt := NewOptimizer(dataSource)
	assert.NotNil(t, opt)

	// Create SELECT without FROM (e.g., SELECT DATABASE())
	stmt := &parser.SQLStatement{
		Type: parser.SQLTypeSelect,
		Select: &parser.SelectStatement{
			Columns: []parser.SelectColumn{
				{Name: "1"},
			},
			From: "", // No FROM clause
		},
	}

	// Optimize
	plan, err := opt.Optimize(ctx, stmt)
	assert.NoError(t, err)
	assert.NotNil(t, plan)

	// Verify plan
	explain := plan.Explain()
	assert.NotEmpty(t, explain)
	t.Logf("No FROM clause Plan explanation:\n%s", explain)
}

// TestOptimizerCriticalPath_ComplexWhere tests complex WHERE conditions
func TestOptimizerCriticalPath_ComplexWhere(t *testing.T) {
	factory := memory.NewMemoryFactory()
	dataSource, err := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	})
	require.NoError(t, err)

	ctx := context.Background()

	// Create test table
	err = dataSource.CreateTable(ctx, &domain.TableInfo{
		Name: "products",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int", Primary: true},
			{Name: "name", Type: "string"},
			{Name: "price", Type: "int"},
			{Name: "quantity", Type: "int"},
		},
	})
	require.NoError(t, err)

	// Create optimizer
	opt := NewOptimizer(dataSource)
	assert.NotNil(t, opt)

	// Create SELECT with complex WHERE (AND condition)
	stmt := &parser.SQLStatement{
		Type: parser.SQLTypeSelect,
		Select: &parser.SelectStatement{
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
						Value: 100,
					},
				},
				Right: &parser.Expression{
					Type:     parser.ExprTypeOperator,
					Operator: "lt",
					Left: &parser.Expression{
						Type:   parser.ExprTypeColumn,
						Column: "quantity",
					},
					Right: &parser.Expression{
						Type:  parser.ExprTypeValue,
						Value: 50,
					},
				},
			},
		},
	}

	// Optimize
	plan, err := opt.Optimize(ctx, stmt)
	assert.NoError(t, err)
	assert.NotNil(t, plan)

	// Verify plan
	explain := plan.Explain()
	assert.NotEmpty(t, explain)
	t.Logf("Complex WHERE Plan explanation:\n%s", explain)
}

// TestOptimizerCriticalPath_WithAlias tests SELECT with column aliases
func TestOptimizerCriticalPath_WithAlias(t *testing.T) {
	factory := memory.NewMemoryFactory()
	dataSource, err := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	})
	require.NoError(t, err)

	ctx := context.Background()

	// Create test table
	err = dataSource.CreateTable(ctx, &domain.TableInfo{
		Name: "users",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int", Primary: true},
			{Name: "first_name", Type: "string"},
			{Name: "last_name", Type: "string"},
		},
	})
	require.NoError(t, err)

	// Create optimizer
	opt := NewOptimizer(dataSource)
	assert.NotNil(t, opt)

	// Create SELECT with aliases
	stmt := &parser.SQLStatement{
		Type: parser.SQLTypeSelect,
		Select: &parser.SelectStatement{
			Columns: []parser.SelectColumn{
				{Name: "id"},
				{Name: "first_name", Alias: "fname"},
				{Name: "last_name", Alias: "lname"},
			},
			From: "users",
		},
	}

	// Optimize
	plan, err := opt.Optimize(ctx, stmt)
	assert.NoError(t, err)
	assert.NotNil(t, plan)

	// Verify plan
	explain := plan.Explain()
	assert.NotEmpty(t, explain)
	t.Logf("Alias Plan explanation:\n%s", explain)
}

// TestOptimizer_CostModel tests that cost model is properly initialized
func TestOptimizer_CostModel(t *testing.T) {
	factory := memory.NewMemoryFactory()
	dataSource, err := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	})
	require.NoError(t, err)

	// Create optimizer
	opt := NewOptimizer(dataSource)

	// Verify cost model is initialized
	assert.NotNil(t, opt.costModel)
	assert.NotNil(t, opt.rules)
	assert.NotNil(t, opt.dataSource)
}

// TestOptimizer_RuleSet tests that rule set is properly initialized
func TestOptimizer_RuleSet(t *testing.T) {
	factory := memory.NewMemoryFactory()
	dataSource, err := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	})
	require.NoError(t, err)

	// Create optimizer
	opt := NewOptimizer(dataSource)

	// Verify rule set is initialized with default rules
	assert.NotNil(t, opt.rules)
	assert.True(t, len(opt.rules) > 0, "Default rule set should contain rules")

	t.Logf("Rule set contains %d rules", len(opt.rules))
}

// TestOptimizerCriticalPath_MultipleOrderBy tests multiple ORDER BY columns
func TestOptimizerCriticalPath_MultipleOrderBy(t *testing.T) {
	factory := memory.NewMemoryFactory()
	dataSource, err := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	})
	require.NoError(t, err)

	ctx := context.Background()

	// Create test table
	err = dataSource.CreateTable(ctx, &domain.TableInfo{
		Name: "multi_sort",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int", Primary: true},
			{Name: "category", Type: "string"},
			{Name: "name", Type: "string"},
		},
	})
	require.NoError(t, err)

	// Create optimizer
	opt := NewOptimizer(dataSource)

	// Create SELECT with multiple ORDER BY
	stmt := &parser.SQLStatement{
		Type: parser.SQLTypeSelect,
		Select: &parser.SelectStatement{
			Columns: []parser.SelectColumn{{Name: "*"}},
			From:    "multi_sort",
		OrderBy: []parser.OrderByItem{
			{Column: "category", Direction: "ASC"},
			{Column: "name", Direction: "DESC"},
		},
		},
	}

	// Optimize
	plan, err := opt.Optimize(ctx, stmt)
	assert.NoError(t, err)
	assert.NotNil(t, plan)

	// Verify plan
	explain := plan.Explain()
	assert.NotEmpty(t, explain)
	t.Logf("Multiple ORDER BY Plan explanation:\n%s", explain)
}

// TestOptimizerCriticalPath_WithLimitOffset tests with LIMIT and OFFSET
func TestOptimizerCriticalPath_WithLimitOffset(t *testing.T) {
	factory := memory.NewMemoryFactory()
	dataSource, err := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	})
	require.NoError(t, err)

	ctx := context.Background()

	// Create test table
	err = dataSource.CreateTable(ctx, &domain.TableInfo{
		Name: "test_offset",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int", Primary: true},
			{Name: "name", Type: "string"},
		},
	})
	require.NoError(t, err)

	// Create optimizer
	opt := NewOptimizer(dataSource)

	// Create SELECT with LIMIT and OFFSET
	stmt := &parser.SQLStatement{
		Type: parser.SQLTypeSelect,
		Select: &parser.SelectStatement{
			Columns: []parser.SelectColumn{{Name: "id"}},
			From:    "test_offset",
			Limit:   ptrInt64(10),
			Offset:  ptrInt64(5),
		},
	}

	// Optimize
	plan, err := opt.Optimize(ctx, stmt)
	assert.NoError(t, err)
	assert.NotNil(t, plan)

	// Verify plan
	explain := plan.Explain()
	assert.NotEmpty(t, explain)
	t.Logf("LIMIT/OFFSET Plan explanation:\n%s", explain)
}

// TestOptimizerCriticalPath_WithWildCard tests with wildcard select
func TestOptimizerCriticalPath_WithWildCard(t *testing.T) {
	factory := memory.NewMemoryFactory()
	dataSource, err := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	})
	require.NoError(t, err)

	ctx := context.Background()

	// Create test table
	err = dataSource.CreateTable(ctx, &domain.TableInfo{
		Name: "wildcard_test",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int", Primary: true},
			{Name: "col1", Type: "string"},
			{Name: "col2", Type: "string"},
		},
	})
	require.NoError(t, err)

	// Create optimizer
	opt := NewOptimizer(dataSource)

	// Create SELECT with wildcard
	stmt := &parser.SQLStatement{
		Type: parser.SQLTypeSelect,
		Select: &parser.SelectStatement{
			Columns: []parser.SelectColumn{{Name: "*"}},
			From:    "wildcard_test",
		},
	}

	// Optimize
	plan, err := opt.Optimize(ctx, stmt)
	assert.NoError(t, err)
	assert.NotNil(t, plan)

	// Verify plan
	explain := plan.Explain()
	assert.NotEmpty(t, explain)
	t.Logf("Wildcard select Plan explanation:\n%s", explain)
}

// TestOptimizerCriticalPath_EdgeCaseEmptyWhere tests with empty WHERE clause
func TestOptimizerCriticalPath_EdgeCaseEmptyWhere(t *testing.T) {
	factory := memory.NewMemoryFactory()
	dataSource, err := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	})
	require.NoError(t, err)

	ctx := context.Background()

	// Create test table
	err = dataSource.CreateTable(ctx, &domain.TableInfo{
		Name: "empty_where_test",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int", Primary: true},
		},
	})
	require.NoError(t, err)

	// Create optimizer
	opt := NewOptimizer(dataSource)

	// Create SELECT with nil WHERE
	stmt := &parser.SQLStatement{
		Type: parser.SQLTypeSelect,
		Select: &parser.SelectStatement{
			Columns: []parser.SelectColumn{{Name: "id"}},
			From:    "empty_where_test",
			Where:   nil, // No WHERE clause
		},
	}

	// Optimize
	plan, err := opt.Optimize(ctx, stmt)
	assert.NoError(t, err)
	assert.NotNil(t, plan)

	// Verify plan
	explain := plan.Explain()
	assert.NotEmpty(t, explain)
	t.Logf("Empty WHERE Plan explanation:\n%s", explain)
}

// TestOptimizerCriticalPath_NullHandling tests NULL value handling
func TestOptimizerCriticalPath_NullHandling(t *testing.T) {
	factory := memory.NewMemoryFactory()
	dataSource, err := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	})
	require.NoError(t, err)

	ctx := context.Background()

	// Create test table with nullable columns
	err = dataSource.CreateTable(ctx, &domain.TableInfo{
		Name: "nullable_test",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int", Primary: true},
			{Name: "value", Type: "string", Nullable: true},
		},
	})
	require.NoError(t, err)

	// Create optimizer
	opt := NewOptimizer(dataSource)

	// Create simple SELECT
	stmt := &parser.SQLStatement{
		Type: parser.SQLTypeSelect,
		Select: &parser.SelectStatement{
			Columns: []parser.SelectColumn{{Name: "*"}},
			From:    "nullable_test",
		},
	}

	// Optimize
	plan, err := opt.Optimize(ctx, stmt)
	assert.NoError(t, err)
	assert.NotNil(t, plan)

	// Verify plan
	explain := plan.Explain()
	assert.NotEmpty(t, explain)
	t.Logf("NULL handling test Plan explanation:\n%s", explain)
}

// Helper function to create pointer to int64
func ptrInt64(i int64) *int64 {
	return &i
}
