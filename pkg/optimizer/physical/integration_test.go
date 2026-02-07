package physical

import (
	"context"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/optimizer"
	"github.com/kasuganosora/sqlexec/pkg/parser"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/resource/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPhysicalExecuteIntegration_IntegrationFull tests full integration of physical execution
func TestPhysicalExecuteIntegration_IntegrationFull(t *testing.T) {
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

	// Insert test data using correct API
	_, err = dataSource.Insert(ctx, "users", []domain.Row{
		{"name": "Alice", "age": 25, "email": "alice@example.com"},
		{"name": "Bob", "age": 30, "email": "bob@example.com"},
		{"name": "Charlie", "age": 35, "email": "charlie@example.com"},
	}, nil)
	require.NoError(t, err)

	// Create physical table scan
	tableInfo, err := dataSource.GetTableInfo(ctx, "users")
	require.NoError(t, err)

	scan := NewPhysicalTableScan("users", tableInfo, dataSource, []domain.Filter{}, nil)

	// Execute scan
	result, err := scan.Execute(ctx)
	require.NoError(t, err)
	assert.NotNil(t, result)

	// Verify result
	assert.Equal(t, 3, len(result.Rows), "Should have 3 rows")
	t.Logf("Table scan executed successfully, got %d rows", len(result.Rows))
}

// TestPhysicalExecuteIntegration_WithLimit tests execution with LIMIT
func TestPhysicalExecuteIntegration_WithLimit(t *testing.T) {
	factory := memory.NewMemoryFactory()
	dataSource, err := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	})
	require.NoError(t, err)

	ctx := context.Background()

	// Create test table
	err = dataSource.CreateTable(ctx, &domain.TableInfo{
		Name: "items",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int", Primary: true, AutoIncrement: true},
			{Name: "value", Type: "int"},
		},
	})
	require.NoError(t, err)

	// Insert 10 rows
	rows := make([]domain.Row, 10)
	for i := 1; i <= 10; i++ {
		rows[i-1] = domain.Row{"value": i * 10}
	}
	_, err = dataSource.Insert(ctx, "items", rows, nil)
	require.NoError(t, err)

	// Create physical table scan with LIMIT
	tableInfo, err := dataSource.GetTableInfo(ctx, "items")
	require.NoError(t, err)

	limitInfo := NewLimitInfo(5, 0)
	scan := NewPhysicalTableScan("items", tableInfo, dataSource, []domain.Filter{}, limitInfo)

	// Execute scan
	result, err := scan.Execute(ctx)
	require.NoError(t, err)
	assert.NotNil(t, result)

	// Verify result
	assert.Equal(t, 5, len(result.Rows), "Should have 5 rows due to LIMIT")
	t.Logf("Table scan with LIMIT executed successfully, got %d rows", len(result.Rows))
}

// TestPhysicalExecuteIntegration_EmptyTable tests execution on empty table
func TestPhysicalExecuteIntegration_EmptyTable(t *testing.T) {
	factory := memory.NewMemoryFactory()
	dataSource, err := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	})
	require.NoError(t, err)

	ctx := context.Background()

	// Create empty test table
	err = dataSource.CreateTable(ctx, &domain.TableInfo{
		Name: "empty_table",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int", Primary: true},
			{Name: "value", Type: "string"},
		},
	})
	require.NoError(t, err)

	// Create physical table scan
	tableInfo, err := dataSource.GetTableInfo(ctx, "empty_table")
	require.NoError(t, err)

	scan := NewPhysicalTableScan("empty_table", tableInfo, dataSource, []domain.Filter{}, nil)

	// Execute scan
	result, err := scan.Execute(ctx)
	require.NoError(t, err)
	assert.NotNil(t, result)

	// Verify result
	assert.Equal(t, 0, len(result.Rows), "Should have 0 rows for empty table")
	t.Logf("Empty table scan executed successfully, got %d rows", len(result.Rows))
}

// TestPhysicalExecuteIntegration_SchemaPropagation tests schema propagation
func TestPhysicalExecuteIntegration_SchemaPropagation(t *testing.T) {
	factory := memory.NewMemoryFactory()
	dataSource, err := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	})
	require.NoError(t, err)

	ctx := context.Background()

	// Create test table
	err = dataSource.CreateTable(ctx, &domain.TableInfo{
		Name: "test_schema",
		Columns: []domain.ColumnInfo{
			{Name: "col1", Type: "int", Nullable: false},
			{Name: "col2", Type: "string", Nullable: true},
			{Name: "col3", Type: "int", Nullable: true},
		},
	})
	require.NoError(t, err)

	// Insert test data
	_, err = dataSource.Insert(ctx, "test_schema", []domain.Row{
		{"col1": 1, "col2": "test", "col3": 100},
	}, nil)
	require.NoError(t, err)

	// Create physical table scan
	tableInfo, err := dataSource.GetTableInfo(ctx, "test_schema")
	require.NoError(t, err)

	scan := NewPhysicalTableScan("test_schema", tableInfo, dataSource, []domain.Filter{}, nil)

	// Verify schema before execution
	schema := scan.Schema()
	assert.Equal(t, 3, len(schema))
	assert.Equal(t, "col1", schema[0].Name)
	assert.Equal(t, "col2", schema[1].Name)
	assert.Equal(t, "col3", schema[2].Name)

	// Execute scan
	result, err := scan.Execute(ctx)
	require.NoError(t, err)
	assert.NotNil(t, result)

	// Verify result schema matches operator schema
	assert.Equal(t, len(schema), len(result.Columns))
	for i, col := range schema {
		assert.Equal(t, col.Name, result.Columns[i].Name)
	}

	t.Logf("Schema propagation test passed, schema has %d columns", len(schema))
}

// TestPhysicalExecuteIntegration_ParallelScan tests parallel scan execution
func TestPhysicalExecuteIntegration_ParallelScan(t *testing.T) {
	factory := memory.NewMemoryFactory()
	dataSource, err := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	})
	require.NoError(t, err)

	ctx := context.Background()

	// Create test table
	err = dataSource.CreateTable(ctx, &domain.TableInfo{
		Name: "large_table",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int", Primary: true},
			{Name: "data", Type: "string"},
		},
	})
	require.NoError(t, err)

	// Insert enough rows to trigger parallel scan (>= 100)
	rows := make([]domain.Row, 150)
	for i := 1; i <= 150; i++ {
		rows[i-1] = domain.Row{"data": string(rune('A' + (i-1)%26))}
	}
	_, err = dataSource.Insert(ctx, "large_table", rows, nil)
	require.NoError(t, err)

	// Create physical table scan (parallel scan should be enabled for >=100 rows)
	tableInfo, err := dataSource.GetTableInfo(ctx, "large_table")
	require.NoError(t, err)

	scan := NewPhysicalTableScan("large_table", tableInfo, dataSource, []domain.Filter{}, nil)

	// Check if parallel scan is enabled
	assert.True(t, scan.IsParallelScanEnabled(), "Parallel scan should be enabled for 150 rows")

	// Execute scan
	result, err := scan.Execute(ctx)
	require.NoError(t, err)
	assert.NotNil(t, result)

	// Verify result
	assert.Equal(t, 150, len(result.Rows), "Should have 150 rows")
	t.Logf("Parallel scan executed successfully, got %d rows", len(result.Rows))
}

// TestPhysicalExecuteIntegration_ProjectionSchema tests projection operator schema
func TestPhysicalExecuteIntegration_ProjectionSchema(t *testing.T) {
	// Create a mock child operator
	child := &PhysicalTableScan{
		TableName: "test_table",
		Columns: []optimizer.ColumnInfo{
			{Name: "id", Type: "int"},
			{Name: "name", Type: "string"},
			{Name: "age", Type: "int"},
		},
		cost:     100.0,
		children: []PhysicalOperator{},
	}

	// Create projection operator
	exprs := []*parser.Expression{
		{Type: parser.ExprTypeColumn, Column: "id"},
		{Type: parser.ExprTypeColumn, Column: "name"},
	}
	aliases := []string{"user_id", "user_name"}

	proj := NewPhysicalProjection(exprs, aliases, child)

	// Verify schema
	schema := proj.Schema()
	assert.Equal(t, 2, len(schema), "Projection should have 2 columns")
	assert.Equal(t, "user_id", schema[0].Name)
	assert.Equal(t, "user_name", schema[1].Name)
}

// TestPhysicalExecuteIntegration_SelectionSchema tests selection operator schema
func TestPhysicalExecuteIntegration_SelectionSchema(t *testing.T) {
	// Create a mock child operator
	child := &PhysicalTableScan{
		TableName: "test_table",
		Columns: []optimizer.ColumnInfo{
			{Name: "id", Type: "int"},
			{Name: "name", Type: "string"},
		},
		cost:     100.0,
		children: []PhysicalOperator{},
	}

	// Create selection operator
	selection := NewPhysicalSelection(
		[]*parser.Expression{
			{Type: parser.ExprTypeColumn, Column: "id"},
		},
		[]domain.Filter{},
		child,
		nil,
	)

	// Verify schema
	schema := selection.Schema()
	assert.Equal(t, 2, len(schema), "Selection should preserve child schema")
	assert.Equal(t, "id", schema[0].Name)
	assert.Equal(t, "name", schema[1].Name)
}

// TestPhysicalExecuteIntegration_LimitSchema tests limit operator schema
func TestPhysicalExecuteIntegration_LimitSchema(t *testing.T) {
	// Create a mock child operator
	child := &PhysicalTableScan{
		TableName: "test_table",
		Columns: []optimizer.ColumnInfo{
			{Name: "id", Type: "int"},
			{Name: "value", Type: "string"},
		},
		cost:     100.0,
		children: []PhysicalOperator{},
	}

	// Create limit operator
	limit := NewPhysicalLimit(10, 0, child)

	// Verify schema
	schema := limit.Schema()
	assert.Equal(t, 2, len(schema), "Limit should preserve child schema")
	assert.Equal(t, "id", schema[0].Name)
	assert.Equal(t, "value", schema[1].Name)
}

// TestPhysicalExecuteIntegration_ExplainOutput tests explain output format
func TestPhysicalExecuteIntegration_ExplainOutput(t *testing.T) {
	factory := memory.NewMemoryFactory()
	dataSource, err := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	})
	require.NoError(t, err)

	ctx := context.Background()

	// Create test table
	err = dataSource.CreateTable(ctx, &domain.TableInfo{
		Name: "explain_test",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int", Primary: true},
		},
	})
	require.NoError(t, err)

	// Create physical table scan
	tableInfo, err := dataSource.GetTableInfo(ctx, "explain_test")
	require.NoError(t, err)

	scan := NewPhysicalTableScan("explain_test", tableInfo, dataSource, []domain.Filter{}, nil)

	// Get explain output
	explain := scan.Explain()
	assert.NotEmpty(t, explain, "Explain should return non-empty string")
	assert.Contains(t, explain, "TableScan", "Explain should contain operator type")
	t.Logf("Explain output: %s", explain)
}

// TestPhysicalExecuteIntegration_CostCalculation tests cost calculation
func TestPhysicalExecuteIntegration_CostCalculation(t *testing.T) {
	factory := memory.NewMemoryFactory()
	dataSource, err := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	})
	require.NoError(t, err)

	ctx := context.Background()

	// Create test table
	err = dataSource.CreateTable(ctx, &domain.TableInfo{
		Name: "cost_test",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int", Primary: true},
			{Name: "value", Type: "int"},
		},
	})
	require.NoError(t, err)

	// Insert rows
	rows := make([]domain.Row, 500)
	for i := 1; i <= 500; i++ {
		rows[i-1] = domain.Row{"value": i}
	}
	_, err = dataSource.Insert(ctx, "cost_test", rows, nil)
	require.NoError(t, err)

	// Create physical table scan
	tableInfo, err := dataSource.GetTableInfo(ctx, "cost_test")
	require.NoError(t, err)

	scan := NewPhysicalTableScan("cost_test", tableInfo, dataSource, []domain.Filter{}, nil)

	// Check cost
	cost := scan.Cost()
	assert.Greater(t, cost, 0.0, "Cost should be positive")
	t.Logf("Table scan cost: %.2f", cost)

	// Execute scan
	result, err := scan.Execute(ctx)
	require.NoError(t, err)
	assert.NotNil(t, result)

	// Cost should correlate with row count
	// (This is a rough check as cost calculation is complex)
	assert.Greater(t, cost, float64(500/10), "Cost should scale with row count")
}

// TestPhysicalExecuteIntegration_BoundaryConditions tests boundary conditions
func TestPhysicalExecuteIntegration_BoundaryConditions(t *testing.T) {
	factory := memory.NewMemoryFactory()
	dataSource, err := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	})
	require.NoError(t, err)

	ctx := context.Background()

	// Create test table
	err = dataSource.CreateTable(ctx, &domain.TableInfo{
		Name: "boundary_test",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int", Primary: true},
		},
	})
	require.NoError(t, err)

	// Insert exactly 99 rows (just below parallel scan threshold)
	rows := make([]domain.Row, 99)
	for i := 1; i <= 99; i++ {
		rows[i-1] = domain.Row{"id": i}
	}
	_, err = dataSource.Insert(ctx, "boundary_test", rows, nil)
	require.NoError(t, err)

	// Create physical table scan with limit to control row count
	tableInfo, err := dataSource.GetTableInfo(ctx, "boundary_test")
	require.NoError(t, err)

	// Use limit to control estimated row count
	limitInfo99 := NewLimitInfo(99, 0)
	scan := NewPhysicalTableScan("boundary_test", tableInfo, dataSource, []domain.Filter{}, limitInfo99)

	// Execute scan
	result, err := scan.Execute(ctx)
	require.NoError(t, err)
	assert.Equal(t, 99, len(result.Rows))

	// Now test with 100 rows
	_, err = dataSource.Insert(ctx, "boundary_test", []domain.Row{
		{"id": 100},
	}, nil)
	require.NoError(t, err)

	limitInfo100 := NewLimitInfo(100, 0)
	scan2 := NewPhysicalTableScan("boundary_test", tableInfo, dataSource, []domain.Filter{}, limitInfo100)

	result2, err := scan2.Execute(ctx)
	require.NoError(t, err)
	assert.Equal(t, 100, len(result2.Rows))

	t.Logf("Boundary condition test passed")
}

// TestPhysicalExecuteIntegration_NullValues tests handling of NULL values
func TestPhysicalExecuteIntegration_NullValues(t *testing.T) {
	factory := memory.NewMemoryFactory()
	dataSource, err := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	})
	require.NoError(t, err)

	ctx := context.Background()

	// Create test table
	err = dataSource.CreateTable(ctx, &domain.TableInfo{
		Name: "null_test",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int", Primary: true},
			{Name: "value", Type: "string", Nullable: true},
		},
	})
	require.NoError(t, err)

	// Insert test data with NULL values
	_, err = dataSource.Insert(ctx, "null_test", []domain.Row{
		{"value": nil},
		{"value": "not null"},
	}, nil)
	require.NoError(t, err)

	// Create physical table scan
	tableInfo, err := dataSource.GetTableInfo(ctx, "null_test")
	require.NoError(t, err)

	scan := NewPhysicalTableScan("null_test", tableInfo, dataSource, []domain.Filter{}, nil)

	// Execute scan
	result, err := scan.Execute(ctx)
	require.NoError(t, err)
	assert.NotNil(t, result)

	// Verify result
	assert.Equal(t, 2, len(result.Rows))
	t.Logf("NULL value handling test passed, got %d rows", len(result.Rows))
}

// TestPhysicalExecuteIntegration_ConcurrentExecutions tests concurrent executions
func TestPhysicalExecuteIntegration_ConcurrentExecutions(t *testing.T) {
	factory := memory.NewMemoryFactory()
	dataSource, err := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	})
	require.NoError(t, err)

	ctx := context.Background()

	// Create test table
	err = dataSource.CreateTable(ctx, &domain.TableInfo{
		Name: "concurrent_test",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int", Primary: true},
			{Name: "value", Type: "string"},
		},
	})
	require.NoError(t, err)

	// Insert test data
	rows := make([]domain.Row, 50)
	for i := 1; i <= 50; i++ {
		rows[i-1] = domain.Row{"value": "test"}
	}
	_, err = dataSource.Insert(ctx, "concurrent_test", rows, nil)
	require.NoError(t, err)

	// Create multiple scanners and execute them concurrently
	tableInfo, err := dataSource.GetTableInfo(ctx, "concurrent_test")
	require.NoError(t, err)

	// Execute concurrent scans
	concurrency := 5
	results := make(chan *domain.QueryResult, concurrency)
	errors := make(chan error, concurrency)

	for i := 0; i < concurrency; i++ {
		go func() {
			scan := NewPhysicalTableScan("concurrent_test", tableInfo, dataSource, []domain.Filter{}, nil)
			result, err := scan.Execute(ctx)
			if err != nil {
				errors <- err
				return
			}
			results <- result
		}()
	}

	// Collect results
	for i := 0; i < concurrency; i++ {
		select {
		case result := <-results:
			assert.Equal(t, 50, len(result.Rows))
		case err := <-errors:
			t.Errorf("Concurrent execution failed: %v", err)
		}
	}

	t.Logf("Concurrent executions test passed with %d concurrent scans", concurrency)
}
