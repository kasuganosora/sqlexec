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
	dataSource.Connect(ctx)

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
	dataSource.Connect(ctx)

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
	assert.Equal(t, 5, len(result.Rows), "Should have 5 rows (limited)")
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
	dataSource.Connect(ctx)

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

// TestPhysicalExecuteIntegration_ParallelScanWithLimit tests parallel scan with LIMIT
func TestPhysicalExecuteIntegration_ParallelScanWithLimit(t *testing.T) {
	factory := memory.NewMemoryFactory()
	dataSource, err := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	})
	require.NoError(t, err)

	ctx := context.Background()
	dataSource.Connect(ctx)

	// Create test table
	err = dataSource.CreateTable(ctx, &domain.TableInfo{
		Name: "limited_table",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int", Primary: true},
			{Name: "value", Type: "int"},
		},
	})
	require.NoError(t, err)

	// Insert 100 rows
	rows := make([]domain.Row, 100)
	for i := 0; i < 100; i++ {
		rows[i] = domain.Row{"value": i}
	}
	_, err = dataSource.Insert(ctx, "limited_table", rows, nil)
	require.NoError(t, err)

	// Create physical table scan with LIMIT (triggers parallel scan path)
	tableInfo, err := dataSource.GetTableInfo(ctx, "limited_table")
	require.NoError(t, err)

	limitInfo := NewLimitInfo(10, 0)
	scan := NewPhysicalTableScan("limited_table", tableInfo, dataSource, []domain.Filter{}, limitInfo)

	// Execute scan - should use parallel scan path (no filters + enableParallelScan + limitInfo)
	result, err := scan.Execute(ctx)
	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, 10, len(result.Rows), "Should have 10 rows (limited)")
}

// TestPhysicalExecuteIntegration_ProjectionSchema tests projection operator schema
func TestPhysicalExecuteIntegration_ProjectionSchema(t *testing.T) {
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

	// Create projection operator
	projection := &PhysicalProjection{
		Columns: []optimizer.ColumnInfo{
			{Name: "id", Type: "int"},
		},
		cost:     20.0,
		children: []PhysicalOperator{},
	}
	projection.SetChildren(child)

	// Verify schema
	schema := projection.Schema()
	assert.Equal(t, 1, len(schema), "Projection should have 1 column")
	assert.Equal(t, "id", schema[0].Name)
}

// TestPhysicalExecuteIntegration_FilterSchema tests filter operator schema
func TestPhysicalExecuteIntegration_FilterSchema(t *testing.T) {
	factory := memory.NewMemoryFactory()
	dataSource, err := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	})
	require.NoError(t, err)

	ctx := context.Background()
	dataSource.Connect(ctx)

	// Create test table
	err = dataSource.CreateTable(ctx, &domain.TableInfo{
		Name: "filter_schema_test",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int", Primary: true},
			{Name: "name", Type: "string"},
			{Name: "age", Type: "int"},
		},
	})
	require.NoError(t, err)

	// Insert test data
	_, err = dataSource.Insert(ctx, "filter_schema_test", []domain.Row{
		{"name": "Alice", "age": 25},
		{"name": "Bob", "age": 30},
	}, nil)
	require.NoError(t, err)

	tableInfo, err := dataSource.GetTableInfo(ctx, "filter_schema_test")
	require.NoError(t, err)

	// Create filter operator
	child := NewPhysicalTableScan("filter_schema_test", tableInfo, dataSource, []domain.Filter{}, nil)
	filter := NewPhysicalSelection([]*parser.Expression{}, []domain.Filter{}, child, nil)

	// Verify schema preserves child schema
	schema := filter.Schema()
	assert.Equal(t, 3, len(schema), "Filter should preserve all columns from child")
	assert.Equal(t, "id", schema[0].Name)
	assert.Equal(t, "name", schema[1].Name)
	assert.Equal(t, "age", schema[2].Name)
}

// TestPhysicalExecuteIntegration_AggregateSchema tests aggregate operator schema
func TestPhysicalExecuteIntegration_AggregateSchema(t *testing.T) {
	factory := memory.NewMemoryFactory()
	dataSource, err := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	})
	require.NoError(t, err)

	ctx := context.Background()
	dataSource.Connect(ctx)

	// Create test table
	err = dataSource.CreateTable(ctx, &domain.TableInfo{
		Name: "aggregate_schema_test",
		Columns: []domain.ColumnInfo{
			{Name: "category", Type: "string"},
			{Name: "amount", Type: "int"},
		},
	})
	require.NoError(t, err)

	// Insert test data
	_, err = dataSource.Insert(ctx, "aggregate_schema_test", []domain.Row{
		{"category": "A", "amount": 100},
		{"category": "B", "amount": 200},
		{"category": "A", "amount": 150},
	}, nil)
	require.NoError(t, err)

	tableInfo, err := dataSource.GetTableInfo(ctx, "aggregate_schema_test")
	require.NoError(t, err)

	// Create aggregate operator
	child := NewPhysicalTableScan("aggregate_schema_test", tableInfo, dataSource, []domain.Filter{}, nil)
	aggFuncs := []*optimizer.AggregationItem{
		{Type: optimizer.Sum, Alias: "total_amount"},
	}
	aggregate := NewPhysicalHashAggregate(aggFuncs, []string{"category"}, child)

	// Verify schema includes group by and aggregate columns
	schema := aggregate.Schema()
	assert.Equal(t, 2, len(schema), "Schema should have group by + aggregate columns")
	assert.Equal(t, "category", schema[0].Name)
	assert.Equal(t, "total_amount", schema[1].Name)
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
	dataSource.Connect(ctx)

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
	dataSource.Connect(ctx)

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
	assert.Equal(t, 500, len(result.Rows))
}

// TestPhysicalExecuteIntegration_ConcurrentExecution tests concurrent execution safety
func TestPhysicalExecuteIntegration_ConcurrentExecution(t *testing.T) {
	factory := memory.NewMemoryFactory()
	dataSource, err := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	})
	require.NoError(t, err)

	ctx := context.Background()
	dataSource.Connect(ctx)

	// Create test table
	err = dataSource.CreateTable(ctx, &domain.TableInfo{
		Name: "concurrent_test",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int", Primary: true, AutoIncrement: true},
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

// TestPhysicalExecuteIntegration_FilterExecution tests filter execution in serial scan
func TestPhysicalExecuteIntegration_FilterExecution(t *testing.T) {
	factory := memory.NewMemoryFactory()
	dataSource, err := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	})
	require.NoError(t, err)

	ctx := context.Background()
	dataSource.Connect(ctx)

	// Create test table
	err = dataSource.CreateTable(ctx, &domain.TableInfo{
		Name: "filter_test",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int", Primary: true, AutoIncrement: true},
			{Name: "age", Type: "int"},
			{Name: "status", Type: "string"},
		},
	})
	require.NoError(t, err)

	// Insert test data
	rows := []domain.Row{
		{"age": 25, "status": "active"},
		{"age": 30, "status": "inactive"},
		{"age": 35, "status": "active"},
		{"age": 20, "status": "active"},
		{"age": 40, "status": "inactive"},
	}
	_, err = dataSource.Insert(ctx, "filter_test", rows, nil)
	require.NoError(t, err)

	// Test single filter
	tableInfo, err := dataSource.GetTableInfo(ctx, "filter_test")
	require.NoError(t, err)

	filters := []domain.Filter{
		{Field: "status", Operator: "=", Value: "active"},
	}
	scan := NewPhysicalTableScan("filter_test", tableInfo, dataSource, filters, nil)

	// Disable parallel scan to test serial scan with filters
	scan.enableParallelScan = false

	result, err := scan.Execute(ctx)
	require.NoError(t, err)
	assert.Equal(t, 3, len(result.Rows), "Should have 3 active rows")

	// Test multiple filters (AND logic)
	filters = []domain.Filter{
		{Field: "status", Operator: "=", Value: "active"},
		{Field: "age", Operator: ">", Value: 25},
	}
	scan = NewPhysicalTableScan("filter_test", tableInfo, dataSource, filters, nil)
	scan.enableParallelScan = false

	result, err = scan.Execute(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, len(result.Rows), "Should have 1 row matching both filters")
}

// TestPhysicalExecuteIntegration_EmptyTable tests scanning empty table
func TestPhysicalExecuteIntegration_EmptyTable(t *testing.T) {
	factory := memory.NewMemoryFactory()
	dataSource, err := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	})
	require.NoError(t, err)

	ctx := context.Background()
	dataSource.Connect(ctx)

	// Create test table without data
	err = dataSource.CreateTable(ctx, &domain.TableInfo{
		Name: "empty_table",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int", Primary: true},
			{Name: "value", Type: "string"},
		},
	})
	require.NoError(t, err)

	tableInfo, err := dataSource.GetTableInfo(ctx, "empty_table")
	require.NoError(t, err)

	// Test empty table scan
	scan := NewPhysicalTableScan("empty_table", tableInfo, dataSource, []domain.Filter{}, nil)
	result, err := scan.Execute(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, len(result.Rows), "Should have 0 rows")

	t.Log("Empty table scan test passed")
}

// TestPhysicalExecuteIntegration_TableScanWithOffsetLimit tests scan with offset and limit
func TestPhysicalExecuteIntegration_TableScanWithOffsetLimit(t *testing.T) {
	factory := memory.NewMemoryFactory()
	dataSource, err := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	})
	require.NoError(t, err)

	ctx := context.Background()
	dataSource.Connect(ctx)

	// Create test table
	err = dataSource.CreateTable(ctx, &domain.TableInfo{
		Name: "offset_test",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int", Primary: true, AutoIncrement: true},
			{Name: "value", Type: "int"},
		},
	})
	require.NoError(t, err)

	// Insert 50 rows
	rows := make([]domain.Row, 50)
	for i := 1; i <= 50; i++ {
		rows[i-1] = domain.Row{"value": i * 10}
	}
	_, err = dataSource.Insert(ctx, "offset_test", rows, nil)
	require.NoError(t, err)

	// Test with offset and limit
	tableInfo, err := dataSource.GetTableInfo(ctx, "offset_test")
	require.NoError(t, err)

	limitInfo := NewLimitInfo(10, 20) // Skip 20, take 10
	scan := NewPhysicalTableScan("offset_test", tableInfo, dataSource, []domain.Filter{}, limitInfo)

	result, err := scan.Execute(ctx)
	require.NoError(t, err)
	assert.Equal(t, 10, len(result.Rows), "Should have 10 rows after offset 20")

	t.Log("Table scan with offset and limit test passed")
}
