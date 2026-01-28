package information_schema

import (
	"context"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/application"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewTablesTable(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{"test"},
	}

	table := NewTablesTable(mockDS)

	assert.NotNil(t, table)
	assert.NotNil(t, table.dsManager)
}

func TestTablesTableGetName(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{"test"},
	}

	table := NewTablesTable(mockDS)

	assert.Equal(t, "tables", table.GetName())
}

func TestTablesTableGetSchema(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{"test"},
	}

	table := NewTablesTable(mockDS)

	schema := table.GetSchema()
	assert.Len(t, schema, 19) // Tables table has 19 columns
	
	// Check some key columns
	columnNames := []string{}
	for _, col := range schema {
		columnNames = append(columnNames, col.Name)
	}
	
	assert.Contains(t, columnNames, "table_catalog")
	assert.Contains(t, columnNames, "table_schema")
	assert.Contains(t, columnNames, "table_name")
	assert.Contains(t, columnNames, "table_type")
	assert.Contains(t, columnNames, "engine")
	assert.Contains(t, columnNames, "table_rows")
}

func TestTablesQuery_AllTables(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{"db1", "db2"},
	}

	table := NewTablesTable(mockDS)
	ctx := context.Background()

	result, err := table.Query(ctx, nil, nil)
	require.NoError(t, err)
	assert.NotNil(t, result)
	
	// Should have tables from both data sources
	// db1 has 3 tables, db2 has 3 tables = 6 total
	assert.True(t, result.Total >= 6)
	assert.Len(t, result.Columns, 19)
}

func TestTablesQuery_WithSchemaFilter(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{"db1", "db2"},
	}

	table := NewTablesTable(mockDS)
	ctx := context.Background()

	// Filter by table_schema = db1
	filters := []application.Filter{
		{
			Field:    "table_schema",
			Operator:  "=",
			Value:     "db1",
		},
	}

	result, err := table.Query(ctx, filters, nil)
	require.NoError(t, err)
	
	// Should only return tables from db1
	for _, row := range result.Rows {
		assert.Equal(t, "db1", row["table_schema"])
	}
}

func TestTablesQuery_WithLimit(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{"test"},
	}

	table := NewTablesTable(mockDS)
	ctx := context.Background()

	options := &application.QueryOptions{
		Limit: 2,
	}

	result, err := table.Query(ctx, nil, options)
	require.NoError(t, err)
	assert.Equal(t, int64(2), result.Total)
}

func TestTablesQuery_WithOffset(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{"test"},
	}

	table := NewTablesTable(mockDS)
	ctx := context.Background()

	options := &application.QueryOptions{
		Limit:  10,
		Offset: 1,
	}

	result, err := table.Query(ctx, nil, options)
	require.NoError(t, err)
	assert.Equal(t, int64(2), result.Total) // Should skip first table
}

func TestTablesQuery_LikeFilter(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{"test"},
	}

	table := NewTablesTable(mockDS)
	ctx := context.Background()

	// Filter by table_name LIKE user%
	filters := []application.Filter{
		{
			Field:    "table_name",
			Operator:  "like",
			Value:     "user%",
		},
	}

	result, err := table.Query(ctx, filters, nil)
	require.NoError(t, err)
	
	// Should return tables starting with 'user'
	for _, row := range result.Rows {
		tableName, _ := row["table_name"].(string)
		assert.Contains(t, tableName, "user")
	}
}

func TestTablesQuery_TableTypeFilter(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{"test"},
	}

	table := NewTablesTable(mockDS)
	ctx := context.Background()

	// Filter by table_type = BASE TABLE
	filters := []application.Filter{
		{
			Field:    "table_type",
			Operator:  "=",
			Value:     "BASE TABLE",
		},
	}

	result, err := table.Query(ctx, filters, nil)
	require.NoError(t, err)
	
	// All returned tables should be BASE TABLE
	for _, row := range result.Rows {
		assert.Equal(t, "BASE TABLE", row["table_type"])
	}
}

func TestTablesQuery_MultipleFilters(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{"test"},
	}

	table := NewTablesTable(mockDS)
	ctx := context.Background()

	// Filter by schema and table type
	filters := []application.Filter{
		{
			Field:    "table_schema",
			Operator:  "=",
			Value:     "test",
		},
		{
			Field:    "table_type",
			Operator:  "=",
			Value:     "BASE TABLE",
		},
	}

	result, err := table.Query(ctx, filters, nil)
	require.NoError(t, err)
	assert.True(t, result.Total > 0)
}

func TestTablesQuery_EmptyDataSources(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{}, // No data sources
	}

	table := NewTablesTable(mockDS)
	ctx := context.Background()

	result, err := table.Query(ctx, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.Total)
}
