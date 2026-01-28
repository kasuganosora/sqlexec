package information_schema

import (
	"context"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/application"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewSchemataTable(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{"test"},
	}

	table := NewSchemataTable(mockDS)

	assert.NotNil(t, table)
	assert.NotNil(t, table.dsManager)
}

func TestSchemataTableGetName(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{"test"},
	}

	table := NewSchemataTable(mockDS)

	assert.Equal(t, "schemata", table.GetName())
}

func TestSchemataTableGetSchema(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{"test"},
	}

	table := NewSchemataTable(mockDS)

	schema := table.GetSchema()
	assert.Len(t, schema, 5)
	
	// Check expected columns
	columnNames := []string{}
	for _, col := range schema {
		columnNames = append(columnNames, col.Name)
	}
	
	assert.Contains(t, columnNames, "catalog_name")
	assert.Contains(t, columnNames, "schema_name")
	assert.Contains(t, columnNames, "default_character_set_name")
	assert.Contains(t, columnNames, "default_collation_name")
	assert.Contains(t, columnNames, "sql_path")
}

func TestSchemataQuery_AllSchemas(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{"production", "development", "test"},
	}

	table := NewSchemataTable(mockDS)
	ctx := context.Background()

	result, err := table.Query(ctx, nil, nil)
	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, int64(3), result.Total)
	assert.Len(t, result.Rows, 3)
	
	// Check first row (production database)
	assert.Equal(t, "production", result.Rows[0]["schema_name"])
	assert.Equal(t, "utf8mb4", result.Rows[0]["default_character_set_name"])
}

func TestSchemataQuery_WithFilter(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{"production", "development", "test"},
	}

	table := NewSchemataTable(mockDS)
	ctx := context.Background()

	// Filter by schema_name = production
	filters := []application.Filter{
		{
			Field:    "schema_name",
			Operator:  "=",
			Value:     "production",
		},
	}

	result, err := table.Query(ctx, filters, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(1), result.Total)
	assert.Len(t, result.Rows, 1)
	assert.Equal(t, "production", result.Rows[0]["schema_name"])
}

func TestSchemataQuery_WithLimit(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{"db1", "db2", "db3", "db4", "db5"},
	}

	table := NewSchemataTable(mockDS)
	ctx := context.Background()

	options := &application.QueryOptions{
		Limit: 2,
	}

	result, err := table.Query(ctx, nil, options)
	require.NoError(t, err)
	assert.Equal(t, int64(2), result.Total)
	assert.Len(t, result.Rows, 2)
}

func TestSchemataQuery_WithOffset(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{"db1", "db2", "db3"},
	}

	table := NewSchemataTable(mockDS)
	ctx := context.Background()

	options := &application.QueryOptions{
		Limit:  10,
		Offset: 1,
	}

	result, err := table.Query(ctx, nil, options)
	require.NoError(t, err)
	assert.Equal(t, int64(2), result.Total) // db2, db3
}

func TestSchemataQuery_LikeFilter(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{"production_db", "development_db", "staging_db"},
	}

	table := NewSchemataTable(mockDS)
	ctx := context.Background()

	// Filter by schema_name LIKE %_db%
	filters := []application.Filter{
		{
			Field:    "schema_name",
			Operator:  "like",
			Value:     "%_db%",
		},
	}

	result, err := table.Query(ctx, filters, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(3), result.Total) // Should match all 3
}

func TestSchemataQuery_EmptyResult(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{}, // No data sources
	}

	table := NewSchemataTable(mockDS)
	ctx := context.Background()

	result, err := table.Query(ctx, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.Total)
	assert.Len(t, result.Rows, 0)
}

func TestSchemataQuery_UnsupportedOperator(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{"test"},
	}

	table := NewSchemataTable(mockDS)
	ctx := context.Background()

	// Try unsupported operator
	filters := []application.Filter{
		{
			Field:    "schema_name",
			Operator:  ">",
			Value:     "test",
		},
	}

	_, err := table.Query(ctx, filters, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported filter operator")
}
