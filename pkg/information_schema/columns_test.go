package information_schema

import (
	"context"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/application"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewColumnsTable(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{"test"},
	}

	table := NewColumnsTable(mockDS)

	assert.NotNil(t, table)
	assert.NotNil(t, table.dsManager)
}

func TestColumnsTableGetName(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{"test"},
	}

	table := NewColumnsTable(mockDS)

	assert.Equal(t, "columns", table.GetName())
}

func TestColumnsTableGetSchema(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{"test"},
	}

	table := NewColumnsTable(mockDS)

	schema := table.GetSchema()
	assert.Len(t, schema, 23) // columns table has 23 columns
	
	// Check some key columns
	columnNames := []string{}
	for _, col := range schema {
		columnNames = append(columnNames, col.Name)
	}
	
	assert.Contains(t, columnNames, "table_catalog")
	assert.Contains(t, columnNames, "table_schema")
	assert.Contains(t, columnNames, "table_name")
	assert.Contains(t, columnNames, "column_name")
	assert.Contains(t, columnNames, "data_type")
	assert.Contains(t, columnNames, "column_key")
	assert.Contains(t, columnNames, "is_nullable")
}

func TestColumnsQuery_AllColumns(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{"test"},
	}

	table := NewColumnsTable(mockDS)
	ctx := context.Background()

	result, err := table.Query(ctx, nil, nil)
	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.True(t, result.Total >= 3) // users table has 3 columns
	
	// Check that column_key is set correctly
	for _, row := range result.Rows {
		columnKey := row["column_key"]
		if columnKey == "PRI" {
			assert.Equal(t, "select,insert,update,references", row["privileges"])
		}
	}
}

func TestColumnsQuery_WithTableSchemaFilter(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{"db1", "db2"},
	}

	table := NewColumnsTable(mockDS)
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
	
	// Should only return columns from db1
	for _, row := range result.Rows {
		assert.Equal(t, "db1", row["table_schema"])
	}
}

func TestColumnsQuery_WithTableNameFilter(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{"test"},
	}

	table := NewColumnsTable(mockDS)
	ctx := context.Background()

	// Filter by table_name = users
	filters := []application.Filter{
		{
			Field:    "table_name",
			Operator:  "=",
			Value:     "users",
		},
	}

	result, err := table.Query(ctx, filters, nil)
	require.NoError(t, err)
	
	// Should only return columns from users table
	for _, row := range result.Rows {
		assert.Equal(t, "users", row["table_name"])
	}
}

func TestColumnsQuery_PrimaryKeyColumn(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{"test"},
	}

	table := NewColumnsTable(mockDS)
	ctx := context.Background()

	// Filter for primary key columns
	filters := []application.Filter{
		{
			Field:    "column_key",
			Operator:  "=",
			Value:     "PRI",
		},
	}

	result, err := table.Query(ctx, filters, nil)
	require.NoError(t, err)
	
	// All returned columns should be primary keys
	for _, row := range result.Rows {
		assert.Equal(t, "PRI", row["column_key"])
	}
}

func TestColumnsQuery_NullableColumns(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{"test"},
	}

	table := NewColumnsTable(mockDS)
	ctx := context.Background()

	// Filter for nullable columns
	filters := []application.Filter{
		{
			Field:    "is_nullable",
			Operator:  "=",
			Value:     "YES",
		},
	}

	result, err := table.Query(ctx, filters, nil)
	require.NoError(t, err)
	
	// All returned columns should be nullable
	for _, row := range result.Rows {
		assert.Equal(t, "YES", row["is_nullable"])
	}
}

func TestColumnsQuery_WithLimit(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{"test"},
	}

	table := NewColumnsTable(mockDS)
	ctx := context.Background()

	options := &application.QueryOptions{
		Limit: 2,
	}

	result, err := table.Query(ctx, nil, options)
	require.NoError(t, err)
	assert.Equal(t, int64(2), result.Total)
}

func TestColumnsQuery_WithOffset(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{"test"},
	}

	table := NewColumnsTable(mockDS)
	ctx := context.Background()

	options := &application.QueryOptions{
		Limit:  10,
		Offset: 1,
	}

	result, err := table.Query(ctx, nil, options)
	require.NoError(t, err)
	// Should skip first column
	assert.True(t, result.Total >= 2)
}

func TestColumnsQuery_OrdinalPosition(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{"test"},
	}

	table := NewColumnsTable(mockDS)
	ctx := context.Background()

	result, err := table.Query(ctx, nil, nil)
	require.NoError(t, err)
	
	// Check ordinal positions are sequential
	ordinalPositions := []int{}
	for _, row := range result.Rows {
		ordinalPos := row["ordinal_position"].(int)
		ordinalPositions = append(ordinalPositions, ordinalPos)
	}
	
	// Verify positions start from 1 and are sequential
	for i, pos := range ordinalPositions {
		assert.Equal(t, i+1, pos)
	}
}

func TestColumnsQuery_MultipleFilters(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{"test"},
	}

	table := NewColumnsTable(mockDS)
	ctx := context.Background()

	// Filter by table and nullable
	filters := []application.Filter{
		{
			Field:    "table_name",
			Operator:  "=",
			Value:     "users",
		},
		{
			Field:    "is_nullable",
			Operator:  "=",
			Value:     "NO",
		},
	}

	result, err := table.Query(ctx, filters, nil)
	require.NoError(t, err)
	
	// All returned columns should be from users table and not nullable
	for _, row := range result.Rows {
		assert.Equal(t, "users", row["table_name"])
		assert.Equal(t, "NO", row["is_nullable"])
	}
}

func TestGetDataType_VARCHAR(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{"test"},
	}

	table := NewColumnsTable(mockDS)
	
	// Test VARCHAR(255)
	dataType := table.getDataType("VARCHAR(255)")
	assert.Equal(t, "varchar", dataType)
}

func TestGetDataType_INT(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{"test"},
	}

	table := NewColumnsTable(mockDS)
	
	// Test INT
	dataType := table.getDataType("INT")
	assert.Equal(t, "int", dataType)
}

func TestGetCharacterMaxLength_VARCHAR(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{"test"},
	}

	table := NewColumnsTable(mockDS)
	
	// Test VARCHAR(100)
	maxLen := table.getCharacterMaxLength("VARCHAR(100)")
	assert.Equal(t, int64(100), maxLen)
}

func TestGetCharacterMaxLength_Default(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{"test"},
	}

	table := NewColumnsTable(mockDS)
	
	// Test unknown type
	maxLen := table.getCharacterMaxLength("UNKNOWN")
	assert.Equal(t, int64(0), maxLen)
}

func TestGetNumericPrecision_INT(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{"test"},
	}

	table := NewColumnsTable(mockDS)
	
	// Test INT
	precision := table.getNumericPrecision("INT")
	assert.Equal(t, int64(10), precision)
}

func TestGetNumericPrecision_BIGINT(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{"test"},
	}

	table := NewColumnsTable(mockDS)
	
	// Test BIGINT
	precision := table.getNumericPrecision("BIGINT")
	assert.Equal(t, int64(19), precision)
}

func TestMatchesFilter_Equality(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{"test"},
	}

	table := NewColumnsTable(mockDS)

	row := domain.Row{
		"column_name": "id",
		"data_type":    "INT",
	}

	filter := application.Filter{
		Field:    "column_name",
		Operator:  "=",
		Value:     "id",
	}

	matches, err := table.matchesFilter(row, filter)
	require.NoError(t, err)
	assert.True(t, matches)
}

func TestMatchesFilter_Inequality(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{"test"},
	}

	table := NewColumnsTable(mockDS)

	row := domain.Row{
		"column_name": "id",
		"data_type":    "INT",
	}

	filter := application.Filter{
		Field:    "column_name",
		Operator:  "!=",
		Value:     "name",
	}

	matches, err := table.matchesFilter(row, filter)
	require.NoError(t, err)
	assert.True(t, matches)
}

func TestMatchesLike_Prefix(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{"test"},
	}

	table := NewColumnsTable(mockDS)

	assert.True(t, table.matchesLike("user_name", "user%"))
	assert.True(t, table.matchesLike("user_name", "user_"))
	assert.False(t, table.matchesLike("user_name", "admin%"))
}

func TestMatchesLike_Suffix(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{"test"},
	}

	table := NewColumnsTable(mockDS)

	assert.True(t, table.matchesLike("user_name", "%name"))
	assert.True(t, table.matchesLike("user_name", "_name"))
	assert.False(t, table.matchesLike("user_name", "%user"))
}

func TestMatchesLike_Exact(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{"test"},
	}

	table := NewColumnsTable(mockDS)

	assert.True(t, table.matchesLike("user_name", "user_name"))
	assert.False(t, table.matchesLike("user_name", "user_name_different"))
}

func TestMatchesLike_Wildcard(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{"test"},
	}

	table := NewColumnsTable(mockDS)

	assert.True(t, table.matchesLike("user_name", "%"))
}
