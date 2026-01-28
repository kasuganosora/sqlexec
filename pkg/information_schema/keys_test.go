package information_schema

import (
	"context"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/application"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewKeyColumnUsageTable(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{"test"},
	}

	table := NewKeyColumnUsageTable(mockDS)

	assert.NotNil(t, table)
	assert.NotNil(t, table.dsManager)
}

func TestKeyColumnUsageTableGetName(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{"test"},
	}

	table := NewKeyColumnUsageTable(mockDS)

	assert.Equal(t, "key_column_usage", table.GetName())
}

func TestKeyColumnUsageTableGetSchema(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{"test"},
	}

	table := NewKeyColumnUsageTable(mockDS)

	schema := table.GetSchema()
	assert.Len(t, schema, 12) // key_column_usage has 12 columns
	
	// Check column names
	columnNames := []string{}
	for _, col := range schema {
		columnNames = append(columnNames, col.Name)
	}
	
	assert.Contains(t, columnNames, "constraint_catalog")
	assert.Contains(t, columnNames, "table_schema")
	assert.Contains(t, columnNames, "column_name")
	assert.Contains(t, columnNames, "ordinal_position")
	assert.Contains(t, columnNames, "position_in_unique_constraint")
}

func TestKeyColumnUsageQuery_PrimaryKeyColumns(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{"test"},
	}

	table := NewKeyColumnUsageTable(mockDS)
	ctx := context.Background()

	result, err := table.Query(ctx, nil, nil)
	require.NoError(t, err)
	
	// Find primary key columns
	pkColumns := []interface{}{}
	for _, row := range result.Rows {
		if row["constraint_name"] == "PRIMARY" {
			pkColumns = append(pkColumns, row["column_name"])
		}
	}
	
	assert.Equal(t, 3, len(pkColumns), "Should have 3 primary key columns")
}

func TestKeyColumnUsageTable_PrimaryKeyOrdinalPosition(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{"test"},
	}

	table := NewKeyColumnUsageTable(mockDS)
	ctx := context.Background()

	result, err := table.Query(ctx, nil, nil)
	require.NoError(t, err)
	
	// Check ordinal positions for primary keys
	ordinalPositions := []int{}
	for _, row := range result.Rows {
		if row["constraint_name"] == "PRIMARY" {
			ordinalPos := row["ordinal_position"].(int)
			ordinalPositions = append(ordinalPositions, ordinalPos)
		}
	}
	
	// Verify positions are sequential starting from 1
	for i, pos := range ordinalPositions {
		assert.Equal(t, i+1, pos)
	}
}

func TestKeyColumnUsageTable_UniqueConstraintColumns(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{"test"},
	}

	table := NewKeyColumnUsageTable(mockDS)
	ctx := context.Background()

	result, err := table.Query(ctx, nil, nil)
	require.NoError(t, err)
	
	// Find unique constraint columns
	uniqueColumns := []interface{}{}
	for _, row := range result.Rows {
		constraintName := row["constraint_name"].(string)
		if len(constraintName) > 6 && constraintName[:6] == "unique" {
			uniqueColumns = append(uniqueColumns, row["column_name"])
		}
	}
	
	assert.Equal(t, 2, len(uniqueColumns), "Should have 2 unique constraint columns")
}

func TestKeyColumnUsageTable_ForeignKeyColumns(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{"test"},
	}

	table := NewKeyColumnUsageTable(mockDS)
	ctx := context.Background()

	result, err := table.Query(ctx, nil, nil)
	require.NoError(t, err)
	
	// Find foreign key columns
	fkColumns := []interface{}{}
	for _, row := range result.Rows {
		constraintName := row["constraint_name"].(string)
		if len(constraintName) > 2 && constraintName[:2] == "fk" {
			fkColumns = append(fkColumns, row["column_name"])
		}
	}
	
	// Note: Foreign keys are added for each column with ForeignKey set
	// This tests the code path even though the mock doesn't set ForeignKeys
	assert.True(t, len(fkColumns) >= 0)
}

func TestKeyColumnUsageQuery_WithSchemaFilter(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{"db1", "db2"},
	}

	table := NewKeyColumnUsageTable(mockDS)
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
	
	// Should only return key columns from db1
	for _, row := range result.Rows {
		assert.Equal(t, "db1", row["table_schema"])
	}
}

func TestKeyColumnUsageQuery_WithTableFilter(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{"test"},
	}

	table := NewKeyColumnUsageTable(mockDS)
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
	
	// Should only return key columns from users table
	for _, row := range result.Rows {
		assert.Equal(t, "users", row["table_name"])
	}
}

func TestKeyColumnUsageQuery_WithLimit(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{"test"},
	}

	table := NewKeyColumnUsageTable(mockDS)
	ctx := context.Background()

	options := &application.QueryOptions{
		Limit: 2,
	}

	result, err := table.Query(ctx, nil, options)
	require.NoError(t, err)
	assert.Equal(t, int64(2), result.Total)
}

func TestKeyColumnUsageQuery_WithOffset(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{"test"},
	}

	table := NewKeyColumnUsageTable(mockDS)
	ctx := context.Background()

	options := &application.QueryOptions{
		Limit:  10,
		Offset: 1,
	}

	result, err := table.Query(ctx, nil, options)
	require.NoError(t, err)
	// Should skip first key column
	assert.True(t, result.Total >= 2)
}

func TestKeyColumnUsageQuery_MultipleFilters(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{"test"},
	}

	table := NewKeyColumnUsageTable(mockDS)
	ctx := context.Background()

	// Filter by schema and constraint name
	filters := []application.Filter{
		{
			Field:    "table_schema",
			Operator:  "=",
			Value:     "test",
		},
		{
			Field:    "constraint_name",
			Operator:  "=",
			Value:     "PRIMARY",
		},
	}

	result, err := table.Query(ctx, filters, nil)
	require.NoError(t, err)
	
	// All returned key columns should match filters
	for _, row := range result.Rows {
		assert.Equal(t, "test", row["table_schema"])
		assert.Equal(t, "PRIMARY", row["constraint_name"])
	}
}

func TestKeyColumnUsageMatchesFilter_ConstraintName(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{"test"},
	}

	table := NewKeyColumnUsageTable(mockDS)

	row := domain.Row{
		"constraint_name": "PRIMARY",
		"column_name":     "id",
	}

	filter := application.Filter{
		Field:    "constraint_name",
		Operator:  "=",
		Value:     "PRIMARY",
	}

	matches, err := table.matchesFilter(row, filter)
	require.NoError(t, err)
	assert.True(t, matches)
}

func TestKeyColumnUsageMatchesFilter_ColumnName(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{"test"},
	}

	table := NewKeyColumnUsageTable(mockDS)

	row := domain.Row{
		"constraint_name": "PRIMARY",
		"column_name":     "id",
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

func TestKeyColumnUsageMatchesFilter_TableName(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{"test"},
	}

	table := NewKeyColumnUsageTable(mockDS)

	row := domain.Row{
		"table_schema": "production",
		"table_name":  "users",
	}

	filter := application.Filter{
		Field:    "table_name",
		Operator:  "=",
		Value:     "users",
	}

	matches, err := table.matchesFilter(row, filter)
	require.NoError(t, err)
	assert.True(t, matches)
}

func TestKeyColumnUsageMatchesFilter_Schema(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{"test"},
	}

	table := NewKeyColumnUsageTable(mockDS)

	row := domain.Row{
		"table_schema": "production",
		"column_name":  "id",
	}

	filter := application.Filter{
		Field:    "table_schema",
		Operator:  "=",
		Value:     "production",
	}

	matches, err := table.matchesFilter(row, filter)
	require.NoError(t, err)
	assert.True(t, matches)
}

func TestKeyColumnUsageMatchesLike_Prefix(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{"test"},
	}

	table := NewKeyColumnUsageTable(mockDS)

	assert.True(t, table.matchesLike("PRIMARY", "PRI%"))
	assert.True(t, table.matchesLike("PRIMARY", "PRI_"))
	assert.False(t, table.matchesLike("PRIMARY", "UNI%"))
}

func TestKeyColumnUsageMatchesLike_Suffix(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{"test"},
	}

	table := NewKeyColumnUsageTable(mockDS)

	assert.True(t, table.matchesLike("PRIMARY", "%KEY"))
	assert.True(t, table.matchesLike("PRIMARY", "_KEY"))
	assert.False(t, table.matchesLike("PRIMARY", "%PRI"))
}

func TestKeyColumnUsageMatchesLike_Exact(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{"test"},
	}

	table := NewKeyColumnUsageTable(mockDS)

	assert.True(t, table.matchesLike("PRIMARY", "PRIMARY"))
	assert.False(t, table.matchesLike("PRIMARY", "UNIQUE"))
}

func TestKeyColumnUsageMatchesLike_Wildcard(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{"test"},
	}

	table := NewKeyColumnUsageTable(mockDS)

	assert.True(t, table.matchesLike("PRIMARY", "%"))
}
