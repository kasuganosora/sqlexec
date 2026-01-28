package information_schema

import (
	"context"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/application"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewTableConstraintsTable(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{"test"},
	}

	table := NewTableConstraintsTable(mockDS)

	assert.NotNil(t, table)
	assert.NotNil(t, table.dsManager)
}

func TestTableConstraintsTableGetName(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{"test"},
	}

	table := NewTableConstraintsTable(mockDS)

	assert.Equal(t, "table_constraints", table.GetName())
}

func TestTableConstraintsTableGetSchema(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{"test"},
	}

	table := NewTableConstraintsTable(mockDS)

	schema := table.GetSchema()
	assert.Len(t, schema, 5) // table_constraints has 5 columns
	
	// Check column names
	columnNames := []string{}
	for _, col := range schema {
		columnNames = append(columnNames, col.Name)
	}
	
	assert.Contains(t, columnNames, "constraint_catalog")
	assert.Contains(t, columnNames, "constraint_schema")
	assert.Contains(t, columnNames, "constraint_name")
	assert.Contains(t, columnNames, "table_schema")
	assert.Contains(t, columnNames, "table_name")
	assert.Contains(t, columnNames, "constraint_type")
}

func TestTableConstraintsQuery_PrimaryKey(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{"test"},
	}

	table := NewTableConstraintsTable(mockDS)
	ctx := context.Background()

	result, err := table.Query(ctx, nil, nil)
	require.NoError(t, err)
	
	// Find PRIMARY KEY constraint
	foundPK := false
	for _, row := range result.Rows {
		if row["constraint_type"] == "PRIMARY KEY" {
			foundPK = true
			assert.Equal(t, "PRIMARY", row["constraint_name"])
			break
		}
	}
	
	assert.True(t, foundPK, "Should have PRIMARY KEY constraint")
}

func TestTableConstraintsQuery_PrimaryKeyWithMultipleColumns(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{"test"},
	}

	table := NewTableConstraintsTable(mockDS)
	ctx := context.Background()

	result, err := table.Query(ctx, nil, nil)
	require.NoError(t, err)
	
	// Count PRIMARY KEY constraints
	pkCount := 0
	for _, row := range result.Rows {
		if row["constraint_type"] == "PRIMARY KEY" {
			pkCount++
		}
	}
	
	assert.Equal(t, 3, pkCount, "Should have 3 PRIMARY KEY constraints")
}

func TestTableConstraintsQuery_UniqueConstraints(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{"test"},
	}

	table := NewTableConstraintsTable(mockDS)
	ctx := context.Background()

	result, err := table.Query(ctx, nil, nil)
	require.NoError(t, err)
	
	// Count UNIQUE constraints
	uniqueCount := 0
	for _, row := range result.Rows {
		if row["constraint_type"] == "UNIQUE" {
			uniqueCount++
		}
	}
	
	assert.Equal(t, 2, uniqueCount, "Should have 2 UNIQUE constraints")
}

func TestTableConstraintsQuery_WithSchemaFilter(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{"db1", "db2"},
	}

	table := NewTableConstraintsTable(mockDS)
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
	
	// Should only return constraints from db1
	for _, row := range result.Rows {
		assert.Equal(t, "db1", row["table_schema"])
	}
}

func TestTableConstraintsQuery_WithTableFilter(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{"test"},
	}

	table := NewTableConstraintsTable(mockDS)
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
	
	// Should only return constraints from users table
	for _, row := range result.Rows {
		assert.Equal(t, "users", row["table_name"])
	}
}

func TestTableConstraintsQuery_WithConstraintTypeFilter(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{"test"},
	}

	table := NewTableConstraintsTable(mockDS)
	ctx := context.Background()

	// Filter by constraint_type = UNIQUE
	filters := []application.Filter{
		{
			Field:    "constraint_type",
			Operator:  "=",
			Value:     "UNIQUE",
		},
	}

	result, err := table.Query(ctx, filters, nil)
	require.NoError(t, err)
	
	// All returned constraints should be UNIQUE
	for _, row := range result.Rows {
		assert.Equal(t, "UNIQUE", row["constraint_type"])
	}
}

func TestTableConstraintsQuery_WithLimit(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{"test"},
	}

	table := NewTableConstraintsTable(mockDS)
	ctx := context.Background()

	options := &application.QueryOptions{
		Limit: 2,
	}

	result, err := table.Query(ctx, nil, options)
	require.NoError(t, err)
	assert.Equal(t, int64(2), result.Total)
}

func TestTableConstraintsQuery_WithOffset(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{"test"},
	}

	table := NewTableConstraintsTable(mockDS)
	ctx := context.Background()

	options := &application.QueryOptions{
		Limit:  10,
		Offset: 1,
	}

	result, err := table.Query(ctx, nil, options)
	require.NoError(t, err)
	// Should skip first constraint
	assert.True(t, result.Total >= 2)
}

func TestTableConstraintsQuery_MultipleFilters(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{"test"},
	}

	table := NewTableConstraintsTable(mockDS)
	ctx := context.Background()

	// Filter by schema and constraint type
	filters := []application.Filter{
		{
			Field:    "table_schema",
			Operator:  "=",
			Value:     "test",
		},
		{
			Field:    "constraint_type",
			Operator:  "=",
			Value:     "PRIMARY KEY",
		},
	}

	result, err := table.Query(ctx, filters, nil)
	require.NoError(t, err)
	
	// All returned constraints should match filters
	for _, row := range result.Rows {
		assert.Equal(t, "test", row["table_schema"])
		assert.Equal(t, "PRIMARY KEY", row["constraint_type"])
	}
}

func TestMatchesFilter_Schema(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{"test"},
	}

	table := NewTableConstraintsTable(mockDS)

	row := domain.Row{
		"table_schema":    "production",
		"constraint_type": "PRIMARY KEY",
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

func TestMatchesFilter_Table(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{"test"},
	}

	table := NewTableConstraintsTable(mockDS)

	row := domain.Row{
		"table_schema":    "production",
		"table_name":      "users",
		"constraint_type": "PRIMARY KEY",
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

func TestMatchesFilter_ConstraintType(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{"test"},
	}

	table := NewTableConstraintsTable(mockDS)

	row := domain.Row{
		"constraint_schema": "production",
		"table_name":      "users",
		"constraint_type":  "UNIQUE",
	}

	filter := application.Filter{
		Field:    "constraint_type",
		Operator:  "=",
		Value:     "UNIQUE",
	}

	matches, err := table.matchesFilter(row, filter)
	require.NoError(t, err)
	assert.True(t, matches)
}

func TestMatchesLike_Prefix(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{"test"},
	}

	table := NewTableConstraintsTable(mockDS)

	assert.True(t, table.matchesLike("PRIMARY", "PRI%"))
	assert.True(t, table.matchesLike("PRIMARY", "PRI_"))
	assert.False(t, table.matchesLike("PRIMARY", "UNI%"))
}

func TestMatchesLike_Suffix(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{"test"},
	}

	table := NewTableConstraintsTable(mockDS)

	assert.True(t, table.matchesLike("PRIMARY", "%KEY"))
	assert.True(t, table.matchesLike("PRIMARY", "_KEY"))
	assert.False(t, table.matchesLike("PRIMARY", "%PRI"))
}

func TestMatchesLike_Exact(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{"test"},
	}

	table := NewTableConstraintsTable(mockDS)

	assert.True(t, table.matchesLike("PRIMARY KEY", "PRIMARY KEY"))
	assert.False(t, table.matchesLike("PRIMARY KEY", "UNIQUE KEY"))
}

func TestMatchesLike_Wildcard(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{"test"},
	}

	table := NewTableConstraintsTable(mockDS)

	assert.True(t, table.matchesLike("PRIMARY KEY", "%"))
}
