package information_schema

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTablesTableGetName(t *testing.T) {
	table := NewTablesTable(nil, nil)
	assert.Equal(t, "tables", table.GetName())
}

func TestTablesTableGetSchema(t *testing.T) {
	table := NewTablesTable(nil, nil)
	schema := table.GetSchema()
	assert.Len(t, schema, 22) // tables table has 22 columns (including table_attributes)
	
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
	assert.Contains(t, columnNames, "table_attributes")
}
