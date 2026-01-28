package information_schema

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTablesTableGetName(t *testing.T) {
	table := NewTablesTable(nil)
	assert.Equal(t, "tables", table.GetName())
}

func TestTablesTableGetSchema(t *testing.T) {
	table := NewTablesTable(nil)
	schema := table.GetSchema()
	assert.Len(t, schema, 21) // tables table has 21 columns
	
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
}
