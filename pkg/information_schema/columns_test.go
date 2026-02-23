package information_schema

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestColumnsTableGetName(t *testing.T) {
	table := NewColumnsTable(nil, nil)
	assert.Equal(t, "columns", table.GetName())
}

func TestColumnsTableGetSchema(t *testing.T) {
	table := NewColumnsTable(nil, nil)
	schema := table.GetSchema()
	assert.Len(t, schema, 21) // columns table has 21 columns

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
