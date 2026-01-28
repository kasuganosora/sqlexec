package information_schema

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestKeyColumnUsageTableGetName(t *testing.T) {
	table := NewKeyColumnUsageTable(nil)
	assert.Equal(t, "key_column_usage", table.GetName())
}

func TestKeyColumnUsageTableGetSchema(t *testing.T) {
	table := NewKeyColumnUsageTable(nil)
	schema := table.GetSchema()
	assert.Len(t, schema, 12) // key_column_usage table has 12 columns
	
	// Check some key columns
	columnNames := []string{}
	for _, col := range schema {
		columnNames = append(columnNames, col.Name)
	}
	
	assert.Contains(t, columnNames, "constraint_catalog")
	assert.Contains(t, columnNames, "constraint_schema")
	assert.Contains(t, columnNames, "constraint_name")
	assert.Contains(t, columnNames, "table_catalog")
	assert.Contains(t, columnNames, "table_schema")
	assert.Contains(t, columnNames, "table_name")
	assert.Contains(t, columnNames, "column_name")
}
