package information_schema

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTableConstraintsTableGetName(t *testing.T) {
	table := NewTableConstraintsTable(nil)
	assert.Equal(t, "table_constraints", table.GetName())
}

func TestTableConstraintsTableGetSchema(t *testing.T) {
	table := NewTableConstraintsTable(nil)
	schema := table.GetSchema()
	assert.Len(t, schema, 6) // table_constraints table has 6 columns

	// Check some key columns
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
