package information_schema

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSchemataTableGetName(t *testing.T) {
	table := NewSchemataTable(nil)
	assert.Equal(t, "schemata", table.GetName())
}

func TestSchemataTableGetSchema(t *testing.T) {
	table := NewSchemataTable(nil)
	schema := table.GetSchema()
	assert.Len(t, schema, 5) // schemata table has 5 columns
	
	// Check some key columns
	columnNames := []string{}
	for _, col := range schema {
		columnNames = append(columnNames, col.Name)
	}
	
	assert.Contains(t, columnNames, "catalog_name")
	assert.Contains(t, columnNames, "schema_name")
	assert.Contains(t, columnNames, "default_character_set_name")
	assert.Contains(t, columnNames, "default_collation_name")
}
