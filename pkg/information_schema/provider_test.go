package information_schema

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewProvider(t *testing.T) {
	provider := NewProvider(nil)

	assert.NotNil(t, provider)
}

func TestProviderInitializeTables(t *testing.T) {
	provider := NewProvider(nil)

	// Check that standard tables are registered
	tableNames := provider.ListVirtualTables()
	assert.Contains(t, tableNames, "schemata")
	assert.Contains(t, tableNames, "tables")
	assert.Contains(t, tableNames, "columns")
	assert.Contains(t, tableNames, "table_constraints")
	assert.Contains(t, tableNames, "key_column_usage")
}

func TestGetVirtualTable(t *testing.T) {
	provider := NewProvider(nil)

	// Get existing table
	table, err := provider.GetVirtualTable("schemata")
	assert.NoError(t, err)
	assert.NotNil(t, table)
	assert.Equal(t, "schemata", table.GetName())

	// Get non-existent table
	_, err = provider.GetVirtualTable("nonexistent_table")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist")
}

func TestListVirtualTables(t *testing.T) {
	provider := NewProvider(nil)

	tables := provider.ListVirtualTables()

	assert.Len(t, tables, 10) // Should have 10 tables
	assert.Contains(t, tables, "schemata")
	assert.Contains(t, tables, "tables")
	assert.Contains(t, tables, "columns")
	assert.Contains(t, tables, "table_constraints")
	assert.Contains(t, tables, "key_column_usage")
	assert.Contains(t, tables, "views")
	assert.Contains(t, tables, "collations")
	assert.Contains(t, tables, "system_variables")
	assert.Contains(t, tables, "plugins")
	assert.Contains(t, tables, "engines")
}

func TestHasTable(t *testing.T) {
	provider := NewProvider(nil)

	// Check existing table
	assert.True(t, provider.HasTable("schemata"))
	assert.True(t, provider.HasTable("tables"))

	// Check case-insensitive lookup
	assert.True(t, provider.HasTable("PLUGINS"))
	assert.True(t, provider.HasTable("plugins"))
	assert.True(t, provider.HasTable("ENGINES"))
	assert.True(t, provider.HasTable("engines"))

	// Check non-existent table
	assert.False(t, provider.HasTable("nonexistent"))
}

func TestGetVirtualTable_CaseInsensitive(t *testing.T) {
	provider := NewProvider(nil)

	// Should work with any casing
	table, err := provider.GetVirtualTable("PLUGINS")
	assert.NoError(t, err)
	assert.NotNil(t, table)

	table, err = provider.GetVirtualTable("plugins")
	assert.NoError(t, err)
	assert.NotNil(t, table)

	table, err = provider.GetVirtualTable("Engines")
	assert.NoError(t, err)
	assert.NotNil(t, table)
}
