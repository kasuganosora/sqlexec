package information_schema

import (
	"context"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/application"
	"github.com/stretchr/testify/assert"
)

// MockDataSourceManager is a simple mock for testing
type MockDataSourceManager struct {
	dataSources []string
}

func (m *MockDataSourceManager) List() []string {
	return m.dataSources
}

func (m *MockDataSourceManager) GetTables(ctx context.Context, name string) ([]string, error) {
	// Mock implementation: return some table names
	return []string{"users", "orders", "products"}, nil
}

func (m *MockDataSourceManager) GetTableInfo(ctx context.Context, dsName, tableName string) (*application.TableInfo, error) {
	// Mock implementation
	return &application.TableInfo{
		Name: tableName,
		Columns: []application.ColumnInfo{
			{Name: "id", Type: "INT", Primary: true},
			{Name: "name", Type: "VARCHAR(255)", Nullable: true},
			{Name: "email", Type: "VARCHAR(255)", Unique: true},
		},
	}, nil
}

func TestNewProvider(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{"db1", "db2"},
	}

	provider := NewProvider(mockDS)

	assert.NotNil(t, provider)
	assert.NotNil(t, provider.dsManager)
	assert.NotNil(t, provider.tables)
}

func TestProviderInitializeTables(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{"test"},
	}

	provider := NewProvider(mockDS)

	// Check that standard tables are registered
	tableNames := provider.ListVirtualTables()
	assert.Contains(t, tableNames, "schemata")
	assert.Contains(t, tableNames, "tables")
	assert.Contains(t, tableNames, "columns")
	assert.Contains(t, tableNames, "table_constraints")
	assert.Contains(t, tableNames, "key_column_usage")
}

func TestGetVirtualTable(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{"test"},
	}

	provider := NewProvider(mockDS)

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

func TestGetVirtualTable_NilProvider(t *testing.T) {
	var provider *Provider
	table, err := provider.GetVirtualTable("schemata")
	assert.Nil(t, table)
	assert.Error(t, err)
}

func TestListVirtualTables(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{"test"},
	}

	provider := NewProvider(mockDS)

	tables := provider.ListVirtualTables()

	assert.Len(t, tables, 5) // Should have 5 tables
	assert.Contains(t, tables, "schemata")
	assert.Contains(t, tables, "tables")
	assert.Contains(t, tables, "columns")
	assert.Contains(t, tables, "table_constraints")
	assert.Contains(t, tables, "key_column_usage")
}

func TestHasTable(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{"test"},
	}

	provider := NewProvider(mockDS)

	// Check existing table
	assert.True(t, provider.HasTable("schemata"))
	assert.True(t, provider.HasTable("tables"))

	// Check non-existent table
	assert.False(t, provider.HasTable("nonexistent"))
}

func TestProviderWithMultipleDataSources(t *testing.T) {
	mockDS := &MockDataSourceManager{
		dataSources: []string{"primary", "replica", "cache"},
	}

	provider := NewProvider(mockDS)

	// Should work with multiple data sources
	assert.NotNil(t, provider)
	assert.Len(t, provider.ListVirtualTables(), 5)
}
