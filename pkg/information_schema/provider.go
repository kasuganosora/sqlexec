package information_schema

import (
	"github.com/kasuganosora/sqlexec/pkg/resource/application"
	"github.com/kasuganosora/sqlexec/pkg/virtual"
)

// Provider implements VirtualTableProvider for information_schema
// It manages all information_schema virtual tables
type Provider struct {
	dsManager *application.DataSourceManager
	tables    map[string]virtual.VirtualTable
}

// NewProvider creates a new information_schema provider
func NewProvider(dsManager *application.DataSourceManager) virtual.VirtualTableProvider {
	p := &Provider{
		dsManager: dsManager,
		tables:    make(map[string]virtual.VirtualTable),
	}
	p.initializeTables()
	return p
}

// initializeTables registers all information_schema virtual tables
func (p *Provider) initializeTables() {
	// Register core information_schema tables
	p.tables["schemata"] = NewSchemataTable(p.dsManager)
	p.tables["tables"] = NewTablesTable(p.dsManager)
	p.tables["columns"] = NewColumnsTable(p.dsManager)
	p.tables["table_constraints"] = NewTableConstraintsTable(p.dsManager)
	p.tables["key_column_usage"] = NewKeyColumnUsageTable(p.dsManager)
}

// GetVirtualTable returns a virtual table by name
func (p *Provider) GetVirtualTable(name string) (virtual.VirtualTable, error) {
	table, exists := p.tables[name]
	if !exists {
		return nil, &TableNotFoundError{Name: name}
	}
	return table, nil
}

// ListVirtualTables returns all available virtual table names
func (p *Provider) ListVirtualTables() []string {
	names := make([]string, 0, len(p.tables))
	for name := range p.tables {
		names = append(names, name)
	}
	return names
}

// HasTable returns true if a virtual table with given name exists
func (p *Provider) HasTable(name string) bool {
	_, exists := p.tables[name]
	return exists
}

// TableNotFoundError is returned when a requested table doesn't exist
type TableNotFoundError struct {
	Name string
}

func (e *TableNotFoundError) Error() string {
	return "information_schema table '" + e.Name + "' does not exist"
}
