package config_schema

import (
	"github.com/kasuganosora/sqlexec/pkg/resource/application"
	"github.com/kasuganosora/sqlexec/pkg/virtual"
)

// Provider implements VirtualTableProvider for the config virtual database
// It manages all config virtual tables (e.g., datasource)
type Provider struct {
	dsManager *application.DataSourceManager
	configDir string
	tables    map[string]virtual.VirtualTable
}

// NewProvider creates a new config provider
func NewProvider(dsManager *application.DataSourceManager, configDir string) *Provider {
	p := &Provider{
		dsManager: dsManager,
		configDir: configDir,
		tables:    make(map[string]virtual.VirtualTable),
	}
	p.initializeTables()
	return p
}

// initializeTables registers all config virtual tables
func (p *Provider) initializeTables() {
	p.tables["datasource"] = NewDatasourceTable(p.dsManager, p.configDir)
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

// TableNotFoundError is returned when a requested config table doesn't exist
type TableNotFoundError struct {
	Name string
}

func (e *TableNotFoundError) Error() string {
	return "config table '" + e.Name + "' does not exist"
}
