package information_schema

import (
	"github.com/kasuganosora/sqlexec/pkg/resource/application"
	"github.com/kasuganosora/sqlexec/server/acl"
	"github.com/kasuganosora/sqlexec/pkg/virtual"
)

// ACLManager interface for ACL operations (to avoid circular dependency)
type ACLManager interface {
	CheckPermission(user, host, permission, db, table, column string) bool
	HasGrantOption(user, host string) bool
	GetUsers() []*acl.User
	IsLoaded() bool
}

// Provider implements VirtualTableProvider for information_schema
// It manages all information_schema virtual tables
type Provider struct {
	dsManager *application.DataSourceManager
	aclManager ACLManager
	tables    map[string]virtual.VirtualTable
}

// NewProvider creates a new information_schema provider
func NewProvider(dsManager *application.DataSourceManager) virtual.VirtualTableProvider {
	p := &Provider{
		dsManager: dsManager,
		aclManager: nil,
		tables:    make(map[string]virtual.VirtualTable),
	}
	p.initializeTables()
	return p
}

// NewProviderWithACL creates a new information_schema provider with ACL support
func NewProviderWithACL(dsManager *application.DataSourceManager, aclMgr ACLManager) virtual.VirtualTableProvider {
	p := &Provider{
		dsManager: dsManager,
		aclManager: aclMgr,
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
	
	// Register MySQL privilege tables (if ACL manager is available)
	if p.aclManager != nil {
		p.tables["USER_PRIVILEGES"] = NewUserPrivilegesTable(p.aclManager)
		p.tables["SCHEMA_PRIVILEGES"] = NewSchemaPrivilegesTable(p.aclManager)
		p.tables["TABLE_PRIVILEGES"] = NewTablePrivilegesTable(p.aclManager)
		p.tables["COLUMN_PRIVILEGES"] = NewColumnPrivilegesTable(p.aclManager)
	}
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

// ListVirtualTablesForUser returns virtual table names visible to the specified user
// Privilege tables are only visible to users with GRANT OPTION
func (p *Provider) ListVirtualTablesForUser(user, host string) []string {
	names := make([]string, 0, len(p.tables))
	for name := range p.tables {
		// Filter out privilege tables for non-privileged users
		if isPrivilegeTable(name) && p.aclManager != nil {
			if !p.aclManager.HasGrantOption(user, host) {
				continue
			}
		}
		names = append(names, name)
	}
	return names
}

// isPrivilegeTable checks if a table is a privilege-related table
func isPrivilegeTable(name string) bool {
	privilegeTables := []string{
		"USER_PRIVILEGES",
		"SCHEMA_PRIVILEGES",
		"TABLE_PRIVILEGES",
		"COLUMN_PRIVILEGES",
	}
	for _, t := range privilegeTables {
		if name == t {
			return true
		}
	}
	return false
}

// TableNotFoundError is returned when a requested table doesn't exist
type TableNotFoundError struct {
	Name string
}

func (e *TableNotFoundError) Error() string {
	return "information_schema table '" + e.Name + "' does not exist"
}
