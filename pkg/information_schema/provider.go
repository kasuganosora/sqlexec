package information_schema

import (
	"strings"

	"github.com/kasuganosora/sqlexec/pkg/resource/application"
	"github.com/kasuganosora/sqlexec/pkg/virtual"
	"github.com/kasuganosora/sqlexec/server/acl"
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
	dsManager   *application.DataSourceManager
	aclManager  ACLManager
	vdbRegistry *virtual.VirtualDatabaseRegistry
	tables      map[string]virtual.VirtualTable
}

// NewProvider creates a new information_schema provider
func NewProvider(dsManager *application.DataSourceManager) virtual.VirtualTableProvider {
	p := &Provider{
		dsManager:  dsManager,
		aclManager: nil,
		tables:     make(map[string]virtual.VirtualTable),
	}
	p.initializeTables()
	return p
}

// NewProviderWithACL creates a new information_schema provider with ACL support
func NewProviderWithACL(dsManager *application.DataSourceManager, aclMgr ACLManager) virtual.VirtualTableProvider {
	p := &Provider{
		dsManager:  dsManager,
		aclManager: aclMgr,
		tables:     make(map[string]virtual.VirtualTable),
	}
	p.initializeTables()
	return p
}

// NewProviderWithRegistry creates a new information_schema provider with virtual database registry
func NewProviderWithRegistry(dsManager *application.DataSourceManager, aclMgr ACLManager, registry *virtual.VirtualDatabaseRegistry) virtual.VirtualTableProvider {
	p := &Provider{
		dsManager:   dsManager,
		aclManager:  aclMgr,
		vdbRegistry: registry,
		tables:      make(map[string]virtual.VirtualTable),
	}
	p.initializeTables()
	return p
}

// initializeTables registers all information_schema virtual tables
func (p *Provider) initializeTables() {
	// Register core information_schema tables
	p.tables["schemata"] = NewSchemataTable(p.dsManager, p.vdbRegistry)
	p.tables["tables"] = NewTablesTable(p.dsManager, p.vdbRegistry)
	p.tables["columns"] = NewColumnsTable(p.dsManager, p.vdbRegistry)
	p.tables["table_constraints"] = NewTableConstraintsTable(p.dsManager)
	p.tables["key_column_usage"] = NewKeyColumnUsageTable(p.dsManager)
	p.tables["views"] = NewViewsTable(p.dsManager)
	p.tables["collations"] = NewCollationsTable()
	p.tables["system_variables"] = NewSystemVariablesTable()
	p.tables["plugins"] = NewPluginsTable()
	p.tables["engines"] = NewEnginesTable(p.dsManager)

	// Register MySQL privilege tables (if ACL manager is available)
	if p.aclManager != nil {
		p.tables["user_privileges"] = NewUserPrivilegesTable(p.aclManager)
		p.tables["schema_privileges"] = NewSchemaPrivilegesTable(p.aclManager)
		p.tables["table_privileges"] = NewTablePrivilegesTable(p.aclManager)
		p.tables["column_privileges"] = NewColumnPrivilegesTable(p.aclManager)
	}
}

// GetVirtualTable returns a virtual table by name (case-insensitive)
func (p *Provider) GetVirtualTable(name string) (virtual.VirtualTable, error) {
	table, exists := p.tables[strings.ToLower(name)]
	if !exists {
		return nil, &TableNotFoundError{Name: name}
	}
	return table, nil
}

// ListVirtualTables returns all available virtual table names
func (p *Provider) ListVirtualTables() []string {
	names := make([]string, 0, len(p.tables))
	for key := range p.tables {
		names = append(names, key)
	}
	return names
}

// HasTable returns true if a virtual table with given name exists (case-insensitive)
func (p *Provider) HasTable(name string) bool {
	_, exists := p.tables[strings.ToLower(name)]
	return exists
}

// ListVirtualTablesForUser returns virtual table names visible to the specified user
// Privilege tables are only visible to users with GRANT OPTION
func (p *Provider) ListVirtualTablesForUser(user, host string) []string {
	names := make([]string, 0, len(p.tables))
	for key := range p.tables {
		// Filter out privilege tables for non-privileged users
		if isPrivilegeTable(key) && p.aclManager != nil {
			if !p.aclManager.HasGrantOption(user, host) {
				continue
			}
		}
		names = append(names, key)
	}
	return names
}

// isPrivilegeTable checks if a table is a privilege-related table
func isPrivilegeTable(name string) bool {
	privilegeTables := []string{
		"user_privileges",
		"schema_privileges",
		"table_privileges",
		"column_privileges",
	}
	lower := strings.ToLower(name)
	for _, t := range privilegeTables {
		if lower == t {
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
