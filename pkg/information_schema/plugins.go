package information_schema

import (
	"context"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/utils"
	"github.com/kasuganosora/sqlexec/pkg/virtual"
)

// PluginsTable represents information_schema.PLUGINS
// It lists all loaded plugins (MariaDB/MySQL compatibility)
type PluginsTable struct{}

// NewPluginsTable creates a new PluginsTable
func NewPluginsTable() virtual.VirtualTable {
	return &PluginsTable{}
}

// GetName returns table name
func (t *PluginsTable) GetName() string {
	return "PLUGINS"
}

// GetSchema returns table schema
func (t *PluginsTable) GetSchema() []domain.ColumnInfo {
	return []domain.ColumnInfo{
		{Name: "PLUGIN_NAME", Type: "varchar(64)", Nullable: false},
		{Name: "PLUGIN_VERSION", Type: "varchar(20)", Nullable: false},
		{Name: "PLUGIN_STATUS", Type: "varchar(10)", Nullable: false},
		{Name: "PLUGIN_TYPE", Type: "varchar(80)", Nullable: false},
		{Name: "PLUGIN_TYPE_VERSION", Type: "varchar(20)", Nullable: false},
		{Name: "PLUGIN_LIBRARY", Type: "varchar(64)", Nullable: true},
		{Name: "PLUGIN_LIBRARY_VERSION", Type: "varchar(20)", Nullable: true},
		{Name: "PLUGIN_AUTHOR", Type: "varchar(64)", Nullable: true},
		{Name: "PLUGIN_DESCRIPTION", Type: "longvarchar", Nullable: true},
		{Name: "PLUGIN_LICENSE", Type: "varchar(80)", Nullable: true},
		{Name: "LOAD_OPTION", Type: "varchar(64)", Nullable: false},
		{Name: "PLUGIN_MATURITY", Type: "varchar(12)", Nullable: true},
		{Name: "PLUGIN_AUTH_VERSION", Type: "varchar(64)", Nullable: true},
	}
}

// Query executes a query against PLUGINS table
func (t *PluginsTable) Query(ctx context.Context, filters []domain.Filter, options *domain.QueryOptions) (*domain.QueryResult, error) {
	// Get plugins list
	plugins := t.getPlugins()

	// Apply filters if provided
	var err error
	if len(filters) > 0 {
		plugins, err = utils.ApplyFilters(plugins, filters)
		if err != nil {
			return nil, err
		}
	}

	// Apply limit/offset if specified
	if options != nil && options.Limit > 0 {
		start := options.Offset
		if start < 0 {
			start = 0
		}
		end := start + int(options.Limit)
		if end > len(plugins) {
			end = len(plugins)
		}
		if start >= len(plugins) {
			plugins = []domain.Row{}
		} else {
			plugins = plugins[start:end]
		}
	}

	return &domain.QueryResult{
		Columns: t.GetSchema(),
		Rows:    plugins,
		Total:   int64(len(plugins)),
	}, nil
}

// getPlugins returns all available plugins
// Returns built-in plugins that are always available
func (t *PluginsTable) getPlugins() []domain.Row {
	// Define built-in plugins
	// Note: partition plugin is NOT included as this server doesn't support partitioning
	pluginDefs := []struct {
		name        string
		version     string
		status      string
		pluginType  string
		typeVersion string
		library     string
		libVersion  string
		author      string
		description string
		license     string
		loadOption  string
		maturity    string
		authVersion string
	}{
		{"MEMORY", "1.0", "ACTIVE", "STORAGE ENGINE", "1.0", "", "", "MySQL AB", "Hash based, stored in memory, useful for temporary tables", "GPL", "FORCE", "Stable", "1.0"},
		{"MyISAM", "1.0", "ACTIVE", "STORAGE ENGINE", "1.0", "", "", "MySQL AB", "MyISAM storage engine", "GPL", "FORCE", "Stable", "1.0"},
		{"InnoDB", "1.0", "ACTIVE", "STORAGE ENGINE", "1.0", "", "", "Oracle Corporation", "Supports transactions, row-level locking, and foreign keys", "GPL", "FORCE", "Stable", "1.0"},
		{"CSV", "1.0", "ACTIVE", "STORAGE ENGINE", "1.0", "", "", "MySQL AB", "CSV storage engine", "GPL", "FORCE", "Stable", "1.0"},
		{"ARIA", "1.0", "ACTIVE", "STORAGE ENGINE", "1.0", "", "", "MariaDB", "Crash-safe tables with MyISAM heritage", "GPL", "FORCE", "Stable", "1.0"},
		{"SEQUENCE", "1.0", "ACTIVE", "STORAGE ENGINE", "1.0", "", "", "MariaDB", "Generated tables filled with dynamic sequence of numbers", "GPL", "FORCE", "Stable", "1.0"},
		{"mysql_native_password", "1.0", "ACTIVE", "AUTHENTICATION", "1.0", "", "", "MySQL AB", "Native MySQL authentication", "GPL", "FORCE", "Stable", "1.0"},
		{"mysql_old_password", "1.0", "ACTIVE", "AUTHENTICATION", "1.0", "", "", "MySQL AB", "Old MySQL-4.0 authentication", "GPL", "FORCE", "Stable", "1.0"},
	}

	rows := make([]domain.Row, 0, len(pluginDefs))
	for _, p := range pluginDefs {
		row := domain.Row{
			"PLUGIN_NAME":            p.name,
			"PLUGIN_VERSION":         p.version,
			"PLUGIN_STATUS":          p.status,
			"PLUGIN_TYPE":            p.pluginType,
			"PLUGIN_TYPE_VERSION":    p.typeVersion,
			"PLUGIN_LIBRARY":         p.library,
			"PLUGIN_LIBRARY_VERSION": p.libVersion,
			"PLUGIN_AUTHOR":          p.author,
			"PLUGIN_DESCRIPTION":     p.description,
			"PLUGIN_LICENSE":         p.license,
			"LOAD_OPTION":            p.loadOption,
			"PLUGIN_MATURITY":        p.maturity,
			"PLUGIN_AUTH_VERSION":    p.authVersion,
		}
		rows = append(rows, row)
	}

	return rows
}
