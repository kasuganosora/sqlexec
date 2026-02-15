package sql

import (
	"database/sql"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// Dialect encapsulates database-engine-specific behavior.
type Dialect interface {
	// DriverName returns the database/sql driver name ("mysql" or "postgres")
	DriverName() string

	// BuildDSN constructs the driver-specific connection string
	BuildDSN(dsCfg *domain.DataSourceConfig, sqlCfg *SQLConfig) (string, error)

	// QuoteIdentifier wraps a table/column name in dialect-specific quoting
	QuoteIdentifier(name string) string

	// Placeholder returns the parameter placeholder for the n-th parameter (1-based)
	Placeholder(n int) string

	// GetTablesQuery returns SQL to list all user tables in the current database
	GetTablesQuery() string

	// GetTableInfoQuery returns SQL to get column metadata; accepts table name as parameter
	GetTableInfoQuery() string

	// MapColumnType converts a database column type to a domain type string
	MapColumnType(dbTypeName string, scanType *sql.ColumnType) string

	// GetDatabaseName returns the virtual database name for this datasource
	GetDatabaseName(dsCfg *domain.DataSourceConfig, sqlCfg *SQLConfig) string
}
