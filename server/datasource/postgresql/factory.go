package postgresql

import (
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	sqlcommon "github.com/kasuganosora/sqlexec/server/datasource/sql"
)

// PostgreSQLFactory creates PostgreSQL datasource instances.
type PostgreSQLFactory struct{}

// NewPostgreSQLFactory creates a new PostgreSQLFactory.
func NewPostgreSQLFactory() *PostgreSQLFactory {
	return &PostgreSQLFactory{}
}

// GetType returns the datasource type.
func (f *PostgreSQLFactory) GetType() domain.DataSourceType {
	return domain.DataSourceTypePostgreSQL
}

// GetMetadata returns the driver metadata for information_schema.ENGINES
func (f *PostgreSQLFactory) GetMetadata() domain.DriverMetadata {
	return domain.DriverMetadata{
		Comment:      "Supports transactions, row-level locking, and foreign keys",
		Transactions: "YES",
		XA:           "YES",
		Savepoints:   "YES",
	}
}

// Create creates a new PostgreSQL datasource from config.
func (f *PostgreSQLFactory) Create(config *domain.DataSourceConfig) (domain.DataSource, error) {
	sqlCfg, err := sqlcommon.ParseSQLConfig(config)
	if err != nil {
		return nil, err
	}
	return NewPostgreSQLDataSource(config, sqlCfg)
}
