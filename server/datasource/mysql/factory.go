package mysql

import (
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	sqlcommon "github.com/kasuganosora/sqlexec/server/datasource/sql"
)

// MySQLFactory creates MySQL datasource instances.
type MySQLFactory struct{}

// NewMySQLFactory creates a new MySQLFactory.
func NewMySQLFactory() *MySQLFactory {
	return &MySQLFactory{}
}

// GetType returns the datasource type.
func (f *MySQLFactory) GetType() domain.DataSourceType {
	return domain.DataSourceTypeMySQL
}

// GetMetadata returns the driver metadata for information_schema.ENGINES
func (f *MySQLFactory) GetMetadata() domain.DriverMetadata {
	return domain.DriverMetadata{
		Comment:      "Supports transactions, row-level locking, and foreign keys",
		Transactions: "YES",
		XA:           "YES",
		Savepoints:   "YES",
	}
}

// Create creates a new MySQL datasource from config.
func (f *MySQLFactory) Create(config *domain.DataSourceConfig) (domain.DataSource, error) {
	sqlCfg, err := sqlcommon.ParseSQLConfig(config)
	if err != nil {
		return nil, err
	}
	return NewMySQLDataSource(config, sqlCfg)
}
