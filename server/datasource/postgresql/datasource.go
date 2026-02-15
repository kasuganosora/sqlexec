package postgresql

import (
	_ "github.com/lib/pq" // PostgreSQL driver

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	sqlcommon "github.com/kasuganosora/sqlexec/server/datasource/sql"
)

// PostgreSQLDataSource wraps SQLCommonDataSource with PostgreSQL-specific dialect.
type PostgreSQLDataSource struct {
	*sqlcommon.SQLCommonDataSource
}

// NewPostgreSQLDataSource creates a new PostgreSQL datasource.
func NewPostgreSQLDataSource(dsCfg *domain.DataSourceConfig, sqlCfg *sqlcommon.SQLConfig) (*PostgreSQLDataSource, error) {
	common := sqlcommon.NewSQLCommonDataSource(dsCfg, sqlCfg, &PostgreSQLDialect{})
	return &PostgreSQLDataSource{SQLCommonDataSource: common}, nil
}
