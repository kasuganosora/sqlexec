package mysql

import (
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	sqlcommon "github.com/kasuganosora/sqlexec/server/datasource/sql"
)

// MySQLDataSource wraps SQLCommonDataSource with MySQL-specific dialect.
type MySQLDataSource struct {
	*sqlcommon.SQLCommonDataSource
}

// NewMySQLDataSource creates a new MySQL datasource.
func NewMySQLDataSource(dsCfg *domain.DataSourceConfig, sqlCfg *sqlcommon.SQLConfig) (*MySQLDataSource, error) {
	common := sqlcommon.NewSQLCommonDataSource(dsCfg, sqlCfg, &MySQLDialect{})
	return &MySQLDataSource{SQLCommonDataSource: common}, nil
}
