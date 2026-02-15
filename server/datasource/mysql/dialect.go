package mysql

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	sqlcommon "github.com/kasuganosora/sqlexec/server/datasource/sql"
)

// MySQLDialect implements sql.Dialect for MySQL.
type MySQLDialect struct{}

func (d *MySQLDialect) DriverName() string { return "mysql" }

func (d *MySQLDialect) BuildDSN(dsCfg *domain.DataSourceConfig, sqlCfg *sqlcommon.SQLConfig) (string, error) {
	port := dsCfg.Port
	if port <= 0 {
		port = 3306
	}

	cfg := mysqldriver.NewConfig()
	cfg.User = dsCfg.Username
	cfg.Passwd = dsCfg.Password
	cfg.Net = "tcp"
	cfg.Addr = fmt.Sprintf("%s:%d", dsCfg.Host, port)
	cfg.DBName = dsCfg.Database
	cfg.AllowNativePasswords = true
	cfg.Collation = sqlCfg.Collation
	cfg.Params = map[string]string{
		"charset": sqlCfg.Charset,
	}

	if sqlCfg.ParseTime != nil && *sqlCfg.ParseTime {
		cfg.ParseTime = true
	}

	if sqlCfg.ConnectTimeout > 0 {
		cfg.Timeout = time.Duration(sqlCfg.ConnectTimeout) * time.Second
	}

	// TLS
	switch strings.ToLower(sqlCfg.SSLMode) {
	case "true", "required", "require":
		cfg.TLSConfig = "true"
	case "skip-verify", "preferred":
		cfg.TLSConfig = "skip-verify"
	case "false", "disable", "":
		cfg.TLSConfig = "false"
	default:
		cfg.TLSConfig = sqlCfg.SSLMode
	}

	return cfg.FormatDSN(), nil
}

func (d *MySQLDialect) QuoteIdentifier(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

func (d *MySQLDialect) Placeholder(n int) string {
	return "?"
}

func (d *MySQLDialect) GetTablesQuery() string {
	return "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_TYPE = 'BASE TABLE'"
}

func (d *MySQLDialect) GetTableInfoQuery() string {
	return `SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_KEY, COLUMN_DEFAULT, EXTRA
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?
ORDER BY ORDINAL_POSITION`
}

func (d *MySQLDialect) MapColumnType(dbTypeName string, scanType *sql.ColumnType) string {
	t := strings.ToLower(dbTypeName)

	// Handle tinyint(1) as bool
	if t == "tinyint(1)" {
		return "bool"
	}

	// Strip parenthesized parameters: varchar(255) -> varchar
	if idx := strings.Index(t, "("); idx >= 0 {
		t = t[:idx]
	}
	t = strings.TrimSpace(t)

	// Handle unsigned suffix
	t = strings.TrimSuffix(t, " unsigned")

	switch t {
	case "tinyint", "smallint", "mediumint", "int", "integer", "bigint":
		return "int"
	case "float":
		return "float64"
	case "double", "decimal", "numeric", "real":
		return "float64"
	case "varchar", "char", "text", "tinytext", "mediumtext", "longtext", "enum", "set":
		return "string"
	case "blob", "tinyblob", "mediumblob", "longblob", "binary", "varbinary":
		return "string"
	case "date":
		return "date"
	case "time":
		return "time"
	case "datetime", "timestamp":
		return "datetime"
	case "year":
		return "int"
	case "bit", "bool", "boolean":
		return "bool"
	case "json":
		return "string"
	default:
		return "string"
	}
}

func (d *MySQLDialect) GetDatabaseName(dsCfg *domain.DataSourceConfig, sqlCfg *sqlcommon.SQLConfig) string {
	if dsCfg.Database != "" {
		return dsCfg.Database
	}
	return dsCfg.Name
}
