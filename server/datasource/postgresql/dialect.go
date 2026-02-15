package postgresql

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	sqlcommon "github.com/kasuganosora/sqlexec/server/datasource/sql"
)

// PostgreSQLDialect implements sql.Dialect for PostgreSQL.
type PostgreSQLDialect struct{}

func (d *PostgreSQLDialect) DriverName() string { return "postgres" }

func (d *PostgreSQLDialect) BuildDSN(dsCfg *domain.DataSourceConfig, sqlCfg *sqlcommon.SQLConfig) (string, error) {
	port := dsCfg.Port
	if port <= 0 {
		port = 5432
	}

	parts := []string{
		fmt.Sprintf("host=%s", dsCfg.Host),
		fmt.Sprintf("port=%d", port),
		fmt.Sprintf("user=%s", dsCfg.Username),
		fmt.Sprintf("password=%s", dsCfg.Password),
		fmt.Sprintf("dbname=%s", dsCfg.Database),
		fmt.Sprintf("sslmode=%s", sqlCfg.SSLMode),
	}

	if sqlCfg.Schema != "" {
		parts = append(parts, fmt.Sprintf("search_path=%s", sqlCfg.Schema))
	}
	if sqlCfg.ConnectTimeout > 0 {
		parts = append(parts, fmt.Sprintf("connect_timeout=%d", sqlCfg.ConnectTimeout))
	}
	if sqlCfg.SSLCert != "" {
		parts = append(parts, fmt.Sprintf("sslcert=%s", sqlCfg.SSLCert))
	}
	if sqlCfg.SSLKey != "" {
		parts = append(parts, fmt.Sprintf("sslkey=%s", sqlCfg.SSLKey))
	}
	if sqlCfg.SSLRootCert != "" {
		parts = append(parts, fmt.Sprintf("sslrootcert=%s", sqlCfg.SSLRootCert))
	}

	return strings.Join(parts, " "), nil
}

func (d *PostgreSQLDialect) QuoteIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func (d *PostgreSQLDialect) Placeholder(n int) string {
	return "$" + strconv.Itoa(n)
}

func (d *PostgreSQLDialect) GetTablesQuery() string {
	return "SELECT table_name FROM information_schema.tables WHERE table_schema = current_schema() AND table_type = 'BASE TABLE'"
}

func (d *PostgreSQLDialect) GetTableInfoQuery() string {
	// Join with key_column_usage to detect primary keys
	return `SELECT c.column_name, c.data_type, c.is_nullable, c.column_default,
       CASE WHEN kcu.column_name IS NOT NULL THEN 'PRI' ELSE '' END AS column_key
FROM information_schema.columns c
LEFT JOIN information_schema.table_constraints tc
  ON tc.table_schema = c.table_schema AND tc.table_name = c.table_name AND tc.constraint_type = 'PRIMARY KEY'
LEFT JOIN information_schema.key_column_usage kcu
  ON kcu.constraint_name = tc.constraint_name AND kcu.table_schema = tc.table_schema AND kcu.column_name = c.column_name
WHERE c.table_schema = current_schema() AND c.table_name = $1
ORDER BY c.ordinal_position`
}

func (d *PostgreSQLDialect) MapColumnType(dbTypeName string, scanType *sql.ColumnType) string {
	t := strings.ToLower(strings.TrimSpace(dbTypeName))

	// Strip ARRAY suffix
	t = strings.TrimSuffix(t, "[]")

	switch t {
	case "smallint", "integer", "bigint", "serial", "bigserial", "smallserial", "int2", "int4", "int8":
		return "int"
	case "real", "float4":
		return "float64"
	case "double precision", "float8", "numeric", "decimal", "money":
		return "float64"
	case "boolean", "bool":
		return "bool"
	case "character varying", "varchar", "character", "char", "text", "name", "citext":
		return "string"
	case "bytea":
		return "string"
	case "date":
		return "date"
	case "time", "time without time zone", "time with time zone", "timetz":
		return "time"
	case "timestamp", "timestamp without time zone", "timestamp with time zone", "timestamptz":
		return "datetime"
	case "interval":
		return "string"
	case "json", "jsonb":
		return "string"
	case "uuid":
		return "string"
	case "inet", "cidr", "macaddr", "macaddr8":
		return "string"
	case "xml":
		return "string"
	case "point", "line", "lseg", "box", "path", "polygon", "circle":
		return "string"
	case "tsvector", "tsquery":
		return "string"
	default:
		// Handle user-defined or composite types
		return "string"
	}
}

func (d *PostgreSQLDialect) GetDatabaseName(dsCfg *domain.DataSourceConfig, sqlCfg *sqlcommon.SQLConfig) string {
	if dsCfg.Database != "" {
		return dsCfg.Database
	}
	return dsCfg.Name
}
