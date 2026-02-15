package gorm

import (
	"database/sql"
	"fmt"

	"github.com/kasuganosora/sqlexec/pkg/api"

	"gorm.io/gorm"
	"gorm.io/gorm/callbacks"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/schema"
)

// Dialector implements gorm.Dialector by routing all SQL through an
// api.Session. Unlike traditional GORM dialectors that need a network
// connection, this one works in-process — queries go directly from
// GORM's SQL builder to sqlexec's parser and optimizer.
type Dialector struct {
	Session *api.Session
	sqlDB   *sql.DB // created during Initialize via our database/sql driver
}

// NewDialector creates a new GORM dialector backed by the given session.
//
//	db, _ := api.NewDB(nil)
//	db.RegisterDataSource("default", memoryDS)
//	session := db.Session()
//	gormDB, _ := gorm.Open(NewDialector(session), &gorm.Config{})
func NewDialector(session *api.Session) gorm.Dialector {
	return &Dialector{Session: session}
}

// Name returns the dialect name.
func (d *Dialector) Name() string {
	return "sqlexec"
}

// Initialize sets up the connection pool and registers GORM's default
// callbacks. This is the critical piece that makes Create/Find/Update/Delete
// actually work.
func (d *Dialector) Initialize(db *gorm.DB) error {
	if d.Session == nil {
		return fmt.Errorf("sqlexec: Dialector.Session must not be nil")
	}

	// Create a *sql.DB backed by our database/sql/driver.
	// This gives GORM a standard ConnPool without a real network connection.
	d.sqlDB = OpenDB(d.Session)
	db.ConnPool = d.sqlDB

	// Register default GORM callbacks (Create, Query, Update, Delete, Row, Raw).
	callbacks.RegisterDefaultCallbacks(db, &callbacks.Config{})

	return nil
}

// Migrator returns the schema migration tool.
func (d *Dialector) Migrator(db *gorm.DB) gorm.Migrator {
	return &Migrator{
		Dialector: d,
		DB:        db,
	}
}

// DataTypeOf maps GORM schema field types to SQL type names.
func (d *Dialector) DataTypeOf(field *schema.Field) string {
	switch field.DataType {
	case schema.Bool:
		return "BOOLEAN"
	case schema.Int, schema.Uint:
		if field.Size <= 8 {
			return "TINYINT"
		} else if field.Size <= 16 {
			return "SMALLINT"
		} else if field.Size <= 32 {
			return "INT"
		}
		return "BIGINT"
	case schema.Float:
		if field.Size <= 32 {
			return "FLOAT"
		}
		return "DOUBLE"
	case schema.String:
		if field.Size > 0 && field.Size <= 65535 {
			return fmt.Sprintf("VARCHAR(%d)", field.Size)
		}
		return "TEXT"
	case schema.Time:
		return "TIMESTAMP"
	case schema.Bytes:
		return "BLOB"
	default:
		return "VARCHAR(255)"
	}
}

// DefaultValueOf returns a clause expression for a field's default value.
func (d *Dialector) DefaultValueOf(field *schema.Field) clause.Expression {
	if field.DefaultValue != "" {
		return clause.Expr{SQL: "DEFAULT"}
	}
	return nil
}

// BindVarTo writes a `?` placeholder for parameter binding (MySQL style).
func (d *Dialector) BindVarTo(writer clause.Writer, _ *gorm.Statement, _ interface{}) {
	writer.WriteByte('?')
}

// QuoteTo quotes an identifier with backticks (MySQL style).
func (d *Dialector) QuoteTo(writer clause.Writer, str string) {
	writer.WriteByte('`')
	writer.WriteString(str)
	writer.WriteByte('`')
}

// Explain returns a human-readable version of the SQL with bound parameters.
func (d *Dialector) Explain(sql string, vars ...interface{}) string {
	return fmt.Sprintf("%s %v", sql, vars)
}

// CloseDB closes the internal *sql.DB created during Initialize.
// Call this when you're done with the GORM DB to release resources.
func (d *Dialector) CloseDB() error {
	if d.sqlDB != nil {
		return d.sqlDB.Close()
	}
	return nil
}
