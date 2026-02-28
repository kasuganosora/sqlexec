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

// ClauseOnConflict is the clause name for ON CONFLICT handling.
const ClauseOnConflict = "ON CONFLICT"

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

	// Register MySQL-compatible clause builders so GORM generates SQL that
	// the TiDB parser can handle (e.g. ON DUPLICATE KEY UPDATE instead of
	// PostgreSQL-style ON CONFLICT ... DO UPDATE SET).
	for k, v := range d.ClauseBuilders() {
		db.ClauseBuilders[k] = v
	}

	// Register default GORM callbacks (Create, Query, Update, Delete, Row, Raw).
	callbacks.RegisterDefaultCallbacks(db, &callbacks.Config{})

	return nil
}

// ClauseBuilders returns MySQL-compatible clause builders.
// The critical one is ON CONFLICT which must generate ON DUPLICATE KEY UPDATE
// syntax instead of PostgreSQL ON CONFLICT ... DO UPDATE SET.
func (d *Dialector) ClauseBuilders() map[string]clause.ClauseBuilder {
	return map[string]clause.ClauseBuilder{
		ClauseOnConflict: func(c clause.Clause, builder clause.Builder) {
			onConflict, ok := c.Expression.(clause.OnConflict)
			if !ok {
				return
			}

			if onConflict.DoNothing {
				// INSERT IGNORE INTO ... (MySQL way of DO NOTHING)
				// We need to rewrite the INSERT prefix. Since GORM has
				// already written "INSERT INTO", we append a dummy ON
				// DUPLICATE KEY UPDATE that sets the first column to itself.
				builder.WriteString("ON DUPLICATE KEY UPDATE ")
				if len(onConflict.DoUpdates) > 0 {
					for idx, assignment := range onConflict.DoUpdates {
						if idx > 0 {
							builder.WriteByte(',')
						}
						builder.WriteQuoted(assignment.Column)
						builder.WriteByte('=')
						builder.WriteQuoted(assignment.Column)
					}
				} else {
					// Fallback: use a self-referencing no-op update
					builder.WriteString("`id`=`id`")
				}
				return
			}

			builder.WriteString("ON DUPLICATE KEY UPDATE ")
			if len(onConflict.DoUpdates) > 0 {
				for idx, assignment := range onConflict.DoUpdates {
					if idx > 0 {
						builder.WriteByte(',')
					}
					builder.WriteQuoted(assignment.Column)
					builder.WriteByte('=')
					// GORM's AssignmentColumns generates clause.Column{Table:"excluded", Name:col}
					// which is PostgreSQL syntax. Convert to MySQL's VALUES(col).
					if col, ok := assignment.Value.(clause.Column); ok && col.Table == "excluded" {
						builder.WriteString("VALUES(")
						builder.WriteQuoted(clause.Column{Name: col.Name})
						builder.WriteByte(')')
					} else {
						builder.AddVar(builder, assignment.Value)
					}
				}
			}
		},
	}
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
