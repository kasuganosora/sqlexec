package gorm

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/kasuganosora/sqlexec/pkg/api"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// ---------------------------------------------------------------------------
// database/sql/driver implementation for sqlexec
//
// This allows GORM (and plain database/sql users) to route SQL through
// api.Session without a network round-trip. The central idea:
//
//   connector → wraps *api.Session
//   conn      → QueryerContext / ExecerContext dispatching to Session
//   rows      → wraps domain.QueryResult, converts to driver.Value
//   execResult→ wraps RowsAffected / LastInsertID
//   noopTx    → no-op Begin/Commit/Rollback (each stmt is atomic via MVCC)
//
// Usage:
//   sqlDB := sql.OpenDB(NewConnector(session))
// ---------------------------------------------------------------------------

// sqlexecDriver is a minimal driver.Driver. Use NewConnector instead of Open.
type sqlexecDriver struct{}

func (d *sqlexecDriver) Open(_ string) (driver.Conn, error) {
	return nil, fmt.Errorf("sqlexec: use sql.OpenDB(NewConnector(session)) instead of sql.Open")
}

// NewConnector creates a driver.Connector that routes all SQL through the
// given api.Session. The resulting connector can be used with sql.OpenDB.
func NewConnector(session *api.Session) driver.Connector {
	return &connector{session: session}
}

// connector implements driver.Connector.
type connector struct {
	session *api.Session
}

func (c *connector) Connect(_ context.Context) (driver.Conn, error) {
	return &conn{session: c.session}, nil
}

func (c *connector) Driver() driver.Driver {
	return &sqlexecDriver{}
}

// conn implements driver.Conn, driver.QueryerContext, and driver.ExecerContext.
// By implementing the Context variants, database/sql skips the Prepare path.
type conn struct {
	session *api.Session
	closed  bool
}

// Prepare is required by driver.Conn but is not used when QueryerContext and
// ExecerContext are implemented.
func (c *conn) Prepare(query string) (driver.Stmt, error) {
	return &stmt{session: c.session, query: query}, nil
}

func (c *conn) Close() error {
	c.closed = true
	return nil
}

func (c *conn) Begin() (driver.Tx, error) {
	// Individual statements are atomic via MVCC. GORM's default callbacks
	// wrap CUD operations in Begin/Commit but the no-op tx is safe here.
	return &noopTx{}, nil
}

// QueryContext routes SELECT / SHOW / DESCRIBE through Session.Query.
func (c *conn) QueryContext(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	iargs := namedValuesToArgs(args)

	q, err := c.session.Query(query, iargs...)
	if err != nil {
		return nil, err
	}

	// Eagerly collect results — api.Query is already fully materialized.
	cols := q.Columns()
	colNames := make([]string, len(cols))
	for i, col := range cols {
		colNames[i] = col.Name
	}

	var data []domain.Row
	for q.Next() {
		data = append(data, q.Row())
	}
	q.Close()

	return &resultRows{columns: colNames, data: data}, nil
}

// ExecContext routes INSERT / UPDATE / DELETE / DDL through Session.Execute.
// If the SQL is a SELECT-like statement (e.g. from gormDB.Exec("SELECT ...")),
// it transparently falls back to Session.Query.
func (c *conn) ExecContext(_ context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	iargs := namedValuesToArgs(args)

	if isReadStatement(query) {
		q, err := c.session.Query(query, iargs...)
		if err != nil {
			return nil, err
		}
		count := int64(q.RowsCount())
		q.Close()
		return &execResult{affected: count}, nil
	}

	r, err := c.session.Execute(query, iargs...)
	if err != nil {
		return nil, err
	}
	return &execResult{affected: r.RowsAffected, insertID: r.LastInsertID}, nil
}

// ---------------------------------------------------------------------------
// stmt — fallback prepared-statement path (rarely used)
// ---------------------------------------------------------------------------

type stmt struct {
	session *api.Session
	query   string
}

func (s *stmt) Close() error  { return nil }
func (s *stmt) NumInput() int { return -1 }

func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	iargs := valuesToArgs(args)
	r, err := s.session.Execute(s.query, iargs...)
	if err != nil {
		return nil, err
	}
	return &execResult{affected: r.RowsAffected, insertID: r.LastInsertID}, nil
}

func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	iargs := valuesToArgs(args)
	q, err := s.session.Query(s.query, iargs...)
	if err != nil {
		return nil, err
	}
	cols := q.Columns()
	colNames := make([]string, len(cols))
	for i, col := range cols {
		colNames[i] = col.Name
	}
	var data []domain.Row
	for q.Next() {
		data = append(data, q.Row())
	}
	q.Close()
	return &resultRows{columns: colNames, data: data}, nil
}

// ---------------------------------------------------------------------------
// resultRows — driver.Rows backed by domain.QueryResult
// ---------------------------------------------------------------------------

type resultRows struct {
	columns []string
	data    []domain.Row
	index   int
}

func (r *resultRows) Columns() []string { return r.columns }
func (r *resultRows) Close() error      { return nil }

func (r *resultRows) Next(dest []driver.Value) error {
	if r.index >= len(r.data) {
		return io.EOF
	}
	row := r.data[r.index]
	for i, col := range r.columns {
		if i < len(dest) {
			dest[i] = toDriverValue(row[col])
		}
	}
	r.index++
	return nil
}

// ---------------------------------------------------------------------------
// execResult — driver.Result
// ---------------------------------------------------------------------------

type execResult struct {
	affected int64
	insertID int64
}

func (r *execResult) LastInsertId() (int64, error) { return r.insertID, nil }
func (r *execResult) RowsAffected() (int64, error) { return r.affected, nil }

// ---------------------------------------------------------------------------
// noopTx — each statement is atomic via MVCC, so tx is a no-op.
// ---------------------------------------------------------------------------

type noopTx struct{}

func (t *noopTx) Commit() error   { return nil }
func (t *noopTx) Rollback() error { return nil }

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// OpenDB is a convenience wrapper: creates a *sql.DB routed through the session.
func OpenDB(session *api.Session) *sql.DB {
	return sql.OpenDB(NewConnector(session))
}

func namedValuesToArgs(named []driver.NamedValue) []interface{} {
	args := make([]interface{}, len(named))
	for i, nv := range named {
		args[i] = nv.Value
	}
	return args
}

func valuesToArgs(vals []driver.Value) []interface{} {
	args := make([]interface{}, len(vals))
	for i, v := range vals {
		args[i] = v
	}
	return args
}

// isReadStatement returns true for SQL that should go through Session.Query.
func isReadStatement(query string) bool {
	q := strings.TrimSpace(query)
	if len(q) < 4 {
		return false
	}
	prefix := strings.ToUpper(q[:4])
	return prefix == "SELE" || prefix == "SHOW" || prefix == "DESC" || prefix == "EXPL"
}

// toDriverValue converts an interface{} to a valid driver.Value.
// driver.Value must be one of: nil, int64, float64, bool, []byte, string, time.Time.
func toDriverValue(v interface{}) driver.Value {
	if v == nil {
		return nil
	}
	switch val := v.(type) {
	case time.Time:
		return val
	case string:
		// Try to parse as timestamp first for GORM compatibility
		if t, err := parseTimeString(val); err == nil {
			return t
		}
		return val
	case int64, float64, bool, []byte:
		return val
	case int:
		return int64(val)
	case int8:
		return int64(val)
	case int16:
		return int64(val)
	case int32:
		return int64(val)
	case uint:
		return int64(val)
	case uint8:
		return int64(val)
	case uint16:
		return int64(val)
	case uint32:
		return int64(val)
	case uint64:
		return int64(val)
	case float32:
		return float64(val)
	default:
		return fmt.Sprintf("%v", val)
	}
}

// parseTimeString attempts to parse a string as a time.Time.
// Supports common timestamp formats used by databases.
func parseTimeString(s string) (time.Time, error) {
	// Common timestamp formats
	formats := []string{
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05.999999999",
		"2006-01-02T15:04:05",
		time.RFC3339,
		time.RFC3339Nano,
		"2006-01-02 15:04:05.999999999 -0700 MST",
		"2006-01-02 15:04:05 -0700 MST",
		"2006-01-02 15:04:05.999999999 -0700",
		"2006-01-02 15:04:05 -0700",
		// Format from fmt.Sprintf("%v", time.Time) with UTC
		"2006-01-02 15:04:05.999999999 +0000 UTC",
		"2006-01-02 15:04:05 +0000 UTC",
		"2006-01-02 15:04:05 +0000",
		// Additional common formats
		"2006-01-02 15:04:05.999999",
		"2006-01-02 15:04:05.999999 -0700",
	}

	for _, format := range formats {
		if t, err := time.Parse(format, s); err == nil {
			return t, nil
		}
	}

	return time.Time{}, fmt.Errorf("not a timestamp")
}
