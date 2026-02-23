package sql

import (
	"database/sql"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// testDialect is a MySQL-style dialect for testing (uses ? placeholders)
type testDialect struct{}

func (d *testDialect) DriverName() string { return "test" }
func (d *testDialect) BuildDSN(dsCfg *domain.DataSourceConfig, sqlCfg *SQLConfig) (string, error) {
	return "", nil
}
func (d *testDialect) QuoteIdentifier(name string) string { return "`" + name + "`" }
func (d *testDialect) Placeholder(n int) string           { return "?" }
func (d *testDialect) GetTablesQuery() string             { return "" }
func (d *testDialect) GetTableInfoQuery() string          { return "" }
func (d *testDialect) MapColumnType(dbTypeName string, scanType *sql.ColumnType) string {
	return "string"
}
func (d *testDialect) GetDatabaseName(dsCfg *domain.DataSourceConfig, sqlCfg *SQLConfig) string {
	return "test"
}

func TestBuildSelectSQL_Basic(t *testing.T) {
	d := &testDialect{}

	sql, params := BuildSelectSQL(d, "users", nil, 0)
	if sql != "SELECT * FROM `users`" {
		t.Errorf("unexpected SQL: %s", sql)
	}
	if len(params) != 0 {
		t.Errorf("expected no params, got %d", len(params))
	}
}

func TestBuildSelectSQL_WithColumns(t *testing.T) {
	d := &testDialect{}

	options := &domain.QueryOptions{
		SelectColumns: []string{"name", "age"},
	}

	sql, _ := BuildSelectSQL(d, "users", options, 0)
	if sql != "SELECT `name`, `age` FROM `users`" {
		t.Errorf("unexpected SQL: %s", sql)
	}
}

func TestBuildSelectSQL_WithFilters(t *testing.T) {
	d := &testDialect{}

	options := &domain.QueryOptions{
		Filters: []domain.Filter{
			{Field: "age", Operator: ">", Value: 18},
		},
	}

	sql, params := BuildSelectSQL(d, "users", options, 0)
	expected := "SELECT * FROM `users` WHERE `age` > ?"
	if sql != expected {
		t.Errorf("expected %q, got %q", expected, sql)
	}
	if len(params) != 1 || params[0] != 18 {
		t.Errorf("unexpected params: %v", params)
	}
}

func TestBuildSelectSQL_WithOrderAndLimit(t *testing.T) {
	d := &testDialect{}

	options := &domain.QueryOptions{
		OrderBy: "created_at",
		Order:   "DESC",
		Limit:   10,
		Offset:  20,
	}

	sql, _ := BuildSelectSQL(d, "users", options, 0)
	expected := "SELECT * FROM `users` ORDER BY `created_at` DESC LIMIT 10 OFFSET 20"
	if sql != expected {
		t.Errorf("expected %q, got %q", expected, sql)
	}
}

func TestBuildWhereClause_IN(t *testing.T) {
	d := &testDialect{}

	filters := []domain.Filter{
		{Field: "status", Operator: "IN", Value: []interface{}{"active", "pending"}},
	}

	clause, params := BuildWhereClause(d, filters, 0)
	expected := "`status` IN (?, ?)"
	if clause != expected {
		t.Errorf("expected %q, got %q", expected, clause)
	}
	if len(params) != 2 {
		t.Errorf("expected 2 params, got %d", len(params))
	}
}

func TestBuildWhereClause_BETWEEN(t *testing.T) {
	d := &testDialect{}

	filters := []domain.Filter{
		{Field: "age", Operator: "BETWEEN", Value: []interface{}{18, 65}},
	}

	clause, params := BuildWhereClause(d, filters, 0)
	expected := "`age` BETWEEN ? AND ?"
	if clause != expected {
		t.Errorf("expected %q, got %q", expected, clause)
	}
	if len(params) != 2 {
		t.Errorf("expected 2 params, got %d", len(params))
	}
}

func TestBuildWhereClause_IsNull(t *testing.T) {
	d := &testDialect{}

	filters := []domain.Filter{
		{Field: "deleted_at", Operator: "IS NULL"},
	}

	clause, params := BuildWhereClause(d, filters, 0)
	if clause != "`deleted_at` IS NULL" {
		t.Errorf("unexpected clause: %s", clause)
	}
	if len(params) != 0 {
		t.Errorf("expected no params, got %d", len(params))
	}
}

func TestBuildWhereClause_NestedLogic(t *testing.T) {
	d := &testDialect{}

	filters := []domain.Filter{
		{
			Logic: "OR",
			SubFilters: []domain.Filter{
				{Field: "status", Operator: "=", Value: "active"},
				{Field: "status", Operator: "=", Value: "pending"},
			},
		},
	}

	clause, params := BuildWhereClause(d, filters, 0)
	expected := "(`status` = ? OR `status` = ?)"
	if clause != expected {
		t.Errorf("expected %q, got %q", expected, clause)
	}
	if len(params) != 2 {
		t.Errorf("expected 2 params, got %d", len(params))
	}
}

func TestBuildInsertSQL(t *testing.T) {
	d := &testDialect{}

	rows := []domain.Row{
		{"name": "Alice", "age": 30},
		{"name": "Bob", "age": 25},
	}

	sql, params, cols := BuildInsertSQL(d, "users", rows)
	if len(cols) != 2 {
		t.Errorf("expected 2 columns, got %d", len(cols))
	}
	if len(params) != 4 {
		t.Errorf("expected 4 params, got %d", len(params))
	}
	// Columns are sorted, so age comes before name
	if cols[0] != "age" || cols[1] != "name" {
		t.Errorf("unexpected columns: %v", cols)
	}
	expectedSQL := "INSERT INTO `users` (`age`, `name`) VALUES (?, ?), (?, ?)"
	if sql != expectedSQL {
		t.Errorf("expected %q, got %q", expectedSQL, sql)
	}
}

func TestBuildUpdateSQL(t *testing.T) {
	d := &testDialect{}

	filters := []domain.Filter{
		{Field: "id", Operator: "=", Value: 1},
	}
	updates := domain.Row{"name": "Updated"}

	sql, params := BuildUpdateSQL(d, "users", filters, updates)
	expected := "UPDATE `users` SET `name` = ? WHERE `id` = ?"
	if sql != expected {
		t.Errorf("expected %q, got %q", expected, sql)
	}
	if len(params) != 2 {
		t.Errorf("expected 2 params, got %d", len(params))
	}
}

func TestBuildDeleteSQL(t *testing.T) {
	d := &testDialect{}

	filters := []domain.Filter{
		{Field: "id", Operator: "=", Value: 5},
	}

	sql, params := BuildDeleteSQL(d, "users", filters)
	expected := "DELETE FROM `users` WHERE `id` = ?"
	if sql != expected {
		t.Errorf("expected %q, got %q", expected, sql)
	}
	if len(params) != 1 || params[0] != 5 {
		t.Errorf("unexpected params: %v", params)
	}
}

func TestBuildDeleteSQL_NoFilters(t *testing.T) {
	d := &testDialect{}

	sql, params := BuildDeleteSQL(d, "users", nil)
	expected := "DELETE FROM `users`"
	if sql != expected {
		t.Errorf("expected %q, got %q", expected, sql)
	}
	if len(params) != 0 {
		t.Errorf("expected no params, got %d", len(params))
	}
}
