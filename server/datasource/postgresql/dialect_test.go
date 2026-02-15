package postgresql

import (
	"strings"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	sqlcommon "github.com/kasuganosora/sqlexec/server/datasource/sql"
)

func TestPostgreSQLDialect_DriverName(t *testing.T) {
	d := &PostgreSQLDialect{}
	if d.DriverName() != "postgres" {
		t.Errorf("expected postgres, got %s", d.DriverName())
	}
}

func TestPostgreSQLDialect_QuoteIdentifier(t *testing.T) {
	d := &PostgreSQLDialect{}
	tests := []struct {
		input, want string
	}{
		{"users", `"users"`},
		{`my"table`, `"my""table"`},
		{"order", `"order"`},
	}
	for _, tt := range tests {
		got := d.QuoteIdentifier(tt.input)
		if got != tt.want {
			t.Errorf("QuoteIdentifier(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestPostgreSQLDialect_Placeholder(t *testing.T) {
	d := &PostgreSQLDialect{}
	tests := []struct {
		n    int
		want string
	}{
		{1, "$1"},
		{2, "$2"},
		{10, "$10"},
	}
	for _, tt := range tests {
		got := d.Placeholder(tt.n)
		if got != tt.want {
			t.Errorf("Placeholder(%d) = %q, want %q", tt.n, got, tt.want)
		}
	}
}

func TestPostgreSQLDialect_MapColumnType(t *testing.T) {
	d := &PostgreSQLDialect{}
	tests := []struct {
		input, want string
	}{
		{"integer", "int"},
		{"bigint", "int"},
		{"smallint", "int"},
		{"serial", "int"},
		{"character varying", "string"},
		{"text", "string"},
		{"double precision", "float64"},
		{"numeric", "float64"},
		{"real", "float64"},
		{"boolean", "bool"},
		{"timestamp without time zone", "datetime"},
		{"timestamptz", "datetime"},
		{"date", "date"},
		{"time", "time"},
		{"json", "string"},
		{"jsonb", "string"},
		{"uuid", "string"},
		{"bytea", "string"},
		{"inet", "string"},
	}
	for _, tt := range tests {
		got := d.MapColumnType(tt.input, nil)
		if got != tt.want {
			t.Errorf("MapColumnType(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestPostgreSQLDialect_BuildDSN(t *testing.T) {
	d := &PostgreSQLDialect{}
	dsCfg := &domain.DataSourceConfig{
		Host:     "localhost",
		Port:     5432,
		Username: "pguser",
		Password: "pgpass",
		Database: "testdb",
	}
	sqlCfg := &sqlcommon.SQLConfig{
		SSLMode:        "disable",
		Schema:         "public",
		ConnectTimeout: 10,
	}

	dsn, err := d.BuildDSN(dsCfg, sqlCfg)
	if err != nil {
		t.Fatalf("BuildDSN error: %v", err)
	}

	for _, expected := range []string{"host=localhost", "port=5432", "user=pguser", "password=pgpass", "dbname=testdb", "sslmode=disable", "search_path=public"} {
		if !strings.Contains(dsn, expected) {
			t.Errorf("DSN %q should contain %q", dsn, expected)
		}
	}
}

func TestPostgreSQLDialect_GetDatabaseName(t *testing.T) {
	d := &PostgreSQLDialect{}
	dsCfg := &domain.DataSourceConfig{
		Name:     "pgds",
		Database: "pgdb",
	}
	if d.GetDatabaseName(dsCfg, &sqlcommon.SQLConfig{}) != "pgdb" {
		t.Error("should return Database field")
	}

	dsCfg.Database = ""
	if d.GetDatabaseName(dsCfg, &sqlcommon.SQLConfig{}) != "pgds" {
		t.Error("should fallback to Name field")
	}
}
