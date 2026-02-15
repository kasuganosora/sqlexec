package mysql

import (
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	sqlcommon "github.com/kasuganosora/sqlexec/server/datasource/sql"
)

func TestMySQLDialect_DriverName(t *testing.T) {
	d := &MySQLDialect{}
	if d.DriverName() != "mysql" {
		t.Errorf("expected mysql, got %s", d.DriverName())
	}
}

func TestMySQLDialect_QuoteIdentifier(t *testing.T) {
	d := &MySQLDialect{}
	tests := []struct {
		input, want string
	}{
		{"users", "`users`"},
		{"my`table", "`my``table`"},
		{"order", "`order`"},
	}
	for _, tt := range tests {
		got := d.QuoteIdentifier(tt.input)
		if got != tt.want {
			t.Errorf("QuoteIdentifier(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestMySQLDialect_Placeholder(t *testing.T) {
	d := &MySQLDialect{}
	for i := 1; i <= 5; i++ {
		if d.Placeholder(i) != "?" {
			t.Errorf("Placeholder(%d) = %q, want ?", i, d.Placeholder(i))
		}
	}
}

func TestMySQLDialect_MapColumnType(t *testing.T) {
	d := &MySQLDialect{}
	tests := []struct {
		input, want string
	}{
		{"bigint", "int"},
		{"int", "int"},
		{"varchar(255)", "string"},
		{"text", "string"},
		{"double", "float64"},
		{"decimal(10,2)", "float64"},
		{"tinyint(1)", "bool"},
		{"tinyint", "int"},
		{"datetime", "datetime"},
		{"timestamp", "datetime"},
		{"date", "date"},
		{"json", "string"},
		{"blob", "string"},
		{"enum('a','b')", "string"},
	}
	for _, tt := range tests {
		got := d.MapColumnType(tt.input, nil)
		if got != tt.want {
			t.Errorf("MapColumnType(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestMySQLDialect_BuildDSN(t *testing.T) {
	d := &MySQLDialect{}
	dsCfg := &domain.DataSourceConfig{
		Host:     "localhost",
		Port:     3306,
		Username: "root",
		Password: "pass",
		Database: "testdb",
	}
	sqlCfg := &sqlcommon.SQLConfig{
		Charset:        "utf8mb4",
		Collation:      "utf8mb4_unicode_ci",
		ConnectTimeout: 10,
		SSLMode:        "disable",
	}
	parseTime := true
	sqlCfg.ParseTime = &parseTime

	dsn, err := d.BuildDSN(dsCfg, sqlCfg)
	if err != nil {
		t.Fatalf("BuildDSN error: %v", err)
	}
	if dsn == "" {
		t.Fatal("DSN should not be empty")
	}
	// Verify DSN contains expected components
	for _, substr := range []string{"root", "pass", "localhost", "3306", "testdb", "utf8mb4"} {
		if !contains(dsn, substr) {
			t.Errorf("DSN %q should contain %q", dsn, substr)
		}
	}
}

func TestMySQLDialect_GetDatabaseName(t *testing.T) {
	d := &MySQLDialect{}
	dsCfg := &domain.DataSourceConfig{
		Name:     "myds",
		Database: "mydb",
	}
	if d.GetDatabaseName(dsCfg, &sqlcommon.SQLConfig{}) != "mydb" {
		t.Error("should return Database field")
	}

	dsCfg.Database = ""
	if d.GetDatabaseName(dsCfg, &sqlcommon.SQLConfig{}) != "myds" {
		t.Error("should fallback to Name field")
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsSubstring(s, substr))
}

func containsSubstring(s, substr string) bool {
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
