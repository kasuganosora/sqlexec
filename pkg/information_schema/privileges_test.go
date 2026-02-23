package information_schema

import (
	"context"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

func setupTestACLManager(t *testing.T) *MockACLManager {
	am := NewMockACLManager()
	return am
}

func TestNewUserPrivilegesTable(t *testing.T) {
	am := setupTestACLManager(t)

	table := NewUserPrivilegesTable(am)
	if table == nil {
		t.Fatal("NewUserPrivilegesTable() returned nil")
	}

	if table.GetName() != "USER_PRIVILEGES" {
		t.Errorf("GetName() = %v, want 'USER_PRIVILEGES'", table.GetName())
	}
}

func TestUserPrivilegesTableGetSchema(t *testing.T) {
	am := setupTestACLManager(t)

	table := NewUserPrivilegesTable(am)
	schema := table.GetSchema()

	if len(schema) == 0 {
		t.Fatal("GetSchema() returned empty schema")
	}

	// Check for required columns
	requiredColumns := []string{
		"GRANTEE", "TABLE_CATALOG", "PRIVILEGE_TYPE", "IS_GRANTABLE",
	}

	columnMap := make(map[string]bool)
	for _, col := range schema {
		columnMap[col.Name] = true
	}

	for _, reqCol := range requiredColumns {
		if !columnMap[reqCol] {
			t.Errorf("GetSchema() missing required column: %v", reqCol)
		}
	}
}

func TestUserPrivilegesTableQuery(t *testing.T) {
	am := setupTestACLManager(t)

	// Create test users with privileges
	am.AddUser("%", "testuser1", "")
	am.Grant("%", "testuser1", []string{"SELECT", "INSERT"})

	am.AddUser("localhost", "testuser2", "")
	am.Grant("localhost", "testuser2", []string{"UPDATE", "DELETE"})

	table := NewUserPrivilegesTable(am)
	result, err := table.Query(context.Background(), nil, nil)
	if err != nil {
		t.Fatalf("Query() error = %v, want nil", err)
	}

	if result == nil {
		t.Fatal("Query() returned nil result")
	}

	// Should have 4 privilege rows (2 users x 2 privileges each)
	if result.Total < 4 {
		t.Errorf("Query() Total = %v, want at least 4", result.Total)
	}

	// Verify columns
	if len(result.Columns) != 4 {
		t.Errorf("Query() returned %v columns, want 4", len(result.Columns))
	}
}

func TestUserPrivilegesTableQueryGranteeFormat(t *testing.T) {
	am := setupTestACLManager(t)

	// Create user
	am.AddUser("%", "testuser", "")
	am.Grant("%", "testuser", []string{"SELECT"})

	table := NewUserPrivilegesTable(am)
	result, err := table.Query(context.Background(), nil, nil)
	if err != nil {
		t.Fatalf("Query() error = %v, want nil", err)
	}

	// Find testuser SELECT privilege row
	var selectRow domain.Row
	for _, row := range result.Rows {
		if row["GRANTEE"] == "'testuser'@'%'" && row["PRIVILEGE_TYPE"] == "SELECT" {
			selectRow = row
			break
		}
	}

	if selectRow == nil {
		t.Fatal("testuser SELECT privilege row not found")
	}

	// Verify GRANTEE format
	if selectRow["GRANTEE"] != "'testuser'@'%'" {
		t.Errorf("GRANTEE = %v, want 'testuser'@'%%'", selectRow["GRANTEE"])
	}

	// Verify TABLE_CATALOG
	if selectRow["TABLE_CATALOG"] != "def" {
		t.Errorf("TABLE_CATALOG = %v, want 'def'", selectRow["TABLE_CATALOG"])
	}
}

func TestUserPrivilegesTableQueryWithFilters(t *testing.T) {
	am := setupTestACLManager(t)

	// Create users
	am.AddUser("%", "user1", "")
	am.Grant("%", "user1", []string{"SELECT"})

	am.AddUser("%", "user2", "")
	am.Grant("%", "user2", []string{"INSERT"})

	table := NewUserPrivilegesTable(am)

	tests := []struct {
		name     string
		filters  []domain.Filter
		wantRows int
	}{
		{
			name:     "Filter by GRANTEE",
			filters:  []domain.Filter{{Field: "GRANTEE", Operator: "=", Value: "'user1'@'%'"}},
			wantRows: 1,
		},
		{
			name:     "Filter by PRIVILEGE_TYPE",
			filters:  []domain.Filter{{Field: "PRIVILEGE_TYPE", Operator: "=", Value: "SELECT"}},
			wantRows: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := table.Query(context.Background(), tt.filters, nil)
			if err != nil {
				t.Fatalf("Query() error = %v, want nil", err)
			}

			if len(result.Rows) < tt.wantRows {
				t.Errorf("Query() returned %v rows, want at least %v", len(result.Rows), tt.wantRows)
			}
		})
	}
}

func TestUserPrivilegesTableQueryWithLimit(t *testing.T) {
	am := setupTestACLManager(t)

	// Create users
	am.AddUser("%", "user1", "")
	am.Grant("%", "user1", []string{"SELECT", "INSERT", "UPDATE"})

	table := NewUserPrivilegesTable(am)

	options := &domain.QueryOptions{
		Limit:  2,
		Offset: 0,
	}

	result, err := table.Query(context.Background(), nil, options)
	if err != nil {
		t.Fatalf("Query() error = %v, want nil", err)
	}

	if len(result.Rows) != 2 {
		t.Errorf("Query() returned %v rows, want 2", len(result.Rows))
	}
}

func TestUserPrivilegesTableQueryWithOffset(t *testing.T) {
	am := setupTestACLManager(t)

	// Create users
	am.AddUser("%", "user1", "")
	am.Grant("%", "user1", []string{"SELECT", "INSERT", "UPDATE"})

	table := NewUserPrivilegesTable(am)

	options := &domain.QueryOptions{
		Limit:  2,
		Offset: 1,
	}

	result, err := table.Query(context.Background(), nil, options)
	if err != nil {
		t.Fatalf("Query() error = %v, want nil", err)
	}

	if len(result.Rows) != 2 {
		t.Errorf("Query() returned %v rows, want 2", len(result.Rows))
	}
}

func TestUserPrivilegesTableIsGrantable(t *testing.T) {
	am := setupTestACLManager(t)

	// Create user with GRANT OPTION
	am.AddUser("%", "grantor", "")
	am.Grant("%", "grantor", []string{"SELECT", "GRANT OPTION"})

	// Create user without GRANT OPTION
	am.AddUser("%", "nongrantor", "")
	am.Grant("%", "nongrantor", []string{"SELECT"})

	table := NewUserPrivilegesTable(am)
	result, err := table.Query(context.Background(), nil, nil)
	if err != nil {
		t.Fatalf("Query() error = %v, want nil", err)
	}

	// Find grantor SELECT privilege
	var grantorRow domain.Row
	for _, row := range result.Rows {
		if row["GRANTEE"] == "'grantor'@'%'" && row["PRIVILEGE_TYPE"] == "SELECT" {
			grantorRow = row
			break
		}
	}

	if grantorRow == nil {
		t.Fatal("grantor SELECT privilege row not found")
	}

	if grantorRow["IS_GRANTABLE"] != "YES" {
		t.Errorf("grantor IS_GRANTABLE = %v, want YES", grantorRow["IS_GRANTABLE"])
	}

	// Find nongrantor SELECT privilege
	var nonGrantorRow domain.Row
	for _, row := range result.Rows {
		if row["GRANTEE"] == "'nongrantor'@'%'" && row["PRIVILEGE_TYPE"] == "SELECT" {
			nonGrantorRow = row
			break
		}
	}

	if nonGrantorRow == nil {
		t.Fatal("nongrantor SELECT privilege row not found")
	}

	if nonGrantorRow["IS_GRANTABLE"] != "NO" {
		t.Errorf("nongrantor IS_GRANTABLE = %v, want NO", nonGrantorRow["IS_GRANTABLE"])
	}
}

func TestUserPrivilegesTableNoGrantOptionRows(t *testing.T) {
	am := setupTestACLManager(t)

	// Create user with only GRANT OPTION
	am.AddUser("%", "testuser", "")
	am.Grant("%", "testuser", []string{"GRANT OPTION"})

	table := NewUserPrivilegesTable(am)
	result, err := table.Query(context.Background(), nil, nil)
	if err != nil {
		t.Fatalf("Query() error = %v, want nil", err)
	}

	// GRANT OPTION itself should not appear in privilege list
	for _, row := range result.Rows {
		if row["GRANTEE"] == "'testuser'@'%'" && row["PRIVILEGE_TYPE"] == "GRANT OPTION" {
			t.Error("GRANT OPTION should not appear in USER_PRIVILEGES")
		}
	}
}

func TestUserPrivilegesTableEmptyPrivileges(t *testing.T) {
	am := setupTestACLManager(t)

	// Create user with no privileges
	am.AddUser("%", "testuser", "")

	table := NewUserPrivilegesTable(am)
	result, err := table.Query(context.Background(), nil, nil)
	if err != nil {
		t.Fatalf("Query() error = %v, want nil", err)
	}

	// User with no privileges should not appear
	for _, row := range result.Rows {
		if row["GRANTEE"] == "'testuser'@'%'" {
			t.Error("User with no privileges should not appear in USER_PRIVILEGES")
		}
	}
}

func TestNewSchemaPrivilegesTable(t *testing.T) {
	am := setupTestACLManager(t)

	table := NewSchemaPrivilegesTable(am)
	if table == nil {
		t.Fatal("NewSchemaPrivilegesTable() returned nil")
	}

	if table.GetName() != "SCHEMA_PRIVILEGES" {
		t.Errorf("GetName() = %v, want 'SCHEMA_PRIVILEGES'", table.GetName())
	}
}

func TestSchemaPrivilegesTableGetSchema(t *testing.T) {
	am := setupTestACLManager(t)

	table := NewSchemaPrivilegesTable(am)
	schema := table.GetSchema()

	if len(schema) == 0 {
		t.Fatal("GetSchema() returned empty schema")
	}

	// Check for required columns
	requiredColumns := []string{
		"GRANTEE", "TABLE_CATALOG", "TABLE_SCHEMA", "PRIVILEGE_TYPE", "IS_GRANTABLE",
	}

	columnMap := make(map[string]bool)
	for _, col := range schema {
		columnMap[col.Name] = true
	}

	for _, reqCol := range requiredColumns {
		if !columnMap[reqCol] {
			t.Errorf("GetSchema() missing required column: %v", reqCol)
		}
	}
}

func TestSchemaPrivilegesTableQuery(t *testing.T) {
	am := setupTestACLManager(t)

	table := NewSchemaPrivilegesTable(am)
	result, err := table.Query(context.Background(), nil, nil)
	if err != nil {
		t.Fatalf("Query() error = %v, want nil", err)
	}

	if result == nil {
		t.Fatal("Query() returned nil result")
	}

	// Currently returns empty result
	if len(result.Rows) != 0 {
		t.Errorf("Query() returned %v rows, want 0", len(result.Rows))
	}
}

func TestNewTablePrivilegesTable(t *testing.T) {
	am := setupTestACLManager(t)

	table := NewTablePrivilegesTable(am)
	if table == nil {
		t.Fatal("NewTablePrivilegesTable() returned nil")
	}

	if table.GetName() != "TABLE_PRIVILEGES" {
		t.Errorf("GetName() = %v, want 'TABLE_PRIVILEGES'", table.GetName())
	}
}

func TestTablePrivilegesTableGetSchema(t *testing.T) {
	am := setupTestACLManager(t)

	table := NewTablePrivilegesTable(am)
	schema := table.GetSchema()

	if len(schema) == 0 {
		t.Fatal("GetSchema() returned empty schema")
	}

	// Check for required columns
	requiredColumns := []string{
		"GRANTEE", "TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME",
		"PRIVILEGE_TYPE", "IS_GRANTABLE",
	}

	columnMap := make(map[string]bool)
	for _, col := range schema {
		columnMap[col.Name] = true
	}

	for _, reqCol := range requiredColumns {
		if !columnMap[reqCol] {
			t.Errorf("GetSchema() missing required column: %v", reqCol)
		}
	}
}

func TestTablePrivilegesTableQuery(t *testing.T) {
	am := setupTestACLManager(t)

	table := NewTablePrivilegesTable(am)
	result, err := table.Query(context.Background(), nil, nil)
	if err != nil {
		t.Fatalf("Query() error = %v, want nil", err)
	}

	if result == nil {
		t.Fatal("Query() returned nil result")
	}

	// Currently returns empty result
	if len(result.Rows) != 0 {
		t.Errorf("Query() returned %v rows, want 0", len(result.Rows))
	}
}

func TestNewColumnPrivilegesTable(t *testing.T) {
	am := setupTestACLManager(t)

	table := NewColumnPrivilegesTable(am)
	if table == nil {
		t.Fatal("NewColumnPrivilegesTable() returned nil")
	}

	if table.GetName() != "COLUMN_PRIVILEGES" {
		t.Errorf("GetName() = %v, want 'COLUMN_PRIVILEGES'", table.GetName())
	}
}

func TestColumnPrivilegesTableGetSchema(t *testing.T) {
	am := setupTestACLManager(t)

	table := NewColumnPrivilegesTable(am)
	schema := table.GetSchema()

	if len(schema) == 0 {
		t.Fatal("GetSchema() returned empty schema")
	}

	// Check for required columns
	requiredColumns := []string{
		"GRANTEE", "TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME",
		"COLUMN_NAME", "PRIVILEGE_TYPE", "IS_GRANTABLE",
	}

	columnMap := make(map[string]bool)
	for _, col := range schema {
		columnMap[col.Name] = true
	}

	for _, reqCol := range requiredColumns {
		if !columnMap[reqCol] {
			t.Errorf("GetSchema() missing required column: %v", reqCol)
		}
	}
}

func TestColumnPrivilegesTableQuery(t *testing.T) {
	am := setupTestACLManager(t)

	table := NewColumnPrivilegesTable(am)
	result, err := table.Query(context.Background(), nil, nil)
	if err != nil {
		t.Fatalf("Query() error = %v, want nil", err)
	}

	if result == nil {
		t.Fatal("Query() returned nil result")
	}

	// Currently returns empty result
	if len(result.Rows) != 0 {
		t.Errorf("Query() returned %v rows, want 0", len(result.Rows))
	}
}

func TestBoolToYN(t *testing.T) {
	tests := []struct {
		name string
		b    bool
		want string
	}{
		{
			name: "True to YES",
			b:    true,
			want: "YES",
		},
		{
			name: "False to NO",
			b:    false,
			want: "NO",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := boolToYN(tt.b)
			if got != tt.want {
				t.Errorf("boolToYN() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestApplyPrivilegeFilters(t *testing.T) {
	rows := []domain.Row{
		{"GRANTEE": "'user1'@'%'", "PRIVILEGE_TYPE": "SELECT"},
		{"GRANTEE": "'user2'@'%'", "PRIVILEGE_TYPE": "INSERT"},
		{"GRANTEE": "'user1'@'%'", "PRIVILEGE_TYPE": "UPDATE"},
	}

	filters := []domain.Filter{
		{Field: "GRANTEE", Operator: "=", Value: "'user1'@'%'"},
	}

	filtered, err := applyPrivilegeFilters(rows, filters)
	if err != nil {
		t.Fatalf("applyPrivilegeFilters() error = %v, want nil", err)
	}

	if len(filtered) != 2 {
		t.Errorf("applyPrivilegeFilters() returned %v rows, want 2", len(filtered))
	}

	for _, row := range filtered {
		if row["GRANTEE"] != "'user1'@'%'" {
			t.Errorf("applyPrivilegeFilters() returned row with GRANTEE = %v, want 'user1'@'%%'", row["GRANTEE"])
		}
	}
}

func TestApplyMultiplePrivilegeFilters(t *testing.T) {
	rows := []domain.Row{
		{"GRANTEE": "'user1'@'%'", "PRIVILEGE_TYPE": "SELECT"},
		{"GRANTEE": "'user1'@'%'", "PRIVILEGE_TYPE": "INSERT"},
		{"GRANTEE": "'user2'@'%'", "PRIVILEGE_TYPE": "SELECT"},
	}

	filters := []domain.Filter{
		{Field: "GRANTEE", Operator: "=", Value: "'user1'@'%'"},
		{Field: "PRIVILEGE_TYPE", Operator: "=", Value: "SELECT"},
	}

	filtered, err := applyPrivilegeFilters(rows, filters)
	if err != nil {
		t.Fatalf("applyPrivilegeFilters() error = %v, want nil", err)
	}

	if len(filtered) != 1 {
		t.Errorf("applyPrivilegeFilters() returned %v rows, want 1", len(filtered))
	}

	if filtered[0]["GRANTEE"] != "'user1'@'%'" || filtered[0]["PRIVILEGE_TYPE"] != "SELECT" {
		t.Error("applyPrivilegeFilters() returned wrong row")
	}
}

func TestMatchesPrivilegeFilter(t *testing.T) {
	row := domain.Row{
		"GRANTEE":        "'testuser'@'%'",
		"PRIVILEGE_TYPE": "SELECT",
		"IS_GRANTABLE":   "NO",
	}

	tests := []struct {
		name    string
		row     domain.Row
		filter  domain.Filter
		want    bool
		wantErr bool
	}{
		{
			name:   "Equal match",
			row:    row,
			filter: domain.Filter{Field: "PRIVILEGE_TYPE", Operator: "=", Value: "SELECT"},
			want:   true,
		},
		{
			name:   "Equal no match",
			row:    row,
			filter: domain.Filter{Field: "PRIVILEGE_TYPE", Operator: "=", Value: "INSERT"},
			want:   false,
		},
		{
			name:   "Not equal match",
			row:    row,
			filter: domain.Filter{Field: "PRIVILEGE_TYPE", Operator: "!=", Value: "INSERT"},
			want:   true,
		},
		{
			name:   "Not equal no match",
			row:    row,
			filter: domain.Filter{Field: "PRIVILEGE_TYPE", Operator: "!=", Value: "SELECT"},
			want:   false,
		},
		{
			name:    "Unsupported operator",
			row:     row,
			filter:  domain.Filter{Field: "PRIVILEGE_TYPE", Operator: ">", Value: "SELECT"},
			want:    false,
			wantErr: true,
		},
		{
			name:   "Non-existent field",
			row:    row,
			filter: domain.Filter{Field: "NonExistent", Operator: "=", Value: "test"},
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := matchesPrivilegeFilter(tt.row, tt.filter)
			if (err != nil) != tt.wantErr {
				t.Errorf("matchesPrivilegeFilter() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("matchesPrivilegeFilter() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMatchesPrivilegeLike(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		pattern string
		want    bool
	}{
		{
			name:    "Wildcard only",
			value:   "anything",
			pattern: "%",
			want:    true,
		},
		{
			name:    "Exact match",
			value:   "SELECT",
			pattern: "SELECT",
			want:    true,
		},
		{
			name:    "Exact match case insensitive",
			value:   "select",
			pattern: "SELECT",
			want:    true,
		},
		{
			name:    "Prefix wildcard",
			value:   "GRANT OPTION",
			pattern: "%OPTION",
			want:    true,
		},
		{
			name:    "Prefix wildcard case insensitive",
			value:   "grant option",
			pattern: "%OPTION",
			want:    true,
		},
		{
			name:    "Prefix wildcard no match",
			value:   "SELECT",
			pattern: "%INSERT",
			want:    false,
		},
		{
			name:    "Suffix wildcard",
			value:   "INSERT PRIVILEGE",
			pattern: "INSERT%",
			want:    true,
		},
		{
			name:    "Suffix wildcard case insensitive",
			value:   "insert privilege",
			pattern: "INSERT%",
			want:    true,
		},
		{
			name:    "Suffix wildcard no match",
			value:   "DELETE",
			pattern: "INSERT%",
			want:    false,
		},
		{
			name:    "No match",
			value:   "SELECT",
			pattern: "INSERT",
			want:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := matchesPrivilegeLike(tt.value, tt.pattern)
			if got != tt.want {
				t.Errorf("matchesPrivilegeLike() = %v, want %v", got, tt.want)
			}
		})
	}
}
