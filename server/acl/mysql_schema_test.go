package acl

import (
	"context"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

func TestNewMySQLUserTable(t *testing.T) {
	tmpDir := t.TempDir()
	am, err := NewACLManager(tmpDir)
	if err != nil {
		t.Fatalf("NewACLManager() failed: %v", err)
	}

	table := NewMySQLUserTable(am)
	if table == nil {
		t.Fatal("NewMySQLUserTable() returned nil")
	}

	if table.GetName() != "user" {
		t.Errorf("GetName() = %v, want 'user'", table.GetName())
	}
}

func TestMySQLUserTableGetSchema(t *testing.T) {
	tmpDir := t.TempDir()
	am, err := NewACLManager(tmpDir)
	if err != nil {
		t.Fatalf("NewACLManager() failed: %v", err)
	}

	table := NewMySQLUserTable(am)
	schema := table.GetSchema()

	if len(schema) == 0 {
		t.Fatal("GetSchema() returned empty schema")
	}

	// Check for essential columns
	requiredColumns := []string{
		"Host", "User", "Password",
		"Select_priv", "Insert_priv", "Update_priv", "Delete_priv",
		"Create_priv", "Drop_priv", "Grant_priv",
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

func TestMySQLUserTableQuery(t *testing.T) {
	tmpDir := t.TempDir()
	am, err := NewACLManager(tmpDir)
	if err != nil {
		t.Fatalf("NewACLManager() failed: %v", err)
	}

	// Create test users
	am.CreateUser("%", "testuser1", "")
	am.CreateUser("localhost", "testuser2", "")
	am.CreateUser("192.168.1.1", "testuser3", "")

	// Grant some privileges
	am.Grant("%", "testuser1", []PermissionType{PrivSelect, PrivInsert}, PermissionLevelDatabase, "", "", "")

	table := NewMySQLUserTable(am)
	result, err := table.Query(context.Background(), nil, nil)
	if err != nil {
		t.Fatalf("Query() error = %v, want nil", err)
	}

	if result == nil {
		t.Fatal("Query() returned nil result")
	}

	// Should have 4 users (3 test + root)
	if result.Total != 4 {
		t.Errorf("Query() Total = %v, want 4", result.Total)
	}

	if len(result.Rows) != 4 {
		t.Errorf("Query() returned %v rows, want 4", len(result.Rows))
	}

	// Verify columns match schema
	if len(result.Columns) != len(table.GetSchema()) {
		t.Errorf("Query() column count mismatch")
	}
}

func TestMySQLUserTableQueryWithFilters(t *testing.T) {
	tmpDir := t.TempDir()
	am, err := NewACLManager(tmpDir)
	if err != nil {
		t.Fatalf("NewACLManager() failed: %v", err)
	}

	// Create test users
	am.CreateUser("%", "testuser1", "")
	am.CreateUser("localhost", "testuser2", "")

	table := NewMySQLUserTable(am)

	tests := []struct {
		name     string
		filters  []domain.Filter
		wantRows int
	}{
		{
			name:     "Filter by user",
			filters:  []domain.Filter{{Field: "User", Operator: "=", Value: "testuser1"}},
			wantRows: 1,
		},
		{
			name:     "Filter by host",
			filters:  []domain.Filter{{Field: "Host", Operator: "=", Value: "%"}},
			wantRows: 2, // testuser1 + root
		},
		{
			name: "Filter by user and host",
			filters: []domain.Filter{
				{Field: "User", Operator: "=", Value: "testuser1"},
				{Field: "Host", Operator: "=", Value: "%"},
			},
			wantRows: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := table.Query(context.Background(), tt.filters, nil)
			if err != nil {
				t.Fatalf("Query() error = %v, want nil", err)
			}

			if len(result.Rows) != tt.wantRows {
				t.Errorf("Query() returned %v rows, want %v", len(result.Rows), tt.wantRows)
			}
		})
	}
}

func TestMySQLUserTableQueryWithLimit(t *testing.T) {
	tmpDir := t.TempDir()
	am, err := NewACLManager(tmpDir)
	if err != nil {
		t.Fatalf("NewACLManager() failed: %v", err)
	}

	// Create test users
	am.CreateUser("%", "user1", "")
	am.CreateUser("%", "user2", "")
	am.CreateUser("%", "user3", "")

	table := NewMySQLUserTable(am)

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

func TestMySQLUserTableQueryWithOffset(t *testing.T) {
	tmpDir := t.TempDir()
	am, err := NewACLManager(tmpDir)
	if err != nil {
		t.Fatalf("NewACLManager() failed: %v", err)
	}

	// Create test users
	am.CreateUser("%", "user1", "")
	am.CreateUser("%", "user2", "")
	am.CreateUser("%", "user3", "")

	table := NewMySQLUserTable(am)

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

func TestMySQLUserTablePrivilegeMapping(t *testing.T) {
	tmpDir := t.TempDir()
	am, err := NewACLManager(tmpDir)
	if err != nil {
		t.Fatalf("NewACLManager() failed: %v", err)
	}

	// Create user and grant global privileges (which go into User.Privileges)
	am.CreateUser("%", "testuser", "")
	am.Grant("%", "testuser", []PermissionType{PrivSelect, PrivInsert, PrivGrant}, PermissionLevelGlobal, "", "", "")

	table := NewMySQLUserTable(am)
	result, err := table.Query(context.Background(), nil, nil)
	if err != nil {
		t.Fatalf("Query() error = %v, want nil", err)
	}

	// Find testuser row
	var testUserRow domain.Row
	for _, row := range result.Rows {
		if row["User"] == "testuser" {
			testUserRow = row
			break
		}
	}

	if testUserRow == nil {
		t.Fatal("testuser row not found")
	}

	// Verify privilege mapping
	if testUserRow["Select_priv"] != "Y" {
		t.Errorf("Select_priv = %v, want Y", testUserRow["Select_priv"])
	}
	if testUserRow["Insert_priv"] != "Y" {
		t.Errorf("Insert_priv = %v, want Y", testUserRow["Insert_priv"])
	}
	if testUserRow["Update_priv"] != "N" {
		t.Errorf("Update_priv = %v, want N", testUserRow["Update_priv"])
	}
	if testUserRow["Grant_priv"] != "Y" {
		t.Errorf("Grant_priv = %v, want Y", testUserRow["Grant_priv"])
	}
}

func TestNewMySQLDBTable(t *testing.T) {
	tmpDir := t.TempDir()
	am, err := NewACLManager(tmpDir)
	if err != nil {
		t.Fatalf("NewACLManager() failed: %v", err)
	}

	table := NewMySQLDBTable(am)
	if table == nil {
		t.Fatal("NewMySQLDBTable() returned nil")
	}

	if table.GetName() != "db" {
		t.Errorf("GetName() = %v, want 'db'", table.GetName())
	}
}

func TestMySQLDBTableGetSchema(t *testing.T) {
	tmpDir := t.TempDir()
	am, err := NewACLManager(tmpDir)
	if err != nil {
		t.Fatalf("NewACLManager() failed: %v", err)
	}

	table := NewMySQLDBTable(am)
	schema := table.GetSchema()

	if len(schema) == 0 {
		t.Fatal("GetSchema() returned empty schema")
	}

	// Check for essential columns
	requiredColumns := []string{"Host", "Db", "User", "Select_priv", "Insert_priv"}

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

func TestMySQLDBTableQuery(t *testing.T) {
	tmpDir := t.TempDir()
	am, err := NewACLManager(tmpDir)
	if err != nil {
		t.Fatalf("NewACLManager() failed: %v", err)
	}

	table := NewMySQLDBTable(am)
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

func TestNewMySQLTablesPrivTable(t *testing.T) {
	tmpDir := t.TempDir()
	am, err := NewACLManager(tmpDir)
	if err != nil {
		t.Fatalf("NewACLManager() failed: %v", err)
	}

	table := NewMySQLTablesPrivTable(am)
	if table == nil {
		t.Fatal("NewMySQLTablesPrivTable() returned nil")
	}

	if table.GetName() != "tables_priv" {
		t.Errorf("GetName() = %v, want 'tables_priv'", table.GetName())
	}
}

func TestMySQLTablesPrivTableGetSchema(t *testing.T) {
	tmpDir := t.TempDir()
	am, err := NewACLManager(tmpDir)
	if err != nil {
		t.Fatalf("NewACLManager() failed: %v", err)
	}

	table := NewMySQLTablesPrivTable(am)
	schema := table.GetSchema()

	if len(schema) == 0 {
		t.Fatal("GetSchema() returned empty schema")
	}

	// Check for essential columns
	requiredColumns := []string{"Host", "Db", "User", "Table_name", "Grantor", "Timestamp", "Table_priv", "Column_priv"}

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

func TestMySQLTablesPrivTableQuery(t *testing.T) {
	tmpDir := t.TempDir()
	am, err := NewACLManager(tmpDir)
	if err != nil {
		t.Fatalf("NewACLManager() failed: %v", err)
	}

	table := NewMySQLTablesPrivTable(am)
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

func TestNewMySQLColumnsPrivTable(t *testing.T) {
	tmpDir := t.TempDir()
	am, err := NewACLManager(tmpDir)
	if err != nil {
		t.Fatalf("NewACLManager() failed: %v", err)
	}

	table := NewMySQLColumnsPrivTable(am)
	if table == nil {
		t.Fatal("NewMySQLColumnsPrivTable() returned nil")
	}

	if table.GetName() != "columns_priv" {
		t.Errorf("GetName() = %v, want 'columns_priv'", table.GetName())
	}
}

func TestMySQLColumnsPrivTableGetSchema(t *testing.T) {
	tmpDir := t.TempDir()
	am, err := NewACLManager(tmpDir)
	if err != nil {
		t.Fatalf("NewACLManager() failed: %v", err)
	}

	table := NewMySQLColumnsPrivTable(am)
	schema := table.GetSchema()

	if len(schema) == 0 {
		t.Fatal("GetSchema() returned empty schema")
	}

	// Check for essential columns
	requiredColumns := []string{"Host", "Db", "User", "Table_name", "Column_name", "Timestamp", "Column_priv"}

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

func TestMySQLColumnsPrivTableQuery(t *testing.T) {
	tmpDir := t.TempDir()
	am, err := NewACLManager(tmpDir)
	if err != nil {
		t.Fatalf("NewACLManager() failed: %v", err)
	}

	table := NewMySQLColumnsPrivTable(am)
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
			name: "True to Y",
			b:    true,
			want: "Y",
		},
		{
			name: "False to N",
			b:    false,
			want: "N",
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

func TestMatchesFilter(t *testing.T) {
	row := domain.Row{
		"Host":     "%",
		"User":     "testuser",
		"Password": "",
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
			filter: domain.Filter{Field: "User", Operator: "=", Value: "testuser"},
			want:   true,
		},
		{
			name:   "Equal no match",
			row:    row,
			filter: domain.Filter{Field: "User", Operator: "=", Value: "otheruser"},
			want:   false,
		},
		{
			name:   "Not equal match",
			row:    row,
			filter: domain.Filter{Field: "User", Operator: "!=", Value: "otheruser"},
			want:   true,
		},
		{
			name:   "Not equal no match",
			row:    row,
			filter: domain.Filter{Field: "User", Operator: "!=", Value: "testuser"},
			want:   false,
		},
		{
			name:    "Unsupported operator",
			row:     row,
			filter:  domain.Filter{Field: "User", Operator: ">", Value: "testuser"},
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
			got, err := matchesFilter(tt.row, tt.filter)
			if (err != nil) != tt.wantErr {
				t.Errorf("matchesFilter() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("matchesFilter() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMatchesLike(t *testing.T) {
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
			value:   "test",
			pattern: "test",
			want:    true,
		},
		{
			name:    "Prefix wildcard - value ends with suffix",
			value:   "testuser",
			pattern: "%user",
			want:    true,
		},
		{
			name:    "Prefix wildcard exact match",
			value:   "user",
			pattern: "%user",
			want:    true,
		},
		{
			name:    "Prefix wildcard no match",
			value:   "testadmin",
			pattern: "%user",
			want:    false,
		},
		{
			name:    "Suffix wildcard - value starts with prefix",
			value:   "testuser",
			pattern: "test%",
			want:    true,
		},
		{
			name:    "Suffix wildcard exact match",
			value:   "test",
			pattern: "test%",
			want:    true,
		},
		{
			name:    "Suffix wildcard no match",
			value:   "admintest",
			pattern: "test%",
			want:    false,
		},
		{
			name:    "No match",
			value:   "different",
			pattern: "test",
			want:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := matchesLike(tt.value, tt.pattern)
			if got != tt.want {
				t.Errorf("matchesLike(%q, %q) = %v, want %v", tt.value, tt.pattern, got, tt.want)
			}
		})
	}
}

func TestApplyFilters(t *testing.T) {
	rows := []domain.Row{
		{"User": "user1", "Host": "%"},
		{"User": "user2", "Host": "localhost"},
		{"User": "user3", "Host": "%"},
	}

	filters := []domain.Filter{
		{Field: "Host", Operator: "=", Value: "%"},
	}

	filtered, err := applyFilters(rows, filters)
	if err != nil {
		t.Fatalf("applyFilters() error = %v, want nil", err)
	}

	if len(filtered) != 2 {
		t.Errorf("applyFilters() returned %v rows, want 2", len(filtered))
	}

	for _, row := range filtered {
		if row["Host"] != "%" {
			t.Errorf("applyFilters() returned row with Host = %v, want %s", row["Host"], "%")
		}
	}
}

func TestApplyMultipleFilters(t *testing.T) {
	rows := []domain.Row{
		{"User": "user1", "Host": "%"},
		{"User": "user2", "Host": "localhost"},
		{"User": "user3", "Host": "%"},
	}

	filters := []domain.Filter{
		{Field: "Host", Operator: "=", Value: "%"},
		{Field: "User", Operator: "=", Value: "user1"},
	}

	filtered, err := applyFilters(rows, filters)
	if err != nil {
		t.Fatalf("applyFilters() error = %v, want nil", err)
	}

	if len(filtered) != 1 {
		t.Errorf("applyFilters() returned %v rows, want 1", len(filtered))
	}

	if filtered[0]["User"] != "user1" {
		t.Errorf("applyFilters() returned wrong user: %v", filtered[0]["User"])
	}
}
