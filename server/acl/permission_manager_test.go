package acl

import (
	"strconv"
	"testing"
)

func TestNewPermissionManager(t *testing.T) {
	pm := NewPermissionManager()
	if pm == nil {
		t.Fatal("NewPermissionManager() returned nil")
	}
	if pm.dbPermissions == nil {
		t.Fatal("NewPermissionManager() dbPermissions map is nil")
	}
	if pm.tablePermissions == nil {
		t.Fatal("NewPermissionManager() tablePermissions map is nil")
	}
	if pm.columnPermissions == nil {
		t.Fatal("NewPermissionManager() columnPermissions map is nil")
	}
}

func TestGrantDatabasePermission(t *testing.T) {
	pm := NewPermissionManager()

	tests := []struct {
		name         string
		host         string
		user         string
		db           string
		permissions  []PermissionType
		wantErr      bool
	}{
		{
			name:        "Grant SELECT permission",
			host:        "%",
			user:        "testuser",
			db:          "testdb",
			permissions: []PermissionType{PrivSelect},
			wantErr:     false,
		},
		{
			name:        "Grant multiple permissions",
			host:        "%",
			user:        "testuser",
			db:          "testdb",
			permissions: []PermissionType{PrivSelect, PrivInsert, PrivUpdate},
			wantErr:     false,
		},
		{
			name:        "Grant ALL PRIVILEGES",
			host:        "%",
			user:        "testuser",
			db:          "testdb",
			permissions: []PermissionType{PrivAllPrivileges},
			wantErr:     false,
		},
		{
			name:        "Grant with wildcard database",
			host:        "%",
			user:        "testuser",
			db:          "%",
			permissions: []PermissionType{PrivSelect},
			wantErr:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := pm.Grant(tt.host, tt.user, tt.permissions, PermissionLevelDatabase, tt.db, "", "")
			if (err != nil) != tt.wantErr {
				t.Errorf("Grant() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr && err == nil {
				// Verify permission was granted
				key := pm.makeDBKey(tt.host, tt.db, tt.user)
				if _, exists := pm.dbPermissions[key]; !exists {
					t.Error("Grant() did not create database permission")
				}
			}
		})
	}
}

func TestGrantTablePermission(t *testing.T) {
	pm := NewPermissionManager()

	err := pm.Grant("%", "testuser", []PermissionType{PrivSelect, PrivInsert}, PermissionLevelTable, "testdb", "testtable", "")
	if err != nil {
		t.Fatalf("Grant() failed: %v", err)
	}

	// Verify permission was granted
	key := pm.makeTableKey("%", "testdb", "testuser", "testtable")
	perm, exists := pm.tablePermissions[key]
	if !exists {
		t.Fatal("Grant() did not create table permission")
	}

	// Verify permissions
	if !perm.Privileges["SELECT"] {
		t.Error("Grant() did not grant SELECT permission")
	}
	if !perm.Privileges["INSERT"] {
		t.Error("Grant() did not grant INSERT permission")
	}
}

func TestGrantColumnPermission(t *testing.T) {
	pm := NewPermissionManager()

	err := pm.Grant("%", "testuser", []PermissionType{PrivSelect, PrivUpdate}, PermissionLevelColumn, "testdb", "testtable", "col1")
	if err != nil {
		t.Fatalf("Grant() failed: %v", err)
	}

	// Verify permission was granted
	key := pm.makeColumnKey("%", "testdb", "testuser", "testtable", "col1")
	perm, exists := pm.columnPermissions[key]
	if !exists {
		t.Fatal("Grant() did not create column permission")
	}

	// Verify permissions
	if !perm.Privileges["SELECT"] {
		t.Error("Grant() did not grant SELECT permission")
	}
	if !perm.Privileges["UPDATE"] {
		t.Error("Grant() did not grant UPDATE permission")
	}
}

func TestGrantUnsupportedLevel(t *testing.T) {
	pm := NewPermissionManager()

	err := pm.Grant("%", "testuser", []PermissionType{PrivSelect}, PermissionLevelGlobal, "", "", "")
	if err == nil {
		t.Error("Grant() should return error for unsupported permission level")
	}
}

func TestGrantAllPrivileges(t *testing.T) {
	pm := NewPermissionManager()

	err := pm.Grant("%", "testuser", []PermissionType{PrivAllPrivileges}, PermissionLevelDatabase, "testdb", "", "")
	if err != nil {
		t.Fatalf("Grant() failed: %v", err)
	}

	// Verify all privileges were granted
	key := pm.makeDBKey("%", "testdb", "testuser")
	perm, exists := pm.dbPermissions[key]
	if !exists {
		t.Fatal("Grant() did not create database permission")
	}

	allPrivTypes := AllPermissionTypes()
	for _, privType := range allPrivTypes {
		if !perm.Privileges[string(privType)] {
			t.Errorf("Grant() did not grant ALL PRIVILEGES, missing %v", privType)
		}
	}
}

func TestRevokeDatabasePermission(t *testing.T) {
	pm := NewPermissionManager()

	// Grant first
	err := pm.Grant("%", "testuser", []PermissionType{PrivSelect, PrivInsert}, PermissionLevelDatabase, "testdb", "", "")
	if err != nil {
		t.Fatalf("Grant() failed: %v", err)
	}

	// Revoke SELECT
	err = pm.Revoke("%", "testuser", []PermissionType{PrivSelect}, PermissionLevelDatabase, "testdb", "", "")
	if err != nil {
		t.Errorf("Revoke() error = %v, want nil", err)
	}

	// Verify SELECT was revoked
	key := pm.makeDBKey("%", "testdb", "testuser")
	perm, exists := pm.dbPermissions[key]
	if !exists {
		t.Fatal("Permission not found after grant")
	}
	if perm.Privileges["SELECT"] {
		t.Error("Revoke() did not revoke SELECT permission")
	}
	if !perm.Privileges["INSERT"] {
		t.Error("Revoke() incorrectly revoked INSERT permission")
	}
}

func TestRevokeTablePermission(t *testing.T) {
	pm := NewPermissionManager()

	// Grant first
	err := pm.Grant("%", "testuser", []PermissionType{PrivSelect, PrivInsert}, PermissionLevelTable, "testdb", "testtable", "")
	if err != nil {
		t.Fatalf("Grant() failed: %v", err)
	}

	// Revoke INSERT
	err = pm.Revoke("%", "testuser", []PermissionType{PrivInsert}, PermissionLevelTable, "testdb", "testtable", "")
	if err != nil {
		t.Errorf("Revoke() error = %v, want nil", err)
	}

	// Verify INSERT was revoked
	key := pm.makeTableKey("%", "testdb", "testuser", "testtable")
	perm, exists := pm.tablePermissions[key]
	if !exists {
		t.Fatal("Permission not found after grant")
	}
	if perm.Privileges["INSERT"] {
		t.Error("Revoke() did not revoke INSERT permission")
	}
	if !perm.Privileges["SELECT"] {
		t.Error("Revoke() incorrectly revoked SELECT permission")
	}
}

func TestRevokeColumnPermission(t *testing.T) {
	pm := NewPermissionManager()

	// Grant first
	err := pm.Grant("%", "testuser", []PermissionType{PrivSelect, PrivUpdate}, PermissionLevelColumn, "testdb", "testtable", "col1")
	if err != nil {
		t.Fatalf("Grant() failed: %v", err)
	}

	// Revoke UPDATE
	err = pm.Revoke("%", "testuser", []PermissionType{PrivUpdate}, PermissionLevelColumn, "testdb", "testtable", "col1")
	if err != nil {
		t.Errorf("Revoke() error = %v, want nil", err)
	}

	// Verify UPDATE was revoked
	key := pm.makeColumnKey("%", "testdb", "testuser", "testtable", "col1")
	perm, exists := pm.columnPermissions[key]
	if !exists {
		t.Fatal("Permission not found after grant")
	}
	if perm.Privileges["UPDATE"] {
		t.Error("Revoke() did not revoke UPDATE permission")
	}
	if !perm.Privileges["SELECT"] {
		t.Error("Revoke() incorrectly revoked SELECT permission")
	}
}

func TestPermissionManagerRevokeAllPrivileges(t *testing.T) {
	pm := NewPermissionManager()

	// Grant all privileges
	err := pm.Grant("%", "testuser", []PermissionType{PrivAllPrivileges}, PermissionLevelDatabase, "testdb", "", "")
	if err != nil {
		t.Fatalf("Grant() failed: %v", err)
	}

	// Revoke all privileges
	err = pm.Revoke("%", "testuser", []PermissionType{PrivAllPrivileges}, PermissionLevelDatabase, "testdb", "", "")
	if err != nil {
		t.Errorf("Revoke() error = %v, want nil", err)
	}

	// Verify all privileges were revoked
	key := pm.makeDBKey("%", "testdb", "testuser")
	perm, exists := pm.dbPermissions[key]
	if !exists {
		t.Fatal("Permission not found after grant")
	}

	allPrivTypes := AllPermissionTypes()
	for _, privType := range allPrivTypes {
		if perm.Privileges[string(privType)] {
			t.Errorf("Revoke() did not revoke all privileges, %v still granted", privType)
		}
	}
}

func TestRevokeNonExistentPermission(t *testing.T) {
	pm := NewPermissionManager()

	err := pm.Revoke("%", "testuser", []PermissionType{PrivSelect}, PermissionLevelDatabase, "nonexistent", "", "")
	if err == nil {
		t.Error("Revoke() should return error for non-existent permission")
	}
}

func TestPermissionManagerCheckPermission(t *testing.T) {
	pm := NewPermissionManager()

	// Grant database-level permissions
	err := pm.Grant("%", "testuser", []PermissionType{PrivSelect, PrivInsert}, PermissionLevelDatabase, "testdb", "", "")
	if err != nil {
		t.Fatalf("Grant() failed: %v", err)
	}

	// Grant table-level permissions
	err = pm.Grant("%", "testuser", []PermissionType{PrivUpdate, PrivDelete}, PermissionLevelTable, "testdb", "testtable", "")
	if err != nil {
		t.Fatalf("Grant() failed: %v", err)
	}

	// Grant column-level permissions
	err = pm.Grant("%", "testuser", []PermissionType{PrivReferences}, PermissionLevelColumn, "testdb", "testtable", "col1")
	if err != nil {
		t.Fatalf("Grant() failed: %v", err)
	}

	tests := []struct {
		name    string
		host    string
		user    string
		priv    PermissionType
		db      string
		table   string
		column  string
		want    bool
	}{
		{
			name:   "Check granted database-level SELECT",
			host:   "%",
			user:   "testuser",
			priv:   PrivSelect,
			db:     "testdb",
			table:  "",
			column: "",
			want:   true,
		},
		{
			name:   "Check granted database-level INSERT",
			host:   "%",
			user:   "testuser",
			priv:   PrivInsert,
			db:     "testdb",
			table:  "",
			column: "",
			want:   true,
		},
		{
			name:   "Check granted table-level UPDATE",
			host:   "%",
			user:   "testuser",
			priv:   PrivUpdate,
			db:     "testdb",
			table:  "testtable",
			column: "",
			want:   true,
		},
		{
			name:   "Check granted table-level DELETE",
			host:   "%",
			user:   "testuser",
			priv:   PrivDelete,
			db:     "testdb",
			table:  "testtable",
			column: "",
			want:   true,
		},
		{
			name:   "Check granted column-level REFERENCES",
			host:   "%",
			user:   "testuser",
			priv:   PrivReferences,
			db:     "testdb",
			table:  "testtable",
			column: "col1",
			want:   true,
		},
		{
			name:   "Check non-granted permission",
			host:   "%",
			user:   "testuser",
			priv:   PrivCreate,
			db:     "testdb",
			table:  "",
			column: "",
			want:   false,
		},
		{
			name:   "Check permission for non-existent table",
			host:   "%",
			user:   "testuser",
			priv:   PrivSelect,
			db:     "testdb",
			table:  "nonexistent",
			column: "",
			want:   true, // Has database-level SELECT, so any table in testdb has SELECT
		},
		{
			name:   "Check permission for non-existent column",
			host:   "%",
			user:   "testuser",
			priv:   PrivReferences,
			db:     "testdb",
			table:  "testtable",
			column: "nonexistent",
			want:   false, // Column-level permission was granted only to "col1", not "nonexistent"
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := pm.CheckPermission(tt.host, tt.user, tt.priv, tt.db, tt.table, tt.column)
			if got != tt.want {
				t.Errorf("CheckPermission() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCheckPermissionWithWildcard(t *testing.T) {
	pm := NewPermissionManager()

	// Grant permissions with wildcard database
	err := pm.Grant("%", "testuser", []PermissionType{PrivSelect}, PermissionLevelDatabase, "%", "", "")
	if err != nil {
		t.Fatalf("Grant() failed: %v", err)
	}

	// Grant permissions with wildcard table
	err = pm.Grant("%", "testuser", []PermissionType{PrivInsert}, PermissionLevelTable, "%", "%", "")
	if err != nil {
		t.Fatalf("Grant() failed: %v", err)
	}

	// Check if wildcard permissions apply to specific databases/tables
	if !pm.CheckPermission("%", "testuser", PrivSelect, "testdb", "", "") {
		t.Error("CheckPermission() should find permission with wildcard database")
	}
	if !pm.CheckPermission("%", "testuser", PrivInsert, "testdb", "testtable", "") {
		t.Error("CheckPermission() should find permission with wildcard table")
	}
}

func TestCheckPermissionWithWildcardHost(t *testing.T) {
	pm := NewPermissionManager()

	// Grant permission with wildcard host
	err := pm.Grant("%", "testuser", []PermissionType{PrivSelect}, PermissionLevelDatabase, "testdb", "", "")
	if err != nil {
		t.Fatalf("Grant() failed: %v", err)
	}

	// Check if permission applies to specific host
	if !pm.CheckPermission("localhost", "testuser", PrivSelect, "testdb", "", "") {
		t.Error("CheckPermission() should find permission with wildcard host")
	}
}

func TestPermissionManagerHasGrantOption(t *testing.T) {
	pm := NewPermissionManager()

	tests := []struct {
		name        string
		setupGrant  bool
		host        string
		user        string
		want        bool
	}{
		{
			name:       "Has GRANT OPTION",
			setupGrant: true,
			host:       "%",
			user:       "testuser",
			want:       true,
		},
		{
			name:       "No GRANT OPTION",
			setupGrant: false,
			host:       "%",
			user:       "testuser",
			want:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pm = NewPermissionManager() // Reset for each test

			if tt.setupGrant {
				err := pm.Grant(tt.host, tt.user, []PermissionType{PrivGrant}, PermissionLevelDatabase, "testdb", "", "")
				if err != nil {
					t.Fatalf("Grant() failed: %v", err)
				}
			}

			got := pm.HasGrantOption(tt.host, tt.user)
			if got != tt.want {
				t.Errorf("HasGrantOption() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLoadPermissions(t *testing.T) {
	pm := NewPermissionManager()

	dbPerms := []DatabasePermission{
		{
			Host:       "%",
			Db:         "testdb",
			User:       "testuser",
			Privileges: map[string]bool{"SELECT": true, "INSERT": false},
		},
	}

	tablePerms := []TablePermission{
		{
			Host:       "%",
			Db:         "testdb",
			User:       "testuser",
			TableName:  "testtable",
			Grantor:    "root",
			Timestamp:  "2024-01-01 00:00:00",
			Privileges: map[string]bool{"UPDATE": true, "DELETE": false},
		},
	}

	columnPerms := []ColumnPermission{
		{
			Host:       "%",
			Db:         "testdb",
			User:       "testuser",
			TableName:  "testtable",
			ColumnName: "col1",
			Timestamp:  "2024-01-01 00:00:00",
			Privileges: map[string]bool{"REFERENCES": true},
		},
	}

	err := pm.LoadPermissions(dbPerms, tablePerms, columnPerms)
	if err != nil {
		t.Errorf("LoadPermissions() error = %v, want nil", err)
	}

	// Verify database permissions were loaded
	if len(pm.dbPermissions) != 1 {
		t.Errorf("LoadPermissions() loaded %v db permissions, want 1", len(pm.dbPermissions))
	}

	// Verify table permissions were loaded
	if len(pm.tablePermissions) != 1 {
		t.Errorf("LoadPermissions() loaded %v table permissions, want 1", len(pm.tablePermissions))
	}

	// Verify column permissions were loaded
	if len(pm.columnPermissions) != 1 {
		t.Errorf("LoadPermissions() loaded %v column permissions, want 1", len(pm.columnPermissions))
	}
}

func TestExportPermissions(t *testing.T) {
	pm := NewPermissionManager()

	// Grant some permissions
	pm.Grant("%", "testuser", []PermissionType{PrivSelect, PrivInsert}, PermissionLevelDatabase, "testdb", "", "")
	pm.Grant("%", "testuser", []PermissionType{PrivUpdate}, PermissionLevelTable, "testdb", "testtable", "")
	pm.Grant("%", "testuser", []PermissionType{PrivReferences}, PermissionLevelColumn, "testdb", "testtable", "col1")

	// Export permissions
	dbPerms, tablePerms, colPerms := pm.ExportPermissions()

	if len(dbPerms) != 1 {
		t.Errorf("ExportPermissions() exported %v db permissions, want 1", len(dbPerms))
	}
	if len(tablePerms) != 1 {
		t.Errorf("ExportPermissions() exported %v table permissions, want 1", len(tablePerms))
	}
	if len(colPerms) != 1 {
		t.Errorf("ExportPermissions() exported %v column permissions, want 1", len(colPerms))
	}
}

func TestMakeDBKey(t *testing.T) {
	pm := NewPermissionManager()

	tests := []struct {
		host string
		db   string
		user string
		want string
	}{
		{"%", "testdb", "testuser", "%:testdb:testuser"},
		{"localhost", "testdb", "testuser", "localhost:testdb:testuser"},
		{"LOCALHOST", "TESTDB", "TESTUSER", "localhost:testdb:testuser"},
		{"192.168.1.1", "my_db", "user@domain", "192.168.1.1:my_db:user@domain"},
	}

	for _, tt := range tests {
		t.Run(tt.host+"@"+tt.user+"@"+tt.db, func(t *testing.T) {
			got := pm.makeDBKey(tt.host, tt.db, tt.user)
			if got != tt.want {
				t.Errorf("makeDBKey() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMakeTableKey(t *testing.T) {
	pm := NewPermissionManager()

	tests := []struct {
		host  string
		db    string
		user  string
		table string
		want  string
	}{
		{"%", "testdb", "testuser", "testtable", "%:testdb:testuser:testtable"},
		{"localhost", "testdb", "testuser", "testtable", "localhost:testdb:testuser:testtable"},
		{"LOCALHOST", "TESTDB", "TESTUSER", "TESTTABLE", "localhost:testdb:testuser:testtable"},
		{"192.168.1.1", "my_db", "user@domain", "my_table", "192.168.1.1:my_db:user@domain:my_table"},
	}

	for _, tt := range tests {
		t.Run(tt.host+"@"+tt.user+"@"+tt.db+"@"+tt.table, func(t *testing.T) {
			got := pm.makeTableKey(tt.host, tt.db, tt.user, tt.table)
			if got != tt.want {
				t.Errorf("makeTableKey() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMakeColumnKey(t *testing.T) {
	pm := NewPermissionManager()

	tests := []struct {
		host   string
		db     string
		user   string
		table  string
		column string
		want   string
	}{
		{"%", "testdb", "testuser", "testtable", "col1", "%:testdb:testuser:testtable:col1"},
		{"localhost", "testdb", "testuser", "testtable", "col1", "localhost:testdb:testuser:testtable:col1"},
		{"LOCALHOST", "TESTDB", "TESTUSER", "TESTTABLE", "COL1", "localhost:testdb:testuser:testtable:col1"},
		{"192.168.1.1", "my_db", "user@domain", "my_table", "my_col", "192.168.1.1:my_db:user@domain:my_table:my_col"},
	}

	for _, tt := range tests {
		t.Run(tt.host+"@"+tt.user+"@"+tt.db+"@"+tt.table+"@"+tt.column, func(t *testing.T) {
			got := pm.makeColumnKey(tt.host, tt.db, tt.user, tt.table, tt.column)
			if got != tt.want {
				t.Errorf("makeColumnKey() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPermissionManagerConcurrentOperations(t *testing.T) {
	pm := NewPermissionManager()
	done := make(chan bool)

	// Grant permissions concurrently
	for i := 0; i < 100; i++ {
		go func(n int) {
			err := pm.Grant("%", "testuser", []PermissionType{PrivSelect}, PermissionLevelDatabase, "testdb"+strconv.Itoa(n), "", "")
			done <- (err == nil)
		}(i)
	}

	// Wait for all operations
	successCount := 0
	for i := 0; i < 100; i++ {
		if <-done {
			successCount++
		}
	}

	// All should succeed
	if successCount != 100 {
		t.Errorf("Concurrent Grant() success count = %v, want 100", successCount)
	}

	// Verify all permissions exist
	if len(pm.dbPermissions) != 100 {
		t.Errorf("Concurrent operations created %v permissions, want 100", len(pm.dbPermissions))
	}
}

func TestContains(t *testing.T) {
	permissions := []PermissionType{PrivSelect, PrivInsert, PrivUpdate}

	tests := []struct {
		name       string
		permission PermissionType
		want       bool
	}{
		{
			name:       "Contains SELECT",
			permission: PrivSelect,
			want:       true,
		},
		{
			name:       "Contains INSERT",
			permission: PrivInsert,
			want:       true,
		},
		{
			name:       "Contains UPDATE",
			permission: PrivUpdate,
			want:       true,
		},
		{
			name:       "Does not contain DELETE",
			permission: PrivDelete,
			want:       false,
		},
		{
			name:       "Does not contain CREATE",
			permission: PrivCreate,
			want:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := contains(permissions, tt.permission)
			if got != tt.want {
				t.Errorf("contains() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPermissionPrecedence(t *testing.T) {
	pm := NewPermissionManager()

	// Grant SELECT at database level
	err := pm.Grant("%", "testuser", []PermissionType{PrivSelect}, PermissionLevelDatabase, "testdb", "", "")
	if err != nil {
		t.Fatalf("Grant() failed: %v", err)
	}

	// Revoke SELECT at table level (should not override database-level)
	err = pm.Grant("%", "testuser", []PermissionType{PrivSelect}, PermissionLevelTable, "testdb", "testtable", "")
	if err != nil {
		t.Fatalf("Grant() failed: %v", err)
	}
	err = pm.Revoke("%", "testuser", []PermissionType{PrivSelect}, PermissionLevelTable, "testdb", "testtable", "")
	if err != nil {
		t.Fatalf("Revoke() failed: %v", err)
	}

	// Should still have SELECT from database level
	if !pm.CheckPermission("%", "testuser", PrivSelect, "testdb", "testtable", "") {
		t.Error("CheckPermission() should find database-level permission")
	}
}
