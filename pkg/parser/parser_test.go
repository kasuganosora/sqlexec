package parser

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewParser(t *testing.T) {
	p := NewParser()
	if p == nil {
		t.Fatal("NewParser returned nil")
	}
	if p.parser == nil {
		t.Error("parser field should be initialized")
	}
}

func TestParseSQL(t *testing.T) {
	p := NewParser()

	tests := []struct {
		name          string
		sql           string
		expectError   bool
		expectedCount int
	}{
		{"Simple SELECT", "SELECT * FROM users", false, 1},
		{"Multiple statements", "SELECT 1; SELECT 2", false, 2},
		{"INSERT statement", "INSERT INTO users (name) VALUES ('John')", false, 1},
		{"UPDATE statement", "UPDATE users SET name = 'Jane'", false, 1},
		{"DELETE statement", "DELETE FROM users WHERE id = 1", false, 1},
		{"Invalid SQL", "SELEC * FROM", true, 0}, // TiDB parser可能会报错
		// 注意：TiDB parser对空字符串可能不报错，而是返回空语句列表
		// {"Empty string", "", true, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmts, err := p.ParseSQL(tt.sql)
			if (err != nil) != tt.expectError {
				t.Errorf("ParseSQL() error = %v, expectError %v", err, tt.expectError)
				return
			}

			if !tt.expectError && len(stmts) != tt.expectedCount {
				t.Errorf("ParseSQL() returned %d statements, want %d",
					len(stmts), tt.expectedCount)
			}
		})
	}
}

func TestParseOneStmt(t *testing.T) {
	p := NewParser()

	tests := []struct {
		name        string
		sql         string
		expectError bool
	}{
		{"Valid SELECT", "SELECT * FROM users", false},
		{"Valid INSERT", "INSERT INTO users (name) VALUES ('John')", false},
		{"Invalid SQL", "INVALID SQL", true},
		{"Empty string", "", true},
		{"Multiple statements", "SELECT 1; SELECT 2", false}, // 返回第一个
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := p.ParseOneStmt(tt.sql)
			if (err != nil) != tt.expectError {
				t.Errorf("ParseOneStmt() error = %v, expectError %v", err, tt.expectError)
			}
			if !tt.expectError && stmt == nil {
				t.Error("ParseOneStmt() should return non-nil statement")
			}
		})
	}
}

func TestParseOneStmtText(t *testing.T) {
	p := NewParser()

	tests := []struct {
		name        string
		sql         string
		expectError bool
	}{
		{"Valid SELECT", "SELECT * FROM users", false},
		{"With leading spaces", "  SELECT * FROM users", false},
		{"With trailing spaces", "SELECT * FROM users  ", false},
		{"Empty string", "", true},
		{"Only spaces", "   ", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := p.ParseOneStmtText(tt.sql)
			if (err != nil) != tt.expectError {
				t.Errorf("ParseOneStmtText() error = %v, expectError %v", err, tt.expectError)
			}
			if !tt.expectError && stmt == nil {
				t.Error("ParseOneStmtText() should return non-nil statement")
			}
		})
	}
}

func TestParseSelectStmt(t *testing.T) {
	p := NewParser()

	tests := []struct {
		name        string
		sql         string
		expectError bool
	}{
		{"Simple SELECT", "SELECT * FROM users", false},
		{"SELECT with columns", "SELECT id, name FROM users", false},
		{"SELECT with WHERE", "SELECT * FROM users WHERE age > 18", false},
		{"SELECT with JOIN", "SELECT * FROM users u JOIN orders o ON u.id = o.user_id", false},
		{"SELECT with GROUP BY", "SELECT dept, COUNT(*) FROM users GROUP BY dept", false},
		{"SELECT with ORDER BY", "SELECT * FROM users ORDER BY name", false},
		{"SELECT with LIMIT", "SELECT * FROM users LIMIT 10", false},
		{"Non-SELECT statement", "INSERT INTO users (name) VALUES ('John')", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := p.ParseSelectStmt(tt.sql)
			if (err != nil) != tt.expectError {
				t.Errorf("ParseSelectStmt() error = %v, expectError %v", err, tt.expectError)
			}
			if !tt.expectError && stmt == nil {
				t.Error("ParseSelectStmt() should return non-nil SelectStmt")
			}
		})
	}
}

func TestParseInsertStmt(t *testing.T) {
	p := NewParser()

	tests := []struct {
		name        string
		sql         string
		expectError bool
	}{
		{"Simple INSERT", "INSERT INTO users (name) VALUES ('John')", false},
		{"INSERT multiple values", "INSERT INTO users (name) VALUES ('John'), ('Jane')", false},
		{"INSERT with SELECT", "INSERT INTO users SELECT * FROM temp_users", false},
		{"Non-INSERT statement", "SELECT * FROM users", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := p.ParseInsertStmt(tt.sql)
			if (err != nil) != tt.expectError {
				t.Errorf("ParseInsertStmt() error = %v, expectError %v", err, tt.expectError)
			}
			if !tt.expectError && stmt == nil {
				t.Error("ParseInsertStmt() should return non-nil InsertStmt")
			}
		})
	}
}

func TestParseUpdateStmt(t *testing.T) {
	p := NewParser()

	tests := []struct {
		name        string
		sql         string
		expectError bool
	}{
		{"Simple UPDATE", "UPDATE users SET name = 'Jane'", false},
		{"UPDATE with WHERE", "UPDATE users SET age = 25 WHERE id = 1", false},
		{"UPDATE multiple columns", "UPDATE users SET name = 'Jane', age = 25 WHERE id = 1", false},
		{"Non-UPDATE statement", "SELECT * FROM users", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := p.ParseUpdateStmt(tt.sql)
			if (err != nil) != tt.expectError {
				t.Errorf("ParseUpdateStmt() error = %v, expectError %v", err, tt.expectError)
			}
			if !tt.expectError && stmt == nil {
				t.Error("ParseUpdateStmt() should return non-nil UpdateStmt")
			}
		})
	}
}

func TestParseDeleteStmt(t *testing.T) {
	p := NewParser()

	tests := []struct {
		name        string
		sql         string
		expectError bool
	}{
		{"Simple DELETE", "DELETE FROM users", false},
		{"DELETE with WHERE", "DELETE FROM users WHERE id = 1", false},
		{"DELETE with complex WHERE", "DELETE FROM users WHERE age > 25 AND status = 'inactive'", false},
		{"Non-DELETE statement", "SELECT * FROM users", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := p.ParseDeleteStmt(tt.sql)
			if (err != nil) != tt.expectError {
				t.Errorf("ParseDeleteStmt() error = %v, expectError %v", err, tt.expectError)
			}
			if !tt.expectError && stmt == nil {
				t.Error("ParseDeleteStmt() should return non-nil DeleteStmt")
			}
		})
	}
}

func TestParseSetStmt(t *testing.T) {
	p := NewParser()

	tests := []struct {
		name        string
		sql         string
		expectError bool
	}{
		{"Simple SET", "SET @var = 10", false},
		{"SET multiple variables", "SET @a = 1, @b = 2", false},
		{"Non-SET statement", "SELECT * FROM users", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := p.ParseSetStmt(tt.sql)
			if (err != nil) != tt.expectError {
				t.Errorf("ParseSetStmt() error = %v, expectError %v", err, tt.expectError)
			}
			if !tt.expectError && stmt == nil {
				t.Error("ParseSetStmt() should return non-nil SetStmt")
			}
		})
	}
}

func TestParseShowStmt(t *testing.T) {
	p := NewParser()

	tests := []struct {
		name        string
		sql         string
		expectError bool
	}{
		{"SHOW TABLES", "SHOW TABLES", false},
		{"SHOW DATABASES", "SHOW DATABASES", false},
		{"SHOW COLUMNS", "SHOW COLUMNS FROM users", false},
		{"SHOW CREATE TABLE", "SHOW CREATE TABLE users", false},
		{"Non-SHOW statement", "SELECT * FROM users", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := p.ParseShowStmt(tt.sql)
			if (err != nil) != tt.expectError {
				t.Errorf("ParseShowStmt() error = %v, expectError %v", err, tt.expectError)
			}
			if !tt.expectError && stmt == nil {
				t.Error("ParseShowStmt() should return non-nil ShowStmt")
			}
		})
	}
}

func TestParseUseStmt(t *testing.T) {
	p := NewParser()

	tests := []struct {
		name        string
		sql         string
		expectError bool
	}{
		{"Simple USE", "USE mydb", false},
		{"USE with backticks", "USE `mydb`", false},
		{"Non-USE statement", "SELECT * FROM users", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := p.ParseUseStmt(tt.sql)
			if (err != nil) != tt.expectError {
				t.Errorf("ParseUseStmt() error = %v, expectError %v", err, tt.expectError)
			}
			if !tt.expectError && stmt == nil {
				t.Error("ParseUseStmt() should return non-nil UseStmt")
			}
		})
	}
}

func TestParseCreateTableStmt(t *testing.T) {
	p := NewParser()

	tests := []struct {
		name        string
		sql         string
		expectError bool
	}{
		{"Simple CREATE TABLE", "CREATE TABLE users (id INT, name VARCHAR(100))", false},
		{"CREATE TABLE with PRIMARY KEY", "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))", false},
		{"CREATE TABLE with multiple columns", "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100), age INT)", false},
		{"Non-CREATE TABLE statement", "SELECT * FROM users", true},
		{"CREATE TABLE with ENGINE=PERSISTENT", "CREATE TABLE users (id INT, name VARCHAR(100)) ENGINE=PERSISTENT", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := p.ParseCreateTableStmt(tt.sql)
			if (err != nil) != tt.expectError {
				t.Errorf("ParseCreateTableStmt() error = %v, expectError %v", err, tt.expectError)
			}
			if !tt.expectError && stmt == nil {
				t.Error("ParseCreateTableStmt() should return non-nil CreateTableStmt")
			}
		})
	}
}

func TestParseCreateTableStmtPersistent(t *testing.T) {
	p := NewParser()
	adapter := NewSQLAdapter()

	// Test ENGINE=PERSISTENT is parsed correctly
	result, err := adapter.Parse("CREATE TABLE persistent_table (id INT PRIMARY KEY, data VARCHAR(255)) ENGINE=PERSISTENT")
	require.NoError(t, err)
	require.NotNil(t, result.Statement)
	assert.True(t, result.Statement.Create.Persistent, "Persistent should be true for ENGINE=PERSISTENT")

	// Test normal table without ENGINE
	result2, err := adapter.Parse("CREATE TABLE normal_table (id INT PRIMARY KEY, data VARCHAR(255))")
	require.NoError(t, err)
	require.NotNil(t, result2.Statement)
	assert.False(t, result2.Statement.Create.Persistent, "Persistent should be false for normal table")

	// Test ENGINE=INNODB (should not be persistent)
	result3, err := adapter.Parse("CREATE TABLE innodb_table (id INT PRIMARY KEY) ENGINE=INNODB")
	require.NoError(t, err)
	assert.False(t, result3.Statement.Create.Persistent, "Persistent should be false for ENGINE=INNODB")

	// Also verify the parser works
	_ = p
}

func TestParseCompositeIndex(t *testing.T) {
	adapter := NewSQLAdapter()

	// Test single column index
	result1, err := adapter.Parse("CREATE INDEX idx_single ON users(email)")
	require.NoError(t, err)
	require.NotNil(t, result1.Statement.CreateIndex)
	assert.Equal(t, []string{"email"}, result1.Statement.CreateIndex.Columns)

	// Test composite (multi-column) index
	result2, err := adapter.Parse("CREATE INDEX idx_composite ON users(first_name, last_name)")
	require.NoError(t, err)
	require.NotNil(t, result2.Statement.CreateIndex)
	assert.Equal(t, []string{"first_name", "last_name"}, result2.Statement.CreateIndex.Columns)

	// Test composite unique index
	result3, err := adapter.Parse("CREATE UNIQUE INDEX idx_unique_composite ON orders(user_id, order_date)")
	require.NoError(t, err)
	require.NotNil(t, result3.Statement.CreateIndex)
	assert.Equal(t, []string{"user_id", "order_date"}, result3.Statement.CreateIndex.Columns)
	assert.True(t, result3.Statement.CreateIndex.Unique)

	// Test three-column composite index
	result4, err := adapter.Parse("CREATE INDEX idx_three ON products(category, brand, price)")
	require.NoError(t, err)
	require.NotNil(t, result4.Statement.CreateIndex)
	assert.Equal(t, []string{"category", "brand", "price"}, result4.Statement.CreateIndex.Columns)
}

func TestParseDropTableStmt(t *testing.T) {
	p := NewParser()

	tests := []struct {
		name        string
		sql         string
		expectError bool
	}{
		{"Simple DROP TABLE", "DROP TABLE users", false},
		{"DROP TABLE IF EXISTS", "DROP TABLE IF EXISTS users", false},
		{"DROP multiple tables", "DROP TABLE users, orders", false},
		{"Non-DROP TABLE statement", "SELECT * FROM users", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := p.ParseDropTableStmt(tt.sql)
			if (err != nil) != tt.expectError {
				t.Errorf("ParseDropTableStmt() error = %v, expectError %v", err, tt.expectError)
			}
			if !tt.expectError && stmt == nil {
				t.Error("ParseDropTableStmt() should return non-nil DropTableStmt")
			}
		})
	}
}

func TestParseCreateDatabaseStmt(t *testing.T) {
	p := NewParser()

	tests := []struct {
		name        string
		sql         string
		expectError bool
	}{
		{"Simple CREATE DATABASE", "CREATE DATABASE mydb", false},
		{"CREATE DATABASE IF NOT EXISTS", "CREATE DATABASE IF NOT EXISTS mydb", false},
		{"Non-CREATE DATABASE statement", "SELECT * FROM users", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := p.ParseCreateDatabaseStmt(tt.sql)
			if (err != nil) != tt.expectError {
				t.Errorf("ParseCreateDatabaseStmt() error = %v, expectError %v", err, tt.expectError)
			}
			if !tt.expectError && stmt == nil {
				t.Error("ParseCreateDatabaseStmt() should return non-nil CreateDatabaseStmt")
			}
		})
	}
}

func TestParseDropDatabaseStmt(t *testing.T) {
	p := NewParser()

	tests := []struct {
		name        string
		sql         string
		expectError bool
	}{
		{"Simple DROP DATABASE", "DROP DATABASE mydb", false},
		{"DROP DATABASE IF EXISTS", "DROP DATABASE IF EXISTS mydb", false},
		{"Non-DROP DATABASE statement", "SELECT * FROM users", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := p.ParseDropDatabaseStmt(tt.sql)
			if (err != nil) != tt.expectError {
				t.Errorf("ParseDropDatabaseStmt() error = %v, expectError %v", err, tt.expectError)
			}
			if !tt.expectError && stmt == nil {
				t.Error("ParseDropDatabaseStmt() should return non-nil DropDatabaseStmt")
			}
		})
	}
}

func TestGetStmtType(t *testing.T) {
	p := NewParser()

	tests := []struct {
		name     string
		sql      string
		expected string
	}{
		{"SELECT", "SELECT * FROM users", "SELECT"},
		{"INSERT", "INSERT INTO users (name) VALUES ('John')", "INSERT"},
		{"UPDATE", "UPDATE users SET name = 'Jane'", "UPDATE"},
		{"DELETE", "DELETE FROM users WHERE id = 1", "DELETE"},
		{"SET", "SET @var = 10", "SET"},
		{"SHOW", "SHOW TABLES", "SHOW"},
		{"USE", "USE mydb", "USE"},
		{"CREATE TABLE", "CREATE TABLE users (id INT)", "CREATE_TABLE"},
		{"DROP TABLE", "DROP TABLE users", "DROP_TABLE"},
		{"CREATE DATABASE", "CREATE DATABASE mydb", "CREATE_DATABASE"},
		{"DROP DATABASE", "DROP DATABASE mydb", "DROP_DATABASE"},
		{"Unknown", "UNKNOWN STATEMENT", "UNKNOWN"},
		{"Nil statement", "", "UNKNOWN"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var stmtType string
			if tt.sql != "" {
				stmt, _ := p.ParseOneStmt(tt.sql)
				stmtType = GetStmtType(stmt)
			} else {
				stmtType = GetStmtType(nil)
			}

			if stmtType != tt.expected {
				t.Errorf("GetStmtType() = %s, want %s", stmtType, tt.expected)
			}
		})
	}
}

func TestIsWriteOperation(t *testing.T) {
	p := NewParser()

	tests := []struct {
		name     string
		sql      string
		expected bool
	}{
		{"INSERT", "INSERT INTO users (name) VALUES ('John')", true},
		{"UPDATE", "UPDATE users SET name = 'Jane'", true},
		{"DELETE", "DELETE FROM users WHERE id = 1", true},
		{"CREATE TABLE", "CREATE TABLE users (id INT)", true},
		{"DROP TABLE", "DROP TABLE users", true},
		{"CREATE DATABASE", "CREATE DATABASE mydb", true},
		{"DROP DATABASE", "DROP DATABASE mydb", true},
		{"SELECT", "SELECT * FROM users", false},
		{"SHOW", "SHOW TABLES", false},
		{"USE", "USE mydb", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, _ := p.ParseOneStmt(tt.sql)
			result := IsWriteOperation(stmt)

			if result != tt.expected {
				t.Errorf("IsWriteOperation() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestIsReadOperation(t *testing.T) {
	p := NewParser()

	tests := []struct {
		name     string
		sql      string
		expected bool
	}{
		{"SELECT", "SELECT * FROM users", true},
		{"SHOW", "SHOW TABLES", true},
		{"INSERT", "INSERT INTO users (name) VALUES ('John')", false},
		{"UPDATE", "UPDATE users SET name = 'Jane'", false},
		{"DELETE", "DELETE FROM users WHERE id = 1", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, _ := p.ParseOneStmt(tt.sql)
			result := IsReadOperation(stmt)

			if result != tt.expected {
				t.Errorf("IsReadOperation() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestIsTransactionOperation(t *testing.T) {
	p := NewParser()

	tests := []struct {
		name     string
		sql      string
		expected bool
	}{
		{"BEGIN", "BEGIN", true},
		{"START TRANSACTION", "START TRANSACTION", true},
		{"COMMIT", "COMMIT", true},
		{"ROLLBACK", "ROLLBACK", true},
		{"SELECT", "SELECT * FROM users", false},
		{"INSERT", "INSERT INTO users (name) VALUES ('John')", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, _ := p.ParseOneStmt(tt.sql)
			result := IsTransactionOperation(stmt)

			if result != tt.expected {
				t.Errorf("IsTransactionOperation() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestComplexSQL(t *testing.T) {
	p := NewParser()

	complexSQL := `
		SELECT 
			u.id,
			u.name,
			COUNT(o.id) as order_count,
			SUM(o.amount) as total_amount
		FROM users u
		LEFT JOIN orders o ON u.id = o.user_id
		WHERE u.age >= 18 AND u.status = 'active'
		GROUP BY u.id, u.name
		HAVING COUNT(o.id) > 0
		ORDER BY total_amount DESC
		LIMIT 100
	`

	stmt, err := p.ParseOneStmt(complexSQL)
	if err != nil {
		t.Errorf("ParseOneStmt() error = %v", err)
	}

	if stmt == nil {
		t.Error("Should parse complex SQL successfully")
	}

	stmtType := GetStmtType(stmt)
	if stmtType != "SELECT" {
		t.Errorf("Statement type = %s, want SELECT", stmtType)
	}
}

func TestSQLWithComments(t *testing.T) {
	p := NewParser()

	sqlWithComments := `
		SELECT * FROM users
		WHERE age > 18 -- Only adults
		ORDER BY name
	`

	stmt, err := p.ParseOneStmt(sqlWithComments)
	if err != nil {
		t.Errorf("ParseOneStmt() error = %v", err)
	}

	if stmt == nil {
		t.Error("Should parse SQL with comments successfully")
	}
}
