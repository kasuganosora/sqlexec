package parser

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
)

func TestSQLAdapter_ParseSelect(t *testing.T) {
	adapter := NewSQLAdapter()

	testCases := []struct {
		name     string
		sql      string
		expected SQLType
	}{
		{
			name:     "简单 SELECT",
			sql:      "SELECT id, name FROM users",
			expected: SQLTypeSelect,
		},
		{
			name:     "带 WHERE 条件",
			sql:      "SELECT id, name FROM users WHERE age > 25",
			expected: SQLTypeSelect,
		},
		{
			name:     "带 ORDER BY",
			sql:      "SELECT * FROM users ORDER BY created_at DESC",
			expected: SQLTypeSelect,
		},
		{
			name:     "带 LIMIT",
			sql:      "SELECT * FROM users LIMIT 10",
			expected: SQLTypeSelect,
		},
		{
			name:     "带 JOIN",
			sql:      "SELECT u.name, o.order_id FROM users u JOIN orders o ON u.id = o.user_id",
			expected: SQLTypeSelect,
		},
		{
			name:     "复杂查询",
			sql:      "SELECT id, name, age FROM users WHERE age > 18 AND status = 'active' ORDER BY created_at DESC LIMIT 20",
			expected: SQLTypeSelect,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := adapter.Parse(tc.sql)
			if err != nil {
				t.Errorf("解析失败: %v", err)
				return
			}

			if !result.Success {
				t.Errorf("解析不成功: %s", result.Error)
				return
			}

			if result.Statement.Type != tc.expected {
				t.Errorf("期望类型 %s, 实际 %s", tc.expected, result.Statement.Type)
			}

			// 打印解析结果
			t.Logf("✓ %s", tc.name)
			printStatement(result.Statement)
		})
	}
}

func TestSQLAdapter_ParseInsert(t *testing.T) {
	adapter := NewSQLAdapter()

	testCases := []struct {
		name string
		sql  string
	}{
		{
			name: "单行插入",
			sql:  "INSERT INTO users (name, age) VALUES ('Alice', 25)",
		},
		{
			name: "多行插入",
			sql:  "INSERT INTO users (name, age) VALUES ('Bob', 30), ('Charlie', 35)",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := adapter.Parse(tc.sql)
			if err != nil {
				t.Errorf("解析失败: %v", err)
				return
			}

			if !result.Success {
				t.Errorf("解析不成功: %s", result.Error)
				return
			}

			if result.Statement.Type != SQLTypeInsert {
				t.Errorf("期望类型 %s, 实际 %s", SQLTypeInsert, result.Statement.Type)
			}

			t.Logf("✓ %s", tc.name)
			printStatement(result.Statement)
		})
	}
}

func TestSQLAdapter_ParseUpdate(t *testing.T) {
	adapter := NewSQLAdapter()

	testCases := []struct {
		name string
		sql  string
	}{
		{
			name: "简单更新",
			sql:  "UPDATE users SET age = 26 WHERE id = 1",
		},
		{
			name: "多字段更新",
			sql:  "UPDATE users SET age = 26, status = 'active' WHERE id = 1",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := adapter.Parse(tc.sql)
			if err != nil {
				t.Errorf("解析失败: %v", err)
				return
			}

			if !result.Success {
				t.Errorf("解析不成功: %s", result.Error)
				return
			}

			if result.Statement.Type != SQLTypeUpdate {
				t.Errorf("期望类型 %s, 实际 %s", SQLTypeUpdate, result.Statement.Type)
			}

			t.Logf("✓ %s", tc.name)
			printStatement(result.Statement)
		})
	}
}

func TestSQLAdapter_ParseDelete(t *testing.T) {
	adapter := NewSQLAdapter()

	testCases := []struct {
		name string
		sql  string
	}{
		{
			name: "简单删除",
			sql:  "DELETE FROM users WHERE id = 1",
		},
		{
			name: "带条件删除",
			sql:  "DELETE FROM users WHERE age < 18",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := adapter.Parse(tc.sql)
			if err != nil {
				t.Errorf("解析失败: %v", err)
				return
			}

			if !result.Success {
				t.Errorf("解析不成功: %s", result.Error)
				return
			}

			if result.Statement.Type != SQLTypeDelete {
				t.Errorf("期望类型 %s, 实际 %s", SQLTypeDelete, result.Statement.Type)
			}

			t.Logf("✓ %s", tc.name)
			printStatement(result.Statement)
		})
	}
}

func TestSQLAdapter_ParseDDL(t *testing.T) {
	adapter := NewSQLAdapter()

	testCases := []struct {
		name string
		sql  string
	}{
		{
			name: "CREATE TABLE",
			sql:  "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255), age INT)",
		},
		{
			name: "DROP TABLE",
			sql:  "DROP TABLE users",
		},
		{
			name: "DROP TABLE IF EXISTS",
			sql:  "DROP TABLE IF EXISTS users",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := adapter.Parse(tc.sql)
			if err != nil {
				t.Errorf("解析失败: %v", err)
				return
			}

			if !result.Success {
				t.Errorf("解析不成功: %s", result.Error)
				return
			}

			t.Logf("✓ %s", tc.name)
			printStatement(result.Statement)
		})
	}
}

func TestSQLAdapter_ParseComplex(t *testing.T) {
	adapter := NewSQLAdapter()

	complexSQL := `
		SELECT 
			u.id, u.name, u.age,
			COUNT(o.order_id) as order_count,
			SUM(o.amount) as total_amount
		FROM users u
		LEFT JOIN orders o ON u.id = o.user_id
		WHERE u.status = 'active' AND o.created_at > '2024-01-01'
		GROUP BY u.id, u.name, u.age
		HAVING COUNT(o.order_id) > 5
		ORDER BY total_amount DESC
		LIMIT 20
	`

	result, err := adapter.Parse(complexSQL)
	if err != nil {
		t.Errorf("解析失败: %v", err)
		return
	}

	if !result.Success {
		t.Errorf("解析不成功: %s", result.Error)
		return
	}

	t.Logf("✓ 复杂查询解析成功")
	printStatement(result.Statement)
}

func printStatement(stmt *SQLStatement) {
	jsonData, _ := json.MarshalIndent(stmt, "", "  ")
	fmt.Printf("解析结果:\n%s\n", string(jsonData))
}

// BenchmarkSQLAdapter_Parse 性能测试
func BenchmarkSQLAdapter_Parse(b *testing.B) {
	adapter := NewSQLAdapter()
	sql := "SELECT id, name, age FROM users WHERE age > 25 AND status = 'active' ORDER BY created_at DESC LIMIT 10"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := adapter.Parse(sql)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestAdapter_ParseShowStatement(t *testing.T) {
	adapter := NewSQLAdapter()

	tests := []struct {
		name     string
		sql      string
		expected string
		table    string
		wantErr  bool
	}{
		{
			name:     "SHOW TABLES",
			sql:      "SHOW TABLES",
			expected: "TABLES",
			wantErr:  false,
		},
		{
			name:     "SHOW TABLES LIKE",
			sql:      "SHOW TABLES LIKE 'user%'",
			expected: "TABLES",
			wantErr:  false,
		},
		{
			name:     "SHOW DATABASES",
			sql:      "SHOW DATABASES",
			expected: "DATABASES",
			wantErr:  false,
		},
		{
			name:     "SHOW COLUMNS FROM table",
			sql:      "SHOW COLUMNS FROM users",
			expected: "COLUMNS",
			table:    "users",
			wantErr:  false,
		},
		{
			name:     "SHOW CREATE TABLE",
			sql:      "SHOW CREATE TABLE users",
			expected: "CREATE_TABLE",
			table:    "users",
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := adapter.Parse(tt.sql)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}

		assert.NoError(t, err)
		assert.True(t, result.Success)
		assert.Equal(t, SQLTypeShow, result.Statement.Type)
		assert.NotNil(t, result.Statement.Show)
			assert.Equal(t, tt.expected, result.Statement.Show.Type)

			if tt.table != "" {
				assert.Equal(t, tt.table, result.Statement.Show.Table)
			}
		})
	}
}

func TestAdapter_ParseDescribeStatement(t *testing.T) {
	adapter := NewSQLAdapter()

	tests := []struct {
		name     string
		sql      string
		table    string
		column   string
		wantErr  bool
	}{
		{
			name:    "DESCRIBE table",
			sql:     "DESCRIBE users",
			table:   "users",
			wantErr: false,
		},
		{
			name:    "DESC table",
			sql:     "DESC users",
			table:   "users",
			wantErr: false,
		},
		{
			name:    "DESCRIBE table column",
			sql:     "DESCRIBE users name",
			table:   "users",
			column:  "name",
			wantErr: false,
		},
		{
			name:    "DESC table column",
			sql:     "DESC users email",
			table:   "users",
			column:  "email",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := adapter.Parse(tt.sql)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}

		assert.NoError(t, err)
		assert.True(t, result.Success)
		assert.Equal(t, SQLTypeDescribe, result.Statement.Type)
		assert.NotNil(t, result.Statement.Describe)
			assert.Equal(t, tt.table, result.Statement.Describe.Table)

			if tt.column != "" {
				assert.Equal(t, tt.column, result.Statement.Describe.Column)
			}
		})
	}
}
