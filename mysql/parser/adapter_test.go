package parser

import (
	"encoding/json"
	"fmt"
	"testing"

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
