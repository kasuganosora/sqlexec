package optimizer

import (
	"context"
	"fmt"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/parser"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// 测试无 FROM 子句的 SELECT 查询
func TestHandleNoFromQuery(t *testing.T) {
	executor := NewOptimizedExecutor(nil, false)
	executor.SetCurrentDB("testdb")

	ctx := context.Background()

	tests := []struct {
		name        string
		sql         string
		expectError bool
		checkResult func(*testing.T, *domain.QueryResult)
	}{
		{
			name:        "SELECT constant",
			sql:         "SELECT 1",
			expectError: false,
			checkResult: func(t *testing.T, result *domain.QueryResult) {
				if result.Total != 1 {
					t.Errorf("Expected 1 row, got %d", result.Total)
				}
				if len(result.Columns) != 1 {
					t.Errorf("Expected 1 column, got %d", len(result.Columns))
				}
				if result.Columns[0].Name != "1" {
					t.Errorf("Expected column name '1', got '%s'", result.Columns[0].Name)
				}
			val, exists := result.Rows[0]["1"]
			if !exists || fmt.Sprintf("%v", val) != "1" {
				t.Errorf("Expected value 1, got %v (type: %T)", val, val)
			}
			},
		},
		{
			name:        "SELECT constant with alias",
			sql:         "SELECT 1 AS result",
			expectError: false,
			checkResult: func(t *testing.T, result *domain.QueryResult) {
				if result.Columns[0].Name != "result" {
					t.Errorf("Expected column name 'result', got '%s'", result.Columns[0].Name)
				}
			val, exists := result.Rows[0]["result"]
			if !exists || fmt.Sprintf("%v", val) != "1" {
				t.Errorf("Expected value 1, got %v (type: %T)", val, val)
			}
			},
		},
		{
			name:        "SELECT NOW()",
			sql:         "SELECT NOW()",
			expectError: false,
			checkResult: func(t *testing.T, result *domain.QueryResult) {
				if result.Total != 1 {
					t.Errorf("Expected 1 row, got %d", result.Total)
				}
				if result.Columns[0].Name != "NOW()" {
					t.Errorf("Expected column name 'NOW()', got '%s'", result.Columns[0].Name)
				}
				val, exists := result.Rows[0]["NOW()"]
				if !exists || val == nil {
					t.Errorf("Expected non-nil time value, got %v", val)
				}
			},
		},
		{
			name:        "SELECT DATABASE()",
			sql:         "SELECT DATABASE()",
			expectError: false,
			checkResult: func(t *testing.T, result *domain.QueryResult) {
				if result.Columns[0].Name != "DATABASE()" {
					t.Errorf("Expected column name 'DATABASE()', got '%s'", result.Columns[0].Name)
				}
				val, exists := result.Rows[0]["DATABASE()"]
				if !exists || val != "testdb" {
					t.Errorf("Expected 'testdb', got %v", val)
				}
			},
		},
		{
			name:        "SELECT 1+1",
			sql:         "SELECT 1+1",
			expectError: false,
			checkResult: func(t *testing.T, result *domain.QueryResult) {
				if result.Columns[0].Name != "1+1" {
					t.Errorf("Expected column name '1+1', got '%s'", result.Columns[0].Name)
				}
			val, exists := result.Rows[0]["1+1"]
			if !exists || fmt.Sprintf("%v", val) != "2" {
				t.Errorf("Expected 2, got %v (type: %T)", val, val)
			}
			},
		},
		{
			name:        "SELECT 2*3",
			sql:         "SELECT 2*3",
			expectError: false,
			checkResult: func(t *testing.T, result *domain.QueryResult) {
				if result.Columns[0].Name != "2*3" {
					t.Errorf("Expected column name '2*3', got '%s'", result.Columns[0].Name)
				}
			val, exists := result.Rows[0]["2*3"]
			if !exists || fmt.Sprintf("%v", val) != "6" {
				t.Errorf("Expected 6, got %v (type: %T)", val, val)
			}
			},
		},
		{
			name:        "SELECT 10/2",
			sql:         "SELECT 10/2",
			expectError: false,
			checkResult: func(t *testing.T, result *domain.QueryResult) {
				if result.Columns[0].Name != "10/2" {
					t.Errorf("Expected column name '10/2', got '%s'", result.Columns[0].Name)
				}
			val, exists := result.Rows[0]["10/2"]
			if !exists || fmt.Sprintf("%v", val) != "5" {
				t.Errorf("Expected 5, got %v (type: %T)", val, val)
			}
		},
		},
		{
			name:        "SELECT @@version_comment",
			sql:         "SELECT @@version_comment",
			expectError: false,
			checkResult: func(t *testing.T, result *domain.QueryResult) {
				// 注意：由于解析器的限制，@@version_comment 的列名可能无法正确提取
				// 检查列名是否为 expr_0 或其他名称
				colName := result.Columns[0].Name
				fmt.Printf("  Column name: '%s'\n", colName)
				// 尝试用列名来获取值
				val, exists := result.Rows[0][colName]
				if !exists {
					// 如果列名不存在，尝试其他可能的名称
					val, exists = result.Rows[0]["expr_0"]
					if !exists {
						val, exists = result.Rows[0]["@@version_comment"]
					}
				}
				if !exists || fmt.Sprintf("%v", val) != "sqlexec MySQL-compatible database" {
					t.Errorf("Expected 'sqlexec MySQL-compatible database', got %v", val)
				}
			},
		},
		{
			name:        "SELECT multiple columns",
			sql:         "SELECT 1, 2, 3",
			expectError: false,
			checkResult: func(t *testing.T, result *domain.QueryResult) {
				if len(result.Columns) != 3 {
					t.Errorf("Expected 3 columns, got %d", len(result.Columns))
				}
			val1, _ := result.Rows[0]["1"]
			val2, _ := result.Rows[0]["2"]
			val3, _ := result.Rows[0]["3"]
			if fmt.Sprintf("%v", val1) != "1" || fmt.Sprintf("%v", val2) != "2" || fmt.Sprintf("%v", val3) != "3" {
				t.Errorf("Expected 1,2,3 got %v,%v,%v (types: %T, %T, %T)", val1, val2, val3, val1, val2, val3)
			}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			adapter := parser.NewSQLAdapter()
			parseResult, err := adapter.Parse(tt.sql)
			if err != nil {
				t.Fatalf("Parse failed: %v", err)
			}

			if !parseResult.Success {
				t.Fatalf("Parse failed: %s", parseResult.Error)
			}

			if parseResult.Statement.Select == nil {
				t.Fatal("Expected SELECT statement")
			}

			result, err := executor.handleNoFromQuery(ctx, parseResult.Statement.Select)
			if tt.expectError {
				if err == nil {
					t.Error("Expected error, got nil")
				}
			} else {
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				if tt.checkResult != nil {
					tt.checkResult(t, result)
				}
			}
		})
	}
}
