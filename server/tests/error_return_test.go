package tests

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/mysqltest"
	"github.com/stretchr/testify/assert"
)

// TestErrorReturn 测试错误是否正确返回给客户端
func TestErrorReturn(t *testing.T) {
	testServer := mysqltest.NewTestServer()

	// 启动服务器
	err := testServer.Start(13315)
	if err != nil {
		t.Fatalf("Failed to start test server: %v", err)
	}
	defer testServer.Stop()

	err = testServer.RunWithClient(func(conn *sql.DB) error {
		// 测试1: 语法错误 - 错误的SHOW DATABASES
		t.Log("测试1: show databaes (语法错误)")
		_, err := conn.Query("show databaes")
		if err == nil {
			return fmt.Errorf("expected error for 'show databaes', got nil")
		}
		t.Logf("  错误: %v", err)
		
		// 验证错误包含正确的SQLState和消息
		// 期望: Error 1064 (42000): ...
		if !contains(err.Error(), "1064") {
			return fmt.Errorf("expected error code 1064, got: %s", err.Error())
		}
		if !contains(err.Error(), "42000") {
			return fmt.Errorf("expected SQLState 42000, got: %s", err.Error())
		}
		if !contains(err.Error(), "databaes") {
			return fmt.Errorf("expected error message to contain 'databaes', got: %s", err.Error())
		}

		// 测试2: 语法错误 - 另一个例子
		t.Log("测试2: SELEC * FROM tables (语法错误)")
		_, err = conn.Query("SELEC * FROM tables")
		if err == nil {
			return fmt.Errorf("expected error for 'SELEC * FROM tables', got nil")
		}
		t.Logf("  错误: %v", err)
		
		if !contains(err.Error(), "1064") {
			return fmt.Errorf("expected error code 1064, got: %s", err.Error())
		}

		// 测试3: 表不存在
		t.Log("测试3: SELECT * FROM non_existent_table")
		_, err = conn.Query("SELECT * FROM non_existent_table")
		if err == nil {
			return fmt.Errorf("expected error for non-existent table, got nil")
		}
		t.Logf("  错误: %v", err)
		
		if !contains(err.Error(), "1146") {
			return fmt.Errorf("expected error code 1146, got: %s", err.Error())
		}

		return nil
	})

	assert.NoError(t, err)
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && strings.Contains(s, substr)
}