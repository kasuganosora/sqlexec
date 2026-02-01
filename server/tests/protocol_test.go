package tests

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/mysqltest"
	"github.com/stretchr/testify/assert"
)

// =====================================================
// 协议层测试 - 测试MySQL协议包的正确处理
// =====================================================

// TestProtocol_Connection 测试连接和认证
func TestProtocol_Connection(t *testing.T) {
	testServer := mysqltest.NewTestServer()

	// 启动服务器
	err := testServer.Start(13301)
	if err != nil {
		t.Fatalf("Failed to start test server: %v", err)
	}
	defer testServer.Stop()

	// 使用 MySQL 客户端连接
	err = testServer.RunWithClient(func(conn *sql.DB) error {
		// 测试连接和Ping
		if err := conn.Ping(); err != nil {
			return fmt.Errorf("ping failed: %w", err)
		}
		return nil
	})

	assert.NoError(t, err)
}

// TestProtocol_SimpleQuery 测试简单的SELECT查询
func TestProtocol_SimpleQuery(t *testing.T) {
	testServer := mysqltest.NewTestServer()

	// 启动服务器
	err := testServer.Start(13300)
	assert.NoError(t, err)
	defer testServer.Stop()

	// 使用 MySQL 客户端连接
	err = testServer.RunWithClient(func(conn *sql.DB) error {
		// 执行简单的查询
		var result string
		err := conn.QueryRow("SELECT 1").Scan(&result)
		return err
	})

	assert.NoError(t, err)
}

// TestProtocol_ErrorPacket 测试错误包序列化
func TestProtocol_ErrorPacket(t *testing.T) {
	testServer := mysqltest.NewTestServer()
	err := testServer.Start(13302)
	assert.NoError(t, err)
	defer testServer.Stop()

	err = testServer.RunWithClient(func(conn *sql.DB) error {
		// 测试语法错误
		_, err := conn.Query("SELEC * FROM table")
		if err == nil {
			return errors.New("expected syntax error")
		}

		// 验证错误信息格式
		if !strings.Contains(err.Error(), "1064") {
			return fmt.Errorf("error code mismatch: got %s, want 1064", err.Error())
		}
		if !strings.Contains(err.Error(), "42000") {
			return fmt.Errorf("SQLState mismatch: got %s, want 42000", err.Error())
		}

		return nil
	})

	assert.NoError(t, err)
}

// TestProtocol_EmptyQuery 测试空查询处理
func TestProtocol_EmptyQuery(t *testing.T) {
	testServer := mysqltest.NewTestServer()
	err := testServer.Start(13303)
	assert.NoError(t, err)
	defer testServer.Stop()

	err = testServer.RunWithClient(func(conn *sql.DB) error {
		// 执行空查询
		_, err := conn.Query("")
		if err == nil {
			return fmt.Errorf("expected error for empty query, got nil")
		}

		t.Logf("正确返回空查询错误: %v", err)

		// 验证连接仍然有效
		err = conn.Ping()
		if err != nil {
			return fmt.Errorf("connection should be active after empty query: %w", err)
		}

		return nil
	})

	assert.NoError(t, err)
}

// TestProtocol_PingKeepAlive 测试Ping保活
func TestProtocol_PingKeepAlive(t *testing.T) {
	testServer := mysqltest.NewTestServer()
	err := testServer.Start(13304)
	assert.NoError(t, err)
	defer testServer.Stop()

	err = testServer.RunWithClient(func(conn *sql.DB) error {
		// 多次Ping测试连接保活
		for i := 0; i < 10; i++ {
			err := conn.Ping()
			if err != nil {
				return fmt.Errorf("ping %d failed: %w", i+1, err)
			}
		}

		t.Log("成功完成10次Ping")
		return nil
	})

	assert.NoError(t, err)
}

// TestProtocol_ErrorReturn 测试错误返回格式
func TestProtocol_ErrorReturn(t *testing.T) {
	testServer := mysqltest.NewTestServer()

	// 启动服务器
	err := testServer.Start(13305)
	if err != nil {
		t.Fatalf("Failed to start test server: %v", err)
	}
	defer testServer.Stop()

	err = testServer.RunWithClient(func(conn *sql.DB) error {
		// 测试1: 语法错误
		t.Log("测试1: show databaes (语法错误)")
		_, err := conn.Query("show databaes")
		if err == nil {
			return fmt.Errorf("expected error for 'show databaes', got nil")
		}
		t.Logf("  错误: %v", err)

		// 验证错误包含正确的SQLState和消息
		if !contains(err.Error(), "1064") {
			return fmt.Errorf("expected error code 1064, got: %s", err.Error())
		}
		if !contains(err.Error(), "42000") {
			return fmt.Errorf("expected SQLState 42000, got: %s", err.Error())
		}
		if !contains(err.Error(), "databaes") {
			return fmt.Errorf("expected error message to contain 'databaes', got: %s", err.Error())
		}

		// 测试2: 表不存在
		t.Log("测试2: SELECT * FROM non_existent_table")
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
