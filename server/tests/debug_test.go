package tests

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/mysqltest"
	"github.com/stretchr/testify/assert"
)

// TestDebugSequence 测试序列号问题
func TestDebugSequence(t *testing.T) {
	testServer := mysqltest.NewTestServer()

	// 启动服务器
	err := testServer.Start(13316)
	if err != nil {
		t.Fatalf("Failed to start test server: %v", err)
	}
	defer testServer.Stop()

	// 直接使用原始连接进行测试
	err = testServer.RunWithClient(func(conn *sql.DB) error {
		// 简单的ping
		if err := conn.Ping(); err != nil {
			return fmt.Errorf("ping failed: %w", err)
		}

		t.Log("Ping successful")
		return nil
	})

	assert.NoError(t, err)
}
