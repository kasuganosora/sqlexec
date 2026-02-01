package tests

import (
	"database/sql"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/mysqltest"
	"github.com/stretchr/testify/assert"
)

// TestSimpleConnection 测试简单的连接
func TestSimpleConnection(t *testing.T) {
	testServer := mysqltest.NewTestServer()

	// 启动服务器
	err := testServer.Start(13315)
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
