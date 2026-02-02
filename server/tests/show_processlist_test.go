package tests

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/kasuganosora/sqlexec/pkg/api"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/resource/memory"
)

// TestShowProcessList 测试 SHOW PROCESSLIST 功能
func TestShowProcessList(t *testing.T) {
	// 创建数据源
	ds := memory.NewMVCCDataSource(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Name:     "test",
		Writable: true,
	})

	ctx := context.Background()
	err := ds.Connect(ctx)
	assert.NoError(t, err)

	// 创建 DB
	db, err := api.NewDB(&api.DBConfig{
		CacheEnabled: true,
		CacheSize:    1000,
		CacheTTL:     300,
		DebugMode:    false,
	})
	assert.NoError(t, err)

	// 注册数据源
	err = db.RegisterDataSource("test", ds)
	assert.NoError(t, err)

	// 创建 session 并设置线程ID
	sess := db.SessionWithOptions(&api.SessionOptions{
		DataSourceName: "test",
	})
	assert.NotNil(t, sess)
	sess.SetThreadID(1001)

	// 执行 SHOW PROCESSLIST（空列表）
	result, err := sess.Query("SHOW PROCESSLIST")
	assert.NotNil(t, result)
	assert.Nil(t, err)

	// 验证列信息
	columns := result.Columns()
	assert.NotEmpty(t, columns)
	columnNames := make([]string, len(columns))
	for i, col := range columns {
		columnNames[i] = col.Name
	}

	// 验证返回的字段名符合 MySQL 标准
	expectedColumns := []string{"Id", "User", "Host", "db", "Command", "Time", "State", "Info"}
	assert.Equal(t, expectedColumns, columnNames)

	// 验证没有返回行（因为没有活跃查询）
	result.Close()
}

// TestShowFullProcessList 测试 SHOW FULL PROCESSLIST 功能
func TestShowFullProcessList(t *testing.T) {
	// 创建数据源
	ds := memory.NewMVCCDataSource(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Name:     "test",
		Writable: true,
	})

	ctx := context.Background()
	err := ds.Connect(ctx)
	assert.NoError(t, err)

	// 创建 DB
	db, err := api.NewDB(&api.DBConfig{
		CacheEnabled: true,
		CacheSize:    1000,
		CacheTTL:     300,
		DebugMode:    false,
	})
	assert.NoError(t, err)

	// 注册数据源
	err = db.RegisterDataSource("test", ds)
	assert.NoError(t, err)

	// 创建 session
	sess := db.SessionWithOptions(&api.SessionOptions{
		DataSourceName: "test",
	})
	assert.NotNil(t, sess)
	sess.SetThreadID(2001)

	// 执行 SHOW FULL PROCESSLIST
	result, err := sess.Query("SHOW FULL PROCESSLIST")
	assert.NotNil(t, result)
	assert.Nil(t, err)

	// 验证列信息与 SHOW PROCESSLIST 相同
	columns := result.Columns()
	assert.Equal(t, 8, len(columns))

	result.Close()
}

// TestShowProcessListFields 测试 PROCESSLIST 字段类型和名称
func TestShowProcessListFields(t *testing.T) {
	// 创建数据源
	ds := memory.NewMVCCDataSource(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Name:     "test",
		Writable: true,
	})

	ctx := context.Background()
	err := ds.Connect(ctx)
	assert.NoError(t, err)

	// 创建 DB
	db, err := api.NewDB(&api.DBConfig{
		CacheEnabled: true,
		CacheSize:    1000,
		CacheTTL:     300,
		DebugMode:    false,
	})
	assert.NoError(t, err)

	// 注册数据源
	err = db.RegisterDataSource("test", ds)
	assert.NoError(t, err)

	// 创建 session
	sess := db.SessionWithOptions(&api.SessionOptions{
		DataSourceName: "test",
	})
	assert.NotNil(t, sess)

	// 执行 SHOW PROCESSLIST
	result, err := sess.Query("SHOW PROCESSLIST")
	assert.NotNil(t, result)
	assert.Nil(t, err)

	// 验证字段类型
	columns := result.Columns()
	expectedFields := map[string]string{
		"Id":     "BIGINT UNSIGNED",
		"User":   "VARCHAR",
		"Host":   "VARCHAR",
		"db":     "VARCHAR",
		"Command": "VARCHAR",
		"Time":   "BIGINT UNSIGNED",
		"State":  "VARCHAR",
		"Info":   "TEXT",
	}

	for _, col := range columns {
		expectedType, ok := expectedFields[col.Name]
		assert.True(t, ok, "Unexpected column: %s", col.Name)
		assert.Equal(t, expectedType, col.Type, "Column %s should have type %s", col.Name, expectedType)
	}

	result.Close()
}
