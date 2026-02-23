package pool

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "modernc.org/sqlite"
)

func getTestDB(t *testing.T) *sql.DB {
	db, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)
	return db
}

func TestNewSQLConnectionPool(t *testing.T) {
	db := getTestDB(t)

	pool := NewSQLConnectionPool(db, 10, 5)

	assert.NotNil(t, pool)
	assert.Equal(t, 10, pool.maxOpen)
	assert.Equal(t, 5, pool.maxIdle)
	assert.Equal(t, db, pool.db)
}

func TestSQLConnectionPool_GetDB(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)

	pool := NewSQLConnectionPool(db, 10, 5)

	retrievedDB := pool.GetDB()

	assert.Equal(t, db, retrievedDB)
}

func TestSQLConnectionPool_Stats(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)

	pool := NewSQLConnectionPool(db, 10, 5)

	stats := pool.Stats()

	assert.Equal(t, 10, stats.MaxOpenConnections)
}

func TestSQLConnectionPool_Close(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)

	pool := NewSQLConnectionPool(db, 10, 5)

	err = pool.Close()

	assert.NoError(t, err)
}

func TestNewConnectionManager(t *testing.T) {
	manager := NewConnectionManager()

	assert.NotNil(t, manager)
	assert.NotNil(t, manager.pools)
}

func TestConnectionManager_RegisterPool(t *testing.T) {
	manager := NewConnectionManager()

	db, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)

	manager.RegisterPool("test", db, 10, 5)

	pool, ok := manager.GetPool("test")

	assert.True(t, ok)
	assert.NotNil(t, pool)
	assert.Equal(t, 10, pool.maxOpen)
}

func TestConnectionManager_GetPool_NotFound(t *testing.T) {
	manager := NewConnectionManager()

	pool, ok := manager.GetPool("nonexistent")

	assert.False(t, ok)
	assert.Nil(t, pool)
}

func TestConnectionManager_Close(t *testing.T) {
	manager := NewConnectionManager()

	db1, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)
	db2, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)

	manager.RegisterPool("pool1", db1, 10, 5)
	manager.RegisterPool("pool2", db2, 10, 5)

	err = manager.Close()

	assert.NoError(t, err)

	// 验证池已被清空
	_, ok := manager.GetPool("pool1")
	assert.False(t, ok)
}

func TestConnectionManager_ConcurrentAccess(t *testing.T) {
	manager := NewConnectionManager()

	done := make(chan bool)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for i := 0; i < 10; i++ {
		go func(i int) {
			db, err := sql.Open("sqlite", ":memory:")
			if err != nil {
				t.Errorf("Failed to open db: %v", err)
				return
			}
			manager.RegisterPool("pool"+string(rune('0'+i)), db, 10, 5)
			done <- true
		}(i)
	}

	// 等待所有注册完成或超时
	for i := 0; i < 10; i++ {
		select {
		case <-done:
		case <-ctx.Done():
			t.Fatal("Test timed out")
		}
	}

	// 验证所有池都已注册
	for i := 0; i < 10; i++ {
		_, ok := manager.GetPool("pool" + string(rune('0'+i)))
		assert.True(t, ok)
	}
}
