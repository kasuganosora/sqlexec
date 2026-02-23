package testing

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/kasuganosora/sqlexec/pkg/api"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/resource/memory"
)

// TestShowProcessList tests SHOW PROCESSLIST functionality
func TestShowProcessList(t *testing.T) {
	// Create data source
	ds := memory.NewMVCCDataSource(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Name:     "test",
		Writable: true,
	})

	ctx := context.Background()
	err := ds.Connect(ctx)
	assert.NoError(t, err)

	// Create DB
	db, err := api.NewDB(&api.DBConfig{
		CacheEnabled: true,
		CacheSize:    1000,
		CacheTTL:     300,
		DebugMode:    false,
	})
	assert.NoError(t, err)

	// Register data source
	err = db.RegisterDataSource("test", ds)
	assert.NoError(t, err)

	// Create session and set thread ID
	sess := db.SessionWithOptions(&api.SessionOptions{
		DataSourceName: "test",
	})
	assert.NotNil(t, sess)
	sess.SetThreadID(1001)

	// Execute SHOW PROCESSLIST (empty list)
	result, err := sess.Query("SHOW PROCESSLIST")
	assert.NotNil(t, result)
	assert.Nil(t, err)

	// Verify column information
	columns := result.Columns()
	assert.NotEmpty(t, columns)
	columnNames := make([]string, len(columns))
	for i, col := range columns {
		columnNames[i] = col.Name
	}

	// Verify returned field names match MySQL standard
	expectedColumns := []string{"Id", "User", "Host", "db", "Command", "Time", "State", "Info"}
	assert.Equal(t, expectedColumns, columnNames)

	// Verify no rows returned (since no active queries)
	result.Close()
}

// TestShowFullProcessList tests SHOW FULL PROCESSLIST functionality
func TestShowFullProcessList(t *testing.T) {
	// Create data source
	ds := memory.NewMVCCDataSource(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Name:     "test",
		Writable: true,
	})

	ctx := context.Background()
	err := ds.Connect(ctx)
	assert.NoError(t, err)

	// Create DB
	db, err := api.NewDB(&api.DBConfig{
		CacheEnabled: true,
		CacheSize:    1000,
		CacheTTL:     300,
		DebugMode:    false,
	})
	assert.NoError(t, err)

	// Register data source
	err = db.RegisterDataSource("test", ds)
	assert.NoError(t, err)

	// Create session
	sess := db.SessionWithOptions(&api.SessionOptions{
		DataSourceName: "test",
	})
	assert.NotNil(t, sess)
	sess.SetThreadID(2001)

	// Execute SHOW FULL PROCESSLIST
	result, err := sess.Query("SHOW FULL PROCESSLIST")
	assert.NotNil(t, result)
	assert.Nil(t, err)

	// Verify column information same as SHOW PROCESSLIST
	columns := result.Columns()
	assert.Equal(t, 8, len(columns))

	result.Close()
}

// TestShowProcessListFields tests PROCESSLIST field types and names
func TestShowProcessListFields(t *testing.T) {
	// Create data source
	ds := memory.NewMVCCDataSource(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Name:     "test",
		Writable: true,
	})

	ctx := context.Background()
	err := ds.Connect(ctx)
	assert.NoError(t, err)

	// Create DB
	db, err := api.NewDB(&api.DBConfig{
		CacheEnabled: true,
		CacheSize:    1000,
		CacheTTL:     300,
		DebugMode:    false,
	})
	assert.NoError(t, err)

	// Register data source
	err = db.RegisterDataSource("test", ds)
	assert.NoError(t, err)

	// Create session
	sess := db.SessionWithOptions(&api.SessionOptions{
		DataSourceName: "test",
	})
	assert.NotNil(t, sess)

	// Execute SHOW PROCESSLIST
	result, err := sess.Query("SHOW PROCESSLIST")
	assert.NotNil(t, result)
	assert.Nil(t, err)

	// Verify field types
	columns := result.Columns()
	expectedFields := map[string]string{
		"Id":      "BIGINT UNSIGNED",
		"User":    "VARCHAR",
		"Host":    "VARCHAR",
		"db":      "VARCHAR",
		"Command": "VARCHAR",
		"Time":    "BIGINT UNSIGNED",
		"State":   "VARCHAR",
		"Info":    "TEXT",
	}

	for _, col := range columns {
		expectedType, ok := expectedFields[col.Name]
		assert.True(t, ok, "Unexpected column: %s", col.Name)
		assert.Equal(t, expectedType, col.Type, "Column %s should have type %s", col.Name, expectedType)
	}

	result.Close()
}
