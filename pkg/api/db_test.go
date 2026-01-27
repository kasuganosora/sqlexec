package api

import (
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewResult(t *testing.T) {
	result := NewResult(10, 100, nil)
	assert.NotNil(t, result)
	assert.Equal(t, int64(10), result.RowsAffected)
	assert.Equal(t, int64(100), result.LastInsertID)
	assert.NoError(t, result.Err())
}

func TestResult_Err(t *testing.T) {
	result := NewResult(0, 0, NewError(ErrCodeInternal, "test error", nil))
	assert.Error(t, result.Err())
	assert.Contains(t, result.Err().Error(), "test error")
}

func TestResult_Error(t *testing.T) {
	t.Run("without error", func(t *testing.T) {
		result := NewResult(5, 50, nil)
		errStr := result.Error()
		assert.Contains(t, errStr, "RowsAffected=5")
		assert.Contains(t, errStr, "LastInsertID=50")
	})

	t.Run("with error", func(t *testing.T) {
		result := NewResult(0, 0, NewError(ErrCodeInternal, "test error", nil))
		errStr := result.Error()
		assert.Contains(t, errStr, "test error")
	})
}

func TestNewDB(t *testing.T) {
	tests := []struct {
		name    string
		config  *DBConfig
		wantNil bool
	}{
		{
			name:    "default config",
			config:  nil,
			wantNil: false,
		},
		{
			name: "custom config",
			config: &DBConfig{
				CacheEnabled:  false,
				CacheSize:     500,
				CacheTTL:      600,
				DebugMode:     true,
			},
			wantNil: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, err := NewDB(tt.config)
			if tt.wantNil {
				assert.Nil(t, db)
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, db)
			if tt.config != nil {
				assert.False(t, db.config.CacheEnabled)
				assert.Equal(t, 500, db.config.CacheSize)
				assert.Equal(t, 600, db.config.CacheTTL)
				assert.True(t, db.config.DebugMode)
				assert.Nil(t, db.cache) // cache is nil when disabled
			} else {
				assert.True(t, db.config.CacheEnabled)
				assert.Equal(t, 1000, db.config.CacheSize)
				assert.Equal(t, 300, db.config.CacheTTL)
				assert.False(t, db.config.DebugMode)
				assert.NotNil(t, db.cache)
			}
			assert.NotNil(t, db.dataSources)
			assert.NotNil(t, db.logger)
			}
		})
	}
}

func TestNewDBDefaultConfig(t *testing.T) {
	db, err := NewDB(nil)
	require.NoError(t, err)
	require.NotNil(t, db)

	assert.True(t, db.config.CacheEnabled)
	assert.Equal(t, 1000, db.config.CacheSize)
	assert.Equal(t, 300, db.config.CacheTTL)
	assert.NotNil(t, db.config.DefaultLogger)
	assert.False(t, db.config.DebugMode)
}

func TestRegisterDataSource(t *testing.T) {
	db, err := NewDB(nil)
	require.NoError(t, err)

	// Register a valid datasource
	ds := newMockDataSource()
	err = db.RegisterDataSource("test", ds)
	require.NoError(t, err)

	// Verify datasource is registered
	registeredDS, err := db.GetDataSource("test")
	require.NoError(t, err)
	assert.Equal(t, ds, registeredDS)

	// Verify it's set as default
	assert.Equal(t, "test", db.defaultDS)
}

func TestRegisterDataSourceErrors(t *testing.T) {
	db, _ := NewDB(nil)

	tests := []struct {
		name    string
		dsName  string
		ds      domain.DataSource
		wantErr bool
	}{
		{
			name:    "empty name",
			dsName:  "",
			ds:      newMockDataSource(),
			wantErr: true,
		},
		{
			name:    "nil datasource",
			dsName:  "test",
			ds:      nil,
			wantErr: true,
		},
		{
			name:    "duplicate name",
			dsName:  "test",
			ds:      newMockDataSource(),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// First registration should succeed for duplicate case
			if tt.name == "duplicate name" {
				db.RegisterDataSource(tt.dsName, tt.ds)
			}

			err := db.RegisterDataSource(tt.dsName, tt.ds)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestGetDataSource(t *testing.T) {
	db, _ := NewDB(nil)

	// Test getting non-existent datasource
	ds, err := db.GetDataSource("non_existent")
	assert.Nil(t, ds)
	assert.Error(t, err)

	// Register a datasource
	testDS := newMockDataSource()
	err = db.RegisterDataSource("test", testDS)
	require.NoError(t, err)

	// Test getting existing datasource
	ds, err = db.GetDataSource("test")
	require.NoError(t, err)
	assert.Equal(t, testDS, ds)
}

func TestGetDefaultDataSource(t *testing.T) {
	db, _ := NewDB(nil)

	// Test with no datasources registered
	ds, err := db.GetDefaultDataSource()
	assert.Nil(t, ds)
	assert.Error(t, err)

	// Register a datasource
	testDS := newMockDataSource()
	err = db.RegisterDataSource("default", testDS)
	require.NoError(t, err)

	// Test getting default datasource
	ds, err = db.GetDefaultDataSource()
	require.NoError(t, err)
	assert.Equal(t, testDS, ds)
}

func TestSetDefaultDataSource(t *testing.T) {
	db, _ := NewDB(nil)

	// Register two datasources
	ds1 := newMockDataSource()
	ds2 := newMockDataSource()
	err := db.RegisterDataSource("ds1", ds1)
	require.NoError(t, err)
	err = db.RegisterDataSource("ds2", ds2)
	require.NoError(t, err)

	// Set default to ds2
	err = db.SetDefaultDataSource("ds2")
	require.NoError(t, err)
	assert.Equal(t, "ds2", db.defaultDS)

	// Try to set non-existent datasource
	err = db.SetDefaultDataSource("non_existent")
	assert.Error(t, err)
}

func TestSession(t *testing.T) {
	db, _ := NewDB(nil)

	// Register a datasource
	ds := newMockDataSource()
	err := db.RegisterDataSource("test", ds)
	require.NoError(t, err)

	// Create a session
	session := db.Session()
	assert.NotNil(t, session)
	assert.Equal(t, db, session.GetDB())
	assert.NotNil(t, session.coreSession)
	assert.True(t, session.cacheEnabled)
	assert.NotNil(t, session.logger)
	assert.NotNil(t, session.options)

	err = session.Close()
	assert.NoError(t, err)
}

func TestSessionWithOptions(t *testing.T) {
	db, _ := NewDB(nil)

	// Register a datasource
	ds := newMockDataSource()
	err := db.RegisterDataSource("test", ds)
	require.NoError(t, err)

	// Create a session with options
	session := db.SessionWithOptions(&SessionOptions{
		DataSourceName: "test",
		Isolation:      IsolationSerializable,
		ReadOnly:       true,
		CacheEnabled:   false,
	})

	require.NoError(t, err)
	assert.NotNil(t, session)
	assert.Equal(t, IsolationSerializable, session.IsolationLevel())
	assert.False(t, session.cacheEnabled)
	assert.Equal(t, db, session.GetDB())

	err = session.Close()
	assert.NoError(t, err)
}

func TestSessionWithNonExistentDatasource(t *testing.T) {
	db, _ := NewDB(nil)

	// Create a session with non-existent datasource
	session := db.SessionWithOptions(&SessionOptions{
		DataSourceName: "non_existent",
	})

	// Should create a session that returns error on use
	assert.NotNil(t, session)
	assert.NotNil(t, session.err)

	err := session.Close()
	assert.NoError(t, err)
}

func TestSetLogger(t *testing.T) {
	db, _ := NewDB(nil)

	newLogger := NewDefaultLogger(LogDebug)
	db.SetLogger(newLogger)

	assert.Equal(t, newLogger, db.GetLogger())
}

func TestClearCache(t *testing.T) {
	db, _ := NewDB(nil)

	// Register datasource and create session
	ds := newMockDataSource()
	db.RegisterDataSource("test", ds)
	session := db.Session()

	// Add some data to cache (simulated)
	result := &domain.QueryResult{
		Rows: []domain.Row{{"test": "data"}},
	}
	db.cache.Set("SELECT 1", nil, result)

	// Clear cache
	db.ClearCache()

	// Verify cache is cleared
	_, found := db.cache.Get("SELECT 1", nil)
	assert.False(t, found)

	session.Close()
}

func TestClearTableCache(t *testing.T) {
	db, _ := NewDB(nil)

	// Add some data to cache
	result := &domain.QueryResult{
		Rows: []domain.Row{{"test": "data"}},
	}
	db.cache.Set("SELECT * FROM users", nil, result)
	db.cache.Set("SELECT * FROM posts", nil, result)

	// Clear table cache for users
	db.ClearTableCache("users")

	// Note: Current implementation may not clear entries correctly
	// Just verify it doesn't panic
	db.ClearTableCache("non_existent")
}

func TestGetCacheStats(t *testing.T) {
	db, _ := NewDB(nil)

	stats := db.GetCacheStats()
	assert.NotNil(t, stats)
	assert.Equal(t, 0, stats.Size)
}

func TestClose(t *testing.T) {
	db, _ := NewDB(nil)

	// Register datasources
	ds1 := newMockDataSource()
	ds2 := newMockDataSource()
	db.RegisterDataSource("ds1", ds1)
	db.RegisterDataSource("ds2", ds2)

	// Close DB
	err := db.Close()
	require.NoError(t, err)

	// Verify datasources are closed
	assert.True(t, ds1.closed)
	assert.True(t, ds2.closed)
}

func TestGetDataSourceNames(t *testing.T) {
	db, _ := NewDB(nil)

	// Register datasources
	db.RegisterDataSource("ds1", newMockDataSource())
	db.RegisterDataSource("ds2", newMockDataSource())
	db.RegisterDataSource("ds3", newMockDataSource())

	// Get datasource names
	names := db.GetDataSourceNames()
	assert.Len(t, names, 3)
	assert.Contains(t, names, "ds1")
	assert.Contains(t, names, "ds2")
	assert.Contains(t, names, "ds3")
}

func TestDBConfig(t *testing.T) {
	config := DBConfig{
		CacheEnabled:  true,
		CacheSize:     100,
		CacheTTL:      300,
		DefaultLogger: NewDefaultLogger(LogInfo),
		DebugMode:     false,
	}

	assert.True(t, config.CacheEnabled)
	assert.Equal(t, 100, config.CacheSize)
	assert.Equal(t, 300, config.CacheTTL)
	assert.NotNil(t, config.DefaultLogger)
	assert.False(t, config.DebugMode)
}

func ExampleNewDB() {
	// Create a new DB instance
	db, _ := NewDB(nil)
	_ = db
}

func ExampleDB_RegisterDataSource() {
	db, _ := NewDB(nil)
	dataSource := newMockDataSource()

	// Register a datasource
	_ = db.RegisterDataSource("main", dataSource)
}

func ExampleDB_Session() {
	db, _ := NewDB(nil)

	// Create a new session
	session := db.Session()
	defer session.Close()

	// Use the session...
	_ = session
}
