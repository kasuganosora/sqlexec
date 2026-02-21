package api

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/stretchr/testify/assert"
)

// ==========================================================================
// Bug 3 (P1): ClearExpired TOCTOU race — between RUnlock and Lock, a
// freshly-set cache entry can be incorrectly deleted.
// ==========================================================================

func TestBug3_ClearExpired_TOCTOU(t *testing.T) {
	cache := NewQueryCache(CacheConfig{
		Enabled: true,
		TTL:     50 * time.Millisecond, // very short TTL
		MaxSize: 100,
	})

	result := &domain.QueryResult{Rows: []domain.Row{{"id": int64(1)}}}

	// Set an entry and wait for it to expire
	cache.Set("SELECT 1", nil, result)
	time.Sleep(100 * time.Millisecond) // entry is now expired

	// Concurrently: ClearExpired scans expired keys, and another goroutine
	// refreshes the entry between scan and delete phases.
	var wg sync.WaitGroup
	wg.Add(2)

	refreshed := make(chan struct{})

	go func() {
		defer wg.Done()
		// Wait for the refresh to happen, then call ClearExpired
		<-refreshed
		cache.ClearExpired()
	}()

	go func() {
		defer wg.Done()
		// Re-set the entry with a fresh TTL
		cache.Set("SELECT 1", nil, result)
		close(refreshed)
	}()

	wg.Wait()

	// After fix: the fresh entry should NOT have been deleted
	_, found := cache.Get("SELECT 1", nil)
	assert.True(t, found, "freshly-set cache entry should not be deleted by ClearExpired")
}

// ==========================================================================
// Bug 4 (P2): DB.Session() reads db.defaultDS without lock
// ==========================================================================

func TestBug4_DBSession_NoLock_Race(t *testing.T) {
	// Run with -race to detect the data race.
	db, err := NewDB(&DBConfig{
		CacheEnabled: false,
		DefaultLogger: NewDefaultLogger(LogError),
	})
	if err != nil {
		t.Fatalf("NewDB failed: %v", err)
	}
	defer db.Close()

	// Register a datasource so Session() can work
	mockDS := &mockDataSourceForRace{}
	_ = db.RegisterDataSource("test", mockDS)

	var wg sync.WaitGroup
	wg.Add(2)

	// One goroutine creates sessions
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			_ = db.Session()
		}
	}()

	// Another goroutine changes the default datasource
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			_ = db.SetDefaultDataSource("test")
		}
	}()

	wg.Wait()
}

// mockDataSourceForRace is a minimal mock that satisfies domain.DataSource
type mockDataSourceForRace struct{}

func (m *mockDataSourceForRace) Connect(_ context.Context) error                 { return nil }
func (m *mockDataSourceForRace) Close(_ context.Context) error                   { return nil }
func (m *mockDataSourceForRace) IsConnected() bool                               { return true }
func (m *mockDataSourceForRace) IsWritable() bool                                { return true }
func (m *mockDataSourceForRace) GetConfig() *domain.DataSourceConfig             { return &domain.DataSourceConfig{} }
func (m *mockDataSourceForRace) GetTables(_ context.Context) ([]string, error)   { return nil, nil }
func (m *mockDataSourceForRace) GetTableInfo(_ context.Context, _ string) (*domain.TableInfo, error) {
	return nil, nil
}
func (m *mockDataSourceForRace) Query(_ context.Context, _ string, _ *domain.QueryOptions) (*domain.QueryResult, error) {
	return &domain.QueryResult{}, nil
}
func (m *mockDataSourceForRace) Insert(_ context.Context, _ string, _ []domain.Row, _ *domain.InsertOptions) (int64, error) {
	return 0, nil
}
func (m *mockDataSourceForRace) Update(_ context.Context, _ string, _ []domain.Filter, _ domain.Row, _ *domain.UpdateOptions) (int64, error) {
	return 0, nil
}
func (m *mockDataSourceForRace) Delete(_ context.Context, _ string, _ []domain.Filter, _ *domain.DeleteOptions) (int64, error) {
	return 0, nil
}
func (m *mockDataSourceForRace) CreateTable(_ context.Context, _ *domain.TableInfo) error { return nil }
func (m *mockDataSourceForRace) DropTable(_ context.Context, _ string) error              { return nil }
func (m *mockDataSourceForRace) TruncateTable(_ context.Context, _ string) error          { return nil }
func (m *mockDataSourceForRace) Execute(_ context.Context, _ string) (*domain.QueryResult, error) {
	return nil, nil
}

// ==========================================================================
// Bug 5 (P2): db.go dead code — line 55 condition is always false
// ==========================================================================

func TestBug5_NewDB_NilConfig_UseEnhancedOptimizer_Default(t *testing.T) {
	// When config is nil, NewDB should create defaults with UseEnhancedOptimizer=true.
	// The old dead code (config.UseEnhancedOptimizer == false && config == nil)
	// was unreachable — config is never nil after the nil check at line 38.
	db, err := NewDB(nil)
	assert.NoError(t, err)
	defer db.Close()

	assert.True(t, db.config.UseEnhancedOptimizer,
		"UseEnhancedOptimizer should default to true when config is nil")
}

func TestBug5_NewDB_ExplicitConfig_UseEnhancedOptimizer(t *testing.T) {
	// When a caller explicitly sets UseEnhancedOptimizer=true, it should be respected.
	db, err := NewDB(&DBConfig{
		UseEnhancedOptimizer: true,
	})
	assert.NoError(t, err)
	defer db.Close()

	assert.True(t, db.config.UseEnhancedOptimizer)
}
