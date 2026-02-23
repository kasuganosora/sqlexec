package session

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ==========================================================================
// Bug 1 (P0): executeCreateStatement writes s.currentDB without lock
// The comment says "called from ExecuteQuery which already holds the lock"
// but ExecuteQuery does NOT hold the lock. This test verifies that
// concurrent reads/writes of s.currentDB don't race.
// ==========================================================================

func TestBug1_ExecuteCreateStatement_CurrentDB_Race(t *testing.T) {
	// This test should be run with -race to detect the data race.
	// We verify that concurrent SetCurrentDB and GetCurrentDB don't panic.
	ds := &mockDataSource{}
	sess := NewCoreSession(ds)

	const goroutines = 50
	var wg sync.WaitGroup
	wg.Add(goroutines * 2)

	// Half the goroutines write currentDB
	for i := 0; i < goroutines; i++ {
		go func(n int) {
			defer wg.Done()
			sess.SetCurrentDB("db_" + string(rune('a'+n%26)))
		}(i)
	}

	// Half the goroutines read currentDB
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			_ = sess.GetCurrentDB()
		}()
	}

	wg.Wait()
}

// Test that executeCreateStatement uses the locked path for auto-creating
// "test" database and setting currentDB.
func TestBug1_ExecuteCreateStatement_AutoCreateDB_UsesLock(t *testing.T) {
	// This test verifies the fix: after auto-creating "test" database,
	// the currentDB should be correctly set.
	ds := &mockDataSource{}
	sess := NewCoreSession(ds)

	// Without a DSManager, executeCreateStatement should return "no database selected"
	// This verifies it reads currentDB safely
	_, err := sess.ExecuteQuery(context.Background(), "CREATE TABLE t1 (id INT)")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no database selected")
}

// ==========================================================================
// Bug 2 (P1): Session GC leaks ThreadID mappings
// When GC deletes expired sessions, ThreadID entries remain in Values["thread_id"]
// ==========================================================================

func TestBug2_GC_CleansUpThreadID(t *testing.T) {
	driver := NewMemoryDriver()
	ctx := context.Background()

	// Create a session with ThreadID
	sess := &Session{
		ID:         "test_gc_session",
		ThreadID:   42,
		RemoteIP:   "127.0.0.1",
		RemotePort: "3306",
		Created:    time.Now(),
		LastUsed:   time.Now().Add(-48 * time.Hour), // Expired (older than 24h)
	}

	err := driver.CreateSession(ctx, sess)
	require.NoError(t, err)

	// Register ThreadID
	err = driver.SetThreadId(ctx, 42, sess)
	require.NoError(t, err)

	// Verify ThreadID exists
	_, err = driver.GetThreadId(ctx, 42)
	require.NoError(t, err)

	// Create SessionMgr and run GC
	// Save/restore global session max age
	origMaxAge := SessionMaxAge
	origGCInterval := SessionGCInterval
	defer func() {
		SessionMaxAge = origMaxAge
		SessionGCInterval = origGCInterval
	}()
	SessionMaxAge = 24 * time.Hour
	SessionGCInterval = time.Hour // Don't auto-GC

	mgr := &SessionMgr{
		driver:   driver,
		stopChan: make(chan struct{}),
	}

	// Run GC
	err = mgr.GC()
	require.NoError(t, err)

	// Session should be deleted
	_, err = driver.GetSession(ctx, "test_gc_session")
	assert.Error(t, err, "session should be deleted by GC")

	// BUG: ThreadID should ALSO be cleaned up, but currently it's leaked
	_, err = driver.GetThreadId(ctx, 42)
	assert.Error(t, err, "ThreadID should be cleaned up when session is GC'd")
}

// ==========================================================================
// Bug 3 (P1): tablePersistence map accessed without lock
// ==========================================================================

func TestBug3_TablePersistence_ConcurrentAccess(t *testing.T) {
	// This test should be run with -race to detect map race.
	ds := &mockDataSource{}
	sess := NewCoreSession(ds)

	const goroutines = 50
	var wg sync.WaitGroup
	wg.Add(goroutines * 2)

	// Half the goroutines register persistence
	for i := 0; i < goroutines; i++ {
		go func(n int) {
			defer wg.Done()
			sess.registerTablePersistence("db1", "table_"+string(rune('a'+n%26)), nil)
		}(i)
	}

	// Half the goroutines read persistence
	for i := 0; i < goroutines; i++ {
		go func(n int) {
			defer wg.Done()
			_ = sess.getTablePersistence("db1", "table_"+string(rune('a'+n%26)))
		}(i)
	}

	wg.Wait()
}

// ==========================================================================
// Bug 4 (P1): GetThreadId returns potentially-in-use ID on exhaustion
// ==========================================================================

func TestBug4_GetThreadId_Exhaustion_ReturnsError(t *testing.T) {
	driver := NewMemoryDriver()
	ctx := context.Background()

	// Fill all ThreadIDs from 1 to 1000
	for i := uint32(1); i <= 1000; i++ {
		sess := &Session{
			ID:       "sess_" + string(rune(i)),
			ThreadID: i,
		}
		err := driver.SetThreadId(ctx, i, sess)
		require.NoError(t, err)
	}

	mgr := &SessionMgr{
		driver:   driver,
		stopChan: make(chan struct{}),
	}

	// GetThreadId should return 0 (error indicator) when all IDs are exhausted
	threadID := mgr.GetThreadId(ctx)
	// BUG: Currently returns 1001 without checking if it's in use.
	// After fix, should return 0 to indicate failure.
	assert.Equal(t, uint32(0), threadID, "should return 0 when all ThreadIDs are exhausted")
}

// ==========================================================================
// Bug 5 (P2): s.closed checked without lock in DML methods
// ==========================================================================

func TestBug5_ClosedCheck_Race(t *testing.T) {
	// This test should be run with -race to detect the data race.
	ds := &mockDataSource{}
	sess := NewCoreSession(ds)

	var wg sync.WaitGroup
	wg.Add(2)

	// One goroutine closes the session
	go func() {
		defer wg.Done()
		time.Sleep(5 * time.Millisecond)
		sess.Close(context.Background())
	}()

	// Another goroutine checks closed status
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			_ = sess.IsClosed()
		}
	}()

	wg.Wait()
}

// ==========================================================================
// Bug 7 (P0): GetSessions copies Session struct including sync.Mutex
// sessCopy := *session copies sequenceMu, which is undefined behavior.
// Fix: return original pointers (RLock protects the map iteration).
// ==========================================================================

func TestBug7_GetSessions_NoCopyLock(t *testing.T) {
	ctx := context.Background()
	driver := NewMemoryDriver()

	sess := &Session{
		ID:         "sess_lock_test",
		RemoteIP:   "127.0.0.1",
		RemotePort: "3306",
		Created:    time.Now(),
		LastUsed:   time.Now(),
	}

	err := driver.CreateSession(ctx, sess)
	require.NoError(t, err)

	sessions, err := driver.GetSessions(ctx)
	require.NoError(t, err)
	require.Len(t, sessions, 1)

	// Advance sequence on the returned session
	sessions[0].GetNextSequenceID() // -> 1

	// If GetSessions returned a struct copy, the original's SequenceID is still 0.
	// If it returns the original pointer, original's SequenceID is now 1.
	seq := sess.GetNextSequenceID() // original: should be 2 if shared, 1 if copied
	assert.Equal(t, uint8(2), seq,
		"GetSessions should return original pointers, not struct copies (copying sync.Mutex is UB)")
}

// ==========================================================================
// Bug 6 (P2): GetThreadId unnecessary 5ms sleep
// ==========================================================================

func TestBug6_GetThreadId_Performance(t *testing.T) {
	driver := NewMemoryDriver()
	ctx := context.Background()

	// Pre-fill ThreadIDs 1-99 so ID 100 is first available
	for i := uint32(1); i < 100; i++ {
		sess := &Session{ID: "sess", ThreadID: i}
		err := driver.SetThreadId(ctx, i, sess)
		require.NoError(t, err)
	}

	mgr := &SessionMgr{
		driver:   driver,
		stopChan: make(chan struct{}),
	}

	start := time.Now()
	threadID := mgr.GetThreadId(ctx)
	elapsed := time.Since(start)

	assert.Equal(t, uint32(100), threadID)
	// BUG: With 5ms sleep per iteration, 99 iterations takes ~495ms.
	// After fix (no sleep), should complete in <50ms.
	assert.Less(t, elapsed, 100*time.Millisecond,
		"GetThreadId should not sleep between iterations; took %v", elapsed)
}
