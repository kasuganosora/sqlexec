package monitor

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ==========================================================================
// Bug 1 (P1): GenerateKey ignores Params, Database, and User fields
// Two queries with the same SQL but different parameters, databases, or
// users collide in the cache, returning incorrect cached results.
// ==========================================================================

func TestBug1_GenerateKey_DifferentParams(t *testing.T) {
	key1 := GenerateKey(&CacheKey{
		SQL:      "SELECT * FROM users WHERE id = ?",
		Params:   []interface{}{1},
		Database: "db1",
		User:     "alice",
	})

	key2 := GenerateKey(&CacheKey{
		SQL:      "SELECT * FROM users WHERE id = ?",
		Params:   []interface{}{2},
		Database: "db1",
		User:     "alice",
	})

	assert.NotEqual(t, key1, key2,
		"same SQL with different params should produce different cache keys")
}

func TestBug1_GenerateKey_DifferentDatabase(t *testing.T) {
	key1 := GenerateKey(&CacheKey{
		SQL:      "SELECT * FROM users",
		Database: "db1",
		User:     "alice",
	})

	key2 := GenerateKey(&CacheKey{
		SQL:      "SELECT * FROM users",
		Database: "db2",
		User:     "alice",
	})

	assert.NotEqual(t, key1, key2,
		"same SQL in different databases should produce different cache keys")
}

func TestBug1_GenerateKey_DifferentUser(t *testing.T) {
	key1 := GenerateKey(&CacheKey{
		SQL:      "SELECT * FROM users",
		Database: "db1",
		User:     "alice",
	})

	key2 := GenerateKey(&CacheKey{
		SQL:      "SELECT * FROM users",
		Database: "db1",
		User:     "bob",
	})

	assert.NotEqual(t, key1, key2,
		"same SQL by different users should produce different cache keys")
}

func TestBug1_GenerateKey_SameInputsSameKey(t *testing.T) {
	key1 := GenerateKey(&CacheKey{
		SQL:      "SELECT 1",
		Params:   []interface{}{42},
		Database: "testdb",
		User:     "root",
	})

	key2 := GenerateKey(&CacheKey{
		SQL:      "SELECT 1",
		Params:   []interface{}{42},
		Database: "testdb",
		User:     "root",
	})

	assert.Equal(t, key1, key2,
		"identical inputs should produce same cache key")
}

// ==========================================================================
// Bug 2 (P1): MonitorContext.End() passes empty SQL to slow query log
// NewMonitorContext receives the sql parameter but doesn't store it.
// End() passes "" as the SQL string, losing all query information.
// ==========================================================================

func TestBug2_MonitorContext_SQL_Preserved(t *testing.T) {
	metrics := NewMetricsCollector()
	// Use a very low threshold so every query is "slow"
	slowQuery := NewSlowQueryAnalyzer(1*time.Nanosecond, 100)

	sql := "SELECT * FROM users WHERE id = 1"
	mc := NewMonitorContext(context.Background(), metrics, slowQuery, sql)
	mc.Start()

	// Simulate some work
	time.Sleep(1 * time.Millisecond)

	mc.End(true, 10, nil)

	// The slow query log should contain the actual SQL
	queries := slowQuery.GetAllSlowQueries()
	require.NotEmpty(t, queries, "should have recorded a slow query")
	assert.Equal(t, sql, queries[0].SQL,
		"slow query log should contain the actual SQL, not empty string")
}

func TestBug2_MonitorContext_SQL_WithError(t *testing.T) {
	metrics := NewMetricsCollector()
	slowQuery := NewSlowQueryAnalyzer(1*time.Nanosecond, 100)

	sql := "INSERT INTO orders VALUES (1, 'test')"
	mc := NewMonitorContext(context.Background(), metrics, slowQuery, sql)
	mc.Start()

	time.Sleep(1 * time.Millisecond)

	mc.End(false, 0, errors.New("table not found"))

	queries := slowQuery.GetAllSlowQueries()
	require.NotEmpty(t, queries)
	assert.Equal(t, sql, queries[0].SQL)
	assert.Equal(t, "table not found", queries[0].Error)
}
