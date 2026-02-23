package join

import (
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/parser"
	"github.com/stretchr/testify/assert"
)

// fixMockCostModel is a mock CostModel for cache key fix tests.
type fixMockCostModel struct{}

func (m *fixMockCostModel) ScanCost(tableName string, rowCount int64, useIndex bool) float64 {
	return 1.0
}

func (m *fixMockCostModel) JoinCost(left, right LogicalPlan, joinType JoinType, conditions []*parser.Expression) float64 {
	return 1.0
}

// fixMockEstimator is a mock CardinalityEstimator for cache key fix tests.
type fixMockEstimator struct{}

func (m *fixMockEstimator) EstimateTableScan(tableName string) int64 {
	return 1000
}

// newTestDPJoinReorder creates a DPJoinReorder instance for testing.
func newTestDPJoinReorder() *DPJoinReorder {
	return NewDPJoinReorder(&fixMockCostModel{}, &fixMockEstimator{}, 10)
}

// TestGenerateCacheKey_SameSetDifferentOrder verifies that two slices containing
// the same table names in different orders produce an identical cache key.
func TestGenerateCacheKey_SameSetDifferentOrder(t *testing.T) {
	dpr := newTestDPJoinReorder()

	key1 := dpr.generateCacheKey([]string{"a", "b", "c"})
	key2 := dpr.generateCacheKey([]string{"c", "a", "b"})

	assert.Equal(t, key1, key2, "same set of tables in different order should produce the same cache key")

	// Additional permutations
	key3 := dpr.generateCacheKey([]string{"b", "c", "a"})
	key4 := dpr.generateCacheKey([]string{"c", "b", "a"})
	assert.Equal(t, key1, key3)
	assert.Equal(t, key1, key4)
}

// TestGenerateCacheKey_DifferentSets verifies that two slices containing
// different table names produce different cache keys.
func TestGenerateCacheKey_DifferentSets(t *testing.T) {
	dpr := newTestDPJoinReorder()

	key1 := dpr.generateCacheKey([]string{"a", "b"})
	key2 := dpr.generateCacheKey([]string{"a", "c"})

	assert.NotEqual(t, key1, key2, "different sets of tables should produce different cache keys")
}

// TestGenerateCacheKey_Empty verifies that an empty slice returns an empty string.
func TestGenerateCacheKey_Empty(t *testing.T) {
	dpr := newTestDPJoinReorder()

	key := dpr.generateCacheKey([]string{})

	assert.Equal(t, "", key, "empty table list should produce an empty cache key")
}

// TestGenerateCacheKey_SingleTable verifies that a single-element slice returns
// just that table name without any separator.
func TestGenerateCacheKey_SingleTable(t *testing.T) {
	dpr := newTestDPJoinReorder()

	key := dpr.generateCacheKey([]string{"orders"})

	assert.Equal(t, "orders", key, "single table should produce a cache key equal to the table name")
}

// TestGenerateCacheKey_Duplicates verifies that a slice with duplicate entries
// produces a deterministic result.
func TestGenerateCacheKey_Duplicates(t *testing.T) {
	dpr := newTestDPJoinReorder()

	key1 := dpr.generateCacheKey([]string{"a", "a"})
	key2 := dpr.generateCacheKey([]string{"a", "a"})

	assert.Equal(t, key1, key2, "duplicate table names should produce a deterministic cache key")
	assert.Contains(t, key1, "a", "cache key should contain the table name")
}

// TestGenerateCacheKey_ConsistentWithCache verifies that setting a value in the
// cache using a key generated from one ordering can be retrieved using a key
// generated from a different ordering of the same tables.
func TestGenerateCacheKey_ConsistentWithCache(t *testing.T) {
	dpr := newTestDPJoinReorder()

	// Generate a key from one ordering and store a result in the cache.
	keySet := dpr.generateCacheKey([]string{"users", "orders", "products"})
	expected := &ReorderResult{
		Order:        []string{"orders", "products", "users"},
		Cost:         42.0,
		JoinTreeType: "left-deep",
	}
	dpr.cache.Set(keySet, expected)

	// Generate a key from a different ordering of the same tables.
	keyGet := dpr.generateCacheKey([]string{"products", "users", "orders"})

	// The keys must be equal, so the cache lookup must succeed.
	assert.Equal(t, keySet, keyGet, "keys generated from different orderings of the same set must be equal")

	result := dpr.cache.Get(keyGet)
	assert.NotNil(t, result, "cache lookup with reordered key should return the stored result")
	assert.Equal(t, expected.Cost, result.Cost)
	assert.Equal(t, expected.Order, result.Order)
	assert.Equal(t, expected.JoinTreeType, result.JoinTreeType)
}
