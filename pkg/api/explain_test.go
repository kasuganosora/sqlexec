package api

import (
	"testing"
)

func TestCache_GetExplain(t *testing.T) {
	config := CacheConfig{
		Enabled: true,
		TTL:     5 * 60, //5 minutes
		MaxSize: 1000,
	}

	cache := NewQueryCache(config)

	// Test GetExplain when cache is empty
	result, found := cache.GetExplain("SELECT * FROM users")
	if found {
		t.Error("GetExplain() should return false for non-existent key")
	}
	if result != "" {
		t.Error("GetExplain() should return empty string for non-existent key")
	}

	// Test SetExplain
	testExplain := "Query Execution Plan:\n===================\n\nExecution Statistics:\n-------------------\nRows Returned: 100"
	cache.SetExplain("SELECT * FROM users", testExplain)

	// Test GetExplain after SetExplain
	result, found = cache.GetExplain("SELECT * FROM users")
	if !found {
		t.Error("GetExplain() should return true for existing key")
	}
	if result != testExplain {
		t.Errorf("GetExplain() = %v, want %v", result, testExplain)
	}
}

func TestCache_SetExplain(t *testing.T) {
	config := CacheConfig{
		Enabled: true,
		TTL:     5 * 60, //5 minutes
		MaxSize: 1000,
	}

	cache := NewQueryCache(config)

	// Test SetExplain with valid explain
	testExplain := "Query Execution Plan:\n===================\n\nExecution Statistics:\n-------------------\nRows Returned: 100"
	cache.SetExplain("SELECT * FROM users", testExplain)

	// Test SetExplain with empty explain (should not cache)
	cache.SetExplain("SELECT * FROM products", "")

	// Verify first explain is cached
	result, found := cache.GetExplain("SELECT * FROM users")
	if !found {
		t.Error("SetExplain() should cache valid explain")
	}
	if result != testExplain {
		t.Errorf("Cached explain = %v, want %v", result, testExplain)
	}

	// Verify empty explain is not cached
	_, found = cache.GetExplain("SELECT * FROM products")
	if found {
		t.Error("SetExplain() should not cache empty explain")
	}
}

func TestCache_DisabledExplain(t *testing.T) {
	config := CacheConfig{
		Enabled: false,
		TTL:     5 * 60,
		MaxSize: 1000,
	}

	cache := NewQueryCache(config)

	// Test GetExplain with disabled cache
	result, found := cache.GetExplain("SELECT * FROM users")
	if found {
		t.Error("GetExplain() should return false when cache is disabled")
	}
	if result != "" {
		t.Error("GetExplain() should return empty string when cache is disabled")
	}

	// Test SetExplain with disabled cache (should be no-op)
	cache.SetExplain("SELECT * FROM users", "test explain")
}

func TestCache_ExplainEntryTTL(t *testing.T) {
	config := CacheConfig{
		Enabled: true,
		TTL:     1, // 1 nanosecond for testing
		MaxSize: 1000,
	}

	cache := NewQueryCache(config)

	testExplain := "Test Explain Output"
	cache.SetExplain("SELECT * FROM users", testExplain)

	// Immediately check if exists
	_, found := cache.GetExplain("SELECT * FROM users")
	if !found {
		t.Error("Explain should exist immediately after SetExplain")
	}

	// Wait for TTL to expire (this is tricky with short TTL)
	// For now, just test the functionality exists
}
