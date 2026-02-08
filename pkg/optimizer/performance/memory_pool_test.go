package performance

import (
	"sync"
	"testing"
)

func TestMemoryPool(t *testing.T) {
	mp := NewMemoryPool()

	// Test initial state
	pool := mp.GetPool("test_pool")
	if pool != nil {
		t.Error("GetPool() should return nil for non-existent pool")
	}

	// Test SetPool
	testPool := &sync.Pool{}
	mp.SetPool("test_pool", testPool)

	retrievedPool := mp.GetPool("test_pool")
	if retrievedPool != testPool {
		t.Error("GetPool() should return the set pool")
	}
}

func TestMemoryPoolMultiplePools(t *testing.T) {
	mp := NewMemoryPool()

	// Set multiple pools
	pool1 := &sync.Pool{}
	pool2 := &sync.Pool{}
	pool3 := &sync.Pool{}

	mp.SetPool("pool1", pool1)
	mp.SetPool("pool2", pool2)
	mp.SetPool("pool3", pool3)

	// Verify each pool
	if mp.GetPool("pool1") != pool1 {
		t.Error("pool1 not retrieved correctly")
	}
	if mp.GetPool("pool2") != pool2 {
		t.Error("pool2 not retrieved correctly")
	}
	if mp.GetPool("pool3") != pool3 {
		t.Error("pool3 not retrieved correctly")
	}
}

func TestMemoryPoolOverwrite(t *testing.T) {
	mp := NewMemoryPool()

	pool1 := &sync.Pool{}
	pool2 := &sync.Pool{}

	// Set first pool
	mp.SetPool("test", pool1)
	if mp.GetPool("test") != pool1 {
		t.Error("First pool not set correctly")
	}

	// Overwrite with second pool
	mp.SetPool("test", pool2)
	if mp.GetPool("test") != pool2 {
		t.Error("Pool not overwritten correctly")
	}
}
