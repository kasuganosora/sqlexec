package workerpool

import (
	"sync"
	"testing"
)

// TestNewRowPool tests row pool creation
func TestNewRowPool(t *testing.T) {
	rp := NewRowPool()
	if rp == nil {
		t.Fatal("NewRowPool returned nil")
	}
}

// TestRowPool_GetPut tests get and put operations
func TestRowPool_GetPut(t *testing.T) {
	rp := NewRowPool()

	// Get a row
	row := rp.Get()
	if row == nil {
		t.Fatal("Get() returned nil")
	}
	if len(row) != 0 {
		t.Errorf("len(row) = %d, want 0", len(row))
	}

	// Add some values
	row["key1"] = "value1"
	row["key2"] = 42

	// Put back
	rp.Put(row)

	// Get again - should be reset
	row2 := rp.Get()
	if len(row2) != 0 {
		t.Errorf("len(row2) = %d, want 0 (should be reset)", len(row2))
	}

	// Put nil should not panic
	rp.Put(nil)
}

// TestRowPool_Stats tests statistics
func TestRowPool_Stats(t *testing.T) {
	rp := NewRowPool()

	// Get multiple rows
	row1 := rp.Get()
	row2 := rp.Get()
	row3 := rp.Get()

	// Put them back
	rp.Put(row1)
	rp.Put(row2)
	rp.Put(row3)

	stats := rp.Stats()
	t.Logf("Stats: Allocations=%d, Reuses=%d, Returns=%d, ReuseRate=%.2f%%",
		stats.Allocations, stats.Reuses, stats.Returns, stats.ReuseRate)

	if stats.Returns != 3 {
		t.Errorf("Returns = %d, want 3", stats.Returns)
	}
}

// TestRowPool_Reuse tests that rows are actually reused
func TestRowPool_Reuse(t *testing.T) {
	rp := NewRowPool()

	// Get and put many times
	for i := 0; i < 100; i++ {
		row := rp.Get()
		rp.Put(row)
	}

	stats := rp.Stats()
	t.Logf("After 100 cycles: Allocations=%d, Reuses=%d", stats.Allocations, stats.Reuses)

	// Should have many reuses
	if stats.Reuses < 90 {
		t.Errorf("Reuses = %d, want >= 90", stats.Reuses)
	}
}

// TestNewRowSlicePool tests row slice pool creation
func TestNewRowSlicePool(t *testing.T) {
	rsp := NewRowSlicePool(16)
	if rsp == nil {
		t.Fatal("NewRowSlicePool returned nil")
	}

	// Test with zero size
	rsp = NewRowSlicePool(0)
	if rsp.initSize != 16 {
		t.Errorf("initSize = %d, want 16 (default)", rsp.initSize)
	}

	// Test with negative size
	rsp = NewRowSlicePool(-1)
	if rsp.initSize != 16 {
		t.Errorf("initSize = %d, want 16 (default)", rsp.initSize)
	}
}

// TestRowSlicePool_GetPut tests row slice pool operations
func TestRowSlicePool_GetPut(t *testing.T) {
	rsp := NewRowSlicePool(16)

	// Get a slice
	slice := rsp.Get()
	if slice == nil {
		t.Fatal("Get() returned nil")
	}
	if len(*slice) != 0 {
		t.Errorf("len(slice) = %d, want 0", len(*slice))
	}

	// Add some rows
	*slice = append(*slice, map[string]interface{}{"key": "value"})
	*slice = append(*slice, map[string]interface{}{"num": 42})

	// Put back
	rsp.Put(slice)

	// Get again - should be reset
	slice2 := rsp.Get()
	if len(*slice2) != 0 {
		t.Errorf("len(slice2) = %d, want 0 (should be reset)", len(*slice2))
	}

	// Put nil should not panic
	rsp.Put(nil)
}

// TestValuePool tests the generic value pool
func TestValuePool(t *testing.T) {
	newFn := func() *int {
		v := 42
		return &v
	}
	resetFn := func(v *int) {
		*v = 0
	}

	vp := NewValuePool(newFn, resetFn)

	// Get a value
	v1 := vp.Get()
	if v1 == nil {
		t.Fatal("Get() returned nil")
	}
	if *v1 != 0 {
		t.Errorf("*v1 = %d, want 0 (should be reset)", *v1)
	}

	// Modify and put back
	*v1 = 100
	vp.Put(v1)

	// Get again
	v2 := vp.Get()
	if *v2 != 0 {
		t.Errorf("*v2 = %d, want 0 (should be reset)", *v2)
	}
}

// TestValuePool_NoReset tests value pool without reset function
func TestValuePool_NoReset(t *testing.T) {
	newFn := func() *int {
		v := 42
		return &v
	}

	vp := NewValuePool(newFn, nil)

	// Get a value
	v1 := vp.Get()
	if *v1 != 42 {
		t.Errorf("*v1 = %d, want 42", *v1)
	}

	vp.Put(v1)
}

// TestValuePool_NoNew tests value pool without new function
func TestValuePool_NoNew(t *testing.T) {
	vp := NewValuePool[int](nil, nil)

	// Get a value (should return zero value)
	v := vp.Get()
	if v != 0 {
		t.Errorf("v = %d, want 0", v)
	}
}

// TestGlobalRowPool tests the global row pool functions
func TestGlobalRowPool(t *testing.T) {
	// Get from global pool
	row := GetRow()
	if row == nil {
		t.Fatal("GetRow() returned nil")
	}

	// Put back
	PutRow(row)

	// Get again
	row2 := GetRow()
	if row2 == nil {
		t.Fatal("GetRow() returned nil on second call")
	}

	PutRow(row2)
}

// TestSlicePool tests the generic slice pool
func TestSlicePool(t *testing.T) {
	sp := NewSlicePool[int](16)

	// Get a slice
	slice := sp.Get()
	if slice == nil {
		t.Fatal("Get() returned nil")
	}
	if len(*slice) != 0 {
		t.Errorf("len(slice) = %d, want 0", len(*slice))
	}

	// Add values
	*slice = append(*slice, 1, 2, 3)

	// Put back
	sp.Put(slice)

	// Get again
	slice2 := sp.Get()
	if len(*slice2) != 0 {
		t.Errorf("len(slice2) = %d, want 0 (should be reset)", len(*slice2))
	}

	// Put nil should not panic
	sp.Put(nil)
}

// TestSlicePool_DefaultSize tests slice pool with default size
func TestSlicePool_DefaultSize(t *testing.T) {
	sp := NewSlicePool[int](0)
	if sp.initSize != 8 {
		t.Errorf("initSize = %d, want 8 (default)", sp.initSize)
	}

	sp = NewSlicePool[int](-1)
	if sp.initSize != 8 {
		t.Errorf("initSize = %d, want 8 (default)", sp.initSize)
	}
}

// TestMapPool tests the generic map pool
func TestMapPool(t *testing.T) {
	mp := NewMapPool[string, int](8)

	// Get a map
	m := mp.Get()
	if m == nil {
		t.Fatal("Get() returned nil")
	}
	if len(m) != 0 {
		t.Errorf("len(m) = %d, want 0", len(m))
	}

	// Add values
	m["a"] = 1
	m["b"] = 2

	// Put back
	mp.Put(m)

	// Get again
	m2 := mp.Get()
	if len(m2) != 0 {
		t.Errorf("len(m2) = %d, want 0 (should be reset)", len(m2))
	}

	// Put nil should not panic
	mp.Put(nil)
}

// TestMapPool_DefaultSize tests map pool with default size
func TestMapPool_DefaultSize(t *testing.T) {
	mp := NewMapPool[string, int](0)
	_ = mp // Just verify it doesn't panic

	mp = NewMapPool[string, int](-1)
	_ = mp
}

// BenchmarkRowPool_Get benchmarks row pool get
func BenchmarkRowPool_Get(b *testing.B) {
	rp := NewRowPool()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		row := rp.Get()
		rp.Put(row)
	}
}

// BenchmarkRowPool_GetWithValues benchmarks get with values
func BenchmarkRowPool_GetWithValues(b *testing.B) {
	rp := NewRowPool()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		row := rp.Get()
		row["key"] = i
		rp.Put(row)
	}
}

// BenchmarkRowPool_Direct benchmarks direct allocation without pool
func BenchmarkRowPool_Direct(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		row := make(map[string]interface{})
		row["key"] = i
		_ = row
	}
}

// BenchmarkGlobalRowPool benchmarks global row pool
func BenchmarkGlobalRowPool(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		row := GetRow()
		PutRow(row)
	}
}

// BenchmarkSlicePool benchmarks slice pool
func BenchmarkSlicePool(b *testing.B) {
	sp := NewSlicePool[int](16)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		slice := sp.Get()
		*slice = append(*slice, i)
		sp.Put(slice)
	}
}

// BenchmarkMapPool benchmarks map pool
func BenchmarkMapPool(b *testing.B) {
	mp := NewMapPool[string, int](8)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m := mp.Get()
		m["key"] = i
		mp.Put(m)
	}
}

// TestSlicePool_NilFromPool tests when pool returns nil
func TestSlicePool_NilFromPool(t *testing.T) {
	sp := &SlicePool[int]{
		pool: sync.Pool{
			New: nil, // Will return nil
		},
	}

	// Get should handle nil from pool
	slice := sp.Get()
	if slice == nil {
		t.Fatal("Get() should not return nil even when pool.New is nil")
	}
	if len(*slice) != 0 {
		t.Errorf("len(slice) = %d, want 0", len(*slice))
	}
}

// TestMapPool_NilFromPool tests when pool returns nil
func TestMapPool_NilFromPool(t *testing.T) {
	mp := &MapPool[string, int]{
		pool: sync.Pool{
			New: nil, // Will return nil
		},
	}

	// Get should handle nil from pool
	m := mp.Get()
	if m == nil {
		t.Fatal("Get() should not return nil even when pool.New is nil")
	}
	if len(m) != 0 {
		t.Errorf("len(m) = %d, want 0", len(m))
	}
}

// TestRowSlicePool_NilFromPool tests when pool returns nil
func TestRowSlicePool_NilFromPool(t *testing.T) {
	rsp := &RowSlicePool{
		pool: sync.Pool{
			New: nil, // Will return nil
		},
		initSize: 8,
	}

	// Get should handle nil from pool
	slice := rsp.Get()
	if slice == nil {
		t.Fatal("Get() should not return nil even when pool.New is nil")
	}
	if len(*slice) != 0 {
		t.Errorf("len(slice) = %d, want 0", len(*slice))
	}
}

// TestRowPool_NilFromPool tests when pool returns nil
func TestRowPool_NilFromPool(t *testing.T) {
	rp := &RowPool{
		pool: sync.Pool{
			New: nil, // Will return nil
		},
	}

	// Get should handle nil from pool
	row := rp.Get()
	if row == nil {
		t.Fatal("Get() should not return nil even when pool.New is nil")
	}
	if len(row) != 0 {
		t.Errorf("len(row) = %d, want 0", len(row))
	}
}
