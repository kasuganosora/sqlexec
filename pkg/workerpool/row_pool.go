package workerpool

import (
	"sync"
	"sync/atomic"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// RowPool is a sync.Pool for reusing domain.Row objects (map[string]interface{})
type RowPool struct {
	pool      sync.Pool
	allocCnt  int64
	reuseCnt  int64
	returnCnt int64
}

// NewRowPool creates a new row pool
func NewRowPool() *RowPool {
	return &RowPool{
		pool: sync.Pool{
			New: func() interface{} {
				return make(domain.Row)
			},
		},
	}
}

// Get retrieves a row from the pool, creating a new one if necessary
func (rp *RowPool) Get() domain.Row {
	v := rp.pool.Get()
	if v == nil {
		atomic.AddInt64(&rp.allocCnt, 1)
		return make(domain.Row)
	}
	atomic.AddInt64(&rp.reuseCnt, 1)
	row := v.(domain.Row)
	// Clear the map for reuse
	for k := range row {
		delete(row, k)
	}
	return row
}

// Put returns a row to the pool for reuse
func (rp *RowPool) Put(row domain.Row) {
	if row == nil {
		return
	}
	// Clear the row data to prevent memory leaks
	for k := range row {
		delete(row, k)
	}
	atomic.AddInt64(&rp.returnCnt, 1)
	rp.pool.Put(row)
}

// Stats returns pool statistics
func (rp *RowPool) Stats() RowPoolStats {
	allocs := atomic.LoadInt64(&rp.allocCnt)
	reuses := atomic.LoadInt64(&rp.reuseCnt)
	returns := atomic.LoadInt64(&rp.returnCnt)
	total := allocs + reuses
	var reuseRate float64
	if total > 0 {
		reuseRate = float64(reuses) / float64(total) * 100
	}
	return RowPoolStats{
		Allocations: allocs,
		Reuses:      reuses,
		Returns:     returns,
		ReuseRate:   reuseRate,
	}
}

// RowPoolStats holds row pool statistics
type RowPoolStats struct {
	Allocations int64
	Reuses      int64
	Returns     int64
	ReuseRate   float64
}

// RowSlicePool is a pool for slices of rows
type RowSlicePool struct {
	pool     sync.Pool
	initSize int
}

// NewRowSlicePool creates a pool for row slices
func NewRowSlicePool(initialSize int) *RowSlicePool {
	if initialSize <= 0 {
		initialSize = 16
	}
	return &RowSlicePool{
		initSize: initialSize,
		pool: sync.Pool{
			New: func() interface{} {
				slice := make([]domain.Row, 0, initialSize)
				return &slice
			},
		},
	}
}

// Get retrieves a row slice from the pool
func (rsp *RowSlicePool) Get() *[]domain.Row {
	v := rsp.pool.Get()
	if v == nil {
		slice := make([]domain.Row, 0, rsp.initSize)
		return &slice
	}
	slice := v.(*[]domain.Row)
	*slice = (*slice)[:0]
	return slice
}

// Put returns a row slice to the pool
func (rsp *RowSlicePool) Put(slice *[]domain.Row) {
	if slice == nil {
		return
	}
	// Clear references
	for i := range *slice {
		(*slice)[i] = nil
	}
	*slice = (*slice)[:0]
	rsp.pool.Put(slice)
}

// ValuePool is a generic pool for any value type
type ValuePool[T any] struct {
	pool    sync.Pool
	newFn   func() T
	resetFn func(T)
}

// NewValuePool creates a new value pool
func NewValuePool[T any](newFn func() T, resetFn func(T)) *ValuePool[T] {
	return &ValuePool[T]{
		newFn:   newFn,
		resetFn: resetFn,
		pool: sync.Pool{
			New: func() interface{} {
				if newFn != nil {
					return newFn()
				}
				var zero T
				return zero
			},
		},
	}
}

// Get retrieves a value from the pool
func (vp *ValuePool[T]) Get() T {
	v := vp.pool.Get().(T)
	if vp.resetFn != nil {
		vp.resetFn(v)
	}
	return v
}

// Put returns a value to the pool
func (vp *ValuePool[T]) Put(v T) {
	vp.pool.Put(v)
}

// Global row pool for convenience
var globalRowPool = NewRowPool()

// GetRow gets a row from the global pool
func GetRow() domain.Row {
	return globalRowPool.Get()
}

// PutRow returns a row to the global pool
func PutRow(row domain.Row) {
	globalRowPool.Put(row)
}

// SlicePool is a generic pool for slices
type SlicePool[T any] struct {
	pool     sync.Pool
	initSize int
}

// NewSlicePool creates a new slice pool
func NewSlicePool[T any](initialSize int) *SlicePool[T] {
	if initialSize <= 0 {
		initialSize = 8
	}
	return &SlicePool[T]{
		initSize: initialSize,
		pool: sync.Pool{
			New: func() interface{} {
				slice := make([]T, 0, initialSize)
				return &slice
			},
		},
	}
}

// Get retrieves a slice from the pool
func (sp *SlicePool[T]) Get() *[]T {
	v := sp.pool.Get()
	if v == nil {
		slice := make([]T, 0, sp.initSize)
		return &slice
	}
	slice := v.(*[]T)
	*slice = (*slice)[:0]
	return slice
}

// Put returns a slice to the pool
func (sp *SlicePool[T]) Put(slice *[]T) {
	if slice == nil {
		return
	}
	*slice = (*slice)[:0]
	sp.pool.Put(slice)
}

// MapPool is a pool for maps
type MapPool[K comparable, V any] struct {
	pool sync.Pool
}

// NewMapPool creates a new map pool
func NewMapPool[K comparable, V any](initialSize int) *MapPool[K, V] {
	if initialSize <= 0 {
		initialSize = 8
	}
	return &MapPool[K, V]{
		pool: sync.Pool{
			New: func() interface{} {
				return make(map[K]V, initialSize)
			},
		},
	}
}

// Get retrieves a map from the pool
func (mp *MapPool[K, V]) Get() map[K]V {
	v := mp.pool.Get()
	if v == nil {
		return make(map[K]V)
	}
	m := v.(map[K]V)
	// Clear the map
	for k := range m {
		delete(m, k)
	}
	return m
}

// Put returns a map to the pool
func (mp *MapPool[K, V]) Put(m map[K]V) {
	if m == nil {
		return
	}
	// Clear the map
	for k := range m {
		delete(m, k)
	}
	mp.pool.Put(m)
}
