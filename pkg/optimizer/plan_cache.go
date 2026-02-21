package optimizer

import (
	"fmt"
	"hash"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kasuganosora/sqlexec/pkg/optimizer/plan"
	"github.com/kasuganosora/sqlexec/pkg/parser"
)

// PlanCache implements a DQ-inspired plan cache.
// In the DQ paper, the Q-table maps state → optimal action.
// Here we map SQL fingerprint → optimized execution plan.
type PlanCache struct {
	mu      sync.RWMutex
	cache   map[uint64]*CachedPlan
	maxSize int
	hits    int64
	misses  int64
}

// CachedPlan stores a cached execution plan with metadata.
type CachedPlan struct {
	Plan       *plan.Plan
	CreatedAt  time.Time
	HitCount   int64
	LastHit    time.Time
	ActualCost float64 // DQ reward feedback: actual execution cost
}

// NewPlanCache creates a plan cache with the given maximum size.
func NewPlanCache(maxSize int) *PlanCache {
	if maxSize <= 0 {
		maxSize = 1024
	}
	return &PlanCache{
		cache:   make(map[uint64]*CachedPlan, maxSize),
		maxSize: maxSize,
	}
}

// Get retrieves a cached plan by SQL fingerprint.
func (pc *PlanCache) Get(fingerprint uint64) (*plan.Plan, bool) {
	pc.mu.RLock()
	entry, ok := pc.cache[fingerprint]
	if !ok {
		pc.mu.RUnlock()
		atomic.AddInt64(&pc.misses, 1)
		return nil, false
	}
	// Read fields under RLock to avoid data race
	cachedPlan := entry.Plan
	lastHit := entry.LastHit
	pc.mu.RUnlock()

	atomic.AddInt64(&pc.hits, 1)
	atomic.AddInt64(&entry.HitCount, 1)

	// Update last hit time under write lock only occasionally
	// to avoid write contention on hot plans
	now := time.Now()
	if now.Sub(lastHit) > time.Second {
		pc.mu.Lock()
		entry.LastHit = now
		pc.mu.Unlock()
	}

	return cachedPlan, true
}

// Put stores an optimized plan in the cache.
func (pc *PlanCache) Put(fingerprint uint64, p *plan.Plan) {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	// Evict if at capacity (simple: remove oldest entry)
	if len(pc.cache) >= pc.maxSize {
		pc.evictOne()
	}

	now := time.Now()
	pc.cache[fingerprint] = &CachedPlan{
		Plan:      p,
		CreatedAt: now,
		LastHit:   now,
	}
}

// UpdateCost records the actual execution cost for a cached plan (DQ reward feedback).
func (pc *PlanCache) UpdateCost(fingerprint uint64, actualCost float64) {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	if entry, ok := pc.cache[fingerprint]; ok {
		// Exponential moving average to smooth cost estimates
		if entry.ActualCost == 0 {
			entry.ActualCost = actualCost
		} else {
			entry.ActualCost = entry.ActualCost*0.7 + actualCost*0.3
		}
	}
}

// Invalidate removes all cached plans (e.g., after DDL).
func (pc *PlanCache) Invalidate() {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	pc.cache = make(map[uint64]*CachedPlan, pc.maxSize)
}

// Stats returns cache hit/miss statistics.
func (pc *PlanCache) Stats() (hits, misses int64) {
	return atomic.LoadInt64(&pc.hits), atomic.LoadInt64(&pc.misses)
}

// Size returns the current number of cached plans.
func (pc *PlanCache) Size() int {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	return len(pc.cache)
}

// evictOne removes the least recently hit entry. Must be called with write lock held.
func (pc *PlanCache) evictOne() {
	var oldestKey uint64
	var oldestTime time.Time
	first := true

	for k, v := range pc.cache {
		if first || v.LastHit.Before(oldestTime) {
			oldestKey = k
			oldestTime = v.LastHit
			first = false
		}
	}

	if !first {
		delete(pc.cache, oldestKey)
	}
}

// SQLFingerprint computes a FNV-1a hash of the SQL statement for cache lookup.
// This provides a fast, collision-resistant fingerprint for plan caching.
func SQLFingerprint(stmt *parser.SQLStatement) uint64 {
	h := fnv.New64a()

	// Hash the statement type
	fmt.Fprintf(h, "type:%v|", stmt.Type)

	// Hash based on statement type
	switch stmt.Type {
	case parser.SQLTypeSelect:
		if stmt.Select != nil {
			fingerprintSelect(h, stmt.Select)
		}
	case parser.SQLTypeInsert:
		if stmt.Insert != nil {
			fmt.Fprintf(h, "insert:%s", stmt.Insert.Table)
		}
	case parser.SQLTypeUpdate:
		if stmt.Update != nil {
			fmt.Fprintf(h, "update:%s", stmt.Update.Table)
		}
	case parser.SQLTypeDelete:
		if stmt.Delete != nil {
			fmt.Fprintf(h, "delete:%s", stmt.Delete.Table)
		}
	}

	return h.Sum64()
}

// fingerprintSelect creates a fingerprint for SELECT statements.
func fingerprintSelect(h hash.Hash64, sel *parser.SelectStatement) {
	// Table name
	fmt.Fprintf(h, "from:%s|", sel.From)

	// Columns
	for _, col := range sel.Columns {
		fmt.Fprintf(h, "col:%s.%s|", col.Table, col.Name)
	}

	// WHERE clause structure
	if sel.Where != nil {
		fingerprintExpr(h, sel.Where)
	}

	// JOINs
	for _, j := range sel.Joins {
		fmt.Fprintf(h, "join:%s.%s|", j.Type, j.Table)
		if j.Condition != nil {
			fingerprintExpr(h, j.Condition)
		}
	}

	// ORDER BY
	for _, ob := range sel.OrderBy {
		fmt.Fprintf(h, "order:%s.%s|", ob.Column, ob.Direction)
	}

	// GROUP BY
	for _, gb := range sel.GroupBy {
		fmt.Fprintf(h, "group:%s|", gb)
	}

	// LIMIT & OFFSET
	if sel.Limit != nil && *sel.Limit > 0 {
		fmt.Fprintf(h, "limit:%d|", *sel.Limit)
	}
	if sel.Offset != nil && *sel.Offset > 0 {
		fmt.Fprintf(h, "offset:%d|", *sel.Offset)
	}
}

// fingerprintExpr fingerprints an expression tree.
func fingerprintExpr(h hash.Hash64, expr *parser.Expression) {
	if expr == nil {
		return
	}

	fmt.Fprintf(h, "(%v:%s:%s:%v|", expr.Type, expr.Column, expr.Operator, expr.Value)

	if expr.Left != nil {
		fingerprintExpr(h, expr.Left)
	}
	if expr.Right != nil {
		fingerprintExpr(h, expr.Right)
	}
}
