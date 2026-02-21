package memory

import (
	"container/list"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

const (
	defaultPageSize      = 4096
	defaultEvictInterval = 5 * time.Second
	autoMemoryFraction   = 0.70 // use 70% of system memory
)

// PagingConfig controls the buffer pool behavior.
type PagingConfig struct {
	Enabled       bool          `json:"enabled"`
	MaxMemoryMB   int           `json:"max_memory_mb"`
	PageSize      int           `json:"page_size"`
	SpillDir      string        `json:"spill_dir"`
	EvictInterval time.Duration `json:"evict_interval"`
}

// DefaultPagingConfig returns sensible defaults.
func DefaultPagingConfig() *PagingConfig {
	return &PagingConfig{
		Enabled:       true,
		MaxMemoryMB:   0, // auto-detect
		PageSize:      defaultPageSize,
		SpillDir:      "",
		EvictInterval: defaultEvictInterval,
	}
}

// lruQueue is a thread-safe LRU tracking structure for eviction candidates.
type lruQueue struct {
	mu       sync.Mutex
	list     *list.List
	elements map[PageID]*list.Element
}

func newLRUQueue() *lruQueue {
	return &lruQueue{
		list:     list.New(),
		elements: make(map[PageID]*list.Element),
	}
}

// Touch moves a page to the back (most recently used).
func (q *lruQueue) Touch(page *RowPage) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if elem, ok := q.elements[page.id]; ok {
		q.list.MoveToBack(elem)
	} else {
		elem := q.list.PushBack(page)
		q.elements[page.id] = elem
	}
}

// Remove removes a page from the LRU queue.
func (q *lruQueue) Remove(page *RowPage) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if elem, ok := q.elements[page.id]; ok {
		q.list.Remove(elem)
		delete(q.elements, page.id)
	}
}

// EvictCandidate returns the least recently used unpinned page, or nil.
// It prefers pages from old MVCC versions (version < latestVersions[table]).
func (q *lruQueue) EvictCandidate(latestVersions map[string]int64) *RowPage {
	q.mu.Lock()
	defer q.mu.Unlock()

	// Tier 1: look for old version pages first
	for elem := q.list.Front(); elem != nil; elem = elem.Next() {
		page := elem.Value.(*RowPage)
		if page.IsPinned() {
			continue
		}
		page.mu.Lock()
		evicted := page.rows == nil && page.onDisk
		page.mu.Unlock()
		if evicted {
			// Already evicted, remove from queue
			q.list.Remove(elem)
			delete(q.elements, page.id)
			continue
		}
		if latest, ok := latestVersions[page.id.Table]; ok && page.id.Version < latest {
			q.list.Remove(elem)
			delete(q.elements, page.id)
			return page
		}
	}

	// Tier 2: any unpinned page (LRU order)
	for elem := q.list.Front(); elem != nil; elem = elem.Next() {
		page := elem.Value.(*RowPage)
		if page.IsPinned() {
			continue
		}
		page.mu.Lock()
		evicted := page.rows == nil && page.onDisk
		page.mu.Unlock()
		if evicted {
			q.list.Remove(elem)
			delete(q.elements, page.id)
			continue
		}
		q.list.Remove(elem)
		delete(q.elements, page.id)
		return page
	}

	return nil
}

// BufferPool manages memory for all RowPages across the system.
// It tracks memory usage, evicts cold pages to disk, and reloads them on demand.
type BufferPool struct {
	maxMemory      int64 // max bytes for row data
	usedMemory     int64 // atomic counter
	spillDir       string
	pageSize       int
	evictInterval  time.Duration
	lru            *lruQueue
	mu             sync.Mutex // protects latestVersions
	latestVersions map[string]int64
	stopCh         chan struct{}
	stopped        int32 // atomic flag
	disabled       bool  // passthrough mode
}

// NewBufferPool creates a new buffer pool. If cfg is nil, passthrough mode is used.
func NewBufferPool(cfg *PagingConfig) *BufferPool {
	if cfg == nil || !cfg.Enabled {
		return &BufferPool{disabled: true, pageSize: defaultPageSize}
	}

	maxMem := int64(cfg.MaxMemoryMB) * 1024 * 1024
	if maxMem <= 0 {
		// Auto-detect: use 70% of system memory
		var memStats runtime.MemStats
		runtime.ReadMemStats(&memStats)
		maxMem = int64(float64(memStats.Sys) * autoMemoryFraction)
		if maxMem < 64*1024*1024 {
			maxMem = 64 * 1024 * 1024 // minimum 64MB
		}
	}

	pageSize := cfg.PageSize
	if pageSize <= 0 {
		pageSize = defaultPageSize
	}

	spillDir := cfg.SpillDir
	if spillDir == "" {
		spillDir = filepath.Join(os.TempDir(), "sqlexec-spill")
	}
	os.MkdirAll(spillDir, 0755)

	evictInterval := cfg.EvictInterval
	if evictInterval <= 0 {
		evictInterval = defaultEvictInterval
	}

	bp := &BufferPool{
		maxMemory:      maxMem,
		spillDir:       spillDir,
		pageSize:       pageSize,
		evictInterval:  evictInterval,
		lru:            newLRUQueue(),
		latestVersions: make(map[string]int64),
		stopCh:         make(chan struct{}),
	}

	go bp.backgroundEvictor()

	return bp
}

// IsDisabled returns true if the buffer pool is in passthrough mode.
func (bp *BufferPool) IsDisabled() bool {
	return bp.disabled
}

// PageSize returns the configured page size (rows per page).
func (bp *BufferPool) PageSize() int {
	return bp.pageSize
}

// Pin loads a page into memory (if evicted) and increments its pin count.
// The caller MUST call Unpin when done reading the rows.
//
// All access to page.rows is protected by page.mu to avoid data races with
// the evictor which sets page.rows = nil under the same lock.
func (bp *BufferPool) Pin(page *RowPage) ([]domain.Row, error) {
	page.Pin()

	if bp.disabled {
		page.mu.Lock()
		rows := page.rows
		page.mu.Unlock()
		return rows, nil
	}

	page.mu.Lock()
	defer page.mu.Unlock()

	// Page already in memory
	if page.rows != nil {
		return page.rows, nil
	}

	if !page.onDisk {
		// Not in memory and not on disk — empty page
		return nil, nil
	}

	// Load from disk
	rows, err := loadPageFromDisk(page.diskPath)
	if err != nil {
		page.Unpin()
		return nil, fmt.Errorf("failed to load page %v from disk: %w", page.id, err)
	}

	page.rows = rows
	page.onDisk = false
	atomic.AddInt64(&bp.usedMemory, page.sizeBytes)

	return page.rows, nil
}

// Unpin decrements pin count. When the last pin is released, the page
// becomes an eviction candidate again. We only touch the LRU queue on
// the transition from pinned → unpinned (pinCount goes to 0).
func (bp *BufferPool) Unpin(page *RowPage) {
	// Guard against going negative (e.g., double-unpin bugs)
	if atomic.LoadInt32(&page.pinCount) <= 0 {
		return
	}
	newCount := atomic.AddInt32(&page.pinCount, -1)

	if bp.disabled {
		return
	}

	// Only touch LRU when transitioning to fully unpinned
	if newCount == 0 {
		bp.lru.Touch(page)
	}
}

// maxSyncEvictions limits the number of pages evicted synchronously during
// Register to avoid blocking the mutation path. The background evictor
// handles any remaining pressure.
const maxSyncEvictions = 4

// Register adds a new page to the buffer pool's memory tracking.
// It evicts a bounded number of pages synchronously if memory exceeds the
// limit; the background evictor handles any remaining pressure asynchronously.
func (bp *BufferPool) Register(page *RowPage) {
	if bp.disabled {
		return
	}

	atomic.AddInt64(&bp.usedMemory, page.sizeBytes)
	bp.lru.Touch(page)

	// Bounded synchronous eviction — avoid blocking the hot mutation path
	for i := 0; i < maxSyncEvictions && atomic.LoadInt64(&bp.usedMemory) > bp.maxMemory; i++ {
		if !bp.TryEvict() {
			break
		}
	}
}

// Unregister removes a page from the pool and cleans up its spill file.
func (bp *BufferPool) Unregister(page *RowPage) {
	if bp.disabled {
		return
	}

	bp.lru.Remove(page)

	page.mu.Lock()
	defer page.mu.Unlock()

	if page.rows != nil {
		atomic.AddInt64(&bp.usedMemory, -page.sizeBytes)
		page.rows = nil
	}

	if page.onDisk && page.diskPath != "" {
		os.Remove(page.diskPath)
		page.onDisk = false
		page.diskPath = ""
	}
}

// UpdateLatestVersion updates the latest version for a table (used for eviction priority).
func (bp *BufferPool) UpdateLatestVersion(table string, version int64) {
	if bp.disabled {
		return
	}
	bp.mu.Lock()
	bp.latestVersions[table] = version
	bp.mu.Unlock()
}

// TryEvict attempts to evict one page from memory to disk.
// Returns true if a page was evicted, false if nothing could be evicted.
func (bp *BufferPool) TryEvict() bool {
	if bp.disabled {
		return false
	}

	bp.mu.Lock()
	versions := make(map[string]int64, len(bp.latestVersions))
	for k, v := range bp.latestVersions {
		versions[k] = v
	}
	bp.mu.Unlock()

	page := bp.lru.EvictCandidate(versions)
	if page == nil {
		return false
	}

	page.mu.Lock()
	defer page.mu.Unlock()

	if page.rows == nil {
		// Already evicted by someone else
		return false
	}

	// Serialize to disk
	diskPath := bp.spillPath(page.id)
	if err := savePageToDisk(page.rows, diskPath); err != nil {
		// Failed to save — put back in LRU, don't evict
		bp.lru.Touch(page)
		return false
	}

	// Free memory
	page.diskPath = diskPath
	page.onDisk = true
	page.rows = nil
	atomic.AddInt64(&bp.usedMemory, -page.sizeBytes)

	return true
}

// MemoryUsage returns current and max memory usage in bytes.
func (bp *BufferPool) MemoryUsage() (used, max int64) {
	return atomic.LoadInt64(&bp.usedMemory), bp.maxMemory
}

// Close stops the background evictor and removes all spill files.
func (bp *BufferPool) Close() error {
	if bp.disabled {
		return nil
	}

	if atomic.CompareAndSwapInt32(&bp.stopped, 0, 1) {
		close(bp.stopCh)
	}

	// Clean up spill directory
	if bp.spillDir != "" {
		os.RemoveAll(bp.spillDir)
	}

	return nil
}

func (bp *BufferPool) backgroundEvictor() {
	ticker := time.NewTicker(bp.evictInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			for atomic.LoadInt64(&bp.usedMemory) > bp.maxMemory {
				if !bp.TryEvict() {
					break
				}
			}
		case <-bp.stopCh:
			return
		}
	}
}

func (bp *BufferPool) spillPath(id PageID) string {
	return filepath.Join(bp.spillDir, fmt.Sprintf("%s_%d_%d.page", id.Table, id.Version, id.Index))
}

// savePageToDisk serializes a slice of rows to disk using the fast binary codec.
func savePageToDisk(rows []domain.Row, path string) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	data, err := encodeRows(rows)
	if err != nil {
		return fmt.Errorf("page encode failed: %w", err)
	}

	return os.WriteFile(path, data, 0644)
}

// loadPageFromDisk deserializes a slice of rows from disk using the fast binary codec.
func loadPageFromDisk(path string) ([]domain.Row, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	rows, err := decodeRows(data)
	if err != nil {
		return nil, fmt.Errorf("page decode failed: %w", err)
	}

	return rows, nil
}
