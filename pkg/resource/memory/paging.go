package memory

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// PageID uniquely identifies a page within the buffer pool.
type PageID struct {
	Table   string
	Version int64
	Index   int
}

// RowPage is the smallest unit of eviction. It holds a slice of rows that
// can be transparently spilled to disk and reloaded on demand.
type RowPage struct {
	id        PageID
	rows      []domain.Row // nil when evicted to disk
	rowCount  int          // always accurate, even when evicted
	sizeBytes int64        // estimated memory footprint in bytes
	onDisk    bool         // true if rows have been serialized to disk
	diskPath  string       // path to the spill file
	pinCount  int32        // atomic: >0 means in use, not evictable
	mu        sync.Mutex
}

// Pin increments the pin count, preventing eviction.
func (p *RowPage) Pin() {
	atomic.AddInt32(&p.pinCount, 1)
}

// Unpin decrements the pin count. Safe to call even if pinCount is already 0.
func (p *RowPage) Unpin() {
	if atomic.LoadInt32(&p.pinCount) <= 0 {
		return
	}
	atomic.AddInt32(&p.pinCount, -1)
}

// IsPinned returns true if the page is currently pinned.
func (p *RowPage) IsPinned() bool {
	return atomic.LoadInt32(&p.pinCount) > 0
}

// IsEvicted returns true if the page's rows are currently on disk.
func (p *RowPage) IsEvicted() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.rows == nil && p.onDisk
}

// estimateRowSize estimates the memory footprint of a single Row in bytes.
func estimateRowSize(row domain.Row) int64 {
	if row == nil {
		return 0
	}
	// Base map overhead: header + buckets
	size := int64(64 + 8*len(row))
	for k, v := range row {
		size += int64(len(k) + 16) // key string header + data
		size += estimateValueSize(v)
	}
	return size
}

// estimateValueSize estimates the memory footprint of a value.
func estimateValueSize(v interface{}) int64 {
	switch val := v.(type) {
	case nil:
		return 0
	case bool:
		_ = val
		return 1
	case int:
		return 8
	case int8, int16:
		return 2
	case int32, float32:
		return 4
	case int64, float64:
		return 8
	case string:
		return int64(len(val) + 16) // string header + data
	case []byte:
		return int64(len(val) + 24) // slice header + data
	case time.Time:
		return 24
	case []float32:
		return int64(len(val)*4 + 24)
	case []interface{}:
		size := int64(24)
		for _, item := range val {
			size += estimateValueSize(item)
		}
		return size
	case map[string]interface{}:
		size := int64(64)
		for k, item := range val {
			size += int64(len(k)+16) + estimateValueSize(item)
		}
		return size
	default:
		return 16 // conservative default for unknown types
	}
}

// estimatePageSize estimates the total memory footprint of a page by sampling.
// If the page has <= sampleSize rows, all rows are measured. Otherwise,
// the first sampleSize rows are measured and the result is extrapolated.
func estimatePageSize(rows []domain.Row) int64 {
	if len(rows) == 0 {
		return 0
	}
	const sampleSize = 16
	n := len(rows)
	if n <= sampleSize {
		var total int64
		for _, row := range rows {
			total += estimateRowSize(row)
		}
		return total
	}
	// Sample and extrapolate
	var sampleTotal int64
	for i := 0; i < sampleSize; i++ {
		sampleTotal += estimateRowSize(rows[i])
	}
	avgRowSize := sampleTotal / int64(sampleSize)
	return avgRowSize * int64(n)
}
