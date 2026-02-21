package memory

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// TestBufferPool_PinEvictRace tests that concurrent Pin and eviction do not
// cause a data race on page.rows. Run with: go test -race
func TestBufferPool_PinEvictRace(t *testing.T) {
	cfg := &PagingConfig{
		Enabled:       true,
		MaxMemoryMB:   1, // Very small to force eviction
		PageSize:      10,
		SpillDir:      t.TempDir(),
		EvictInterval: 1 * time.Millisecond, // Very aggressive eviction
	}
	bp := NewBufferPool(cfg)
	defer bp.Close()

	// Create pages with some data
	pages := make([]*RowPage, 20)
	for i := 0; i < len(pages); i++ {
		rows := make([]domain.Row, 10)
		for j := range rows {
			rows[j] = domain.Row{"val": int64(i*10 + j)}
		}
		page := &RowPage{
			id:        PageID{Table: "test", Version: 1, Index: i},
			rows:      rows,
			rowCount:  len(rows),
			sizeBytes: estimatePageSize(rows),
		}
		pages[i] = page
		bp.Register(page)
	}
	bp.UpdateLatestVersion("test", 1)

	// Concurrently Pin/Unpin and trigger eviction
	var wg sync.WaitGroup
	var errors int64

	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for iter := 0; iter < 200; iter++ {
				pageIdx := (goroutineID*200 + iter) % len(pages)
				page := pages[pageIdx]
				rows, err := bp.Pin(page)
				if err != nil {
					atomic.AddInt64(&errors, 1)
					continue
				}
				// Do something with rows (simulate read)
				if rows != nil {
					_ = len(rows)
				}
				bp.Unpin(page)
			}
		}(g)
	}

	// Concurrently evict
	for g := 0; g < 2; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for iter := 0; iter < 200; iter++ {
				bp.TryEvict()
				time.Sleep(100 * time.Microsecond)
			}
		}()
	}

	wg.Wait()

	// The test passes if no data race is detected by -race.
	// Some Pin errors are expected when pages are being evicted.
	t.Logf("Pin errors (expected some): %d", atomic.LoadInt64(&errors))
}

// TestBufferPool_UnpinNegativePinCount verifies that Unpin on an unpinned
// page does not cause pinCount to go negative.
func TestBufferPool_UnpinNegativePinCount(t *testing.T) {
	page := &RowPage{
		id:       PageID{Table: "test", Version: 1, Index: 0},
		rows:     []domain.Row{{"a": 1}},
		rowCount: 1,
	}

	// Pin once, unpin twice — pinCount should not go below 0
	page.Pin()
	page.Unpin()

	// This second unpin should be safe and pinCount should be clamped at 0
	page.Unpin()

	count := atomic.LoadInt32(&page.pinCount)
	if count < 0 {
		t.Errorf("pinCount went negative: %d", count)
	}
}
