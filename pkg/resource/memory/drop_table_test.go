package memory

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// TestDropTable_ReleasesPagedRows verifies that DropTable releases all
// PagedRows across all versions, freeing buffer pool memory.
func TestDropTable_ReleasesPagedRows(t *testing.T) {
	cfg := &PagingConfig{
		Enabled:     true,
		MaxMemoryMB: 100,
		PageSize:    100,
		SpillDir:    t.TempDir(),
	}
	ds := NewMVCCDataSource(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Name:     "test",
		Writable: true,
	}, cfg)
	ctx := context.Background()
	ds.Connect(ctx)

	ds.CreateTable(ctx, &domain.TableInfo{
		Name: "bigdata",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "INTEGER"},
			{Name: "payload", Type: "VARCHAR"},
		},
	})

	// Insert data to create multiple versions
	for i := 0; i < 3; i++ {
		rows := make([]domain.Row, 100)
		for j := range rows {
			rows[j] = domain.Row{"id": int64(j), "payload": "data"}
		}
		ds.Insert(ctx, "bigdata", rows, nil)
	}

	// Record memory before drop
	usedBefore := atomic.LoadInt64(&ds.bufferPool.usedMemory)
	if usedBefore == 0 {
		t.Skip("buffer pool has no tracked memory; passthrough mode")
	}

	// Drop the table
	ds.DropTable(ctx, "bigdata")

	// Memory should be released
	usedAfter := atomic.LoadInt64(&ds.bufferPool.usedMemory)
	if usedAfter >= usedBefore {
		t.Errorf("DropTable did not release buffer pool memory: before=%d, after=%d", usedBefore, usedAfter)
	}
}

// TestTruncateTable_ReleasesOldVersions verifies that TruncateTable releases
// the old version's PagedRows.
func TestTruncateTable_ReleasesOldVersions(t *testing.T) {
	cfg := &PagingConfig{
		Enabled:     true,
		MaxMemoryMB: 100,
		PageSize:    100,
		SpillDir:    t.TempDir(),
	}
	ds := NewMVCCDataSource(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Name:     "test",
		Writable: true,
	}, cfg)
	ctx := context.Background()
	ds.Connect(ctx)

	ds.CreateTable(ctx, &domain.TableInfo{
		Name: "data",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "INTEGER"},
		},
	})

	// Insert data
	rows := make([]domain.Row, 500)
	for j := range rows {
		rows[j] = domain.Row{"id": int64(j)}
	}
	ds.Insert(ctx, "data", rows, nil)

	usedBefore := atomic.LoadInt64(&ds.bufferPool.usedMemory)
	if usedBefore == 0 {
		t.Skip("buffer pool has no tracked memory")
	}

	// Truncate
	ds.TruncateTable(ctx, "data")

	// After GC, old version memory should be freed
	ds.mu.Lock()
	ds.gcOldVersions()
	ds.mu.Unlock()

	usedAfter := atomic.LoadInt64(&ds.bufferPool.usedMemory)
	if usedAfter >= usedBefore {
		t.Errorf("TruncateTable did not release old version memory: before=%d, after=%d", usedBefore, usedAfter)
	}
}
