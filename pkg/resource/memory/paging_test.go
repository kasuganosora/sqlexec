package memory

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// ==================== RowPage Tests ====================

func TestRowPage_PinUnpin(t *testing.T) {
	page := &RowPage{
		id:       PageID{Table: "test", Version: 1, Index: 0},
		rows:     []domain.Row{{"a": 1}},
		rowCount: 1,
	}

	if page.IsPinned() {
		t.Fatal("new page should not be pinned")
	}

	page.Pin()
	if !page.IsPinned() {
		t.Fatal("page should be pinned after Pin()")
	}

	page.Pin()
	page.Unpin()
	if !page.IsPinned() {
		t.Fatal("page should still be pinned (pin count = 1)")
	}

	page.Unpin()
	if page.IsPinned() {
		t.Fatal("page should not be pinned after all Unpins")
	}
}

func TestEstimateRowSize(t *testing.T) {
	row := domain.Row{
		"id":   int64(42),
		"name": "hello",
		"rate": float64(3.14),
		"flag": true,
	}
	size := estimateRowSize(row)
	if size <= 0 {
		t.Fatalf("expected positive size, got %d", size)
	}
	// Should be at least the overhead + key sizes
	if size < 64 {
		t.Fatalf("expected size >= 64 (overhead), got %d", size)
	}
}

func TestEstimatePageSize(t *testing.T) {
	rows := []domain.Row{
		{"id": int64(1), "name": "alice"},
		{"id": int64(2), "name": "bob"},
	}
	size := estimatePageSize(rows)
	if size <= 0 {
		t.Fatalf("expected positive size, got %d", size)
	}
}

func TestEstimatePageSize_Empty(t *testing.T) {
	size := estimatePageSize(nil)
	if size != 0 {
		t.Fatalf("expected 0 for nil rows, got %d", size)
	}
	size = estimatePageSize([]domain.Row{})
	if size != 0 {
		t.Fatalf("expected 0 for empty rows, got %d", size)
	}
}

// ==================== PagedRows Tests ====================

func TestPagedRows_NewAndMaterialize(t *testing.T) {
	rows := makeTestRows(100)
	pr := NewPagedRows(nil, rows, 10, "test", 1)

	if pr.Len() != 100 {
		t.Fatalf("expected 100 rows, got %d", pr.Len())
	}

	materialized := pr.Materialize()
	if len(materialized) != 100 {
		t.Fatalf("expected 100 materialized rows, got %d", len(materialized))
	}

	for i, row := range materialized {
		if row["id"] != int64(i) {
			t.Fatalf("row %d: expected id=%d, got %v", i, i, row["id"])
		}
	}
}

func TestPagedRows_Get(t *testing.T) {
	rows := makeTestRows(50)
	pr := NewPagedRows(nil, rows, 10, "test", 1)

	for i := 0; i < 50; i++ {
		row := pr.Get(i)
		if row == nil {
			t.Fatalf("Get(%d) returned nil", i)
		}
		if row["id"] != int64(i) {
			t.Fatalf("Get(%d): expected id=%d, got %v", i, i, row["id"])
		}
	}

	// Out of bounds
	if pr.Get(-1) != nil {
		t.Fatal("Get(-1) should return nil")
	}
	if pr.Get(50) != nil {
		t.Fatal("Get(50) should return nil")
	}
}

func TestPagedRows_Range(t *testing.T) {
	rows := makeTestRows(25)
	pr := NewPagedRows(nil, rows, 10, "test", 1)

	var collected []int64
	pr.Range(func(idx int, row domain.Row) bool {
		collected = append(collected, row["id"].(int64))
		return true
	})

	if len(collected) != 25 {
		t.Fatalf("expected 25 rows, got %d", len(collected))
	}
}

func TestPagedRows_RangeEarlyStop(t *testing.T) {
	rows := makeTestRows(100)
	pr := NewPagedRows(nil, rows, 10, "test", 1)

	count := 0
	pr.Range(func(idx int, row domain.Row) bool {
		count++
		return count < 5
	})

	if count != 5 {
		t.Fatalf("expected 5 iterations, got %d", count)
	}
}

func TestPagedRows_Empty(t *testing.T) {
	pr := NewEmptyPagedRows(nil, 10)
	if pr.Len() != 0 {
		t.Fatalf("expected 0 rows, got %d", pr.Len())
	}
	materialized := pr.Materialize()
	if len(materialized) != 0 {
		t.Fatalf("expected empty materialized, got %d rows", len(materialized))
	}
}

func TestPagedRows_NilSafety(t *testing.T) {
	var pr *PagedRows
	if pr.Len() != 0 {
		t.Fatal("nil PagedRows.Len() should return 0")
	}
	if pr.Get(0) != nil {
		t.Fatal("nil PagedRows.Get() should return nil")
	}
	if pr.Materialize() != nil {
		t.Fatal("nil PagedRows.Materialize() should return nil")
	}
	// Should not panic
	pr.Range(func(int, domain.Row) bool { return true })
	pr.Release()
}

// ==================== BufferPool Tests ====================

func TestBufferPool_PassthroughMode(t *testing.T) {
	bp := NewBufferPool(nil)
	if !bp.IsDisabled() {
		t.Fatal("nil config should create disabled pool")
	}

	bp2 := NewBufferPool(&PagingConfig{Enabled: false})
	if !bp2.IsDisabled() {
		t.Fatal("Enabled=false should create disabled pool")
	}
}

func TestBufferPool_PinUnpin(t *testing.T) {
	dir := t.TempDir()
	bp := NewBufferPool(&PagingConfig{
		Enabled:       true,
		MaxMemoryMB:   100,
		PageSize:      10,
		SpillDir:      dir,
		EvictInterval: time.Hour, // don't auto-evict during test
	})
	defer bp.Close()

	page := &RowPage{
		id:        PageID{Table: "t", Version: 1, Index: 0},
		rows:      []domain.Row{{"x": 1}},
		rowCount:  1,
		sizeBytes: 100,
	}
	bp.Register(page)

	rows, err := bp.Pin(page)
	if err != nil {
		t.Fatalf("Pin failed: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}

	bp.Unpin(page)
}

func TestBufferPool_EvictAndReload(t *testing.T) {
	dir := t.TempDir()
	// Very small memory limit to force eviction
	bp := NewBufferPool(&PagingConfig{
		Enabled:       true,
		MaxMemoryMB:   1, // 1MB
		PageSize:      10,
		SpillDir:      dir,
		EvictInterval: time.Hour,
	})
	defer bp.Close()

	// Create pages that exceed 1MB
	var pages []*RowPage
	for i := 0; i < 20; i++ {
		rows := makeTestRows(100)
		page := &RowPage{
			id:        PageID{Table: "t", Version: 1, Index: i},
			rows:      rows,
			rowCount:  100,
			sizeBytes: 100 * 1024, // 100KB each, 20 pages = 2MB
		}
		pages = append(pages, page)
		bp.Register(page)
	}

	// Some pages should have been evicted
	evictedCount := 0
	for _, page := range pages {
		page.mu.Lock()
		if page.rows == nil && page.onDisk {
			evictedCount++
		}
		page.mu.Unlock()
	}

	if evictedCount == 0 {
		t.Fatal("expected some pages to be evicted")
	}

	// Verify we can reload evicted pages
	for _, page := range pages {
		rows, err := bp.Pin(page)
		if err != nil {
			t.Fatalf("failed to pin page %v: %v", page.id, err)
		}
		if len(rows) != 100 {
			t.Fatalf("expected 100 rows after reload, got %d", len(rows))
		}
		bp.Unpin(page)
	}
}

func TestBufferPool_VersionPriorityEviction(t *testing.T) {
	dir := t.TempDir()
	bp := NewBufferPool(&PagingConfig{
		Enabled:       true,
		MaxMemoryMB:   1,
		PageSize:      10,
		SpillDir:      dir,
		EvictInterval: time.Hour,
	})
	defer bp.Close()

	bp.UpdateLatestVersion("t", 2)

	// Old version page
	oldPage := &RowPage{
		id:        PageID{Table: "t", Version: 1, Index: 0},
		rows:      makeTestRows(10),
		rowCount:  10,
		sizeBytes: 500 * 1024,
	}
	// New version page
	newPage := &RowPage{
		id:        PageID{Table: "t", Version: 2, Index: 0},
		rows:      makeTestRows(10),
		rowCount:  10,
		sizeBytes: 500 * 1024,
	}

	bp.Register(oldPage)
	bp.Register(newPage)

	// Force eviction
	bp.TryEvict()

	// Old version should be evicted first
	oldPage.mu.Lock()
	oldEvicted := oldPage.rows == nil && oldPage.onDisk
	oldPage.mu.Unlock()

	if !oldEvicted {
		t.Fatal("old version page should be evicted first")
	}
}

func TestBufferPool_Unregister(t *testing.T) {
	dir := t.TempDir()
	bp := NewBufferPool(&PagingConfig{
		Enabled:       true,
		MaxMemoryMB:   100,
		PageSize:      10,
		SpillDir:      dir,
		EvictInterval: time.Hour,
	})
	defer bp.Close()

	page := &RowPage{
		id:        PageID{Table: "t", Version: 1, Index: 0},
		rows:      makeTestRows(5),
		rowCount:  5,
		sizeBytes: 1000,
	}
	bp.Register(page)

	used1, _ := bp.MemoryUsage()
	if used1 <= 0 {
		t.Fatal("expected positive memory usage after register")
	}

	bp.Unregister(page)

	used2, _ := bp.MemoryUsage()
	if used2 != 0 {
		t.Fatalf("expected 0 memory usage after unregister, got %d", used2)
	}
}

func TestBufferPool_Close(t *testing.T) {
	dir := t.TempDir()
	bp := NewBufferPool(&PagingConfig{
		Enabled:       true,
		MaxMemoryMB:   100,
		PageSize:      10,
		SpillDir:      filepath.Join(dir, "spill"),
		EvictInterval: time.Hour,
	})

	// Register and force evict to create spill file
	page := &RowPage{
		id:        PageID{Table: "t", Version: 1, Index: 0},
		rows:      makeTestRows(5),
		rowCount:  5,
		sizeBytes: 1000,
	}
	bp.Register(page)

	err := bp.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Spill directory should be cleaned up
	if _, err := os.Stat(filepath.Join(dir, "spill")); err == nil {
		t.Fatal("spill directory should be removed after Close")
	}
}

// ==================== PagedRows + BufferPool Integration ====================

func TestPagedRows_WithBufferPool(t *testing.T) {
	dir := t.TempDir()
	bp := NewBufferPool(&PagingConfig{
		Enabled:       true,
		MaxMemoryMB:   100,
		PageSize:      10,
		SpillDir:      dir,
		EvictInterval: time.Hour,
	})
	defer bp.Close()

	rows := makeTestRows(50)
	pr := NewPagedRows(bp, rows, 10, "test", 1)

	if pr.Len() != 50 {
		t.Fatalf("expected 50, got %d", pr.Len())
	}

	// Materialize
	mat := pr.Materialize()
	if len(mat) != 50 {
		t.Fatalf("expected 50, got %d", len(mat))
	}

	// Get individual
	for i := 0; i < 50; i++ {
		row := pr.Get(i)
		if row == nil || row["id"] != int64(i) {
			t.Fatalf("Get(%d) failed", i)
		}
	}

	// Release
	pr.Release()
	used, _ := bp.MemoryUsage()
	if used != 0 {
		t.Fatalf("expected 0 memory after Release, got %d", used)
	}
}

func TestPagedRows_WithEviction(t *testing.T) {
	dir := t.TempDir()
	bp := NewBufferPool(&PagingConfig{
		Enabled:     true,
		MaxMemoryMB: 1, // Very small to force eviction
		PageSize:    100,
		SpillDir:    dir,
		EvictInterval: time.Hour,
	})
	defer bp.Close()

	// Create enough data to exceed 1MB
	rows := makeTestRows(5000)
	pr := NewPagedRows(bp, rows, 100, "test", 1)

	// Despite eviction, all data should be accessible
	mat := pr.Materialize()
	if len(mat) != 5000 {
		t.Fatalf("expected 5000 rows, got %d", len(mat))
	}

	// Verify data integrity after eviction + reload
	for i := 0; i < 5000; i++ {
		if mat[i]["id"] != int64(i) {
			t.Fatalf("data corruption at row %d: expected %d, got %v", i, i, mat[i]["id"])
		}
	}

	pr.Release()
}

// ==================== Concurrent Access Tests ====================

func TestPagedRows_ConcurrentGet(t *testing.T) {
	rows := makeTestRows(100)
	pr := NewPagedRows(nil, rows, 10, "test", 1)

	var wg sync.WaitGroup
	for g := 0; g < 10; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				row := pr.Get(i)
				if row == nil {
					t.Errorf("concurrent Get(%d) returned nil", i)
					return
				}
			}
		}()
	}
	wg.Wait()
}

func TestBufferPool_ConcurrentPinUnpin(t *testing.T) {
	dir := t.TempDir()
	bp := NewBufferPool(&PagingConfig{
		Enabled:       true,
		MaxMemoryMB:   100,
		PageSize:      10,
		SpillDir:      dir,
		EvictInterval: time.Hour,
	})
	defer bp.Close()

	page := &RowPage{
		id:        PageID{Table: "t", Version: 1, Index: 0},
		rows:      makeTestRows(10),
		rowCount:  10,
		sizeBytes: 1000,
	}
	bp.Register(page)

	var wg sync.WaitGroup
	for g := 0; g < 20; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				rows, err := bp.Pin(page)
				if err != nil {
					t.Errorf("concurrent Pin failed: %v", err)
					return
				}
				if len(rows) != 10 {
					t.Errorf("expected 10 rows, got %d", len(rows))
				}
				bp.Unpin(page)
			}
		}()
	}
	wg.Wait()
}

// ==================== Codec Tests ====================

func TestCodec_RoundTrip(t *testing.T) {
	rows := []domain.Row{
		{"id": int64(1), "name": "alice", "score": float64(99.5), "active": true, "data": []byte{1, 2, 3}},
		{"id": int64(2), "name": "bob", "score": float64(0), "active": false, "nil_val": nil},
		{"id": int64(3), "age": int(30), "small": int32(42), "rate": float32(1.5)},
	}

	encoded, err := encodeRows(rows)
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	decoded, err := decodeRows(encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if len(decoded) != len(rows) {
		t.Fatalf("expected %d rows, got %d", len(rows), len(decoded))
	}

	// Check first row
	if decoded[0]["id"] != int64(1) {
		t.Errorf("row 0 id: expected 1, got %v", decoded[0]["id"])
	}
	if decoded[0]["name"] != "alice" {
		t.Errorf("row 0 name: expected alice, got %v", decoded[0]["name"])
	}
	if decoded[0]["active"] != true {
		t.Errorf("row 0 active: expected true, got %v", decoded[0]["active"])
	}

	// Check nil values
	if decoded[1]["nil_val"] != nil {
		t.Errorf("row 1 nil_val: expected nil, got %v", decoded[1]["nil_val"])
	}

	// Check int types
	if decoded[2]["age"] != int(30) {
		t.Errorf("row 2 age: expected 30, got %v (%T)", decoded[2]["age"], decoded[2]["age"])
	}
}

func TestCodec_TimeRoundTrip(t *testing.T) {
	now := time.Now().Truncate(time.Millisecond)
	rows := []domain.Row{
		{"ts": now},
	}

	encoded, err := encodeRows(rows)
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}
	decoded, err := decodeRows(encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	ts, ok := decoded[0]["ts"].(time.Time)
	if !ok {
		t.Fatalf("expected time.Time, got %T", decoded[0]["ts"])
	}
	if !ts.Equal(now) {
		t.Errorf("time mismatch: expected %v, got %v", now, ts)
	}
}

func TestCodec_Empty(t *testing.T) {
	encoded, err := encodeRows([]domain.Row{})
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}
	decoded, err := decodeRows(encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if len(decoded) != 0 {
		t.Fatalf("expected 0 rows, got %d", len(decoded))
	}
}

func TestCodec_LargeDataset(t *testing.T) {
	rows := makeTestRows(10000)
	encoded, err := encodeRows(rows)
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}
	decoded, err := decodeRows(encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if len(decoded) != 10000 {
		t.Fatalf("expected 10000 rows, got %d", len(decoded))
	}
	for i := 0; i < 10000; i++ {
		if decoded[i]["id"] != int64(i) {
			t.Fatalf("row %d id mismatch", i)
		}
	}
}

// ==================== AppendPage Tests ====================

func TestNewPagedRowsBuilder_AppendPage(t *testing.T) {
	pr := NewPagedRowsBuilder(nil, 10, "test", 1)

	if pr.Len() != 0 {
		t.Fatalf("expected 0 rows initially, got %d", pr.Len())
	}

	// Append first page
	pr.AppendPage(makeTestRows(10))
	if pr.Len() != 10 {
		t.Fatalf("expected 10 rows after first append, got %d", pr.Len())
	}

	// Append second page
	pr.AppendPage(makeTestRows(5))
	if pr.Len() != 15 {
		t.Fatalf("expected 15 rows after second append, got %d", pr.Len())
	}

	// Materialize and verify
	mat := pr.Materialize()
	if len(mat) != 15 {
		t.Fatalf("expected 15 materialized rows, got %d", len(mat))
	}
}

func TestNewPagedRowsBuilder_AppendPage_WithBufferPool(t *testing.T) {
	dir := t.TempDir()
	bp := NewBufferPool(&PagingConfig{
		Enabled:       true,
		MaxMemoryMB:   1,
		PageSize:      100,
		SpillDir:      dir,
		EvictInterval: time.Hour,
	})
	defer bp.Close()

	pr := NewPagedRowsBuilder(bp, 100, "test", 1)

	// Append 50 pages of 100 rows = 5000 rows (exceeds 1MB, triggers eviction)
	for i := 0; i < 50; i++ {
		rows := make([]domain.Row, 100)
		for j := 0; j < 100; j++ {
			rows[j] = domain.Row{
				"id":   int64(i*100 + j),
				"name": "some_name_value_here",
				"val":  float64(j) * 1.5,
			}
		}
		pr.AppendPage(rows)
	}

	if pr.Len() != 5000 {
		t.Fatalf("expected 5000 rows, got %d", pr.Len())
	}

	// Verify all data is accessible despite evictions
	mat := pr.Materialize()
	if len(mat) != 5000 {
		t.Fatalf("expected 5000 materialized rows, got %d", len(mat))
	}

	// Verify data integrity
	for i := 0; i < 5000; i++ {
		if mat[i]["id"] != int64(i) {
			t.Fatalf("data corruption at row %d: expected %d, got %v", i, i, mat[i]["id"])
		}
	}

	pr.Release()
}

func TestNewPagedRowsBuilder_EmptyAppend(t *testing.T) {
	pr := NewPagedRowsBuilder(nil, 10, "test", 1)
	pr.AppendPage([]domain.Row{})

	if pr.Len() != 0 {
		t.Fatalf("expected 0 rows, got %d", pr.Len())
	}

	mat := pr.Materialize()
	if len(mat) != 0 {
		t.Fatalf("expected 0 materialized, got %d", len(mat))
	}
}

func TestNewPagedRowsBuilder_Get(t *testing.T) {
	pr := NewPagedRowsBuilder(nil, 3, "test", 1)

	// Append two pages: [0,1,2] and [3,4]
	page1 := make([]domain.Row, 3)
	for i := 0; i < 3; i++ {
		page1[i] = domain.Row{"id": int64(i)}
	}
	pr.AppendPage(page1)

	page2 := make([]domain.Row, 2)
	for i := 0; i < 2; i++ {
		page2[i] = domain.Row{"id": int64(i + 3)}
	}
	pr.AppendPage(page2)

	// Get from each page
	for i := 0; i < 5; i++ {
		row := pr.Get(i)
		if row == nil {
			t.Fatalf("Get(%d) returned nil", i)
		}
		if row["id"] != int64(i) {
			t.Fatalf("Get(%d): expected id=%d, got %v", i, i, row["id"])
		}
	}

	if pr.Get(5) != nil {
		t.Fatal("Get(5) should return nil for out of bounds")
	}
}

// ==================== BulkLoad Tests ====================

func TestBulkLoad_Basic(t *testing.T) {
	ctx := context.Background()
	ds := NewMVCCDataSource(nil)
	ds.Connect(ctx)

	tableInfo := &domain.TableInfo{
		Name: "t",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "INT", Primary: true},
			{Name: "name", Type: "VARCHAR"},
		},
	}
	if err := ds.CreateTable(ctx, tableInfo); err != nil {
		t.Fatal(err)
	}

	// BulkLoad with 3 pages
	err := ds.BulkLoad("t", func(addPage func([]domain.Row)) error {
		addPage([]domain.Row{{"id": int64(1), "name": "a"}, {"id": int64(2), "name": "b"}})
		addPage([]domain.Row{{"id": int64(3), "name": "c"}})
		addPage([]domain.Row{{"id": int64(4), "name": "d"}, {"id": int64(5), "name": "e"}})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// Verify all rows accessible
	result, err := ds.Query(ctx, "t", &domain.QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if result.Total != 5 {
		t.Fatalf("expected 5 rows, got %d", result.Total)
	}
}

func TestBulkLoad_EmptyTable(t *testing.T) {
	ctx := context.Background()
	ds := NewMVCCDataSource(nil)
	ds.Connect(ctx)

	tableInfo := &domain.TableInfo{
		Name: "empty",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "INT", Primary: true},
		},
	}
	if err := ds.CreateTable(ctx, tableInfo); err != nil {
		t.Fatal(err)
	}

	err := ds.BulkLoad("empty", func(addPage func([]domain.Row)) error {
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	result, err := ds.Query(ctx, "empty", &domain.QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if result.Total != 0 {
		t.Fatalf("expected 0 rows, got %d", result.Total)
	}
}

func TestBulkLoad_NonexistentTable(t *testing.T) {
	ctx := context.Background()
	ds := NewMVCCDataSource(nil)
	ds.Connect(ctx)

	err := ds.BulkLoad("nonexistent", func(addPage func([]domain.Row)) error {
		return nil
	})
	if err == nil {
		t.Fatal("expected error for nonexistent table")
	}
}

func TestBulkLoad_WithBufferPool(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	ds := NewMVCCDataSource(nil, &PagingConfig{
		Enabled:       true,
		MaxMemoryMB:   1,
		PageSize:      100,
		SpillDir:      dir,
		EvictInterval: time.Hour,
	})
	ds.Connect(ctx)
	defer ds.Close(ctx)

	tableInfo := &domain.TableInfo{
		Name: "large",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "INT", Primary: true},
			{Name: "data", Type: "VARCHAR"},
		},
	}
	if err := ds.CreateTable(ctx, tableInfo); err != nil {
		t.Fatal(err)
	}

	totalRows := 5000
	pageSize := 100

	// BulkLoad large dataset through buffer pool
	err := ds.BulkLoad("large", func(addPage func([]domain.Row)) error {
		for start := 0; start < totalRows; start += pageSize {
			end := start + pageSize
			if end > totalRows {
				end = totalRows
			}
			batch := make([]domain.Row, end-start)
			for i := range batch {
				batch[i] = domain.Row{
					"id":   int64(start + i),
					"data": "some_data_value_for_testing",
				}
			}
			addPage(batch)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// Verify all data accessible
	result, err := ds.Query(ctx, "large", &domain.QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if result.Total != int64(totalRows) {
		t.Fatalf("expected %d rows, got %d", totalRows, result.Total)
	}
}

// ==================== Helpers ====================

func makeTestRows(n int) []domain.Row {
	rows := make([]domain.Row, n)
	for i := 0; i < n; i++ {
		rows[i] = domain.Row{
			"id":   int64(i),
			"name": "row_" + string(rune('A'+i%26)),
			"val":  float64(i) * 1.5,
		}
	}
	return rows
}
