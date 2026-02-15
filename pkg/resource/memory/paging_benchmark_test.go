package memory

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// ==================== Micro-Benchmarks ====================

// --- NewPagedRows construction overhead ---

func BenchmarkNewPagedRows_Passthrough(b *testing.B) {
	for _, n := range []int{100, 1000, 10000, 100000} {
		rows := makeBenchRows(n)
		b.Run(fmt.Sprintf("rows=%d", n), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = NewPagedRows(nil, rows, 0, "bench", 1)
			}
		})
	}
}

func BenchmarkNewPagedRows_WithPool(b *testing.B) {
	dir := b.TempDir()
	bp := NewBufferPool(&PagingConfig{
		Enabled:       true,
		MaxMemoryMB:   1024,
		PageSize:      4096,
		SpillDir:      dir,
		EvictInterval: time.Hour,
	})
	defer bp.Close()

	for _, n := range []int{100, 1000, 10000, 100000} {
		rows := makeBenchRows(n)
		b.Run(fmt.Sprintf("rows=%d", n), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				pr := NewPagedRows(bp, rows, 0, "bench", int64(i))
				pr.Release()
			}
		})
	}
}

// --- Materialize overhead ---

func BenchmarkMaterialize_Passthrough(b *testing.B) {
	for _, n := range []int{100, 1000, 10000, 100000} {
		rows := makeBenchRows(n)
		pr := NewPagedRows(nil, rows, 0, "bench", 1)
		b.Run(fmt.Sprintf("rows=%d", n), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				mat := pr.Materialize()
				if len(mat) != n {
					b.Fatal("wrong count")
				}
			}
		})
	}
}

func BenchmarkMaterialize_WithPool(b *testing.B) {
	dir := b.TempDir()
	bp := NewBufferPool(&PagingConfig{
		Enabled:       true,
		MaxMemoryMB:   1024,
		PageSize:      4096,
		SpillDir:      dir,
		EvictInterval: time.Hour,
	})
	defer bp.Close()

	for _, n := range []int{100, 1000, 10000, 100000} {
		rows := makeBenchRows(n)
		pr := NewPagedRows(bp, rows, 0, "bench", 1)
		b.Run(fmt.Sprintf("rows=%d", n), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				mat := pr.Materialize()
				if len(mat) != n {
					b.Fatal("wrong count")
				}
			}
		})
		pr.Release()
	}
}

// --- Direct slice vs PagedRows baseline comparison ---

func BenchmarkDirectSliceAccess(b *testing.B) {
	for _, n := range []int{1000, 10000, 100000} {
		rows := makeBenchRows(n)
		b.Run(fmt.Sprintf("rows=%d", n), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for _, row := range rows {
					_ = row["id"]
				}
			}
		})
	}
}

func BenchmarkPagedRowsMaterializeAndIterate(b *testing.B) {
	for _, n := range []int{1000, 10000, 100000} {
		rows := makeBenchRows(n)
		pr := NewPagedRows(nil, rows, 0, "bench", 1)
		b.Run(fmt.Sprintf("rows=%d", n), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				mat := pr.Materialize()
				for _, row := range mat {
					_ = row["id"]
				}
			}
		})
	}
}

// --- Get single row ---

func BenchmarkGet_Passthrough(b *testing.B) {
	rows := makeBenchRows(10000)
	pr := NewPagedRows(nil, rows, 4096, "bench", 1)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = pr.Get(i % 10000)
	}
}

func BenchmarkGet_WithPool(b *testing.B) {
	dir := b.TempDir()
	bp := NewBufferPool(&PagingConfig{
		Enabled:       true,
		MaxMemoryMB:   1024,
		PageSize:      4096,
		SpillDir:      dir,
		EvictInterval: time.Hour,
	})
	defer bp.Close()

	rows := makeBenchRows(10000)
	pr := NewPagedRows(bp, rows, 4096, "bench", 1)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = pr.Get(i % 10000)
	}
	b.StopTimer()
	pr.Release()
}

// --- Range iteration ---

func BenchmarkRange_Passthrough(b *testing.B) {
	for _, n := range []int{1000, 10000, 100000} {
		rows := makeBenchRows(n)
		pr := NewPagedRows(nil, rows, 0, "bench", 1)
		b.Run(fmt.Sprintf("rows=%d", n), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				pr.Range(func(idx int, row domain.Row) bool {
					_ = row["id"]
					return true
				})
			}
		})
	}
}

func BenchmarkRange_WithPool(b *testing.B) {
	dir := b.TempDir()
	bp := NewBufferPool(&PagingConfig{
		Enabled:       true,
		MaxMemoryMB:   1024,
		PageSize:      4096,
		SpillDir:      dir,
		EvictInterval: time.Hour,
	})
	defer bp.Close()

	for _, n := range []int{1000, 10000, 100000} {
		rows := makeBenchRows(n)
		pr := NewPagedRows(bp, rows, 0, "bench", 1)
		b.Run(fmt.Sprintf("rows=%d", n), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				pr.Range(func(idx int, row domain.Row) bool {
					_ = row["id"]
					return true
				})
			}
		})
		pr.Release()
	}
}

// --- Pin/Unpin hot path ---

func BenchmarkPinUnpin_Passthrough(b *testing.B) {
	bp := NewBufferPool(nil)
	page := &RowPage{
		id:   PageID{Table: "t", Version: 1, Index: 0},
		rows: makeBenchRows(100),
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, _ := bp.Pin(page)
		_ = rows
		bp.Unpin(page)
	}
}

func BenchmarkPinUnpin_Enabled_InMemory(b *testing.B) {
	dir := b.TempDir()
	bp := NewBufferPool(&PagingConfig{
		Enabled:       true,
		MaxMemoryMB:   1024,
		PageSize:      4096,
		SpillDir:      dir,
		EvictInterval: time.Hour,
	})
	defer bp.Close()

	page := &RowPage{
		id:        PageID{Table: "t", Version: 1, Index: 0},
		rows:      makeBenchRows(100),
		rowCount:  100,
		sizeBytes: 10000,
	}
	bp.Register(page)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, _ := bp.Pin(page)
		_ = rows
		bp.Unpin(page)
	}
}

// --- Eviction + reload latency ---

func BenchmarkEvictAndReload(b *testing.B) {
	dir := b.TempDir()
	bp := NewBufferPool(&PagingConfig{
		Enabled:       true,
		MaxMemoryMB:   1024,
		PageSize:      4096,
		SpillDir:      dir,
		EvictInterval: time.Hour,
	})
	defer bp.Close()

	for _, n := range []int{100, 1000, 4096} {
		b.Run(fmt.Sprintf("pageRows=%d", n), func(b *testing.B) {
			rows := makeBenchRows(n)
			page := &RowPage{
				id:        PageID{Table: "bench", Version: 1, Index: 0},
				rows:      rows,
				rowCount:  n,
				sizeBytes: estimatePageSize(rows),
			}
			bp.Register(page)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Evict
				page.mu.Lock()
				diskPath := bp.spillPath(page.id)
				_ = savePageToDisk(page.rows, diskPath)
				savedRows := page.rows
				page.rows = nil
				page.onDisk = true
				page.diskPath = diskPath
				page.mu.Unlock()

				// Reload
				loaded, err := loadPageFromDisk(diskPath)
				if err != nil {
					b.Fatal(err)
				}
				page.mu.Lock()
				page.rows = loaded
				page.onDisk = false
				page.mu.Unlock()

				_ = savedRows
			}
			b.StopTimer()
			bp.Unregister(page)
		})
	}
}

// --- Codec serialization (in-memory only, no disk I/O) ---

func BenchmarkCodecEncode(b *testing.B) {
	for _, n := range []int{100, 1000, 4096} {
		rows := makeBenchRows(n)
		b.Run(fmt.Sprintf("rows=%d", n), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				data, err := encodeRows(rows)
				if err != nil {
					b.Fatal(err)
				}
				_ = data
			}
		})
	}
}

func BenchmarkCodecDecode(b *testing.B) {
	for _, n := range []int{100, 1000, 4096} {
		rows := makeBenchRows(n)
		data, err := encodeRows(rows)
		if err != nil {
			b.Fatal(err)
		}

		b.Run(fmt.Sprintf("rows=%d", n), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				decoded, err := decodeRows(data)
				if err != nil {
					b.Fatal(err)
				}
				if len(decoded) != n {
					b.Fatal("wrong count")
				}
			}
		})
	}
}

// --- Disk spill (codec + file I/O) ---

func BenchmarkSpillToDisk(b *testing.B) {
	for _, n := range []int{100, 1000, 4096} {
		rows := makeBenchRows(n)
		b.Run(fmt.Sprintf("rows=%d", n), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				path := fmt.Sprintf("%s/bench_%d.page", b.TempDir(), i)
				if err := savePageToDisk(rows, path); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkLoadFromDisk(b *testing.B) {
	for _, n := range []int{100, 1000, 4096} {
		rows := makeBenchRows(n)
		dir := b.TempDir()
		path := fmt.Sprintf("%s/bench.page", dir)
		if err := savePageToDisk(rows, path); err != nil {
			b.Fatal(err)
		}

		b.Run(fmt.Sprintf("rows=%d", n), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				loaded, err := loadPageFromDisk(path)
				if err != nil {
					b.Fatal(err)
				}
				if len(loaded) != n {
					b.Fatal("wrong count")
				}
			}
		})
	}
}

// --- LRU queue overhead ---

func BenchmarkLRUTouch(b *testing.B) {
	q := newLRUQueue()
	pages := make([]*RowPage, 1000)
	for i := range pages {
		pages[i] = &RowPage{id: PageID{Table: "t", Version: 1, Index: i}}
		q.Touch(pages[i])
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.Touch(pages[i%1000])
	}
}

func BenchmarkLRURemove(b *testing.B) {
	pages := make([]*RowPage, b.N)
	q := newLRUQueue()
	for i := range pages {
		pages[i] = &RowPage{id: PageID{Table: "t", Version: 1, Index: i}}
		q.Touch(pages[i])
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.Remove(pages[i])
	}
}

// --- estimatePageSize overhead ---

func BenchmarkEstimatePageSize(b *testing.B) {
	for _, n := range []int{100, 1000, 4096} {
		rows := makeBenchRows(n)
		b.Run(fmt.Sprintf("rows=%d", n), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = estimatePageSize(rows)
			}
		})
	}
}

// ==================== End-to-End Integration Benchmarks ====================

// Benchmark the full CRUD path through MVCCDataSource with passthrough pool

func BenchmarkE2E_Insert_Passthrough(b *testing.B) {
	ds := NewMVCCDataSource(nil)
	ds.Connect(context.Background())
	ctx := context.Background()
	ds.CreateTable(ctx, &domain.TableInfo{
		Name:    "bench",
		Columns: benchColumns(),
	})

	rows := makeBenchRows(100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ds.Insert(ctx, "bench", rows, nil)
	}
}

func BenchmarkE2E_Insert_WithPool(b *testing.B) {
	dir := b.TempDir()
	ds := NewMVCCDataSource(nil, &PagingConfig{
		Enabled:       true,
		MaxMemoryMB:   512,
		PageSize:      4096,
		SpillDir:      dir,
		EvictInterval: time.Hour,
	})
	ds.Connect(context.Background())
	ctx := context.Background()
	ds.CreateTable(ctx, &domain.TableInfo{
		Name:    "bench",
		Columns: benchColumns(),
	})

	rows := makeBenchRows(100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ds.Insert(ctx, "bench", rows, nil)
	}
	b.StopTimer()
	ds.Close(ctx)
}

func BenchmarkE2E_Query_Passthrough(b *testing.B) {
	ds := NewMVCCDataSource(nil)
	ds.Connect(context.Background())
	ctx := context.Background()
	ds.CreateTable(ctx, &domain.TableInfo{
		Name:    "bench",
		Columns: benchColumns(),
	})
	ds.Insert(ctx, "bench", makeBenchRows(10000), nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := ds.Query(ctx, "bench", &domain.QueryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		if len(result.Rows) != 10000 {
			b.Fatal("wrong row count")
		}
	}
}

func BenchmarkE2E_Query_WithPool(b *testing.B) {
	dir := b.TempDir()
	ds := NewMVCCDataSource(nil, &PagingConfig{
		Enabled:       true,
		MaxMemoryMB:   512,
		PageSize:      4096,
		SpillDir:      dir,
		EvictInterval: time.Hour,
	})
	ds.Connect(context.Background())
	ctx := context.Background()
	ds.CreateTable(ctx, &domain.TableInfo{
		Name:    "bench",
		Columns: benchColumns(),
	})
	ds.Insert(ctx, "bench", makeBenchRows(10000), nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := ds.Query(ctx, "bench", &domain.QueryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		if len(result.Rows) != 10000 {
			b.Fatal("wrong row count")
		}
	}
	b.StopTimer()
	ds.Close(ctx)
}

func BenchmarkE2E_QueryWithFilter_Passthrough(b *testing.B) {
	ds := NewMVCCDataSource(nil)
	ds.Connect(context.Background())
	ctx := context.Background()
	ds.CreateTable(ctx, &domain.TableInfo{
		Name:    "bench",
		Columns: benchColumns(),
	})
	ds.Insert(ctx, "bench", makeBenchRows(10000), nil)

	opts := &domain.QueryOptions{
		Filters: []domain.Filter{{Field: "id", Operator: "=", Value: int64(5000)}},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := ds.Query(ctx, "bench", opts)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkE2E_QueryWithFilter_WithPool(b *testing.B) {
	dir := b.TempDir()
	ds := NewMVCCDataSource(nil, &PagingConfig{
		Enabled:       true,
		MaxMemoryMB:   512,
		PageSize:      4096,
		SpillDir:      dir,
		EvictInterval: time.Hour,
	})
	ds.Connect(context.Background())
	ctx := context.Background()
	ds.CreateTable(ctx, &domain.TableInfo{
		Name:    "bench",
		Columns: benchColumns(),
	})
	ds.Insert(ctx, "bench", makeBenchRows(10000), nil)

	opts := &domain.QueryOptions{
		Filters: []domain.Filter{{Field: "id", Operator: "=", Value: int64(5000)}},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := ds.Query(ctx, "bench", opts)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	ds.Close(ctx)
}

// --- Page size tuning benchmark ---

func BenchmarkPageSizeTuning(b *testing.B) {
	for _, pageSize := range []int{64, 256, 1024, 4096, 16384} {
		rows := makeBenchRows(100000)
		b.Run(fmt.Sprintf("pageSize=%d", pageSize), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				pr := NewPagedRows(nil, rows, pageSize, "bench", 1)
				mat := pr.Materialize()
				if len(mat) != 100000 {
					b.Fatal("wrong count")
				}
			}
		})
	}
}

// ==================== Helpers ====================

func makeBenchRows(n int) []domain.Row {
	rows := make([]domain.Row, n)
	for i := 0; i < n; i++ {
		rows[i] = domain.Row{
			"id":         int64(i),
			"name":       fmt.Sprintf("user_%d", i),
			"email":      fmt.Sprintf("user_%d@example.com", i),
			"age":        int64(20 + i%50),
			"score":      float64(i) * 1.5,
			"active":     i%2 == 0,
			"created_at": time.Now(),
		}
	}
	return rows
}

func benchColumns() []domain.ColumnInfo {
	return []domain.ColumnInfo{
		{Name: "id", Type: "INTEGER"},
		{Name: "name", Type: "TEXT"},
		{Name: "email", Type: "TEXT"},
		{Name: "age", Type: "INTEGER"},
		{Name: "score", Type: "REAL"},
		{Name: "active", Type: "BOOLEAN"},
		{Name: "created_at", Type: "TIMESTAMP"},
	}
}
