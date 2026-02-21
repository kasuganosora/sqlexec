package memory

import (
	"log"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// PagedRows replaces []domain.Row in TableData. It divides rows into
// fixed-size pages managed by a BufferPool that can transparently spill
// cold pages to disk and reload them on demand.
type PagedRows struct {
	pool      *BufferPool
	pages     []*RowPage
	totalRows int
	pageSize  int
	// table and version are stored for incremental AppendPage usage
	table   string
	version int64
}

// NewPagedRows creates a PagedRows by splitting the given rows into pages
// and registering them with the buffer pool.
func NewPagedRows(pool *BufferPool, rows []domain.Row, pageSize int, table string, version int64) *PagedRows {
	if pageSize <= 0 {
		if pool != nil && pool.pageSize > 0 {
			pageSize = pool.pageSize
		} else {
			pageSize = defaultPageSize
		}
	}

	totalRows := len(rows)
	numPages := (totalRows + pageSize - 1) / pageSize
	if numPages == 0 {
		numPages = 1 // at least one empty page
	}

	pages := make([]*RowPage, 0, numPages)
	for i := 0; i < totalRows; i += pageSize {
		end := i + pageSize
		if end > totalRows {
			end = totalRows
		}
		pageRows := rows[i:end]
		page := &RowPage{
			id: PageID{
				Table:   table,
				Version: version,
				Index:   len(pages),
			},
			rows:      pageRows,
			rowCount:  len(pageRows),
			sizeBytes: estimatePageSize(pageRows),
		}
		pages = append(pages, page)

		if pool != nil {
			pool.Register(page)
		}
	}

	// Handle empty input
	if totalRows == 0 {
		page := &RowPage{
			id: PageID{
				Table:   table,
				Version: version,
				Index:   0,
			},
			rows:      []domain.Row{},
			rowCount:  0,
			sizeBytes: 0,
		}
		pages = append(pages, page)
	}

	return &PagedRows{
		pool:      pool,
		pages:     pages,
		totalRows: totalRows,
		pageSize:  pageSize,
	}
}

// NewPagedRowsBuilder creates an empty PagedRows ready for incremental AppendPage calls.
// Unlike NewPagedRows which takes all rows upfront, this allows streaming construction
// where each page is registered with the buffer pool as it is added, enabling the pool
// to evict earlier pages and keep peak memory bounded to O(pageSize).
func NewPagedRowsBuilder(pool *BufferPool, pageSize int, table string, version int64) *PagedRows {
	if pageSize <= 0 {
		if pool != nil && pool.pageSize > 0 {
			pageSize = pool.pageSize
		} else {
			pageSize = defaultPageSize
		}
	}
	return &PagedRows{
		pool:     pool,
		pages:    make([]*RowPage, 0),
		pageSize: pageSize,
		table:    table,
		version:  version,
	}
}

// AppendPage adds a page of rows to the PagedRows. The caller transfers ownership
// of the rows slice — it must not be modified after this call. The page is immediately
// registered with the buffer pool, which may trigger eviction of cold pages.
func (pr *PagedRows) AppendPage(rows []domain.Row) {
	page := &RowPage{
		id: PageID{
			Table:   pr.table,
			Version: pr.version,
			Index:   len(pr.pages),
		},
		rows:      rows,
		rowCount:  len(rows),
		sizeBytes: estimatePageSize(rows),
	}
	pr.pages = append(pr.pages, page)
	pr.totalRows += len(rows)
	if pr.pool != nil {
		pr.pool.Register(page)
	}
}

// NewEmptyPagedRows creates a PagedRows with zero rows (for CreateTable/TruncateTable).
func NewEmptyPagedRows(pool *BufferPool, pageSize int) *PagedRows {
	if pageSize <= 0 {
		if pool != nil && pool.pageSize > 0 {
			pageSize = pool.pageSize
		} else {
			pageSize = defaultPageSize
		}
	}
	return &PagedRows{
		pool:      pool,
		pages:     []*RowPage{{rows: []domain.Row{}, rowCount: 0}},
		totalRows: 0,
		pageSize:  pageSize,
	}
}

// Len returns the total number of rows across all pages.
func (pr *PagedRows) Len() int {
	if pr == nil {
		return 0
	}
	return pr.totalRows
}

// Get returns the row at the given index. It pins and unpins the containing
// page automatically. For bulk access, prefer Materialize() or Range().
func (pr *PagedRows) Get(i int) domain.Row {
	if pr == nil || i < 0 || i >= pr.totalRows {
		return nil
	}

	pageIdx := i / pr.pageSize
	if pageIdx >= len(pr.pages) {
		return nil
	}
	page := pr.pages[pageIdx]
	offset := i % pr.pageSize

	if pr.pool != nil && !pr.pool.disabled {
		rows, err := pr.pool.Pin(page)
		if err != nil {
			return nil
		}
		var row domain.Row
		if offset < len(rows) {
			row = rows[offset]
		}
		pr.pool.Unpin(page)
		return row
	}

	// Passthrough mode
	if offset >= len(page.rows) {
		return nil
	}
	return page.rows[offset]
}

// Materialize loads all pages into memory and returns a flat []domain.Row slice.
// This is the primary compatibility bridge — existing code that uses []domain.Row
// calls this method. Pages remain pinned only during materialization; once the
// slice is returned, pages are unpinned and become eviction candidates.
func (pr *PagedRows) Materialize() []domain.Row {
	if pr == nil {
		return nil
	}
	if pr.totalRows == 0 {
		return []domain.Row{}
	}

	result := make([]domain.Row, 0, pr.totalRows)

	for _, page := range pr.pages {
		if pr.pool != nil && !pr.pool.disabled {
			rows, err := pr.pool.Pin(page)
			if err != nil {
				// Log the error — the returned slice will have fewer rows than expected
				log.Printf("[WARN] Materialize: failed to load page %v: %v (skipping %d rows)", page.id, err, page.rowCount)
				continue
			}
			result = append(result, rows...)
			pr.pool.Unpin(page)
		} else {
			result = append(result, page.rows...)
		}
	}

	return result
}

// Range iterates over all rows, calling fn for each. If fn returns false,
// iteration stops. Pages are pinned one at a time during iteration.
func (pr *PagedRows) Range(fn func(index int, row domain.Row) bool) {
	if pr == nil {
		return
	}

	globalIdx := 0
	for _, page := range pr.pages {
		var rows []domain.Row

		if pr.pool != nil && !pr.pool.disabled {
			var err error
			rows, err = pr.pool.Pin(page)
			if err != nil {
				globalIdx += page.rowCount
				continue
			}
		} else {
			rows = page.rows
		}

		for _, row := range rows {
			if !fn(globalIdx, row) {
				if pr.pool != nil && !pr.pool.disabled {
					pr.pool.Unpin(page)
				}
				return
			}
			globalIdx++
		}

		if pr.pool != nil && !pr.pool.disabled {
			pr.pool.Unpin(page)
		}
	}
}

// Release unregisters all pages from the buffer pool and cleans up spill files.
// Call this when a TableData version is garbage collected.
func (pr *PagedRows) Release() {
	if pr == nil || pr.pool == nil {
		return
	}

	for _, page := range pr.pages {
		pr.pool.Unregister(page)
	}
	pr.pages = nil
	pr.totalRows = 0
}
