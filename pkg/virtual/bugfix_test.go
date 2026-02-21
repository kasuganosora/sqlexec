package virtual

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

// ==========================================================================
// Bug 6 (P1): VirtualDataSource.Query() and WritableVirtualDataSource.Query()
// panic when options is nil because options.Filters dereferences nil pointer.
// ==========================================================================

func TestBug6_VirtualDataSource_Query_NilOptions(t *testing.T) {
	table := &mockTable{}
	ds := NewVirtualDataSource(table)

	ctx := context.Background()
	// BUG: This panics with nil pointer dereference because options.Filters
	// is accessed without checking if options is nil.
	result, err := ds.Query(ctx, "mock_table", nil)
	assert.NoError(t, err)
	assert.NotNil(t, result)
}

func TestBug6_WritableVirtualDataSource_Query_NilOptions(t *testing.T) {
	table := &mockTable{}
	ds := NewWritableVirtualDataSource(table, "test")

	ctx := context.Background()
	// BUG: Same nil pointer dereference as VirtualDataSource.
	result, err := ds.Query(ctx, "mock_table", nil)
	assert.NoError(t, err)
	assert.NotNil(t, result)
}

// ==========================================================================
// Bug 7 (P2): WritableVirtualDataSource.GetTables() missing nil provider check
// ==========================================================================

func TestBug7_WritableVirtualDataSource_GetTables_NilProvider(t *testing.T) {
	ds := NewWritableVirtualDataSource(nil, "test")

	ctx := context.Background()
	// BUG: Panics because w.provider is nil — no nil check unlike VirtualDataSource.
	_, err := ds.GetTables(ctx)
	assert.Error(t, err, "should return error when provider is nil")
}
