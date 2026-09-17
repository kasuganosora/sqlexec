package parquet

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParquetAdapter_WALCrashRecovery(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "crash")
	ctx := context.Background()
	config := &domain.DataSourceConfig{
		Type:     domain.DataSourceTypeParquet,
		Name:     dir,
		Writable: true,
		Options: map[string]interface{}{
			"writable":       true,
			"flush_interval": "1h",
		},
	}

	first := NewParquetAdapter(config)
	require.NoError(t, first.Connect(ctx))

	tableInfo := &domain.TableInfo{
		Name: "users",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int64", Nullable: false, Primary: true},
			{Name: "name", Type: "string", Nullable: true},
		},
	}
	require.NoError(t, first.CreateTable(ctx, tableInfo))
	n, err := first.Insert(ctx, "users", []domain.Row{{"id": int64(1), "name": "Alice"}}, nil)
	require.NoError(t, err)
	require.Equal(t, int64(1), n)

	// Simulate an unclean shutdown: persist WAL without flushing parquet pages.
	if first.stopCh != nil {
		close(first.stopCh)
	}
	if first.flushTicker != nil {
		first.flushTicker.Stop()
	}
	require.NoError(t, first.wal.Close())

	second := NewParquetAdapter(config)
	require.NoError(t, second.Connect(ctx))
	t.Cleanup(func() { _ = second.Close(ctx) })

	result, err := second.Query(ctx, "users", nil)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Len(t, result.Rows, 1)
	assert.Equal(t, int64(1), result.Rows[0]["id"])
	assert.Equal(t, "Alice", result.Rows[0]["name"])
}
