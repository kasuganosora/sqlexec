package memory

import (
	"context"
	"strings"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/stretchr/testify/require"
)

// TestQuery_ReturnsNotConnectedError verifies that Query returns an explicit
// "not connected" error (not "table not found") when the datasource is closed.
func TestQuery_ReturnsNotConnectedError(t *testing.T) {
	ds := NewMVCCDataSource(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Name:     "test",
		Writable: true,
	})
	ctx := context.Background()
	require.NoError(t, ds.Connect(ctx))

	// Create a table while connected
	require.NoError(t, ds.CreateTable(ctx, &domain.TableInfo{
		Name:    "users",
		Columns: []domain.ColumnInfo{{Name: "id", Type: "INTEGER"}},
	}))
	_, err := ds.Insert(ctx, "users", []domain.Row{{"id": int64(1)}}, nil)
	require.NoError(t, err)

	// Close — marks as not connected
	require.NoError(t, ds.Close(ctx))

	// Query should return "not connected", not panic or return stale data
	_, err = ds.Query(ctx, "users", &domain.QueryOptions{})
	if err == nil {
		t.Error("expected error when querying closed datasource, got nil")
	} else if !strings.Contains(err.Error(), "not connected") {
		t.Errorf("expected 'not connected' error, got: %v", err)
	}
}

// TestFilter_ReturnsNotConnectedError verifies that Filter returns an explicit
// "not connected" error when the datasource is closed.
func TestFilter_ReturnsNotConnectedError(t *testing.T) {
	ds := NewMVCCDataSource(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Name:     "test",
		Writable: true,
	})
	ctx := context.Background()
	require.NoError(t, ds.Connect(ctx))

	require.NoError(t, ds.CreateTable(ctx, &domain.TableInfo{
		Name:    "users",
		Columns: []domain.ColumnInfo{{Name: "id", Type: "INTEGER"}},
	}))

	require.NoError(t, ds.Close(ctx))

	_, _, err := ds.Filter(ctx, "users", domain.Filter{}, 0, 10)
	if err == nil {
		t.Error("expected error when filtering closed datasource, got nil")
	} else if !strings.Contains(err.Error(), "not connected") {
		t.Errorf("expected 'not connected' error, got: %v", err)
	}
}
