package testing

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kasuganosora/sqlexec/pkg/information_schema"
	"github.com/kasuganosora/sqlexec/pkg/resource/application"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// TestPrivilegeTablesVisibility directly tests VirtualTable permission check
func TestPrivilegeTablesVisibility(t *testing.T) {
	ctx := context.Background()

	// Create data source manager
	dsManager := application.NewDataSourceManager()

	// Create TablesTable
	tablesTable := information_schema.NewTablesTable(dsManager)

	t.Run("Root user can see privilege tables", func(t *testing.T) {
		// Root user can see all tables
		options := &domain.QueryOptions{
			User: "root",
		}

		result, err := tablesTable.Query(ctx, nil, options)
		require.NoError(t, err)
		require.NotNil(t, result)

		t.Logf("Tables seen by root user: %d", result.Total)

		// Should see 9 tables
		assert.Equal(t, int64(9), result.Total)
	})

	t.Run("Normal user cannot see privilege tables", func(t *testing.T) {
		// Normal user can only see basic 5 tables
		options := &domain.QueryOptions{
			User: "normal_user",
		}

		result, err := tablesTable.Query(ctx, nil, options)
		require.NoError(t, err)
		require.NotNil(t, result)

		t.Logf("Tables seen by normal user: %d", result.Total)

		// Should only see 5 tables
		assert.Equal(t, int64(5), result.Total)
	})

	t.Run("No user setting cannot see privilege tables", func(t *testing.T) {
		// Without user info, cannot see privilege tables
		options := &domain.QueryOptions{
			User: "",
		}

		result, err := tablesTable.Query(ctx, nil, options)
		require.NoError(t, err)
		require.NotNil(t, result)

		t.Logf("Tables seen without user: %d", result.Total)

		// Should only see 5 tables
		assert.Equal(t, int64(5), result.Total)
	})
}
