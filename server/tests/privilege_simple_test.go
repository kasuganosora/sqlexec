package tests

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kasuganosora/sqlexec/pkg/information_schema"
	"github.com/kasuganosora/sqlexec/pkg/resource/application"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// TestPrivilegeTablesVisibility 直接测试 VirtualTable 权限检查
func TestPrivilegeTablesVisibility(t *testing.T) {
	ctx := context.Background()

	// 创建数据源管理器
	dsManager := application.NewDataSourceManager()

	// 创建 TablesTable
	tablesTable := information_schema.NewTablesTable(dsManager)

	t.Run("Root用户可以看到权限表", func(t *testing.T) {
		// root 用户可以看到所有表
		options := &domain.QueryOptions{
			User: "root",
		}

		result, err := tablesTable.Query(ctx, nil, options)
		require.NoError(t, err)
		require.NotNil(t, result)

		t.Logf("Root用户看到的表数: %d", result.Total)

		// 应该能看到 9 个表
		assert.Equal(t, int64(9), result.Total)
	})

	t.Run("普通用户看不到权限表", func(t *testing.T) {
		// 普通用户只能看到基本的 5 个表
		options := &domain.QueryOptions{
			User: "normal_user",
		}

		result, err := tablesTable.Query(ctx, nil, options)
		require.NoError(t, err)
		require.NotNil(t, result)

		t.Logf("普通用户看到的表数: %d", result.Total)

		// 应该只能看到 5 个表
		assert.Equal(t, int64(5), result.Total)
	})

	t.Run("无用户设置时看不到权限表", func(t *testing.T) {
		// 无用户信息时看不到权限表
		options := &domain.QueryOptions{
			User: "",
		}

		result, err := tablesTable.Query(ctx, nil, options)
		require.NoError(t, err)
		require.NotNil(t, result)

		t.Logf("无用户时看到的表数: %d", result.Total)

		// 应该只能看到 5 个表
		assert.Equal(t, int64(5), result.Total)
	})
}
