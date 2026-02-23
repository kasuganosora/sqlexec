package testutils

import (
	"context"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/resource/memory"
	"github.com/stretchr/testify/require"
)

// MemoryTestHelper 内存数据源测试辅助器
// 提供快速创建内存数据源、表和数据的能力
type MemoryTestHelper struct {
	ds      domain.DataSource
	factory *memory.MemoryFactory
	ctx     context.Context
}

// NewMemoryTestHelper 创建内存测试辅助器
// 自动创建并连接内存数据源
func NewMemoryTestHelper(t *testing.T) *MemoryTestHelper {
	factory := memory.NewMemoryFactory()
	config := &domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	}

	ds, err := factory.Create(config)
	require.NoError(t, err, "Failed to create memory datasource")

	ctx := context.Background()
	err = ds.Connect(ctx)
	require.NoError(t, err, "Failed to connect to memory datasource")

	return &MemoryTestHelper{
		ds:      ds,
		factory: factory,
		ctx:     ctx,
	}
}

// GetDataSource 获取数据源
func (h *MemoryTestHelper) GetDataSource() domain.DataSource {
	return h.ds
}

// GetContext 获取测试context
func (h *MemoryTestHelper) GetContext() context.Context {
	return h.ctx
}

// CreateTable 创建测试表
func (h *MemoryTestHelper) CreateTable(t *testing.T, tableInfo *domain.TableInfo) {
	err := h.ds.CreateTable(h.ctx, tableInfo)
	require.NoError(t, err, "Failed to create table %s", tableInfo.Name)
}

// InsertData 插入测试数据
func (h *MemoryTestHelper) InsertData(t *testing.T, tableName string, rows []domain.Row) {
	_, err := h.ds.Insert(h.ctx, tableName, rows, &domain.InsertOptions{})
	require.NoError(t, err, "Failed to insert data into %s", tableName)
}

// QueryData 查询测试数据
func (h *MemoryTestHelper) QueryData(t *testing.T, tableName string, options *domain.QueryOptions) *domain.QueryResult {
	result, err := h.ds.Query(h.ctx, tableName, options)
	require.NoError(t, err, "Failed to query table %s", tableName)
	return result
}

// Cleanup 清理资源
func (h *MemoryTestHelper) Cleanup() error {
	if h.ds != nil {
		return h.ds.Close(h.ctx)
	}
	return nil
}

// Example: 快速创建包含用户表的测试环境
/*
func TestUserQueries(t *testing.T) {
    helper := testutils.NewMemoryTestHelper(t)
    defer helper.Cleanup()

    // 创建users表
    helper.CreateTable(t, &domain.TableInfo{
        Name: "users",
        Columns: []domain.ColumnInfo{
            {Name: "id", Type: "int64", Primary: true},
            {Name: "name", Type: "string"},
            {Name: "email", Type: "string"},
        },
    })

    // 插入测试数据
    helper.InsertData(t, "users", []domain.Row{
        {"id": int64(1), "name": "Alice", "email": "alice@example.com"},
        {"id": int64(2), "name": "Bob", "email": "bob@example.com"},
    })

    // 执行测试...
}
*/
