package memory

import (
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewIndexManager(t *testing.T) {
	mgr := NewIndexManager()

	assert.NotNil(t, mgr)
	assert.NotNil(t, mgr.tables)
	assert.Len(t, mgr.tables, 0)
}

func TestIndexManager_CreateIndex_BTree(t *testing.T) {
	mgr := NewIndexManager()

	idx, err := mgr.CreateIndex("test_table", "test_column", IndexTypeBTree, false)

	assert.NoError(t, err)
	assert.NotNil(t, idx)

	info := idx.GetIndexInfo()
	assert.Equal(t, "test_table", info.TableName)
	assert.Equal(t, "test_column", info.Column)
	assert.Equal(t, IndexTypeBTree, info.Type)
	assert.False(t, info.Unique)
}

func TestIndexManager_CreateIndex_Hash(t *testing.T) {
	mgr := NewIndexManager()

	idx, err := mgr.CreateIndex("test_table", "test_column", IndexTypeHash, true)

	assert.NoError(t, err)
	assert.NotNil(t, idx)

	info := idx.GetIndexInfo()
	assert.Equal(t, "test_table", info.TableName)
	assert.Equal(t, IndexTypeHash, info.Type)
	assert.True(t, info.Unique)
}

func TestIndexManager_CreateIndex_FullText(t *testing.T) {
	mgr := NewIndexManager()

	idx, err := mgr.CreateIndex("test_table", "test_column", IndexTypeFullText, false)

	assert.NoError(t, err)
	assert.NotNil(t, idx)

	info := idx.GetIndexInfo()
	assert.Equal(t, "test_table", info.TableName)
	assert.Equal(t, IndexTypeFullText, info.Type)
	assert.False(t, info.Unique)
}

func TestIndexManager_CreateIndex_DuplicateColumn(t *testing.T) {
	mgr := NewIndexManager()

	// 创建第一个索引
	_, err := mgr.CreateIndex("test_table", "test_column", IndexTypeBTree, false)
	require.NoError(t, err)

	// 尝试在同一列创建第二个索引
	_, err = mgr.CreateIndex("test_table", "test_column", IndexTypeHash, false)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "index already exists")
}

func TestIndexManager_CreateIndex_UnsupportedType(t *testing.T) {
	mgr := NewIndexManager()

	_, err := mgr.CreateIndex("test_table", "test_column", "unsupported", false)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported index type")
}

func TestIndexManager_CreateIndex_MultipleTables(t *testing.T) {
	mgr := NewIndexManager()

	// 为多个表创建索引
	idx1, err := mgr.CreateIndex("table1", "col1", IndexTypeBTree, false)
	require.NoError(t, err)

	idx2, err := mgr.CreateIndex("table2", "col2", IndexTypeHash, true)
	require.NoError(t, err)

	assert.NotNil(t, idx1)
	assert.NotNil(t, idx2)

	// 验证两个表都有索引
	assert.Contains(t, mgr.tables, "table1")
	assert.Contains(t, mgr.tables, "table2")
}

func TestIndexManager_CreateIndex_MultipleColumns(t *testing.T) {
	mgr := NewIndexManager()

	// 为同一表的多列创建索引
	idx1, err := mgr.CreateIndex("test_table", "col1", IndexTypeBTree, false)
	require.NoError(t, err)

	idx2, err := mgr.CreateIndex("test_table", "col2", IndexTypeHash, false)
	require.NoError(t, err)

	assert.NotNil(t, idx1)
	assert.NotNil(t, idx2)

	info1 := idx1.GetIndexInfo()
	info2 := idx2.GetIndexInfo()
	assert.Equal(t, "col1", info1.Column)
	assert.Equal(t, "col2", info2.Column)
}

func TestIndexManager_GetIndex(t *testing.T) {
	mgr := NewIndexManager()

	// 创建索引
	createdIdx, err := mgr.CreateIndex("test_table", "test_column", IndexTypeBTree, false)
	require.NoError(t, err)

	// 获取索引
	retrievedIdx, err := mgr.GetIndex("test_table", "test_column")

	assert.NoError(t, err)
	assert.NotNil(t, retrievedIdx)
	assert.Equal(t, createdIdx.GetIndexInfo().Name, retrievedIdx.GetIndexInfo().Name)
}

func TestIndexManager_GetIndex_TableNotFound(t *testing.T) {
	mgr := NewIndexManager()

	_, err := mgr.GetIndex("nonexistent_table", "test_column")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "table not found")
}

func TestIndexManager_GetIndex_ColumnNotFound(t *testing.T) {
	mgr := NewIndexManager()

	_, err := mgr.CreateIndex("test_table", "col1", IndexTypeBTree, false)
	require.NoError(t, err)

	_, err = mgr.GetIndex("test_table", "nonexistent_column")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "index not found")
}

func TestIndexManager_DropIndex(t *testing.T) {
	mgr := NewIndexManager()

	// 创建索引
	idx, err := mgr.CreateIndex("test_table", "test_column", IndexTypeBTree, false)
	require.NoError(t, err)
	indexName := idx.GetIndexInfo().Name

	// 删除索引
	err = mgr.DropIndex("test_table", indexName)
	assert.NoError(t, err)

	// 验证索引已删除
	_, err = mgr.GetIndex("test_table", "test_column")
	assert.Error(t, err)
}

func TestIndexManager_DropIndex_TableNotFound(t *testing.T) {
	mgr := NewIndexManager()

	err := mgr.DropIndex("nonexistent_table", "some_index")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "table not found")
}

func TestIndexManager_DropIndex_IndexNotFound(t *testing.T) {
	mgr := NewIndexManager()

	err := mgr.DropIndex("test_table", "nonexistent_index")
	assert.Error(t, err)
}

func TestIndexManager_DropIndex_DoesNotDeleteTable(t *testing.T) {
	mgr := NewIndexManager()

	// 创建两个索引
	idx1, err := mgr.CreateIndex("test_table", "col1", IndexTypeBTree, false)
	require.NoError(t, err)
	_, err = mgr.CreateIndex("test_table", "col2", IndexTypeHash, false)
	require.NoError(t, err)

	// 删除一个索引
	err = mgr.DropIndex("test_table", idx1.GetIndexInfo().Name)
	assert.NoError(t, err)

	// 表仍然存在
	_, err = mgr.GetIndex("test_table", "col2")
	assert.NoError(t, err)
}

func TestIndexManager_DropTableIndexes(t *testing.T) {
	mgr := NewIndexManager()

	// 为表创建多个索引
	_, err := mgr.CreateIndex("test_table", "col1", IndexTypeBTree, false)
	require.NoError(t, err)
	_, err = mgr.CreateIndex("test_table", "col2", IndexTypeHash, false)
	require.NoError(t, err)
	_, err = mgr.CreateIndex("test_table", "col3", IndexTypeFullText, false)
	require.NoError(t, err)

	// 删除表的所有索引
	err = mgr.DropTableIndexes("test_table")
	assert.NoError(t, err)

	// 验证表已被删除
	_, err = mgr.GetIndex("test_table", "col1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "table not found")

	_, err = mgr.GetIndex("test_table", "col2")
	assert.Error(t, err)

	_, err = mgr.GetIndex("test_table", "col3")
	assert.Error(t, err)
}

func TestIndexManager_DropTableIndexes_TableNotFound(t *testing.T) {
	mgr := NewIndexManager()

	err := mgr.DropTableIndexes("nonexistent_table")
	assert.NoError(t, err) // 不应该报错
}

func TestIndexManager_RebuildIndex_BTree(t *testing.T) {
	mgr := NewIndexManager()

	// 创建索引
	_, err := mgr.CreateIndex("test_table", "test_column", IndexTypeBTree, false)
	require.NoError(t, err)

	// 准备数据
	schema := &domain.TableInfo{
		Name: "test_table",
		Columns: []domain.ColumnInfo{
			{Name: "test_column", Type: "INT"},
		},
	}
	rows := []domain.Row{
		{"test_column": 1},
		{"test_column": 2},
		{"test_column": 3},
	}

	// 重建索引
	err = mgr.RebuildIndex("test_table", schema, rows)
	assert.NoError(t, err)
}

func TestIndexManager_RebuildIndex_Hash(t *testing.T) {
	mgr := NewIndexManager()

	// 创建索引
	_, err := mgr.CreateIndex("test_table", "test_column", IndexTypeHash, false)
	require.NoError(t, err)

	// 准备数据
	schema := &domain.TableInfo{
		Name: "test_table",
		Columns: []domain.ColumnInfo{
			{Name: "test_column", Type: "VARCHAR"},
		},
	}
	rows := []domain.Row{
		{"test_column": "value1"},
		{"test_column": "value2"},
		{"test_column": "value3"},
	}

	// 重建索引
	err = mgr.RebuildIndex("test_table", schema, rows)
	assert.NoError(t, err)
}

func TestIndexManager_RebuildIndex_FullText(t *testing.T) {
	mgr := NewIndexManager()

	// 创建索引
	_, err := mgr.CreateIndex("test_table", "content", IndexTypeFullText, false)
	require.NoError(t, err)

	// 准备数据
	schema := &domain.TableInfo{
		Name: "test_table",
		Columns: []domain.ColumnInfo{
			{Name: "content", Type: "TEXT"},
		},
	}
	rows := []domain.Row{
		{"content": "hello world"},
		{"content": "test document"},
		{"content": "another example"},
	}

	// 重建索引
	err = mgr.RebuildIndex("test_table", schema, rows)
	assert.NoError(t, err)
}

func TestIndexManager_RebuildIndex_TableNotFound(t *testing.T) {
	mgr := NewIndexManager()

	schema := &domain.TableInfo{Name: "test_table"}
	rows := []domain.Row{}

	err := mgr.RebuildIndex("nonexistent_table", schema, rows)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "table not found")
}

func TestIndexManager_RebuildIndex_EmptyRows(t *testing.T) {
	mgr := NewIndexManager()

	// 创建索引
	_, err := mgr.CreateIndex("test_table", "test_column", IndexTypeBTree, false)
	require.NoError(t, err)

	// 准备空数据
	schema := &domain.TableInfo{
		Name: "test_table",
		Columns: []domain.ColumnInfo{
			{Name: "test_column", Type: "INT"},
		},
	}
	rows := []domain.Row{}

	// 重建索引（应该成功）
	err = mgr.RebuildIndex("test_table", schema, rows)
	assert.NoError(t, err)
}

func TestIndexManager_GetTableIndexes(t *testing.T) {
	mgr := NewIndexManager()

	// 创建多个索引
	idx1, err := mgr.CreateIndex("test_table", "col1", IndexTypeBTree, false)
	require.NoError(t, err)
	idx2, err := mgr.CreateIndex("test_table", "col2", IndexTypeHash, true)
	require.NoError(t, err)
	idx3, err := mgr.CreateIndex("test_table", "col3", IndexTypeFullText, false)
	require.NoError(t, err)

	// 获取所有索引
	infos, err := mgr.GetTableIndexes("test_table")

	assert.NoError(t, err)
	assert.NotNil(t, infos)
	assert.Len(t, infos, 3)

	// 验证索引信息
	indexNames := make(map[string]bool)
	for _, info := range infos {
		indexNames[info.Name] = true
	}
	assert.True(t, indexNames[idx1.GetIndexInfo().Name])
	assert.True(t, indexNames[idx2.GetIndexInfo().Name])
	assert.True(t, indexNames[idx3.GetIndexInfo().Name])
}

func TestIndexManager_GetTableIndexes_Empty(t *testing.T) {
	mgr := NewIndexManager()

	// 先创建表（通过创建索引）
	_, err := mgr.CreateIndex("test_table", "col1", IndexTypeBTree, false)
	require.NoError(t, err)

	// 删除所有索引
	err = mgr.DropTableIndexes("test_table")
	require.NoError(t, err)

	// 重新创建表
	_, err = mgr.CreateIndex("test_table", "col1", IndexTypeBTree, false)
	require.NoError(t, err)

	// 获取索引
	infos, err := mgr.GetTableIndexes("test_table")
	assert.NoError(t, err)
	assert.Len(t, infos, 1)
}

func TestIndexManager_GetTableIndexes_TableNotFound(t *testing.T) {
	mgr := NewIndexManager()

	_, err := mgr.GetTableIndexes("nonexistent_table")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "table not found")
}

func TestIndexManager_ConcurrentOperations(t *testing.T) {
	mgr := NewIndexManager()

	// 并发创建索引
	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func(i int) {
			colName := "col" + string(rune('0'+i))
			_, err := mgr.CreateIndex("test_table", colName, IndexTypeBTree, false)
			assert.NoError(t, err)
			done <- true
		}(i)
	}

	// 等待所有goroutine完成
	for i := 0; i < 10; i++ {
		<-done
	}

	// 验证所有索引都已创建
	infos, err := mgr.GetTableIndexes("test_table")
	assert.NoError(t, err)
	assert.Len(t, infos, 10)
}

func TestIndexManager_IndexOperations_AfterRebuild(t *testing.T) {
	mgr := NewIndexManager()

	// 创建索引
	_, err := mgr.CreateIndex("test_table", "test_column", IndexTypeHash, false)
	require.NoError(t, err)

	// 准备数据
	schema := &domain.TableInfo{
		Name: "test_table",
		Columns: []domain.ColumnInfo{
			{Name: "test_column", Type: "INT"},
		},
	}
	rows := []domain.Row{
		{"test_column": 1},
		{"test_column": 2},
		{"test_column": 3},
	}

	// 重建索引
	err = mgr.RebuildIndex("test_table", schema, rows)
	require.NoError(t, err)

	// 验证索引仍然可以正常使用
	idx, err := mgr.GetIndex("test_table", "test_column")
	assert.NoError(t, err)
	assert.NotNil(t, idx)
}

func TestIndexManager_MultipleIndexesSameColumnDifferentTables(t *testing.T) {
	mgr := NewIndexManager()

	// 在不同表的同名列上创建索引
	idx1, err := mgr.CreateIndex("table1", "id", IndexTypeBTree, true)
	require.NoError(t, err)

	idx2, err := mgr.CreateIndex("table2", "id", IndexTypeHash, true)
	require.NoError(t, err)

	// 验证它们是不同的索引
	assert.NotEqual(t, idx1.GetIndexInfo().Name, idx2.GetIndexInfo().Name)
	assert.Equal(t, "table1", idx1.GetIndexInfo().TableName)
	assert.Equal(t, "table2", idx2.GetIndexInfo().TableName)

	// 验证它们可以独立获取
	retrieved1, err := mgr.GetIndex("table1", "id")
	assert.NoError(t, err)
	assert.Equal(t, idx1.GetIndexInfo().Name, retrieved1.GetIndexInfo().Name)

	retrieved2, err := mgr.GetIndex("table2", "id")
	assert.NoError(t, err)
	assert.Equal(t, idx2.GetIndexInfo().Name, retrieved2.GetIndexInfo().Name)
}
