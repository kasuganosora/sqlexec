package slice

import (
	"context"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/resource/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewSliceAdapter_MapSlice(t *testing.T) {
	// 准备测试数据（使用指针）
	data := &[]map[string]any{
		{"id": 1, "name": "Alice", "age": 30},
		{"id": 2, "name": "Bob", "age": 25},
		{"id": 3, "name": "Charlie", "age": 35},
	}

	// 创建 adapter
	adapter, err := NewSliceAdapter(data, "users", "testdb", true, true)
	require.NoError(t, err)
	require.NotNil(t, adapter)
	assert.True(t, adapter.IsWritable())
	assert.True(t, adapter.SupportsMVCC())
	assert.Equal(t, "users", adapter.GetTableName())
	assert.Equal(t, "testdb", adapter.GetDatabaseName())

	// 验证表结构
	ctx := context.Background()
	schema, err := adapter.GetTableInfo(ctx, "users")
	require.NoError(t, err)
	assert.Equal(t, "users", schema.Name)
	assert.Len(t, schema.Columns, 3)
}

func TestNewSliceAdapter_StructSlice(t *testing.T) {
	// 定义测试结构体
	type User struct {
		ID   int    `json:"id"`
		Name string `json:"name"`
		Age  int    `json:"age"`
	}

	// 准备测试数据（使用指针）
	data := &[]User{
		{ID: 1, Name: "Alice", Age: 30},
		{ID: 2, Name: "Bob", Age: 25},
		{ID: 3, Name: "Charlie", Age: 35},
	}

	// 创建 adapter
	adapter, err := NewSliceAdapter(data, "users", "testdb", true, true)
	require.NoError(t, err)
	require.NotNil(t, adapter)
	assert.True(t, adapter.IsWritable())
	assert.True(t, adapter.SupportsMVCC())

	// 验证表结构
	ctx := context.Background()
	schema, err := adapter.GetTableInfo(ctx, "users")
	require.NoError(t, err)
	assert.Equal(t, "users", schema.Name)
	assert.Len(t, schema.Columns, 3)
}

func TestNewSliceAdapter_EmptySlice(t *testing.T) {
	// 测试空 map slice
	data := []map[string]any{}

	adapter, err := NewSliceAdapter(data, "empty", "testdb", true, true)
	require.NoError(t, err)
	require.NotNil(t, adapter)

	// 验证表结构为空
	ctx := context.Background()
	schema, err := adapter.GetTableInfo(ctx, "empty")
	require.NoError(t, err)
	assert.Equal(t, "empty", schema.Name)
	assert.Len(t, schema.Columns, 0)
}

func TestNewSliceAdapter_ReadOnly(t *testing.T) {
	data := []map[string]any{
		{"id": 1, "name": "Alice"},
	}

	// 创建只读 adapter
	adapter, err := NewSliceAdapter(data, "users", "testdb", false, false)
	require.NoError(t, err)
	assert.False(t, adapter.IsWritable())
	assert.False(t, adapter.SupportsMVCC())
}

func TestSliceAdapter_Query(t *testing.T) {
	data := []map[string]any{
		{"id": 1, "name": "Alice", "age": 30},
		{"id": 2, "name": "Bob", "age": 25},
		{"id": 3, "name": "Charlie", "age": 35},
	}

	adapter, err := NewSliceAdapter(data, "users", "testdb", true, true)
	require.NoError(t, err)
	require.NoError(t, adapter.Connect(context.Background()))

	// 查询所有数据
	result, err := adapter.Query(context.Background(), "users", &domain.QueryOptions{})
	require.NoError(t, err)
	assert.Len(t, result.Rows, 3)

	// 验证第一条数据
	assert.Equal(t, 1, result.Rows[0]["id"])
	assert.Equal(t, "Alice", result.Rows[0]["name"])
	assert.Equal(t, 30, result.Rows[0]["age"])
}

func TestSliceAdapter_QueryWithFilter(t *testing.T) {
	data := []map[string]any{
		{"id": 1, "name": "Alice", "age": 30},
		{"id": 2, "name": "Bob", "age": 25},
		{"id": 3, "name": "Charlie", "age": 35},
	}

	adapter, err := NewSliceAdapter(data, "users", "testdb", true, true)
	require.NoError(t, err)
	require.NoError(t, adapter.Connect(context.Background()))

	// 查询 age > 28 的数据
	result, err := adapter.Query(context.Background(), "users", &domain.QueryOptions{
		Filters: []domain.Filter{
			{Field: "age", Operator: ">", Value: int64(28)},
		},
	})
	require.NoError(t, err)
	assert.Len(t, result.Rows, 2) // Alice(30) 和 Charlie(35)
}

func TestSliceAdapter_Insert(t *testing.T) {
	// 使用指针测试可写
	data := &[]map[string]any{
		{"id": 1, "name": "Alice"},
	}

	adapter, err := NewSliceAdapter(data, "users", "testdb", true, true)
	require.NoError(t, err)
	require.NoError(t, adapter.Connect(context.Background()))

	// 插入新数据
	newRow := domain.Row{"id": 2, "name": "Bob"}
	_, err = adapter.Insert(context.Background(), "users", []domain.Row{newRow}, &domain.InsertOptions{})
	require.NoError(t, err)

	// 验证数据已插入
	result, err := adapter.Query(context.Background(), "users", &domain.QueryOptions{})
	require.NoError(t, err)
	assert.Len(t, result.Rows, 2)
}

func TestSliceAdapter_Update(t *testing.T) {
	// 使用指针测试可写
	data := &[]map[string]any{
		{"id": 1, "name": "Alice", "age": 30},
		{"id": 2, "name": "Bob", "age": 25},
	}

	adapter, err := NewSliceAdapter(data, "users", "testdb", true, true)
	require.NoError(t, err)
	require.NoError(t, adapter.Connect(context.Background()))

	// 更新 Alice 的年龄
	_, err = adapter.Update(context.Background(), "users", []domain.Filter{
		{Field: "name", Operator: "=", Value: "Alice"},
	}, domain.Row{"id": 1, "name": "Alice", "age": 31}, &domain.UpdateOptions{})
	require.NoError(t, err)

	// 验证数据已更新
	result, err := adapter.Query(context.Background(), "users", &domain.QueryOptions{
		Filters: []domain.Filter{
			{Field: "name", Operator: "=", Value: "Alice"},
		},
	})
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)
	assert.Equal(t, 31, result.Rows[0]["age"])
}

func TestSliceAdapter_Delete(t *testing.T) {
	// 使用指针测试可写
	data := &[]map[string]any{
		{"id": 1, "name": "Alice"},
		{"id": 2, "name": "Bob"},
	}

	adapter, err := NewSliceAdapter(data, "users", "testdb", true, true)
	require.NoError(t, err)
	require.NoError(t, adapter.Connect(context.Background()))

	// 删除 Bob
	_, err = adapter.Delete(context.Background(), "users", []domain.Filter{
		{Field: "name", Operator: "=", Value: "Bob"},
	}, &domain.DeleteOptions{})
	require.NoError(t, err)

	// 验证数据已删除
	result, err := adapter.Query(context.Background(), "users", &domain.QueryOptions{})
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)
	assert.Equal(t, "Alice", result.Rows[0]["name"])
}

func TestSliceAdapter_Truncate(t *testing.T) {
	// 使用指针测试可写
	data := &[]map[string]any{
		{"id": 1, "name": "Alice"},
		{"id": 2, "name": "Bob"},
	}

	adapter, err := NewSliceAdapter(data, "users", "testdb", true, true)
	require.NoError(t, err)
	require.NoError(t, adapter.Connect(context.Background()))

	// 清空表
	err = adapter.TruncateTable(context.Background(), "users")
	require.NoError(t, err)

	// 验证表已清空
	result, err := adapter.Query(context.Background(), "users", &domain.QueryOptions{})
	require.NoError(t, err)
	assert.Len(t, result.Rows, 0)
}

func TestSliceAdapter_ReadOnlyOperations(t *testing.T) {
	data := []map[string]any{
		{"id": 1, "name": "Alice"},
	}

	// 创建只读 adapter
	adapter, err := NewSliceAdapter(data, "users", "testdb", false, false)
	require.NoError(t, err)
	require.NoError(t, adapter.Connect(context.Background()))

	// 查询应该成功
	_, err = adapter.Query(context.Background(), "users", &domain.QueryOptions{})
	require.NoError(t, err)

	// 写入操作应该失败
	ctx := context.Background()
	_, err = adapter.Insert(ctx, "users", []domain.Row{{"id": 2, "name": "Bob"}}, &domain.InsertOptions{})
	assert.Error(t, err)

	_, err = adapter.Update(ctx, "users", []domain.Filter{{Field: "id", Operator: "=", Value: int64(1)}}, domain.Row{}, &domain.UpdateOptions{})
	assert.Error(t, err)

	_, err = adapter.Delete(ctx, "users", []domain.Filter{{Field: "id", Operator: "=", Value: int64(1)}}, &domain.DeleteOptions{})
	assert.Error(t, err)

	err = adapter.TruncateTable(ctx, "users")
	assert.Error(t, err)
}

func TestSliceAdapter_SyncToOriginal(t *testing.T) {
	// 使用指针来测试同步
	originalData := &[]map[string]any{
		{"id": 1, "name": "Alice"},
		{"id": 2, "name": "Bob"},
	}

	adapter, err := NewSliceAdapter(originalData, "users", "testdb", true, true)
	require.NoError(t, err)
	require.NoError(t, adapter.Connect(context.Background()))

	// 插入新数据
	_, err = adapter.Insert(context.Background(), "users", []domain.Row{{"id": 3, "name": "Charlie"}}, &domain.InsertOptions{})
	require.NoError(t, err)

	// 手动同步回原始数据
	err = adapter.SyncToOriginal(context.Background())
	require.NoError(t, err)

	// 验证原始数据已更新
	assert.Len(t, *originalData, 3)
	assert.Equal(t, "Charlie", (*originalData)[2]["name"])
}

func TestSliceAdapter_SyncToOriginal_WithTransaction(t *testing.T) {
	// 测试通过commit自动同步
	originalData := &[]map[string]any{
		{"id": 1, "name": "Alice"},
		{"id": 2, "name": "Bob"},
	}

	adapter, err := NewSliceAdapter(originalData, "users", "testdb", true, true)
	require.NoError(t, err)
	require.NoError(t, adapter.Connect(context.Background()))

	ctx := context.Background()

	// 开始事务
	txnID, err := adapter.BeginTx(ctx, false)
	require.NoError(t, err)

	// 使用事务上下文插入数据
	txnCtx := memory.SetTransactionID(ctx, txnID)
	_, err = adapter.Insert(txnCtx, "users", []domain.Row{{"id": 3, "name": "Charlie"}}, &domain.InsertOptions{})
	require.NoError(t, err)

	// 原始数据此时还未更新（只有commit后才会更新）
	assert.Len(t, *originalData, 2)

	// 提交事务（会自动调用SyncToOriginal）
	err = adapter.CommitTx(ctx, txnID)
	require.NoError(t, err)

	// 验证原始数据已更新
	assert.Len(t, *originalData, 3)
	assert.Equal(t, "Charlie", (*originalData)[2]["name"])
}

func TestSliceAdapter_SyncToOriginal_StructSlice(t *testing.T) {
	type User struct {
		ID   int    `json:"id"`
		Name string `json:"name"`
	}

	originalData := &[]User{
		{ID: 1, Name: "Alice"},
		{ID: 2, Name: "Bob"},
	}

	adapter, err := NewSliceAdapter(originalData, "users", "testdb", true, true)
	require.NoError(t, err)
	require.NoError(t, adapter.Connect(context.Background()))

	// 插入新数据（struct支持写入，但不支持同步回原变量）
	_, err = adapter.Insert(context.Background(), "users", []domain.Row{{"id": 3, "name": "Charlie"}}, &domain.InsertOptions{})
	require.NoError(t, err)

	// 结构体不支持同步回原始数据
	err = adapter.SyncToOriginal(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "only supported for []map[string]any")
}

func TestSliceFactory_Create(t *testing.T) {
	data := []map[string]any{
		{"id": 1, "name": "Alice"},
	}

	config := &domain.DataSourceConfig{
		Options: map[string]interface{}{
			"data":          data,
			"table_name":    "users",
			"database_name": "testdb",
			"writable":      true,
			"mvcc_supported": true,
		},
	}

	factory := NewFactory()
	ds, err := factory.Create(config)
	require.NoError(t, err)
	require.NotNil(t, ds)

	adapter, ok := ds.(*SliceAdapter)
	require.True(t, ok)
	assert.Equal(t, "users", adapter.GetTableName())
	assert.Equal(t, "testdb", adapter.GetDatabaseName())
}

func TestSliceFactory_Create_InvalidConfig(t *testing.T) {
	factory := NewFactory()

	// 测试空配置
	_, err := factory.Create(nil)
	assert.Error(t, err)

	// 测试缺少 data
	config := &domain.DataSourceConfig{
		Options: map[string]interface{}{
			"table_name": "users",
		},
	}
	_, err = factory.Create(config)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "missing 'data' option")

	// 测试缺少 table_name
	config = &domain.DataSourceConfig{
		Options: map[string]interface{}{
			"data": []map[string]any{},
		},
	}
	_, err = factory.Create(config)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "missing or invalid 'table_name' option")
}

func TestSliceFactory_GetDataSourceType(t *testing.T) {
	factory := NewFactory()
	assert.Equal(t, "slice", string(factory.GetType()))
}

func TestSliceFactory_Description(t *testing.T) {
	factory := NewFactory()
	desc := factory.Description()
	assert.Contains(t, desc, "Slice")
	assert.Contains(t, desc, "map")
	assert.Contains(t, desc, "struct")
}

func TestSliceAdapter_MVCC_Transactions(t *testing.T) {
	data := []map[string]any{
		{"id": 1, "name": "Alice", "balance": 100},
		{"id": 2, "name": "Bob", "balance": 100},
	}

	adapter, err := NewSliceAdapter(&data, "users", "testdb", true, true)
	require.NoError(t, err)
	require.NoError(t, adapter.Connect(context.Background()))

	ctx := context.Background()

	// 开始事务
	txnID, err := adapter.BeginTx(ctx, false)
	require.NoError(t, err)
	assert.Greater(t, txnID, int64(0))

	// 使用带事务ID的上下文
	txnCtx := memory.SetTransactionID(ctx, txnID)

	// 在事务中修改数据
	_, err = adapter.Update(txnCtx, "users", []domain.Filter{
		{Field: "id", Operator: "=", Value: int64(1)},
	}, domain.Row{"id": int64(1), "name": "Alice", "balance": int64(150)}, &domain.UpdateOptions{})
	require.NoError(t, err)

	// 在事务中查询应该看到修改后的数据
	result, err := adapter.Query(txnCtx, "users", &domain.QueryOptions{
		Filters: []domain.Filter{
			{Field: "id", Operator: "=", Value: int64(1)},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, int64(150), result.Rows[0]["balance"])

	// 提交事务
	err = adapter.CommitTx(ctx, txnID)
	require.NoError(t, err)

	// 验证数据已持久化
	result, err = adapter.Query(ctx, "users", &domain.QueryOptions{
		Filters: []domain.Filter{
			{Field: "id", Operator: "=", Value: int64(1)},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, int64(150), result.Rows[0]["balance"])
}

func TestSliceAdapter_MVCC_Rollback(t *testing.T) {
	data := []map[string]any{
		{"id": 1, "name": "Alice", "balance": 100},
	}

	adapter, err := NewSliceAdapter(&data, "users", "testdb", true, true)
	require.NoError(t, err)
	require.NoError(t, adapter.Connect(context.Background()))

	ctx := context.Background()

	// 查询初始数据
	result, err := adapter.Query(ctx, "users", &domain.QueryOptions{})
	require.NoError(t, err)
	initialBalance := result.Rows[0]["balance"]
	// 测试类型断言，可能是int或int64
	switch v := initialBalance.(type) {
	case int64:
		assert.Equal(t, int64(100), v)
	case int:
		assert.Equal(t, 100, v)
	default:
		assert.Equal(t, 100, v)
	}

	// 开始事务
	txnID, err := adapter.BeginTx(ctx, false)
	require.NoError(t, err)

	// 使用带事务ID的上下文
	txnCtx := memory.SetTransactionID(ctx, txnID)

	// 在事务中修改数据
	_, err = adapter.Update(txnCtx, "users", []domain.Filter{
		{Field: "id", Operator: "=", Value: int64(1)},
	}, domain.Row{"id": int64(1), "name": "Alice", "balance": int64(200)}, &domain.UpdateOptions{})
	require.NoError(t, err)

	// 在事务中查询应该看到修改后的数据
	result, err = adapter.Query(txnCtx, "users", &domain.QueryOptions{})
	require.NoError(t, err)
	assert.Equal(t, int64(200), result.Rows[0]["balance"])

	// 回滚事务（丢弃事务快照）
	err = adapter.RollbackTx(ctx, txnID)
	require.NoError(t, err)

	// 回滚后查询，应该看到初始数据
	result, err = adapter.Query(ctx, "users", &domain.QueryOptions{})
	require.NoError(t, err)
	// 回滚后应该看到原来的数据（balance=100）
	balance := result.Rows[0]["balance"]
	switch v := balance.(type) {
	case int64:
		assert.Equal(t, int64(100), v)
	case int:
		assert.Equal(t, 100, v)
	default:
		assert.Equal(t, 100, v)
	}
}

func TestNewSliceAdapter_NilData(t *testing.T) {
	_, err := NewSliceAdapter(nil, "users", "testdb", true, true)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cannot be nil")
}

func TestNewSliceAdapter_InvalidType(t *testing.T) {
	// 传入非切片类型
	data := "not a slice"

	_, err := NewSliceAdapter(data, "users", "testdb", true, true)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "must be a slice")
}

func TestSliceAdapter_PointerSlice(t *testing.T) {
	// 测试指向切片的指针
	data := []map[string]any{
		{"id": 1, "name": "Alice"},
	}

	adapter, err := NewSliceAdapter(&data, "users", "testdb", true, true)
	require.NoError(t, err)
	require.NotNil(t, adapter)

	// 验证数据已加载
	result, err := adapter.Query(context.Background(), "users", &domain.QueryOptions{})
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)
}

func TestNewSliceAdapter_NonPointer_AutoReadOnly(t *testing.T) {
	// 测试非指针自动设为只读
	data := []map[string]any{
		{"id": 1, "name": "Alice"},
	}

	// 传入非指针，writable参数会被忽略并自动设为 false
	adapter, err := NewSliceAdapter(data, "users", "testdb", true, true)
	require.NoError(t, err)
	require.NotNil(t, adapter)

	// 非指针应该自动为不可写
	assert.False(t, adapter.IsWritable())
}

func TestNewSliceAdapter_Pointer_Writable(t *testing.T) {
	// 测试指针允许写
	data := []map[string]any{
		{"id": 1, "name": "Alice"},
	}

	adapter, err := NewSliceAdapter(&data, "users", "testdb", true, true)
	require.NoError(t, err)
	require.NotNil(t, adapter)

	// 指针应该是可写的
	assert.True(t, adapter.IsWritable())
}

func TestSliceAdapter_Struct_NotWritable(t *testing.T) {
	// 测试struct不支持写回
	type User struct {
		ID   int    `json:"id"`
		Name string `json:"name"`
	}

	data := &[]User{
		{ID: 1, Name: "Alice"},
	}

	adapter, err := NewSliceAdapter(data, "users", "testdb", true, true)
	require.NoError(t, err)
	require.NoError(t, adapter.Connect(context.Background()))

	// struct 不支持写回，但可以在内存中修改
	assert.True(t, adapter.IsWritable())

	// SyncToOriginal 应该失败
	err = adapter.SyncToOriginal(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "only supported for []map[string]any")
}
