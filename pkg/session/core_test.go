package session

import (
	"context"
	"fmt"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewCoreSession(t *testing.T) {
	ds := &mockDataSource{}
	sess := NewCoreSession(ds)

	assert.NotNil(t, sess)
	assert.NotNil(t, sess.GetDataSource())
	assert.NotNil(t, sess.GetExecutor())
	assert.NotNil(t, sess.GetAdapter())
	assert.False(t, sess.IsClosed())
}

func TestNewCoreSessionWithDSManager(t *testing.T) {
	ds := &mockDataSource{}
	sess := NewCoreSessionWithDSManager(ds, nil)

	assert.NotNil(t, sess)
	assert.NotNil(t, sess.GetDataSource())
	assert.NotNil(t, sess.GetExecutor())
	assert.NotNil(t, sess.GetAdapter())
	assert.False(t, sess.IsClosed())
}

func TestCoreSession_GetCurrentDB(t *testing.T) {
	ds := &mockDataSource{}
	sess := NewCoreSessionWithDSManager(ds, nil)

	db := sess.GetCurrentDB()
	assert.Equal(t, "", db)
}

func TestCoreSession_AddTempTable(t *testing.T) {
	ds := &mockDataSource{}
	sess := NewCoreSession(ds)

	sess.AddTempTable("temp_table1")
	sess.AddTempTable("temp_table2")

	tempTables := sess.GetTempTables()
	assert.Len(t, tempTables, 2)
	assert.Contains(t, tempTables, "temp_table1")
	assert.Contains(t, tempTables, "temp_table2")
}

func TestCoreSession_RemoveTempTable(t *testing.T) {
	ds := &mockDataSource{}
	sess := NewCoreSession(ds)

	sess.AddTempTable("temp_table1")
	sess.AddTempTable("temp_table2")

	sess.RemoveTempTable("temp_table1")

	tempTables := sess.GetTempTables()
	assert.Len(t, tempTables, 1)
	assert.NotContains(t, tempTables, "temp_table1")
	assert.Contains(t, tempTables, "temp_table2")
}

func TestCoreSession_GetTempTables_Empty(t *testing.T) {
	ds := &mockDataSource{}
	sess := NewCoreSession(ds)

	tempTables := sess.GetTempTables()
	assert.Len(t, tempTables, 0)
}

func TestCoreSession_Close(t *testing.T) {
	ds := &mockDataSource{}
	sess := NewCoreSession(ds)

	err := sess.Close(context.Background())
	assert.NoError(t, err)
	assert.True(t, sess.IsClosed())
}

func TestCoreSession_InTx(t *testing.T) {
	ds := &mockDataSource{}
	sess := NewCoreSession(ds)

	assert.False(t, sess.InTx())
}

func TestCoreSession_BeginTx(t *testing.T) {
	ds := &mockDataSource{}
	sess := NewCoreSession(ds)

	ctx := context.Background()
	txn, err := sess.BeginTx(ctx)

	assert.NoError(t, err)
	assert.NotNil(t, txn)
}

func TestCoreSession_CommitTx(t *testing.T) {
	ds := &mockDataSource{}
	sess := NewCoreSession(ds)

	ctx := context.Background()

	// 开始事务
	_, err := sess.BeginTx(ctx)
	require.NoError(t, err)

	// 提交事务
	err = sess.CommitTx(ctx)
	assert.NoError(t, err)
	assert.False(t, sess.InTx())
}

func TestCoreSession_RollbackTx(t *testing.T) {
	ds := &mockDataSource{}
	sess := NewCoreSession(ds)

	ctx := context.Background()

	// 开始事务
	_, err := sess.BeginTx(ctx)
	require.NoError(t, err)

	// 回滚事务
	err = sess.RollbackTx(ctx)
	assert.NoError(t, err)
	assert.False(t, sess.InTx())
}

func TestCoreSession_TransactionState(t *testing.T) {
	ds := &mockDataSource{}
	sess := NewCoreSession(ds)

	ctx := context.Background()

	// 初始状态：不在事务中
	assert.False(t, sess.InTx())

	// 开始事务
	_, err := sess.BeginTx(ctx)
	require.NoError(t, err)
	assert.True(t, sess.InTx())

	// 提交事务
	err = sess.CommitTx(ctx)
	require.NoError(t, err)
	assert.False(t, sess.InTx())
}

func TestCoreSession_CommitTx_NotInTransaction(t *testing.T) {
	ds := &mockDataSource{}
	sess := NewCoreSession(ds)

	ctx := context.Background()

	// 不在事务中直接提交应该返回错误
	err := sess.CommitTx(ctx)
	assert.Error(t, err)
}

func TestCoreSession_RollbackTx_NotInTransaction(t *testing.T) {
	ds := &mockDataSource{}
	sess := NewCoreSession(ds)

	ctx := context.Background()

	// 不在事务中直接回滚应该返回错误
	err := sess.RollbackTx(ctx)
	assert.Error(t, err)
}

func TestCoreSession_GetDataSource(t *testing.T) {
	ds := &mockDataSource{}
	sess := NewCoreSession(ds)

	retrievedDS := sess.GetDataSource()
	assert.Equal(t, ds, retrievedDS)
}

func TestCoreSession_GetExecutor(t *testing.T) {
	ds := &mockDataSource{}
	sess := NewCoreSession(ds)

	executor := sess.GetExecutor()
	assert.NotNil(t, executor)
}

func TestCoreSession_GetAdapter(t *testing.T) {
	ds := &mockDataSource{}
	sess := NewCoreSession(ds)

	adapter := sess.GetAdapter()
	assert.NotNil(t, adapter)
}

func TestCoreSession_TempTableManagement(t *testing.T) {
	ds := &mockDataSource{}
	sess := NewCoreSession(ds)

	// 添加多个临时表
	sess.AddTempTable("temp1")
	sess.AddTempTable("temp2")
	sess.AddTempTable("temp3")

	// 验证列表
	tempTables := sess.GetTempTables()
	assert.Len(t, tempTables, 3)

	// 移除一个
	sess.RemoveTempTable("temp2")
	tempTables = sess.GetTempTables()
	assert.Len(t, tempTables, 2)
	assert.NotContains(t, tempTables, "temp2")
	assert.Contains(t, tempTables, "temp1")
	assert.Contains(t, tempTables, "temp3")
}

func TestCoreSession_CloseTwice(t *testing.T) {
	ds := &mockDataSource{}
	sess := NewCoreSession(ds)

	ctx := context.Background()

	// 第一次关闭
	err := sess.Close(ctx)
	assert.NoError(t, err)
	assert.True(t, sess.IsClosed())

	// 第二次关闭应该成功（幂等操作）
	err = sess.Close(ctx)
	assert.NoError(t, err) // Close是幂等的，多次调用应该都成功
}

func TestCoreSession_OperationAfterClose(t *testing.T) {
	ds := &mockDataSource{}
	sess := NewCoreSession(ds)

	ctx := context.Background()

	// 关闭会话
	err := sess.Close(ctx)
	require.NoError(t, err)

	// 尝试执行操作应该失败
	_, err = sess.BeginTx(ctx)
	assert.Error(t, err)

	sess.AddTempTable("temp") // 应该被忽略
}

func TestCoreSession_ConcurrentAccess(t *testing.T) {
	ds := &mockDataSource{}
	sess := NewCoreSession(ds)

	// 并发测试
	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func(n int) {
			sess.AddTempTable(fmt.Sprintf("temp%d", n))
			done <- true
		}(i)
	}

	// 等待所有goroutine完成
	for i := 0; i < 10; i++ {
		<-done
	}

	tempTables := sess.GetTempTables()
	assert.Len(t, tempTables, 10)
}

func TestCoreSession_TempTableRemoval_NonExistent(t *testing.T) {
	ds := &mockDataSource{}
	sess := NewCoreSession(ds)

	// 尝试移除不存在的临时表（不应该panic）
	sess.RemoveTempTable("nonexistent")

	tempTables := sess.GetTempTables()
	assert.Len(t, tempTables, 0)
}

// Mock data source for testing
type mockDataSource struct{}

func (m *mockDataSource) Connect(ctx context.Context) error {
	return nil
}

func (m *mockDataSource) Close(ctx context.Context) error {
	return nil
}

func (m *mockDataSource) IsConnected() bool {
	return true
}

func (m *mockDataSource) IsWritable() bool {
	return true
}

func (m *mockDataSource) GetConfig() *domain.DataSourceConfig {
	return &domain.DataSourceConfig{}
}

func (m *mockDataSource) SupportsMVCC() bool {
	return true
}

func (m *mockDataSource) BeginTransaction(ctx context.Context, options *domain.TransactionOptions) (domain.Transaction, error) {
	return &mockTransaction{}, nil
}

func (m *mockDataSource) GetTables(ctx context.Context) ([]string, error) {
	return []string{}, nil
}

func (m *mockDataSource) GetAllTables(ctx context.Context) (map[string][]domain.ColumnInfo, error) {
	return map[string][]domain.ColumnInfo{}, nil
}

func (m *mockDataSource) GetTemporaryTables(ctx context.Context) ([]string, error) {
	return []string{}, nil
}

func (m *mockDataSource) GetTableInfo(ctx context.Context, tableName string) (*domain.TableInfo, error) {
	return nil, nil
}

func (m *mockDataSource) CreateTable(ctx context.Context, schema *domain.TableInfo) error {
	return nil
}

func (m *mockDataSource) DropTable(ctx context.Context, tableName string) error {
	return nil
}

func (m *mockDataSource) TruncateTable(ctx context.Context, tableName string) error {
	return nil
}

func (m *mockDataSource) CreateIndex(ctx context.Context, tableName, columnName string, indexType string, unique bool) error {
	return nil
}

func (m *mockDataSource) DropIndex(ctx context.Context, indexName string) error {
	return nil
}

func (m *mockDataSource) Query(ctx context.Context, tableName string, options *domain.QueryOptions) (*domain.QueryResult, error) {
	return &domain.QueryResult{}, nil
}

func (m *mockDataSource) Insert(ctx context.Context, tableName string, rows []domain.Row, options *domain.InsertOptions) (int64, error) {
	return 0, nil
}

func (m *mockDataSource) Update(ctx context.Context, tableName string, filters []domain.Filter, updates domain.Row, options *domain.UpdateOptions) (int64, error) {
	return 0, nil
}

func (m *mockDataSource) Delete(ctx context.Context, tableName string, filters []domain.Filter, options *domain.DeleteOptions) (int64, error) {
	return 0, nil
}

func (m *mockDataSource) Execute(ctx context.Context, sql string) (*domain.QueryResult, error) {
	return &domain.QueryResult{}, nil
}

type mockTransaction struct{}

func (m *mockTransaction) Commit(ctx context.Context) error {
	return nil
}

func (m *mockTransaction) Rollback(ctx context.Context) error {
	return nil
}

func (m *mockTransaction) Execute(ctx context.Context, sql string) (*domain.QueryResult, error) {
	return &domain.QueryResult{}, nil
}

func (m *mockTransaction) Query(ctx context.Context, tableName string, options *domain.QueryOptions) (*domain.QueryResult, error) {
	return &domain.QueryResult{}, nil
}

func (m *mockTransaction) Insert(ctx context.Context, tableName string, rows []domain.Row, options *domain.InsertOptions) (int64, error) {
	return 0, nil
}

func (m *mockTransaction) Update(ctx context.Context, tableName string, filters []domain.Filter, updates domain.Row, options *domain.UpdateOptions) (int64, error) {
	return 0, nil
}

func (m *mockTransaction) Delete(ctx context.Context, tableName string, filters []domain.Filter, options *domain.DeleteOptions) (int64, error) {
	return 0, nil
}
