package dataaccess

import (
	"context"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type TestDataSource struct {
	connected bool
}

func (t *TestDataSource) Connect(ctx context.Context) error {
	t.connected = true
	return nil
}

func (t *TestDataSource) Close(ctx context.Context) error {
	t.connected = false
	return nil
}

func (t *TestDataSource) IsConnected() bool {
	return t.connected
}

func (t *TestDataSource) IsWritable() bool {
	return true
}

func (t *TestDataSource) GetConfig() *domain.DataSourceConfig {
	return &domain.DataSourceConfig{
		Type: domain.DataSourceTypeMemory,
	}
}

func (t *TestDataSource) GetTables(ctx context.Context) ([]string, error) {
	return []string{"table1", "table2"}, nil
}

func (t *TestDataSource) GetTableInfo(ctx context.Context, tableName string) (*domain.TableInfo, error) {
	return &domain.TableInfo{
		Name: tableName,
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int"},
		},
	}, nil
}

func (t *TestDataSource) Query(ctx context.Context, tableName string, options *domain.QueryOptions) (*domain.QueryResult, error) {
	return &domain.QueryResult{
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int"},
		},
		Rows: []domain.Row{
			{"id": 1},
		},
	}, nil
}

func (t *TestDataSource) Insert(ctx context.Context, tableName string, rows []domain.Row, options *domain.InsertOptions) (int64, error) {
	return 1, nil
}

func (t *TestDataSource) Update(ctx context.Context, tableName string, filters []domain.Filter, updates domain.Row, options *domain.UpdateOptions) (int64, error) {
	return 1, nil
}

func (t *TestDataSource) Delete(ctx context.Context, tableName string, filters []domain.Filter, options *domain.DeleteOptions) (int64, error) {
	return 1, nil
}

func (t *TestDataSource) CreateTable(ctx context.Context, tableInfo *domain.TableInfo) error {
	return nil
}

func (t *TestDataSource) DropTable(ctx context.Context, tableName string) error {
	return nil
}

func (t *TestDataSource) TruncateTable(ctx context.Context, tableName string) error {
	return nil
}

func (t *TestDataSource) Execute(ctx context.Context, sql string) (*domain.QueryResult, error) {
	return nil, nil
}

func TestNewManager(t *testing.T) {
	ds := &TestDataSource{}
	manager := NewManager(ds)

	assert.NotNil(t, manager)
	assert.NotNil(t, manager.dataSources)
	assert.NotNil(t, manager.connections)
}

func TestManager_RegisterDataSource(t *testing.T) {
	ds := &TestDataSource{}
	manager := NewManager(ds)

	newDs := &TestDataSource{}
	err := manager.RegisterDataSource("new_source", newDs)
	require.NoError(t, err)

	retrievedDs, err := manager.GetDataSource("new_source")
	require.NoError(t, err)
	assert.Equal(t, newDs, retrievedDs)
}

func TestManager_RegisterDataSource_Duplicate(t *testing.T) {
	ds := &TestDataSource{}
	manager := NewManager(ds)

	err := manager.RegisterDataSource("default", &TestDataSource{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "already registered")
}

func TestManager_GetDataSource_NotFound(t *testing.T) {
	ds := &TestDataSource{}
	manager := NewManager(ds)

	_, err := manager.GetDataSource("nonexistent")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestManager_AcquireConnection(t *testing.T) {
	ds := &TestDataSource{}
	manager := NewManager(ds)

	err := manager.AcquireConnection("conn1")
	require.NoError(t, err)

	err = manager.AcquireConnection("conn1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "already acquired")
}

func TestManager_ReleaseConnection(t *testing.T) {
	ds := &TestDataSource{}
	manager := NewManager(ds)

	manager.AcquireConnection("conn1")
	manager.ReleaseConnection("conn1")

	err := manager.AcquireConnection("conn1")
	assert.NoError(t, err)
}

func TestManager_GetStats(t *testing.T) {
	ds := &TestDataSource{}
	manager := NewManager(ds)

	stats := manager.GetStats()
	assert.NotNil(t, stats)
	assert.Equal(t, 1, stats["data_sources"])
	assert.Equal(t, 0, stats["connections"])
}

func TestManager_HealthCheck(t *testing.T) {
	ds := &TestDataSource{}
	manager := NewManager(ds)

	ctx := context.Background()
	health := manager.HealthCheck(ctx)

	assert.NotNil(t, health)
	assert.Contains(t, health, "default")
}

func TestManager_RegisterDataSource_Multiple(t *testing.T) {
	ds := &TestDataSource{}
	manager := NewManager(ds)

	ds1 := &TestDataSource{}
	ds2 := &TestDataSource{}
	ds3 := &TestDataSource{}

	err1 := manager.RegisterDataSource("source1", ds1)
	err2 := manager.RegisterDataSource("source2", ds2)
	err3 := manager.RegisterDataSource("source3", ds3)

	require.NoError(t, err1)
	require.NoError(t, err2)
	require.NoError(t, err3)

	stats := manager.GetStats()
	assert.Equal(t, 4, stats["data_sources"]) // default + 3 new
}

func TestManager_AcquireRelease_Multiple(t *testing.T) {
	ds := &TestDataSource{}
	manager := NewManager(ds)

	// Acquire multiple connections
	_ = manager.AcquireConnection("conn1")
	_ = manager.AcquireConnection("conn2")
	_ = manager.AcquireConnection("conn3")

	stats := manager.GetStats()
	assert.Equal(t, 3, stats["connections"])

	// Release all
	manager.ReleaseConnection("conn1")
	manager.ReleaseConnection("conn2")
	manager.ReleaseConnection("conn3")

	stats = manager.GetStats()
	assert.Equal(t, 0, stats["connections"])
}
