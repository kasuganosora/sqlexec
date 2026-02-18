package dataaccess

import (
	"context"
	"errors"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/stretchr/testify/assert"
)

type ErrorDataSource struct {
	connected bool
}

func (e *ErrorDataSource) Connect(ctx context.Context) error {
	return errors.New("connect error")
}

func (e *ErrorDataSource) Close(ctx context.Context) error {
	return nil
}

func (e *ErrorDataSource) IsConnected() bool {
	return e.connected
}

func (e *ErrorDataSource) IsWritable() bool {
	return true
}

func (e *ErrorDataSource) GetConfig() *domain.DataSourceConfig {
	return &domain.DataSourceConfig{
		Type: domain.DataSourceTypeMemory,
	}
}

func (e *ErrorDataSource) GetTables(ctx context.Context) ([]string, error) {
	return nil, errors.New("get tables error")
}

func (e *ErrorDataSource) GetTableInfo(ctx context.Context, tableName string) (*domain.TableInfo, error) {
	return nil, errors.New("get table info error")
}

func (e *ErrorDataSource) Query(ctx context.Context, tableName string, options *domain.QueryOptions) (*domain.QueryResult, error) {
	return nil, errors.New("query error")
}

func (e *ErrorDataSource) Insert(ctx context.Context, tableName string, rows []domain.Row, options *domain.InsertOptions) (int64, error) {
	return 0, errors.New("insert error")
}

func (e *ErrorDataSource) Update(ctx context.Context, tableName string, filters []domain.Filter, updates domain.Row, options *domain.UpdateOptions) (int64, error) {
	return 0, errors.New("update error")
}

func (e *ErrorDataSource) Delete(ctx context.Context, tableName string, filters []domain.Filter, options *domain.DeleteOptions) (int64, error) {
	return 0, errors.New("delete error")
}

func (e *ErrorDataSource) CreateTable(ctx context.Context, tableInfo *domain.TableInfo) error {
	return errors.New("create table error")
}

func (e *ErrorDataSource) DropTable(ctx context.Context, tableName string) error {
	return errors.New("drop table error")
}

func (e *ErrorDataSource) TruncateTable(ctx context.Context, tableName string) error {
	return errors.New("truncate table error")
}

func (e *ErrorDataSource) Execute(ctx context.Context, sql string) (*domain.QueryResult, error) {
	return nil, errors.New("execute error")
}

func TestDataService_Query_Error(t *testing.T) {
	ds := &ErrorDataSource{}
	service := NewDataService(ds)

	ctx := context.Background()
	options := &QueryOptions{
		SelectColumns: []string{"id"},
	}

	_, err := service.Query(ctx, "test_table", options)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed")
}

func TestDataService_Query_RouteError(t *testing.T) {
	ds := &TestDataSource{}
	service := NewDataService(ds).(*DataService)
	
	// 不初始化 router 的 manager
	service.router = NewRouter()
	
	ctx := context.Background()
	options := &QueryOptions{
		SelectColumns: []string{"id"},
	}
	
	_, err := service.Query(ctx, "test_table", options)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "route failed")
}

func TestDataService_Insert_Error(t *testing.T) {
	ds := &ErrorDataSource{}
	service := NewDataService(ds)
	
	ctx := context.Background()
	data := map[string]interface{}{
		"id": 1,
	}
	
	_, err := service.Insert(ctx, "test_table", data)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "insert data failed")
}

func TestDataService_Update_Error(t *testing.T) {
	ds := &ErrorDataSource{}
	service := NewDataService(ds)
	
	ctx := context.Background()
	data := map[string]interface{}{
		"name": "Updated",
	}
	where := &domain.Filter{
		Field:    "id",
		Operator: "=",
		Value:    1,
	}
	
	err := service.Update(ctx, "test_table", data, where)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "update data failed")
}

func TestDataService_Delete_Error(t *testing.T) {
	ds := &ErrorDataSource{}
	service := NewDataService(ds)
	
	ctx := context.Background()
	where := &domain.Filter{
		Field:    "id",
		Operator: "=",
		Value:    1,
	}
	
	err := service.Delete(ctx, "test_table", where)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "delete data failed")
}

func TestDataService_Filter_Error(t *testing.T) {
	ds := &ErrorDataSource{}
	service := NewDataService(ds)
	
	ctx := context.Background()
	filter := domain.Filter{
		Field:    "id",
		Operator: ">",
		Value:    0,
	}
	
	_, _, err := service.Filter(ctx, "test_table", filter, 0, 10)
	assert.Error(t, err)
}

func TestDataService_GetTableInfo_Error(t *testing.T) {
	ds := &ErrorDataSource{}
	service := NewDataService(ds)
	
	ctx := context.Background()
	
	_, err := service.GetTableInfo(ctx, "test_table")
	assert.Error(t, err)
}
