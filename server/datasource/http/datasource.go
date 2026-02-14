package http

import (
	"context"
	"fmt"
	"sync"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

const dsType = "http"

// HTTPDataSource 实现 domain.DataSource 和 domain.FilterableDataSource
type HTTPDataSource struct {
	mu        sync.RWMutex
	config    *domain.DataSourceConfig
	httpCfg   *HTTPConfig
	client    *HTTPClient
	connected bool
}

// NewHTTPDataSource 创建 HTTP 数据源实例
func NewHTTPDataSource(dsCfg *domain.DataSourceConfig, httpCfg *HTTPConfig) (*HTTPDataSource, error) {
	client, err := NewHTTPClient(dsCfg, httpCfg)
	if err != nil {
		return nil, err
	}

	return &HTTPDataSource{
		config:  dsCfg,
		httpCfg: httpCfg,
		client:  client,
	}, nil
}

// Connect 连接数据源（执行健康检查）
func (ds *HTTPDataSource) Connect(ctx context.Context) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	if err := ds.client.HealthCheck(); err != nil {
		return &domain.ErrConnectionFailed{
			DataSourceType: dsType,
			Reason:         err.Error(),
		}
	}
	ds.connected = true
	return nil
}

// Close 关闭连接
func (ds *HTTPDataSource) Close(ctx context.Context) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	ds.connected = false
	return nil
}

// IsConnected 检查是否已连接
func (ds *HTTPDataSource) IsConnected() bool {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
	return ds.connected
}

// IsWritable 检查是否可写
func (ds *HTTPDataSource) IsWritable() bool {
	return ds.config.Writable
}

// GetConfig 获取数据源配置
func (ds *HTTPDataSource) GetConfig() *domain.DataSourceConfig {
	return ds.config
}

// GetTables 获取所有表名
func (ds *HTTPDataSource) GetTables(ctx context.Context) ([]string, error) {
	if !ds.IsConnected() {
		return nil, domain.NewErrNotConnected(dsType)
	}

	var resp TablesResponse
	if err := ds.client.DoGet(ds.httpCfg.Paths.Tables, "", &resp); err != nil {
		return nil, fmt.Errorf("get tables failed: %w", err)
	}

	// 反向映射：HTTP 表名 → SQL 表名
	if ds.httpCfg.TableAlias != nil {
		reverseAlias := make(map[string]string)
		for sqlName, httpName := range ds.httpCfg.TableAlias {
			reverseAlias[httpName] = sqlName
		}
		result := make([]string, len(resp.Tables))
		for i, t := range resp.Tables {
			if sqlName, ok := reverseAlias[t]; ok {
				result[i] = sqlName
			} else {
				result[i] = t
			}
		}
		return result, nil
	}

	return resp.Tables, nil
}

// GetTableInfo 获取表结构信息
func (ds *HTTPDataSource) GetTableInfo(ctx context.Context, tableName string) (*domain.TableInfo, error) {
	if !ds.IsConnected() {
		return nil, domain.NewErrNotConnected(dsType)
	}

	httpTable := ds.httpCfg.ResolveTableName(tableName)

	var resp SchemaResponse
	if err := ds.client.DoGet(ds.httpCfg.Paths.Schema, httpTable, &resp); err != nil {
		return nil, fmt.Errorf("get table info failed: %w", err)
	}

	return &domain.TableInfo{
		Name:    tableName, // 返回 SQL 表名
		Columns: resp.Columns,
	}, nil
}

// Query 查询数据
func (ds *HTTPDataSource) Query(ctx context.Context, tableName string, options *domain.QueryOptions) (*domain.QueryResult, error) {
	if !ds.IsConnected() {
		return nil, domain.NewErrNotConnected(dsType)
	}

	// ACL 检查
	if options != nil && options.User != "" {
		if err := ds.httpCfg.CheckACL(options.User, "SELECT"); err != nil {
			return nil, err
		}
	}

	httpTable := ds.httpCfg.ResolveTableName(tableName)

	req := &QueryRequest{}
	if options != nil {
		req.Filters = options.Filters
		req.OrderBy = options.OrderBy
		req.Order = options.Order
		req.Limit = options.Limit
		req.Offset = options.Offset
		req.SelectColumns = options.SelectColumns
	}

	var resp QueryResponse
	if err := ds.client.DoPost(ds.httpCfg.Paths.Query, httpTable, req, &resp); err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}

	return &domain.QueryResult{
		Columns: resp.Columns,
		Rows:    resp.Rows,
		Total:   resp.Total,
	}, nil
}

// Insert 插入数据
func (ds *HTTPDataSource) Insert(ctx context.Context, tableName string, rows []domain.Row, options *domain.InsertOptions) (int64, error) {
	if !ds.IsConnected() {
		return 0, domain.NewErrNotConnected(dsType)
	}
	if !ds.config.Writable {
		return 0, domain.NewErrReadOnly(dsType, "INSERT")
	}

	httpTable := ds.httpCfg.ResolveTableName(tableName)

	req := &InsertRequest{
		Rows:    rows,
		Options: options,
	}

	var resp MutationResponse
	if err := ds.client.DoPost(ds.httpCfg.Paths.Insert, httpTable, req, &resp); err != nil {
		return 0, fmt.Errorf("insert failed: %w", err)
	}

	return resp.Affected, nil
}

// Update 更新数据
func (ds *HTTPDataSource) Update(ctx context.Context, tableName string, filters []domain.Filter, updates domain.Row, options *domain.UpdateOptions) (int64, error) {
	if !ds.IsConnected() {
		return 0, domain.NewErrNotConnected(dsType)
	}
	if !ds.config.Writable {
		return 0, domain.NewErrReadOnly(dsType, "UPDATE")
	}

	httpTable := ds.httpCfg.ResolveTableName(tableName)

	req := &UpdateRequest{
		Filters: filters,
		Updates: updates,
		Options: options,
	}

	var resp MutationResponse
	if err := ds.client.DoPost(ds.httpCfg.Paths.Update, httpTable, req, &resp); err != nil {
		return 0, fmt.Errorf("update failed: %w", err)
	}

	return resp.Affected, nil
}

// Delete 删除数据
func (ds *HTTPDataSource) Delete(ctx context.Context, tableName string, filters []domain.Filter, options *domain.DeleteOptions) (int64, error) {
	if !ds.IsConnected() {
		return 0, domain.NewErrNotConnected(dsType)
	}
	if !ds.config.Writable {
		return 0, domain.NewErrReadOnly(dsType, "DELETE")
	}

	httpTable := ds.httpCfg.ResolveTableName(tableName)

	req := &DeleteRequest{
		Filters: filters,
		Options: options,
	}

	var resp MutationResponse
	if err := ds.client.DoPost(ds.httpCfg.Paths.Delete, httpTable, req, &resp); err != nil {
		return 0, fmt.Errorf("delete failed: %w", err)
	}

	return resp.Affected, nil
}

// CreateTable HTTP 数据源不支持 DDL
func (ds *HTTPDataSource) CreateTable(ctx context.Context, tableInfo *domain.TableInfo) error {
	return domain.NewErrUnsupportedOperation(dsType, "CREATE TABLE")
}

// DropTable HTTP 数据源不支持 DDL
func (ds *HTTPDataSource) DropTable(ctx context.Context, tableName string) error {
	return domain.NewErrUnsupportedOperation(dsType, "DROP TABLE")
}

// TruncateTable HTTP 数据源不支持 DDL
func (ds *HTTPDataSource) TruncateTable(ctx context.Context, tableName string) error {
	return domain.NewErrUnsupportedOperation(dsType, "TRUNCATE TABLE")
}

// Execute HTTP 数据源不支持原始 SQL
func (ds *HTTPDataSource) Execute(ctx context.Context, sql string) (*domain.QueryResult, error) {
	return nil, domain.NewErrUnsupportedOperation(dsType, "EXECUTE")
}

// ── FilterableDataSource 接口 ──

// SupportsFiltering 总是返回 true，HTTP 数据源天然支持过滤下推
func (ds *HTTPDataSource) SupportsFiltering(tableName string) bool {
	return true
}

// Filter 执行过滤和分页操作（直接转发给 HTTP 端点）
func (ds *HTTPDataSource) Filter(ctx context.Context, tableName string, filter domain.Filter, offset, limit int) ([]domain.Row, int64, error) {
	if !ds.IsConnected() {
		return nil, 0, domain.NewErrNotConnected(dsType)
	}

	httpTable := ds.httpCfg.ResolveTableName(tableName)

	req := &QueryRequest{
		Offset: offset,
		Limit:  limit,
	}

	// 将单个 filter 转换为 filters 列表
	if filter.Field != "" {
		req.Filters = []domain.Filter{filter}
	} else if filter.Logic != "" && len(filter.SubFilters) > 0 {
		req.Filters = []domain.Filter{filter}
	}

	var resp QueryResponse
	if err := ds.client.DoPost(ds.httpCfg.Paths.Query, httpTable, req, &resp); err != nil {
		return nil, 0, fmt.Errorf("filter failed: %w", err)
	}

	return resp.Rows, resp.Total, nil
}

// GetDatabaseName 返回此 HTTP 数据源在 SQL 中的数据库名
func (ds *HTTPDataSource) GetDatabaseName() string {
	return ds.httpCfg.Database
}
