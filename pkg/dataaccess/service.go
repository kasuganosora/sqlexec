package dataaccess

import (
	"context"
	"fmt"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// Service 数据访问服务接口
type Service interface {
	// Query 查询数据
	Query(ctx context.Context, tableName string, options *QueryOptions) (*domain.QueryResult, error)

	// Filter 过滤数据（支持下推）
	Filter(ctx context.Context, tableName string, filter domain.Filter, offset, limit int) ([]domain.Row, int64, error)

	// GetTableInfo 获取表信息
	GetTableInfo(ctx context.Context, tableName string) (*domain.TableInfo, error)

	// Insert 插入数据
	Insert(ctx context.Context, tableName string, data map[string]interface{}) error

	// Update 更新数据
	Update(ctx context.Context, tableName string, data map[string]interface{}, where *domain.Filter) error

	// Delete 删除数据
	Delete(ctx context.Context, tableName string, where *domain.Filter) error
}

// InsertData 实现Service的Insert方法
func (s *DataService) Insert(ctx context.Context, tableName string, data map[string]interface{}) error {
	// 转换为Row格式
	row := make(domain.Row)
	for k, v := range data {
		row[k] = v
	}

	// 构建插入选项
	options := &domain.InsertOptions{}

	// 调用数据源的Insert方法
	_, err := s.dataSource.Insert(ctx, tableName, []domain.Row{row}, options)
	if err != nil {
		return fmt.Errorf("insert data failed: %w", err)
	}

	return nil
}

// UpdateData 实现Service的Update方法
func (s *DataService) Update(ctx context.Context, tableName string, data map[string]interface{}, where *domain.Filter) error {
	// 转换为Row格式
	row := make(domain.Row)
	for k, v := range data {
		row[k] = v
	}

	// 构建过滤器
	var filters []domain.Filter
	if where != nil {
		filters = []domain.Filter{*where}
	}

	// 构建更新选项
	options := &domain.UpdateOptions{}

	// 调用数据源的Update方法
	_, err := s.dataSource.Update(ctx, tableName, filters, row, options)
	if err != nil {
		return fmt.Errorf("update data failed: %w", err)
	}

	return nil
}

// DeleteData 实现Service的Delete方法
func (s *DataService) Delete(ctx context.Context, tableName string, where *domain.Filter) error {
	// 构建过滤器
	var filters []domain.Filter
	if where != nil {
		filters = []domain.Filter{*where}
	}

	// 构建删除选项
	options := &domain.DeleteOptions{}

	// 调用数据源的Delete方法
	_, err := s.dataSource.Delete(ctx, tableName, filters, options)
	if err != nil {
		return fmt.Errorf("delete data failed: %w", err)
	}

	return nil
}

// QueryOptions 查询选项
type QueryOptions struct {
	SelectColumns []string
	Filters       []domain.Filter
	Offset        int
	Limit         int
	OrderBy       []string
}

// DataService 数据访问服务实现
type DataService struct {
	dataSource domain.DataSource
	manager    *Manager
	router     *Router
}

// NewDataService 创建数据访问服务
func NewDataService(dataSource domain.DataSource) Service {
	ds := &DataService{
		dataSource: dataSource,
		manager:    NewManager(dataSource),
		router:     NewRouter(),
	}
	// 设置router的manager
	ds.router.SetManager(ds.manager)
	return ds
}

// Query 查询数据
func (s *DataService) Query(ctx context.Context, tableName string, options *QueryOptions) (*domain.QueryResult, error) {
	fmt.Printf("  [DATAACCESS] Query: 表=%s, 列数=%d\n", tableName, len(options.SelectColumns))

	// 通过路由器选择数据源
	ds, err := s.router.Route(tableName)
	if err != nil {
		return nil, fmt.Errorf("route failed: %w", err)
	}

	// 构建查询选项
	queryOptions := &domain.QueryOptions{
		SelectColumns: options.SelectColumns,
		Offset:        options.Offset,
		Limit:         options.Limit,
	}

	// 查询数据
	result, err := ds.Query(ctx, tableName, queryOptions)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}

	return result, nil
}

// Filter 过滤数据
func (s *DataService) Filter(ctx context.Context, tableName string, filter domain.Filter, offset, limit int) ([]domain.Row, int64, error) {
	queryOptions := &domain.QueryOptions{
		Filters: []domain.Filter{filter},
		Offset:  offset,
		Limit:   limit,
	}

	result, err := s.dataSource.Query(ctx, tableName, queryOptions)
	if err != nil {
		return nil, 0, err
	}
	return result.Rows, result.Total, nil
}

// GetTableInfo 获取表信息
func (s *DataService) GetTableInfo(ctx context.Context, tableName string) (*domain.TableInfo, error) {
	return s.dataSource.GetTableInfo(ctx, tableName)
}

// selectColumns 选择指定列
func (s *DataService) selectColumns(result *domain.QueryResult, selectColumns []string, tableInfo *domain.TableInfo) *domain.QueryResult {
	// 如果选择所有列，直接返回
	if len(selectColumns) == 0 {
		return result
	}

	// 构建列映射
	columnMap := make(map[string]bool)
	for _, col := range selectColumns {
		columnMap[col] = true
	}

	// 选择列
	filteredRows := make([]domain.Row, len(result.Rows))
	for i, row := range result.Rows {
		filteredRow := make(domain.Row)
		for colName, colValue := range row {
			if columnMap[colName] {
				filteredRow[colName] = colValue
			}
		}
		filteredRows[i] = filteredRow
	}

	// 构建输出列信息
	outputColumns := make([]domain.ColumnInfo, 0)
	for _, col := range tableInfo.Columns {
		if columnMap[col.Name] {
			outputColumns = append(outputColumns, col)
		}
	}

	return &domain.QueryResult{
		Columns: outputColumns,
		Rows:    filteredRows,
		Total:   result.Total,
	}
}
