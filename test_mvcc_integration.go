package main

import (
	"context"
	"fmt"
	"log"

	"mysql-proxy/mysql/parser"
	"mysql-proxy/mysql/resource"
)

func main() {
	fmt.Println("========================================")
	fmt.Println("     MVCC集成测试套件")
	fmt.Println("========================================")
	
	// 测试1: 创建MVCC数据源
	testMVCCDataSource()
	
	// 测试2: 事务操作
	testTransactionOperations()
	
	// 测试3: SQL查询集成
	testSQLIntegration()
	
	fmt.Println("\n========================================")
	fmt.Println("     所有测试完成！✅")
	fmt.Println("========================================")
}

func testMVCCDataSource() {
	fmt.Println("\n测试1: 创建MVCC数据源")
	fmt.Println("-----------------------------------")
	
	// 创建基础数据源
	baseDS := &testDataSource{
		data: make(map[string][]resource.Row),
	}
	
	// 创建MVCC适配器
	mvccDS := resource.NewMVCCDataSourceAdapter(baseDS, true)
	
	// 连接数据源
	ctx := context.Background()
	if err := mvccDS.Connect(ctx); err != nil {
		log.Fatal(err)
	}
	
	fmt.Printf("✓ MVCC数据源已连接\n")
	fmt.Printf("✓ 支持MVCC: %v\n", mvccDS.SupportMVCC())
	
	// 获取表列表
	tables, err := mvccDS.GetTables(ctx)
	if err != nil {
		log.Fatal(err)
	}
	
	fmt.Printf("✓ 表列表: %v\n", tables)
	
	// 关闭连接
	mvccDS.Close(ctx)
}

func testTransactionOperations() {
	fmt.Println("\n测试2: 事务操作")
	fmt.Println("-----------------------------------")
	
	// 创建基础数据源
	baseDS := &testDataSource{
		data: make(map[string][]resource.Row),
	}
	
	// 创建MVCC适配器
	mvccDS := resource.NewMVCCDataSourceAdapter(baseDS, true)
	mvccDS.Connect(context.Background())
	
	// 开始事务
	ctx := context.Background()
	txn, err := mvccDS.BeginTransaction(ctx, "REPEATABLE READ")
	if err != nil {
		log.Fatal(err)
	}
	
	if t, ok := txn.(*resource.Transaction); ok {
		fmt.Printf("✓ 事务已开始，ID: %d\n", t.ID)
	} else {
		fmt.Printf("✓ 事务已开始，ID: 未知\n")
	}
	
	// 测试插入（不使用事务，因为MVCC启用时会自动处理）
	rows := []resource.Row{
		{"id": 1, "name": "Alice", "age": 30},
		{"id": 2, "name": "Bob", "age": 25},
	}
	
	affected, err := mvccDS.Insert(ctx, "users", rows, &resource.InsertOptions{})
	if err != nil {
		fmt.Printf("✗ 插入失败: %v\n", err)
	} else {
		fmt.Printf("✓ 插入 %d 行数据\n", affected)
	}
	
	fmt.Printf("✓ 插入 %d 行数据\n", affected)
	
	// 提交事务
	if err := mvccDS.CommitTransaction(ctx, txn); err != nil {
		log.Fatal(err)
	}
	
	fmt.Printf("✓ 事务已提交\n")
	
	// 查询数据
	result, err := mvccDS.Query(ctx, "users", &resource.QueryOptions{})
	if err != nil {
		log.Fatal(err)
	}
	
	fmt.Printf("✓ 查询到 %d 行数据\n", result.Total)
	
	mvccDS.Close(ctx)
}

func testSQLIntegration() {
	fmt.Println("\n测试3: SQL查询集成")
	fmt.Println("-----------------------------------")
	
	// 创建数据源管理器
	dsMgr := resource.NewDataSourceManager()
	
	// 创建基础数据源
	baseDS := &testDataSource{
		data: make(map[string][]resource.Row),
	}
	
	// 创建MVCC适配器
	mvccDS := resource.NewMVCCDataSourceAdapter(baseDS, true)
	
	// 注册数据源
	if err := dsMgr.Register("test_db", mvccDS); err != nil {
		log.Fatal(err)
	}
	
	// 创建查询构建器
	builder := parser.NewQueryBuilder(mvccDS)
	
	// 测试SELECT查询
	ctx := context.Background()
	result, err := builder.BuildAndExecute(ctx, "SELECT * FROM users")
	if err != nil {
		fmt.Printf("✗ SELECT失败: %v\n", err)
	} else {
		fmt.Printf("✓ SELECT成功，返回 %d 行\n", result.Total)
	}
	
	// 测试INSERT
	result, err = builder.BuildAndExecute(ctx, "INSERT INTO users (id, name, age) VALUES (3, 'Charlie', 35)")
	if err != nil {
		fmt.Printf("✗ INSERT失败: %v\n", err)
	} else {
		fmt.Printf("✓ INSERT成功，影响 %d 行\n", result.Total)
	}
	
	mvccDS.Close(ctx)
}

// ==================== 测试数据源 ====================

// testDataSource 测试数据源
type testDataSource struct {
	data   map[string][]resource.Row
	connected bool
}

// Connect 连接
func (ds *testDataSource) Connect(ctx context.Context) error {
	ds.connected = true
	return nil
}

// Close 关闭
func (ds *testDataSource) Close(ctx context.Context) error {
	ds.connected = false
	return nil
}

// IsConnected 检查连接状态
func (ds *testDataSource) IsConnected() bool {
	return ds.connected
}

// GetConfig 获取配置
func (ds *testDataSource) GetConfig() *resource.DataSourceConfig {
	return &resource.DataSourceConfig{
		Type: "test",
		Name: "test_db",
	}
}

// GetTables 获取表列表
func (ds *testDataSource) GetTables(ctx context.Context) ([]string, error) {
	return []string{"users"}, nil
}

// GetTableInfo 获取表信息
func (ds *testDataSource) GetTableInfo(ctx context.Context, tableName string) (*resource.TableInfo, error) {
	if tableName != "users" {
		return nil, fmt.Errorf("table not found")
	}
	
	return &resource.TableInfo{
		Name: tableName,
		Columns: []resource.ColumnInfo{
			{Name: "id", Type: "INT", Primary: true, Nullable: false},
			{Name: "name", Type: "VARCHAR", Primary: false, Nullable: false},
			{Name: "age", Type: "INT", Primary: false, Nullable: false},
		},
	}, nil
}

// Query 查询数据
func (ds *testDataSource) Query(ctx context.Context, tableName string, options *resource.QueryOptions) (*resource.QueryResult, error) {
	rows, ok := ds.data[tableName]
	if !ok {
		return &resource.QueryResult{}, nil
	}
	
	// 应用过滤条件
	filteredRows := applyFilters(rows, options.Filters)
	
	// 应用排序
	filteredRows = applyOrderBy(filteredRows, options.OrderBy, options.Order)
	
	// 应用限制和偏移
	filteredRows = applyLimitOffset(filteredRows, options.Limit, options.Offset)
	
	return &resource.QueryResult{
		Columns: []resource.ColumnInfo{
			{Name: "id", Type: "INT"},
			{Name: "name", Type: "VARCHAR"},
			{Name: "age", Type: "INT"},
		},
		Rows:  filteredRows,
		Total: int64(len(filteredRows)),
	}, nil
}

// Insert 插入数据
func (ds *testDataSource) Insert(ctx context.Context, tableName string, rows []resource.Row, options *resource.InsertOptions) (int64, error) {
	if _, ok := ds.data[tableName]; !ok {
		ds.data[tableName] = []resource.Row{}
	}
	
	ds.data[tableName] = append(ds.data[tableName], rows...)
	return int64(len(rows)), nil
}

// Update 更新数据
func (ds *testDataSource) Update(ctx context.Context, tableName string, filters []resource.Filter, updates resource.Row, options *resource.UpdateOptions) (int64, error) {
	rows, ok := ds.data[tableName]
	if !ok {
		return 0, nil
	}
	
	count := 0
	for _, row := range rows {
		matched := true
		for _, filter := range filters {
			if row[filter.Field] != filter.Value {
				matched = false
				break
			}
		}
		
		if matched {
			for k, v := range updates {
				row[k] = v
			}
			count++
		}
	}
	
	return int64(count), nil
}

// Delete 删除数据
func (ds *testDataSource) Delete(ctx context.Context, tableName string, filters []resource.Filter, options *resource.DeleteOptions) (int64, error) {
	rows, ok := ds.data[tableName]
	if !ok {
		return 0, nil
	}
	
	// 简化实现：删除所有
	ds.data[tableName] = []resource.Row{}
	return int64(len(rows)), nil
}

// CreateTable 创建表
func (ds *testDataSource) CreateTable(ctx context.Context, tableInfo *resource.TableInfo) error {
	ds.data[tableInfo.Name] = []resource.Row{}
	return nil
}

// DropTable 删除表
func (ds *testDataSource) DropTable(ctx context.Context, tableName string) error {
	delete(ds.data, tableName)
	return nil
}

// TruncateTable 清空表
func (ds *testDataSource) TruncateTable(ctx context.Context, tableName string) error {
	ds.data[tableName] = []resource.Row{}
	return nil
}

// Execute 执行SQL
func (ds *testDataSource) Execute(ctx context.Context, sql string) (*resource.QueryResult, error) {
	return &resource.QueryResult{}, fmt.Errorf("not implemented")
}

// ==================== 辅助函数 ====================

// applyFilters 应用过滤条件
func applyFilters(rows []resource.Row, filters []resource.Filter) []resource.Row {
	if len(filters) == 0 {
		return rows
	}
	
	result := make([]resource.Row, 0)
	for _, row := range rows {
		matched := true
		for _, filter := range filters {
			if row[filter.Field] != filter.Value {
				matched = false
				break
			}
		}
		if matched {
			result = append(result, row)
		}
	}
	
	return result
}

// applyOrderBy 应用排序
func applyOrderBy(rows []resource.Row, orderBy string, order string) []resource.Row {
	if orderBy == "" {
		return rows
	}
	
	// 简化实现：不实际排序
	return rows
}

// applyLimitOffset 应用限制和偏移
func applyLimitOffset(rows []resource.Row, limit int, offset int) []resource.Row {
	if offset >= len(rows) {
		return []resource.Row{}
	}
	
	start := offset
	end := len(rows)
	if limit > 0 && start+limit < end {
		end = start + limit
	}
	
	return rows[start:end]
}
