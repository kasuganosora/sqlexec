package resource

import (
	"context"
	"fmt"
	"testing"
)

// ExampleDataSourceUsage 演示数据源的使用方法
func ExampleDataSourceUsage() {
	ctx := context.Background()

	// 创建内存数据�?
	memoryConfig := &DataSourceConfig{
		Type: DataSourceTypeMemory,
		Name: "test_memory",
	}
	
	memoryDS, err := CreateDataSource(memoryConfig)
	if err != nil {
		fmt.Println("创建数据源失�?", err)
		return
	}
	
	// 连接数据�?
	if err := memoryDS.Connect(ctx); err != nil {
		fmt.Println("连接数据源失�?", err)
		return
	}
	defer memoryDS.Close(ctx)
	
	// 创建�?
	tableInfo := &TableInfo{
		Name: "users",
		Columns: []ColumnInfo{
			{Name: "id", Type: "int", Nullable: false, Primary: true},
			{Name: "name", Type: "varchar", Nullable: false},
			{Name: "email", Type: "varchar", Nullable: false},
			{Name: "age", Type: "int", Nullable: true},
		},
	}
	
	if err := memoryDS.CreateTable(ctx, tableInfo); err != nil {
		fmt.Println("创建表失�?", err)
		return
	}
	
	// 插入数据
	rows := []Row{
		{"name": "Alice", "email": "alice@example.com", "age": 25},
		{"name": "Bob", "email": "bob@example.com", "age": 30},
		{"name": "Charlie", "email": "charlie@example.com", "age": 35},
	}
	
	inserted, err := memoryDS.Insert(ctx, "users", rows, nil)
	if err != nil {
		fmt.Println("插入数据失败:", err)
		return
	}
	fmt.Printf("插入�?%d 行数据\n", inserted)
	
	// 查询数据
	result, err := memoryDS.Query(ctx, "users", &QueryOptions{
		Filters: []Filter{
			{Field: "age", Operator: ">=", Value: 30},
		},
		OrderBy: "age",
		Order:   "ASC",
	})
	if err != nil {
		fmt.Println("查询数据失败:", err)
		return
	}
	
	fmt.Printf("查询�?%d 行数�?\n", len(result.Rows))
	for _, row := range result.Rows {
		fmt.Printf("  ID: %v, Name: %v, Email: %v, Age: %v\n",
			row["id"], row["name"], row["email"], row["age"])
	}
	
	// 更新数据
	updates := Row{"age": 31}
	updated, err := memoryDS.Update(ctx, "users",
		[]Filter{{Field: "name", Operator: "=", Value: "Bob"}},
		updates, nil)
	if err != nil {
		fmt.Println("更新数据失败:", err)
		return
	}
	fmt.Printf("更新�?%d 行数据\n", updated)
	
	// 删除数据
	deleted, err := memoryDS.Delete(ctx, "users",
		[]Filter{{Field: "age", Operator: "<", Value: 30}},
		nil)
	if err != nil {
		fmt.Println("删除数据失败:", err)
		return
	}
	fmt.Printf("删除�?%d 行数据\n", deleted)
	
	// 输出: 
	// 插入�?3 行数�?
	// 查询�?2 行数�?
	//   ID: 2, Name: Bob, Email: bob@example.com, Age: 30
	//   ID: 3, Name: Charlie, Email: charlie@example.com, Age: 35
	// 更新�?1 行数�?
	// 删除�?1 行数�?
}

// ExampleDataSourceManagerUsage 演示数据源管理器的使用方�?
func ExampleDataSourceManagerUsage() {
	ctx := context.Background()
	
	// 创建数据源管理器
	manager := NewDataSourceManager()
	
	// 创建并注册内存数据源
	memoryConfig := &DataSourceConfig{
		Type: DataSourceTypeMemory,
		Name: "memory_db",
	}
	
	if err := manager.CreateAndRegister(ctx, "memory", memoryConfig); err != nil {
		fmt.Println("注册数据源失�?", err)
		return
	}
	defer manager.CloseAll(ctx)
	
	// 创建并注册另一个内存数据源
	memoryConfig2 := &DataSourceConfig{
		Type: DataSourceTypeMemory,
		Name: "cache_db",
	}
	
	if err := manager.CreateAndRegister(ctx, "cache", memoryConfig2); err != nil {
		fmt.Println("注册数据源失�?", err)
		return
	}
	
	// 设置默认数据�?
	if err := manager.SetDefault("memory"); err != nil {
		fmt.Println("设置默认数据源失�?", err)
		return
	}
	
	// 列出所有数据源
	sources := manager.List()
	fmt.Println("已注册的数据�?", sources)
	
	// 获取数据源状�?
	status := manager.GetStatus()
	for name, connected := range status {
		fmt.Printf("  %s: %v\n", name, connected)
	}
	
	// 获取默认数据源名�?
	defaultDS := manager.GetDefaultName()
	fmt.Println("默认数据�?", defaultDS)
	
	// 输出:
	// 已注册的数据�? [memory cache]
	//   memory: true
	//   cache: true
	// 默认数据�? memory
}

// ExampleMySQLDataSourceUsage 演示MySQL数据源的使用方法
func ExampleMySQLDataSourceUsage() {
	ctx := context.Background()
	
	// 创建MySQL数据源配�?
	mysqlConfig := &DataSourceConfig{
		Type:     DataSourceTypeMySQL,
		Name:     "production_db",
		Host:     "localhost",
		Port:     3306,
		Username: "root",
		Password: "password",
		Database: "test",
	}
	
	// 创建MySQL数据�?
	mysqlDS, err := CreateDataSource(mysqlConfig)
	if err != nil {
		fmt.Println("创建MySQL数据源失�?", err)
		return
	}
	
	// 连接MySQL数据�?
	if err := mysqlDS.Connect(ctx); err != nil {
		fmt.Println("连接MySQL失败:", err)
		return
	}
	defer mysqlDS.Close(ctx)
	
	// 获取所有表
	tables, err := mysqlDS.GetTables(ctx)
	if err != nil {
		fmt.Println("获取表列表失�?", err)
		return
	}
	
	fmt.Println("数据库中的表:", tables)
	
	// 获取表信�?
	if len(tables) > 0 {
		tableInfo, err := mysqlDS.GetTableInfo(ctx, tables[0])
		if err != nil {
			fmt.Println("获取表信息失�?", err)
			return
		}
		
		fmt.Printf("�?%s 的信�?\n", tableInfo.Name)
		for _, col := range tableInfo.Columns {
			fmt.Printf("  %s: %s%s\n", col.Name, col.Type,
				map[bool]string{true: " (主键)", false: ""}[col.Primary])
		}
	}
	
	// 查询数据
	result, err := mysqlDS.Query(ctx, "users", &QueryOptions{
		Filters: []Filter{
			{Field: "age", Operator: ">=", Value: 18},
		},
		Limit: 10,
	})
	if err != nil {
		fmt.Println("查询数据失败:", err)
		return
	}
	
	fmt.Printf("查询�?%d 行数据\n", len(result.Rows))
	
	// 插入数据
	newUser := Row{
		"name":  "New User",
		"email": "newuser@example.com",
		"age":   25,
	}
	
	inserted, err := mysqlDS.Insert(ctx, "users", []Row{newUser}, nil)
	if err != nil {
		fmt.Println("插入数据失败:", err)
		return
	}
	fmt.Printf("插入�?%d 行数据\n", inserted)
	
	// 更新数据
	updated, err := mysqlDS.Update(ctx, "users",
		[]Filter{{Field: "name", Operator: "=", Value: "New User"}},
		Row{"email": "updated@example.com"}, nil)
	if err != nil {
		fmt.Println("更新数据失败:", err)
		return
	}
	fmt.Printf("更新�?%d 行数据\n", updated)
	
	// 删除数据
	deleted, err := mysqlDS.Delete(ctx, "users",
		[]Filter{{Field: "name", Operator: "=", Value: "New User"}},
		nil)
	if err != nil {
		fmt.Println("删除数据失败:", err)
		return
	}
	fmt.Printf("删除�?%d 行数据\n", deleted)
	
	// 执行自定义SQL
	queryResult, err := mysqlDS.Execute(ctx, "SELECT COUNT(*) as count FROM users")
	if err != nil {
		fmt.Println("执行SQL失败:", err)
		return
	}
	
	if len(queryResult.Rows) > 0 {
		fmt.Printf("用户总数: %v\n", queryResult.Rows[0]["count"])
	}
	
	// 输出示例（实际输出取决于数据库内容）:
	// 数据库中的表: [users products orders]
	// �?users 的信�?
	//   id: int (主键)
	//   name: varchar
	//   email: varchar
	//   age: int
	// 查询�?5 行数�?
	// 插入�?1 行数�?
	// 更新�?1 行数�?
	// 删除�?1 行数�?
	// 用户总数: 5
}

// TestMemoryDataSource 测试内存数据�?
func TestMemoryDataSource(t *testing.T) {
	ctx := context.Background()
	
	// 创建数据�?
	config := &DataSourceConfig{
		Type: DataSourceTypeMemory,
		Name: "test",
	}
	
	ds, err := CreateDataSource(config)
	if err != nil {
		t.Fatal("创建数据源失�?", err)
	}
	
	if err := ds.Connect(ctx); err != nil {
		t.Fatal("连接数据源失�?", err)
	}
	defer ds.Close(ctx)
	
	// 测试连接状�?
	if !ds.IsConnected() {
		t.Fatal("数据源未连接")
	}
	
	// 创建�?
	tableInfo := &TableInfo{
		Name: "test_table",
		Columns: []ColumnInfo{
			{Name: "id", Type: "int", Nullable: false, Primary: true},
			{Name: "value", Type: "varchar", Nullable: false},
		},
	}
	
	if err := ds.CreateTable(ctx, tableInfo); err != nil {
		t.Fatal("创建表失�?", err)
	}
	
	// 测试获取表列�?
	tables, err := ds.GetTables(ctx)
	if err != nil {
		t.Fatal("获取表列表失�?", err)
	}
	
	if len(tables) != 1 || tables[0] != "test_table" {
		t.Fatal("表列表不正确")
	}
	
	// 测试插入数据
	rows := []Row{
		{"value": "test1"},
		{"value": "test2"},
	}
	
	inserted, err := ds.Insert(ctx, "test_table", rows, nil)
	if err != nil {
		t.Fatal("插入数据失败:", err)
	}
	
	if inserted != 2 {
		t.Fatalf("期望插入2�? 实际插入%d�?, inserted)
	}
	
	// 测试查询数据
	result, err := ds.Query(ctx, "test_table", nil)
	if err != nil {
		t.Fatal("查询数据失败:", err)
	}
	
	if len(result.Rows) != 2 {
		t.Fatalf("期望查询�?�? 实际查询�?d�?, len(result.Rows))
	}
	
	// 测试更新数据
	updated, err := ds.Update(ctx, "test_table",
		[]Filter{{Field: "id", Operator: "=", Value: 1}},
		Row{"value": "updated1"}, nil)
	if err != nil {
		t.Fatal("更新数据失败:", err)
	}
	
	if updated != 1 {
		t.Fatalf("期望更新1�? 实际更新%d�?, updated)
	}
	
	// 测试删除数据
	deleted, err := ds.Delete(ctx, "test_table",
		[]Filter{{Field: "id", Operator: "=", Value: 1}},
		nil)
	if err != nil {
		t.Fatal("删除数据失败:", err)
	}
	
	if deleted != 1 {
		t.Fatalf("期望删除1�? 实际删除%d�?, deleted)
	}
	
	// 测试删除�?
	if err := ds.DropTable(ctx, "test_table"); err != nil {
		t.Fatal("删除表失�?", err)
	}
}

// TestDataSourceManager 测试数据源管理器
func TestDataSourceManager(t *testing.T) {
	ctx := context.Background()
	
	manager := NewDataSourceManager()
	
	// 测试创建并注册数据源
	config := &DataSourceConfig{
		Type: DataSourceTypeMemory,
		Name: "test_db",
	}
	
	if err := manager.CreateAndRegister(ctx, "test", config); err != nil {
		t.Fatal("创建并注册数据源失败:", err)
	}
	
	// 测试列出数据�?
	sources := manager.List()
	if len(sources) != 1 || sources[0] != "test" {
		t.Fatal("数据源列表不正确")
	}
	
	// 测试获取数据�?
	ds, err := manager.Get("test")
	if err != nil {
		t.Fatal("获取数据源失�?", err)
	}
	
	if !ds.IsConnected() {
		t.Fatal("数据源未连接")
	}
	
	// 测试设置默认数据�?
	if err := manager.SetDefault("test"); err != nil {
		t.Fatal("设置默认数据源失�?", err)
	}
	
	defaultDS, err := manager.GetDefault()
	if err != nil {
		t.Fatal("获取默认数据源失�?", err)
	}
	
	if defaultDS != ds {
		t.Fatal("默认数据源不正确")
	}
	
	// 测试获取状�?
	status := manager.GetStatus()
	if !status["test"] {
		t.Fatal("数据源状态不正确")
	}
	
	// 测试注销数据�?
	if err := manager.Unregister("test"); err != nil {
		t.Fatal("注销数据源失�?", err)
	}
	
	sources = manager.List()
	if len(sources) != 0 {
		t.Fatal("注销后数据源列表应该为空")
	}
}

// TestSupportedDataSourceTypes 测试支持的数据源类型
func TestSupportedDataSourceTypes(t *testing.T) {
	types := GetSupportedTypes()
	
	if len(types) == 0 {
		t.Fatal("没有支持的数据源类型")
	}
	
	// 检查是否包含内存数据源
	hasMemory := false
	for _, typ := range types {
		if typ == DataSourceTypeMemory {
			hasMemory = true
			break
		}
	}
	
	if !hasMemory {
		t.Fatal("应该支持内存数据源类�?)
	}
}

// TestDataSourceFactory 测试数据源工�?
func TestDataSourceFactory(t *testing.T) {
	factory := NewMemoryFactory()
	
	// 测试GetType
	if factory.GetType() != DataSourceTypeMemory {
		t.Fatal("工厂类型不正�?)
	}
	
	// 测试Create
	config := &DataSourceConfig{
		Type: DataSourceTypeMemory,
		Name: "test",
	}
	
	ds, err := factory.Create(config)
	if err != nil {
		t.Fatal("创建数据源失�?", err)
	}
	
	if ds == nil {
		t.Fatal("创建的数据源为nil")
	}
}
