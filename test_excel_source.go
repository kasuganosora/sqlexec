package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"mysql-proxy/mysql/resource"
)

func main() {
	ctx := context.Background()

	// 创建临时文件
	tmpDir := os.TempDir()
	excelPath := filepath.Join(tmpDir, "test_excel.xlsx")

	// 确保测试结束后删除临时文件
	defer os.Remove(excelPath)

	// 如果文件已存在，先删除
	if _, err := os.Stat(excelPath); err == nil {
		os.Remove(excelPath)
	}

	fmt.Println("=== Excel数据源测试 ===\n")

	// 测试1: 创建Excel文件并插入数据
	fmt.Println("=== 测试1: 创建Excel文件并插入数据 ===")
	testCreateAndInsert(ctx, excelPath)

	// 测试2: 读取数据
	fmt.Println("\n=== 测试2: 读取数据 ===")
	testReadData(ctx, excelPath)

	// 测试3: 查询过滤
	fmt.Println("\n=== 测试3: 查询过滤 ===")
	testQueryFilters(ctx, excelPath)

	// 测试4: 排序和分页
	fmt.Println("\n=== 测试4: 排序和分页 ===")
	testSortAndPagination(ctx, excelPath)

	// 测试5: 工作表操作
	fmt.Println("\n=== 测试5: 工作表操作 ===")
	testSheetOperations(ctx, excelPath)

	fmt.Println("\n✅ 所有Excel数据源测试通过!")
}

// testCreateAndInsert 测试创建和插入
func testCreateAndInsert(ctx context.Context, excelPath string) {
	config := resource.DefaultExcelConfig(excelPath)
	config.ReadOnly = false // 需要写入权限

	source := resource.NewExcelSource(config)

	if err := source.Connect(ctx); err != nil {
		log.Fatalf("连接失败: %v", err)
	}

	// 创建表（工作表）
	tableInfo := &resource.TableInfo{
		Name:   "Users",
		Schema: filepath.Base(excelPath),
		Columns: []resource.ColumnInfo{
			{Name: "ID", Type: "STRING", Primary: true},
			{Name: "Name", Type: "STRING", Nullable: false},
			{Name: "Age", Type: "STRING"},
			{Name: "Email", Type: "STRING"},
		},
	}

	// 删除所有现有工作表
	tables, _ := source.GetTables(ctx)
	for _, table := range tables {
		if err := source.DropTable(ctx, table); err != nil {
			log.Printf("删除工作表失败: %v", err)
		}
	}

	if err := source.CreateTable(ctx, tableInfo); err != nil {
		log.Fatalf("创建表失败: %v", err)
	}

	// 插入数据
	rows := []resource.Row{
		{"ID": "1", "Name": "Alice", "Age": "25", "Email": "alice@example.com"},
		{"ID": "2", "Name": "Bob", "Age": "30", "Email": "bob@example.com"},
		{"ID": "3", "Name": "Charlie", "Age": "35", "Email": "charlie@example.com"},
	}

	count, err := source.Insert(ctx, "Users", rows, nil)
	if err != nil {
		log.Fatalf("插入数据失败: %v", err)
	}

	if count != 3 {
		log.Fatalf("插入数量不匹配: expected 3, got %d", count)
	}

	if err := source.Close(ctx); err != nil {
		log.Fatalf("关闭连接失败: %v", err)
	}

	fmt.Println("✅ 创建和插入测试通过")
}

// testReadData 测试读取数据
func testReadData(ctx context.Context, excelPath string) {
	config := resource.DefaultExcelConfig(excelPath)
	source := resource.NewExcelSource(config)

	if err := source.Connect(ctx); err != nil {
		log.Fatalf("连接失败: %v", err)
	}
	defer source.Close(ctx)

	// 查询所有数据
	result, err := source.Query(ctx, "Users", nil)
	if err != nil {
		log.Fatalf("查询失败: %v", err)
	}

	if len(result.Rows) != 3 {
		log.Fatalf("查询结果数量不匹配: expected 3, got %d", len(result.Rows))
	}

	// 获取表信息
	tableInfo, err := source.GetTableInfo(ctx, "Users")
	if err != nil {
		log.Fatalf("获取表信息失败: %v", err)
	}

	if len(tableInfo.Columns) != 4 {
		log.Fatalf("列数量不匹配: expected 4, got %d", len(tableInfo.Columns))
	}

	// 获取所有表（工作表）
	tables, err := source.GetTables(ctx)
	if err != nil {
		log.Fatalf("获取工作表失败: %v", err)
	}

	if len(tables) != 1 {
		log.Fatalf("工作表数量不匹配: expected 1, got %d", len(tables))
	}

	fmt.Println("✅ 读取数据测试通过")
}

// testQueryFilters 测试查询过滤
func testQueryFilters(ctx context.Context, excelPath string) {
	config := resource.DefaultExcelConfig(excelPath)
	source := resource.NewExcelSource(config)

	if err := source.Connect(ctx); err != nil {
		log.Fatalf("连接失败: %v", err)
	}
	defer source.Close(ctx)

	// 测试等值查询
	filters := []resource.Filter{
		{Field: "Name", Operator: "=", Value: "Alice"},
	}

	result, err := source.Query(ctx, "Users", &resource.QueryOptions{Filters: filters})
	if err != nil {
		log.Fatalf("查询失败: %v", err)
	}

	if len(result.Rows) != 1 {
		log.Fatalf("等值查询结果数量不匹配: expected 1, got %d", len(result.Rows))
	}

	// 测试LIKE查询
	filters = []resource.Filter{
		{Field: "Email", Operator: "LIKE", Value: "%@example.com"},
	}

	result, err = source.Query(ctx, "Users", &resource.QueryOptions{Filters: filters})
	if err != nil {
		log.Fatalf("LIKE查询失败: %v", err)
	}

	if len(result.Rows) != 3 {
		log.Fatalf("LIKE查询结果数量不匹配: expected 3, got %d", len(result.Rows))
	}

	// 测试数值比较
	filters = []resource.Filter{
		{Field: "Age", Operator: ">=", Value: "30"},
	}

	result, err = source.Query(ctx, "Users", &resource.QueryOptions{Filters: filters})
	if err != nil {
		log.Fatalf("数值比较查询失败: %v", err)
	}

	if len(result.Rows) != 2 {
		log.Fatalf("数值比较查询结果数量不匹配: expected 2, got %d", len(result.Rows))
	}

	fmt.Println("✅ 查询过滤测试通过")
}

// testSortAndPagination 测试排序和分页
func testSortAndPagination(ctx context.Context, excelPath string) {
	config := resource.DefaultExcelConfig(excelPath)
	source := resource.NewExcelSource(config)

	if err := source.Connect(ctx); err != nil {
		log.Fatalf("连接失败: %v", err)
	}
	defer source.Close(ctx)

	// 测试升序排序
	options := &resource.QueryOptions{
		OrderBy: "Age",
		Order:   "ASC",
	}

	result, err := source.Query(ctx, "Users", options)
	if err != nil {
		log.Fatalf("升序排序查询失败: %v", err)
	}

	if len(result.Rows) == 0 {
		log.Fatalf("排序结果为空")
	}

	// 测试降序排序
	options = &resource.QueryOptions{
		OrderBy: "Age",
		Order:   "DESC",
	}

	result, err = source.Query(ctx, "Users", options)
	if err != nil {
		log.Fatalf("降序排序查询失败: %v", err)
	}

	if len(result.Rows) == 0 {
		log.Fatalf("降序排序结果为空")
	}

	// 测试分页
	options = &resource.QueryOptions{
		Limit:  2,
		Offset: 0,
	}

	result, err = source.Query(ctx, "Users", options)
	if err != nil {
		log.Fatalf("分页查询失败: %v", err)
	}

	if len(result.Rows) != 2 {
		log.Fatalf("分页结果数量不匹配: expected 2, got %d", len(result.Rows))
	}

	fmt.Println("✅ 排序和分页测试通过")
}

// testSheetOperations 测试工作表操作
func testSheetOperations(ctx context.Context, excelPath string) {
	config := resource.DefaultExcelConfig(excelPath)
	config.ReadOnly = false
	source := resource.NewExcelSource(config)

	if err := source.Connect(ctx); err != nil {
		log.Fatalf("连接失败: %v", err)
	}

	// 先删除Users工作表（如果存在）
	if err := source.DropTable(ctx, "Users"); err != nil {
		// 工作表可能不存在，忽略错误
		log.Printf("删除Users工作表: %v", err)
	}

	// 创建新工作表
	newTableInfo := &resource.TableInfo{
		Name:   "Products",
		Schema: filepath.Base(excelPath),
		Columns: []resource.ColumnInfo{
			{Name: "ID", Type: "STRING", Primary: true},
			{Name: "Name", Type: "STRING"},
			{Name: "Price", Type: "STRING"},
		},
	}

	if err := source.CreateTable(ctx, newTableInfo); err != nil {
		log.Fatalf("创建工作表失败: %v", err)
	}

	// 插入数据
	rows := []resource.Row{
		{"ID": "1", "Name": "Product A", "Price": "99.99"},
		{"ID": "2", "Name": "Product B", "Price": "199.99"},
	}

	count, err := source.Insert(ctx, "Products", rows, nil)
	if err != nil {
		log.Fatalf("插入数据失败: %v", err)
	}

	if count != 2 {
		log.Fatalf("插入数量不匹配: expected 2, got %d", count)
	}

	// 测试清空表
	if err := source.TruncateTable(ctx, "Products"); err != nil {
		log.Fatalf("清空表失败: %v", err)
	}

	// 验证清空
	result, err := source.Query(ctx, "Products", nil)
	if err != nil {
		log.Fatalf("查询失败: %v", err)
	}

	if len(result.Rows) != 0 {
		log.Fatalf("清空表失败: expected 0 rows, got %d", len(result.Rows))
	}

	// 测试删除表
	if err := source.DropTable(ctx, "Products"); err != nil {
		log.Fatalf("删除表失败: %v", err)
	}

	if err := source.Close(ctx); err != nil {
		log.Fatalf("关闭连接失败: %v", err)
	}

	// 等待文件写入完成
	time.Sleep(100 * time.Millisecond)

	// 重新连接并验证
	config.ReadOnly = true
	source2 := resource.NewExcelSource(config)
	if err := source2.Connect(ctx); err != nil {
		log.Fatalf("重新连接失败: %v", err)
	}
	defer source2.Close(ctx)

	tables, err := source2.GetTables(ctx)
	if err != nil {
		log.Fatalf("获取工作表失败: %v", err)
	}

	if len(tables) != 1 {
		log.Fatalf("工作表数量不匹配: expected 1 (Users), got %d", len(tables))
	}

	fmt.Println("✅ 工作表操作测试通过")
}
