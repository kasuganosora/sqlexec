package main

import (
	"fmt"
	"mysql-proxy/mysql/resource"
	"time"
)

func main() {
	fmt.Println("=== 索引测试 ===\n")

	// 创建索引管理器
	indexMgr := resource.NewIndexManager()

	// 创建测试表
	createTestTable(indexMgr)

	// 测试1: 创建哈希索引
	fmt.Println("=== 测试1: 哈希索引 ===")
	testHashIndex(indexMgr)

	// 测试2: 创建B树索引
	fmt.Println("\n=== 测试2: B树索引 ===")
	testBTreeIndex(indexMgr)

	// 测试3: 索引查找性能对比
	fmt.Println("\n=== 测试3: 索引查找性能对比 ===")
	testIndexPerformance(indexMgr)

	// 测试4: 唯一索引
	fmt.Println("\n=== 测试4: 唯一索引 ===")
	testUniqueIndex(indexMgr)

	// 测试5: 主键索引
	fmt.Println("\n=== 测试5: 主键索引 ===")
	testPrimaryKeyIndex(indexMgr)

	// 测试6: 范围查询
	fmt.Println("\n=== 测试6: 范围查询 ===")
	testRangeQuery(indexMgr)

	fmt.Println("\n=== 所有测试完成 ===")
}

func createTestTable(mgr *resource.IndexManager) {
	// 创建测试数据
	rows := []resource.Row{
		{"id": 1, "name": "Alice", "age": 25, "salary": 50000},
		{"id": 2, "name": "Bob", "age": 30, "salary": 60000},
		{"id": 3, "name": "Charlie", "age": 28, "salary": 55000},
		{"id": 4, "name": "David", "age": 35, "salary": 70000},
		{"id": 5, "name": "Eve", "age": 27, "salary": 52000},
		{"id": 6, "name": "Frank", "age": 40, "salary": 80000},
		{"id": 7, "name": "Grace", "age": 33, "salary": 65000},
		{"id": 8, "name": "Henry", "age": 45, "salary": 90000},
		{"id": 9, "name": "Ivy", "age": 29, "salary": 58000},
		{"id": 10, "name": "Jack", "age": 50, "salary": 100000},
	}

	// 注册表
	mgr.RegisterTable("employees", rows)

	fmt.Printf("创建测试表，包含 %d 行数据\n", len(rows))
	for i, row := range rows {
		fmt.Printf("  [%d] %v\n", i, row)
	}
}

func testHashIndex(mgr *resource.IndexManager) {
	// 创建哈希索引
	hashIndexInfo := resource.NewIndexInfo(
		"idx_name_hash",
		"employees",
		[]string{"name"},
		resource.IndexTypeHash,
		false,
		false,
	)

	err := mgr.CreateIndex("employees", hashIndexInfo)
	if err != nil {
		fmt.Printf("创建哈希索引失败: %v\n", err)
		return
	}

	fmt.Println("✓ 成功创建哈希索引 idx_name_hash")

	// 测试精确查找
	idx, _ := mgr.GetIndex("employees", "idx_name_hash")

	// 查找name = "David"的记录
	indices := idx.Lookup("David")
	if len(indices) > 0 {
		fmt.Printf("  查找 'David': 找到 %d 行\n", len(indices))
	}

	// 查找不存在的记录
	indices = idx.Lookup("NonExistent")
	if len(indices) == 0 {
		fmt.Println("  查找 'NonExistent': 找到 0 行 (正确)")
	}

	// 范围查询（哈希索引不支持）
	start := time.Now()
	indices = idx.RangeLookup(30, 40)
	elapsed := time.Since(start)
	fmt.Printf("  范围查询 age 30-40: 找到 %d 行 (耗时: %v)\n", len(indices), elapsed)
	fmt.Println("  注意: 哈希索引不支持高效的范围查询")
}

func testBTreeIndex(mgr *resource.IndexManager) {
	// 创建B树索引
	btreeIndexInfo := resource.NewIndexInfo(
		"idx_age_btree",
		"employees",
		[]string{"age"},
		resource.IndexTypeBTree,
		false,
		false,
	)

	err := mgr.CreateIndex("employees", btreeIndexInfo)
	if err != nil {
		fmt.Printf("创建B树索引失败: %v\n", err)
		return
	}

	fmt.Println("✓ 成功创建B树索引 idx_age_btree")

	// 测试精确查找
	idx, _ := mgr.GetIndex("employees", "idx_age_btree")

	// 查找age = 30的记录
	indices := idx.Lookup(30)
	if len(indices) > 0 {
		fmt.Printf("  查找 age=30: 找到 %d 行\n", len(indices))
	}

	// 测试范围查询
	start := time.Now()
	indices = idx.RangeLookup(28, 35)
	elapsed := time.Since(start)
	fmt.Printf("  范围查询 age 28-35: 找到 %d 行 (耗时: %v)\n", len(indices), elapsed)

	// 测试范围查询（大范围）
	start = time.Now()
	indices = idx.RangeLookup(25, 50)
	elapsed = time.Since(start)
	fmt.Printf("  范围查询 age 25-50: 找到 %d 行 (耗时: %v)\n", len(indices), elapsed)
}

func testIndexPerformance(mgr *resource.IndexManager) {
	// 创建中等测试数据
	largeRows := make([]resource.Row, 1000)
	for i := 0; i < 1000; i++ {
		largeRows[i] = resource.Row{
			"id":     i + 1,
			"name":   fmt.Sprintf("User%d", i),
			"age":    (i % 50) + 20,
			"salary": 30000 + (i % 70)*1000,
		}
	}

	// 注册大数据表
	mgr.RegisterTable("large_table", largeRows)
	fmt.Printf("创建大数据表，包含 %d 行\n", len(largeRows))

	// 创建索引
	indexInfo := resource.NewIndexInfo(
		"idx_id",
		"large_table",
		[]string{"id"},
		resource.IndexTypeBTree,
		true,
		false,
	)

	err := mgr.CreateIndex("large_table", indexInfo)
	if err != nil {
		fmt.Printf("创建索引失败: %v\n", err)
		return
	}

	fmt.Println("✓ 成功创建B树索引")

	// 测试查找性能
	testId := 500

	// 使用索引查找
	idx, _ := mgr.GetIndex("large_table", "idx_id")
	start := time.Now()
	indices := idx.Lookup(testId)
	elapsed := time.Since(start)
	fmt.Printf("  索引查找 id=%d: 找到 %d 行 (耗时: %v)\n", testId, len(indices), elapsed)

	// 无索引查找（模拟）
	start = time.Now()
	found := false
	for _, row := range largeRows {
		if row["id"] == testId {
			found = true
			break
		}
	}
	elapsed = time.Since(start)
	if found {
		fmt.Printf("  全表扫描查找 id=%d: 找到 (耗时: %v)\n", testId, elapsed)
	}

	// 测试范围查询性能
	start = time.Now()
	indices = idx.RangeLookup(400, 600)
	elapsed = time.Since(start)
	fmt.Printf("  索引范围查询 id 400-600: 找到 %d 行 (耗时: %v)\n", len(indices), elapsed)
}

func testUniqueIndex(mgr *resource.IndexManager) {
	// 创建唯一索引
	uniqueIndexInfo := resource.NewIndexInfo(
		"idx_email_unique",
		"employees",
		[]string{"id"},
		resource.IndexTypeHash,
		true,
		false,
	)

	err := mgr.CreateIndex("employees", uniqueIndexInfo)
	if err != nil {
		fmt.Printf("创建唯一索引失败: %v\n", err)
		return
	}

	fmt.Println("✓ 成功创建唯一索引 idx_email_unique")

	// 查找记录
	idx, _ := mgr.GetIndex("employees", "idx_email_unique")
	indices := idx.Lookup(1)
	fmt.Printf("  查找 id=1: 找到 %d 行\n", len(indices))

	// 测试重复插入（模拟）
	fmt.Println("  唯一索引会确保键的唯一性")
	fmt.Println("  在实际实现中，插入重复键会失败")
}

func testPrimaryKeyIndex(mgr *resource.IndexManager) {
	// 创建主键索引
	pkIndexInfo := resource.NewIndexInfo(
		"pk_id",
		"employees",
		[]string{"id"},
		resource.IndexTypeBTree,
		true,
		true,
	)

	err := mgr.CreateIndex("employees", pkIndexInfo)
	if err != nil {
		fmt.Printf("创建主键索引失败: %v\n", err)
		return
	}

	fmt.Println("✓ 成功创建主键索引 pk_id")

	// 查找记录
	idx, _ := mgr.GetIndex("employees", "pk_id")
	indices := idx.Lookup(5)
	fmt.Printf("  通过主键查找 id=5: 找到 %d 行\n", len(indices))

	// 列出所有索引
	indexes := mgr.ListIndexes("employees")
	fmt.Printf("\n  表 'employees' 的所有索引 (%d 个):\n", len(indexes))
	for _, idxInfo := range indexes {
		primaryStr := ""
		if idxInfo.Primary {
			primaryStr = " [PRIMARY]"
		}
		uniqueStr := ""
		if idxInfo.Unique {
			uniqueStr = " [UNIQUE]"
		}
		fmt.Printf("    - %s (%s)%s%s\n", idxInfo.Name, idxInfo.Type, uniqueStr, primaryStr)
	}
}

func testRangeQuery(mgr *resource.IndexManager) {
	// 获取B树索引
	idx, _ := mgr.GetIndex("employees", "idx_age_btree")

	fmt.Println("测试不同范围的查询:")

	// 范围1: 年轻员工
	ranges := []struct {
		min  interface{}
		max  interface{}
		desc  string
	}{
		{20, 29, "年轻员工 (20-29岁)"},
		{30, 39, "中年员工 (30-39岁)"},
		{40, 50, "资深员工 (40-50岁)"},
		{25, 35, "部分员工 (25-35岁)"},
		{28, 33, "特定年龄范围 (28-33岁)"},
	}

	for _, r := range ranges {
		start := time.Now()
		indices := idx.RangeLookup(r.min, r.max)
		elapsed := time.Since(start)
		fmt.Printf("  %s: 找到 %d 行 (耗时: %v)\n", r.desc, len(indices), elapsed)
	}
}
