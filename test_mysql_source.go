package main

import (
	"context"
	"fmt"
	"mysql-proxy/mysql/resource"
)

func main() {
	fmt.Println("=== MySQL 数据源测试 ===\n")

	// 测试1: 连接池
	fmt.Println("=== 测试1: 连接池 ===")
	testConnectionPool()

	// 测试2: 语句缓存
	fmt.Println("\n=== 测试2: 语句缓存 ===")
	testStatementCache()

	// 测试3: 查询缓存
	fmt.Println("\n=== 测试3: 查询缓存 ===")
	testQueryCache()

	// 测试4: 慢查询日志
	fmt.Println("\n=== 测试4: 慢查询日志 ===")
	testSlowQueryLogger()

	fmt.Println("\n=== 所有测试完成 ===")
}

func testConnectionPool() {
	fmt.Println("创建连接池...")
	pool := resource.NewConnectionPool()

	// 设置参数
	pool.SetMaxOpenConns(5)
	pool.SetMaxIdleConns(2)
	pool.SetConnMaxLifetime(30)
	pool.SetIdleTimeout(5)

	fmt.Println("✓ 连接池创建成功")

	// 模拟连接获取和释放
	for i := 0; i < 3; i++ {
		// 获取连接
		conn, err := pool.Get()
		if err != nil {
			fmt.Printf("  获取连接失败: %v\n", err)
			continue
		}

		fmt.Printf("  [%d] 获取连接成功\n", i+1)

		// 模拟使用
		// (在实际使用中，这里会有数据库操作）

		// 释放连接
		if conn != nil {
			pool.Release(conn)
			fmt.Printf("  [%d] 释放连接成功\n", i+1)
		}
	}

	// 获取统计信息
	stats := pool.Stats()
	fmt.Printf("\n连接池统计:\n")
	fmt.Printf("  最大连接数: %v\n", stats["max_open"])
	fmt.Printf("  最大空闲数: %v\n", stats["max_idle"])
	fmt.Printf("  当前连接数: %v\n", stats["current_open"])
	fmt.Printf("  总创建数: %v\n", stats["total_created"])
	fmt.Printf("  总获取数: %v\n", stats["total_acquired"])
	fmt.Printf("  总释放数: %v\n", stats["total_released"])
	fmt.Printf("  总错误数: %v\n", stats["total_errors"])

	// 关闭连接池
	pool.Close()
	fmt.Println("✓ 连接池已关闭")
}

func testStatementCache() {
	fmt.Println("创建语句缓存...")
	cache := resource.NewStatementCache()

	// 设置缓存大小
	// (内部已设置最大值）

	fmt.Println("✓ 语句缓存创建成功")

	// 模拟语句获取
	queries := []string{
		"SELECT * FROM users WHERE id = ?",
		"SELECT * FROM products WHERE category = ?",
		"INSERT INTO orders (user_id, product_id) VALUES (?, ?)",
		"UPDATE users SET name = ? WHERE id = ?",
		"DELETE FROM temp WHERE created_at < ?",
	}

	for _, query := range queries {
		fmt.Printf("  缓存查询: %s\n", query)
		// 注意：这里不实际执行，因为没有真实的数据库连接
		// 在实际使用中，会通过 Get(conn, query) 方法使用
	}

	// 使某个表的缓存失效
	cache.InvalidateTable("users")
	fmt.Println("\n使 'users' 表的缓存失效")

	// 获取统计信息
	stats := cache.Stats()
	fmt.Printf("\n语句缓存统计:\n")
	fmt.Printf("  缓存大小: %v\n", stats["size"])
	fmt.Printf("  最大大小: %v\n", stats["max_size"])
	fmt.Printf("  总使用数: %v\n", stats["total_uses"])
	fmt.Printf("  命中数: %v\n", stats["hit_count"])
	fmt.Printf("  命中率: %.2f%%\n", stats["hit_rate"].(float64)*100)
	fmt.Printf("  修改的表: %v\n", stats["modified_tables"])

	// 清空缓存
	cache.Clear()
	fmt.Println("\n✓ 语句缓存已清空")
}

func testQueryCache() {
	fmt.Println("创建查询缓存...")
	cache := resource.NewQueryCache()

	// 设置TTL
	// (内部已设置5分钟）

	fmt.Println("✓ 查询缓存创建成功")

	// 模拟查询缓存
	tableName := "users"

	// 创建测试结果
	result1 := &resource.QueryResult{
		Columns: []resource.ColumnInfo{
			{Name: "id", Type: "INT"},
			{Name: "name", Type: "VARCHAR"},
			{Name: "age", Type: "INT"},
		},
		Rows: []resource.Row{
			{"id": 1, "name": "Alice", "age": 25},
			{"id": 2, "name": "Bob", "age": 30},
		},
		Total: 2,
	}

	result2 := &resource.QueryResult{
		Columns: []resource.ColumnInfo{
			{Name: "id", Type: "INT"},
			{Name: "name", Type: "VARCHAR"},
		},
		Rows: []resource.Row{
			{"id": 1, "name": "Alice"},
			{"id": 2, "name": "Bob"},
			{"id": 3, "name": "Charlie"},
		},
		Total: 3,
	}

	query1 := fmt.Sprintf("SELECT * FROM %s WHERE age > 25", tableName)
	query2 := fmt.Sprintf("SELECT * FROM %s", tableName)

	// 缓存查询
	cache.Set(query1, result1)
	fmt.Printf("  缓存查询1: %s\n", query1)

	cache.Set(query2, result2)
	fmt.Printf("  缓存查询2: %s\n", query2)

	// 从缓存获取
	cached1, hit1 := cache.Get(query1)
	fmt.Printf("  获取缓存1: 命中=%v, 行数=%d\n", hit1, len(cached1.Rows))

	cached2, hit2 := cache.Get(query2)
	fmt.Printf("  获取缓存2: 命中=%v, 行数=%d\n", hit2, len(cached2.Rows))

	// 使表缓存失效
	cache.Invalidate(tableName)
	fmt.Println("\n使表的缓存失效")

	// 再次获取（应该未命中）
	cached1, hit1 = cache.Get(query1)
	fmt.Printf("  失效后获取缓存1: 命中=%v, 命中=%v\n", hit1, cached1 != nil)

	// 获取统计信息
	stats := cache.Stats()
	fmt.Printf("\n查询缓存统计:\n")
	fmt.Printf("  缓存大小: %v\n", stats["size"])
	fmt.Printf("  最大大小: %v\n", stats["max_size"])
	fmt.Printf("  TTL: %v\n", stats["ttl"])
	fmt.Printf("  总访问数: %v\n", stats["total_access"])
	fmt.Printf("  命中数: %v\n", stats["hit_count"])
	fmt.Printf("  命中率: %.2f%%\n", stats["hit_rate"].(float64)*100)

	// 清空缓存
	cache.Clear()
	fmt.Println("\n✓ 查询缓存已清空")
}

func testSlowQueryLogger() {
	fmt.Println("创建慢查询日志器...")
	logger := resource.NewSlowQueryLogger()

	// 设置阈值
	logger.SetThreshold(100)

	fmt.Println("✓ 慢查询日志器创建成功")

	// 模拟记录查询
	queries := []struct {
		sql      string
		duration int // 毫秒
	}{
		{"SELECT * FROM users", 50},
		{"SELECT * FROM orders WHERE user_id = 1", 120},
		{"SELECT * FROM products WHERE category = 'electronics'", 200},
		{"SELECT * FROM large_table WHERE status = 'active'", 150},
		{"INSERT INTO logs (message) VALUES (?)", 80},
		{"UPDATE users SET last_login = NOW() WHERE id = 1", 90},
		{"DELETE FROM temp WHERE created_at < DATE_SUB(NOW(), INTERVAL 1 DAY)", 300},
	}

	for _, q := range queries {
		logger.Log(q.sql, resource.Duration(q.duration))
		if q.duration >= 100 {
			fmt.Printf("  [慢查询] %s (%dms)\n", q.sql, q.duration)
		}
	}

	// 获取统计信息
	stats := logger.Stats()
	fmt.Printf("\n慢查询日志统计:\n")
	fmt.Printf("  慢查询数量: %v\n", stats["count"])
	fmt.Printf("  阈值: %v\n", stats["threshold"])
	fmt.Printf("  平均时长: %v\n", stats["avg_duration"])
	fmt.Printf("  最长时长: %v\n", stats["max_duration"])
	fmt.Printf("  总时长: %v\n", stats["total_duration"])

	// 获取日志
	logs := logger.GetLogs()
	fmt.Printf("\n慢查询日志 (%d 条):\n", len(logs))
	for i, log := range logs {
		fmt.Printf("  [%d] %s (%v)\n", i+1, log.Query, log.Duration)
	}

	// 清空日志
	logger.Clear()
	fmt.Println("\n✓ 慢查询日志已清空")
}
