package main

import (
	"context"
	"fmt"
	"mysql-proxy/mysql/mvcc"
	"mysql-proxy/mysql/resource"
	"time"
)

func main() {
	fmt.Println("=== MVCC 事务测试 ===\n")

	// 创建支持MVCC的内存数据源
	config := &resource.DataSourceConfig{
		Name:     "memory-mvcc-test",
		Writable: true,
	}
	mvccSource := resource.NewMVCCMemorySource(config)

	// 初始化表
	ctx := context.Background()
	tableInfo := &resource.TableInfo{
		Name:   "users",
		Schema: "test",
		Columns: []resource.ColumnInfo{
			{Name: "id", Type: "INT", Primary: true},
			{Name: "name", Type: "VARCHAR"},
			{Name: "age", Type: "INT"},
		},
	}
	err := mvccSource.CreateTable(ctx, tableInfo)
	if err != nil {
		fmt.Printf("创建表失败: %v\n", err)
		return
	}

	// 插入初始数据
	initialData := []resource.Row{
		{"id": 1, "name": "Alice", "age": 25},
		{"id": 2, "name": "Bob", "age": 30},
		{"id": 3, "name": "Charlie", "age": 28},
	}
	_, err = mvccSource.Insert(ctx, "users", initialData, nil)
	if err != nil {
		fmt.Printf("插入初始数据失败: %v\n", err)
		return
	}

	fmt.Println("初始数据:")
	printQueryResult(mvccSource.Query(ctx, "users", nil))

	// 测试1: 读未提交（Read Uncommitted）
	fmt.Println("\n=== 测试1: 读未提交隔离级别 ===")
	testReadUncommitted(mvccSource, ctx)

	// 测试2: 读已提交（Read Committed）
	fmt.Println("\n=== 测试2: 读已提交隔离级别 ===")
	testReadCommitted(mvccSource, ctx)

	// 测试3: 可重复读（Repeatable Read）
	fmt.Println("\n=== 测试3: 可重复读隔离级别 ===")
	testRepeatableRead(mvccSource, ctx)

	// 测试4: 串行化（Serializable）
	fmt.Println("\n=== 测试4: 串行化隔离级别 ===")
	testSerializable(mvccSource, ctx)

	// 测试5: 事务回滚
	fmt.Println("\n=== 测试5: 事务回滚 ===")
	testRollback(mvccSource, ctx)

	// 测试6: 并发事务
	fmt.Println("\n=== 测试6: 并发事务 ===")
	testConcurrentTransactions(mvccSource, ctx)

	fmt.Println("\n=== 所有测试完成 ===")
}

func printQueryResult(result *resource.QueryResult, err error) {
	if err != nil {
		fmt.Printf("查询错误: %v\n", err)
		return
	}
	fmt.Printf("返回 %d 行数据:\n", len(result.Rows))
	for _, row := range result.Rows {
		fmt.Printf("  %v\n", row)
	}
}

func testReadUncommitted(source *resource.MVCCMemorySource, ctx context.Context) {
	// 开始事务1
	xid1, err := source.BeginTransaction(mvcc.ReadUncommitted)
	if err != nil {
		fmt.Printf("开始事务1失败: %v\n", err)
		return
	}
	fmt.Printf("事务1 (XID=%d) 开始\n", xid1)

	// 事务1插入新数据
	newData := []resource.Row{
		{"id": 4, "name": "David", "age": 35},
	}
	_, err = source.InsertWithTransaction(ctx, "users", newData, nil, xid1)
	if err != nil {
		fmt.Printf("事务1插入失败: %v\n", err)
		return
	}
	fmt.Println("事务1插入: {id:4, name:David, age:35} (未提交)")

	// 事务2查询（应该能看到未提交的数据，因为使用读未提交）
	xid2, err := source.BeginTransaction(mvcc.ReadUncommitted)
	if err != nil {
		fmt.Printf("开始事务2失败: %v\n", err)
		return
	}
	fmt.Printf("事务2 (XID=%d) 查询:\n", xid2)

	// 使用非事务查询（读未提交应该能看到所有数据）
	result, err := source.Query(ctx, "users", nil)
	if err != nil {
		fmt.Printf("事务2查询失败: %v\n", err)
	}
	fmt.Printf("  查询到 %d 行数据\n", len(result.Rows))

	// 提交事务1
	err = source.CommitTransaction(xid1)
	if err != nil {
		fmt.Printf("提交事务1失败: %v\n", err)
		return
	}
	fmt.Println("事务1已提交")

	// 回滚事务2
	source.RollbackTransaction(xid2)
}

func testReadCommitted(source *resource.MVCCMemorySource, ctx context.Context) {
	// 开始事务1
	xid1, err := source.BeginTransaction(mvcc.ReadCommitted)
	if err != nil {
		fmt.Printf("开始事务1失败: %v\n", err)
		return
	}
	fmt.Printf("事务1 (XID=%d) 开始\n", xid1)

	// 事务1插入新数据
	newData := []resource.Row{
		{"id": 5, "name": "Eve", "age": 27},
	}
	_, err = source.InsertWithTransaction(ctx, "users", newData, nil, xid1)
	if err != nil {
		fmt.Printf("事务1插入失败: %v\n", err)
		return
	}
	fmt.Println("事务1插入: {id:5, name:Eve, age:27} (未提交)")

	// 事务2查询（读已提交，应该看不到未提交的数据）
	xid2, err := source.BeginTransaction(mvcc.ReadCommitted)
	if err != nil {
		fmt.Printf("开始事务2失败: %v\n", err)
		return
	}
	fmt.Printf("事务2 (XID=%d) 查询:\n", xid2)

	result, err := source.QueryWithTransaction(ctx, "users", nil, xid2)
	if err != nil {
		fmt.Printf("事务2查询失败: %v\n", err)
		return
	}
	fmt.Printf("  查询到 %d 行数据 (应该看不到未提交的id=5)\n", len(result.Rows))

	// 提交事务1
	err = source.CommitTransaction(xid1)
	if err != nil {
		fmt.Printf("提交事务1失败: %v\n", err)
		return
	}
	fmt.Println("事务1已提交")

	// 事务2再次查询（现在应该能看到提交的数据）
	result, err = source.QueryWithTransaction(ctx, "users", nil, xid2)
	if err != nil {
		fmt.Printf("事务2再次查询失败: %v\n", err)
		return
	}
	fmt.Printf("  事务2再次查询到 %d 行数据 (现在应该能看到id=5)\n", len(result.Rows))

	// 提交事务2
	source.CommitTransaction(xid2)
}

func testRepeatableRead(source *resource.MVCCMemorySource, ctx context.Context) {
	// 开始事务1
	xid1, err := source.BeginTransaction(mvcc.RepeatableRead)
	if err != nil {
		fmt.Printf("开始事务1失败: %v\n", err)
		return
	}
	fmt.Printf("事务1 (XID=%d) 开始\n", xid1)

	// 事务1第一次查询
	result1, err := source.QueryWithTransaction(ctx, "users", nil, xid1)
	if err != nil {
		fmt.Printf("事务1第一次查询失败: %v\n", err)
		return
	}
	initialCount := len(result1.Rows)
	fmt.Printf("事务1第一次查询: %d 行数据\n", initialCount)

	// 事务2插入新数据并提交
	xid2, err := source.BeginTransaction(mvcc.ReadCommitted)
	if err != nil {
		fmt.Printf("开始事务2失败: %v\n", err)
		return
	}

	newData := []resource.Row{
		{"id": 6, "name": "Frank", "age": 40},
	}
	source.InsertWithTransaction(ctx, "users", newData, nil, xid2)
	source.CommitTransaction(xid2)
	fmt.Println("事务2插入 {id:6, name:Frank, age:40} 并提交")

	// 事务1再次查询（应该看到相同的数据，可重复读）
	result2, err := source.QueryWithTransaction(ctx, "users", nil, xid1)
	if err != nil {
		fmt.Printf("事务1再次查询失败: %v\n", err)
		return
	}
	secondCount := len(result2.Rows)
	fmt.Printf("事务1第二次查询: %d 行数据 (应该和第一次相同: %d)\n", secondCount, initialCount)

	if initialCount == secondCount {
		fmt.Println("✓ 可重复读测试通过")
	} else {
		fmt.Println("✗ 可重复读测试失败")
	}

	// 提交事务1
	source.CommitTransaction(xid1)
}

func testSerializable(source *resource.MVCCMemorySource, ctx context.Context) {
	// 开始事务1（串行化）
	xid1, err := source.BeginTransaction(mvcc.Serializable)
	if err != nil {
		fmt.Printf("开始事务1失败: %v\n", err)
		return
	}
	fmt.Printf("事务1 (XID=%d) 开始 (串行化)\n", xid1)

	// 事务1查询age > 30的用户
	options := &resource.QueryOptions{
		Filters: []resource.Filter{
			{Field: "age", Operator: ">", Value: 30},
		},
	}

	result1, err := source.QueryWithTransaction(ctx, "users", options, xid1)
	if err != nil {
		fmt.Printf("事务1查询失败: %v\n", err)
		return
	}
	fmt.Printf("事务1查询 age > 30: %d 行数据\n", len(result1.Rows))

	// 事务2插入age=40的用户并提交
	xid2, err := source.BeginTransaction(mvcc.ReadCommitted)
	if err != nil {
		fmt.Printf("开始事务2失败: %v\n", err)
		return
	}

	newData := []resource.Row{
		{"id": 7, "name": "Grace", "age": 40},
	}
	source.InsertWithTransaction(ctx, "users", newData, nil, xid2)
	source.CommitTransaction(xid2)
	fmt.Println("事务2插入 {id:7, name:Grace, age:40} 并提交")

	// 事务1再次查询（应该看到相同的结果，幻读保护）
	result2, err := source.QueryWithTransaction(ctx, "users", options, xid1)
	if err != nil {
		fmt.Printf("事务1再次查询失败: %v\n", err)
		return
	}
	fmt.Printf("事务1再次查询 age > 30: %d 行数据\n", len(result2.Rows))

	// 提交事务1
	source.CommitTransaction(xid1)
	fmt.Println("✓ 串行化测试完成")
}

func testRollback(source *resource.MVCCMemorySource, ctx context.Context) {
	// 查询当前数据
	result, err := source.Query(ctx, "users", nil)
	if err != nil {
		fmt.Printf("查询失败: %v\n", err)
		return
	}
	initialCount := len(result.Rows)
	fmt.Printf("当前数据: %d 行\n", initialCount)

	// 开始事务
	xid, err := source.BeginTransaction(mvcc.ReadCommitted)
	if err != nil {
		fmt.Printf("开始事务失败: %v\n", err)
		return
	}
	fmt.Printf("事务 (XID=%d) 开始\n", xid)

	// 插入新数据
	newData := []resource.Row{
		{"id": 8, "name": "Henry", "age": 33},
		{"id": 9, "name": "Ivy", "age": 29},
	}
	_, err = source.InsertWithTransaction(ctx, "users", newData, nil, xid)
	if err != nil {
		fmt.Printf("插入失败: %v\n", err)
		return
	}
	fmt.Println("事务插入 2 行数据 (未提交)")

	// 事务内查询
	result, err = source.QueryWithTransaction(ctx, "users", nil, xid)
	if err != nil {
		fmt.Printf("事务内查询失败: %v\n", err)
		return
	}
	fmt.Printf("事务内查询: %d 行数据\n", len(result.Rows))

	// 回滚事务
	err = source.RollbackTransaction(xid)
	if err != nil {
		fmt.Printf("回滚事务失败: %v\n", err)
		return
	}
	fmt.Println("事务已回滚")

	// 再次查询（应该回到初始状态）
	result, err = source.Query(ctx, "users", nil)
	if err != nil {
		fmt.Printf("查询失败: %v\n", err)
		return
	}
	finalCount := len(result.Rows)
	fmt.Printf("回滚后数据: %d 行\n", finalCount)

	if initialCount == finalCount {
		fmt.Println("✓ 回滚测试通过")
	} else {
		fmt.Println("✗ 回滚测试失败")
	}
}

func testConcurrentTransactions(source *resource.MVCCMemorySource, ctx context.Context) {
	// 开始多个并发事务
	done := make(chan bool, 3)

	// 事务1: 插入数据
	go func() {
		xid, _ := source.BeginTransaction(mvcc.ReadCommitted)
		time.Sleep(10 * time.Millisecond)
		newData := []resource.Row{
			{"id": 10, "name": "Jack", "age": 45},
		}
		source.InsertWithTransaction(ctx, "users", newData, nil, xid)
		time.Sleep(10 * time.Millisecond)
		source.CommitTransaction(xid)
		fmt.Println("事务1完成: 插入 Jack")
		done <- true
	}()

	// 事务2: 更新数据
	go func() {
		xid, _ := source.BeginTransaction(mvcc.ReadCommitted)
		time.Sleep(5 * time.Millisecond)
		updates := resource.Row{"age": 31}
		source.UpdateWithTransaction(ctx, "users", []resource.Filter{
			{Field: "id", Operator: "=", Value: 2},
		}, updates, nil, xid)
		time.Sleep(15 * time.Millisecond)
		source.CommitTransaction(xid)
		fmt.Println("事务2完成: 更新 Bob的年龄")
		done <- true
	}()

	// 事务3: 删除数据
	go func() {
		xid, _ := source.BeginTransaction(mvcc.ReadCommitted)
		time.Sleep(8 * time.Millisecond)
		source.DeleteWithTransaction(ctx, "users", []resource.Filter{
			{Field: "id", Operator: "=", Value: 1},
		}, nil, xid)
		time.Sleep(12 * time.Millisecond)
		source.CommitTransaction(xid)
		fmt.Println("事务3完成: 删除 Alice")
		done <- true
	}()

	// 等待所有事务完成
	for i := 0; i < 3; i++ {
		<-done
	}

	// 查询最终结果
	time.Sleep(50 * time.Millisecond)
	result, err := source.Query(ctx, "users", nil)
	if err != nil {
		fmt.Printf("查询失败: %v\n", err)
		return
	}

	fmt.Printf("\n最终数据: %d 行\n", len(result.Rows))
	for _, row := range result.Rows {
		fmt.Printf("  %v\n", row)
	}
	fmt.Println("✓ 并发事务测试完成")
}
