package main

import (
	"fmt"
	"log"
	"time"

	"mysql-proxy/mysql/mvcc"
)

func main() {
	fmt.Println("========== MVCC (PostgreSQL风格) 测试 ==========\n")

	// 测试1: 基础MVCC功能
	test1_BasicMVCC()

	// 测试2: 事务隔离级别
	test2_IsolationLevels()

	// 测试3: 数据源能力检测
	test3_DataSourceCapability()

	// 测试4: MVCC降级
	test4_MVCCDowngrade()

	// 测试5: 版本可见性
	test5_VersionVisibility()

	// 测试6: 事务冲突
	test6_TransactionConflict()

	fmt.Println("\n========== 所有MVCC测试完成 ==========")
}

// ============ 测试1: 基础MVCC功能 ============
func test1_BasicMVCC() {
	fmt.Println("=== 测试1: 基础MVCC功能 ===")

	// 创建MVCC管理器
	config := mvcc.DefaultConfig()
	mgr := mvcc.NewManager(config)

	// 创建内存数据源
	dataSource := mvcc.NewMemoryDataSource("test_db")

	// 开始事务
	txn, err := mgr.Begin(mvcc.RepeatableRead, dataSource.GetFeatures())
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("事务ID: %d\n", txn.XID())
	fmt.Printf("隔离级别: %s\n", mvcc.IsolationLevelToString(txn.Level()))
	fmt.Printf("快照: %s\n", txn.Snapshot())
	fmt.Printf("是否使用MVCC: %v\n", txn.IsMVCC())

	// 提交事务
	if err := mgr.Commit(txn); err != nil {
		log.Fatal(err)
	}

	// 检查事务状态
	status, _ := mgr.GetTransactionStatus(txn.XID())
	fmt.Printf("事务状态: %s\n\n", mvcc.StatusToString(status))
}

// ============ 测试2: 事务隔离级别 ============
func test2_IsolationLevels() {
	fmt.Println("=== 测试2: 事务隔离级别 ===")

	levels := []mvcc.IsolationLevel{
		mvcc.ReadUncommitted,
		mvcc.ReadCommitted,
		mvcc.RepeatableRead,
		mvcc.Serializable,
	}

	dataSource := mvcc.NewMemoryDataSource("test_db")

	for _, level := range levels {
		mgr := mvcc.NewManager(mvcc.DefaultConfig())
		txn, _ := mgr.Begin(level, dataSource.GetFeatures())

		fmt.Printf("%s: 快照xmin=%d, xmax=%d, 活跃事务数=%d\n",
			mvcc.IsolationLevelToString(level),
			txn.Snapshot().Xmin(),
			txn.Snapshot().Xmax(),
			len(txn.Snapshot().XIP()))

		mgr.Commit(txn)
	}

	fmt.Println()
}

// ============ 测试3: 数据源能力检测 ============
func test3_DataSourceCapability() {
	fmt.Println("=== 测试3: 数据源能力检测 ===")

	// 创建数据源注册表
	registry := mvcc.NewDataSourceRegistry()

	// 注册内存数据源 (支持MVCC)
	memoryDS := mvcc.NewMemoryDataSource("memory_db")
	registry.Register("memory_db", memoryDS.GetFeatures())

	// 注册非MVCC数据源
	nonMVCCDS := mvcc.NewNonMVCCDataSource("flat_file_db")
	registry.Register("flat_file_db", nonMVCCDS.GetFeatures())

	// 列出所有数据源
	fmt.Println("已注册的数据源:")
	for _, features := range registry.List() {
		fmt.Printf("  - %s: 能力=%d, 读=%v, 写=%v\n",
			features.Name, features.Capability, features.SupportsRead, features.SupportsWrite)
	}

	// 检查MVCC支持
	fmt.Println("\n检查MVCC支持:")
	sources := []string{"memory_db", "flat_file_db"}
	allSupported, unsupported := registry.CheckMVCCSupport(sources...)
	fmt.Printf("  完全支持: %v\n", allSupported)
	fmt.Printf("  不支持的数据源: %v\n", unsupported)

	fmt.Println()
}

// ============ 测试4: MVCC降级 ============
func test4_MVCCDowngrade() {
	fmt.Println("=== 测试4: MVCC降级 ===")

	// 创建MVCC管理器 (启用自动降级)
	config := mvcc.DefaultConfig()
	config.EnableWarning = true
	config.AutoDowngrade = true
	mgr := mvcc.NewManager(config)

	// 创建降级处理器
	registry := mvcc.NewDataSourceRegistry()
	registry.Register("memory_db", mvcc.NewMemoryDataSource("memory_db").GetFeatures())
	registry.Register("flat_file_db", mvcc.NewNonMVCCDataSource("flat_file_db").GetFeatures())

	handler := mvcc.NewDowngradeHandler(mgr, registry)

	// 测试1: 全部支持MVCC
	fmt.Println("测试1: 全部支持MVCC的数据源")
	sources1 := []string{"memory_db"}
	_, err := handler.CheckBeforeQuery(sources1, false)
	if err == nil {
		fmt.Println("  ✓ 查询成功")
	} else {
		fmt.Printf("  ✗ 查询失败: %v\n", err)
	}

	// 测试2: 混合数据源
	fmt.Println("\n测试2: 混合数据源 (部分不支持MVCC)")
	sources2 := []string{"memory_db", "flat_file_db"}
	_, err = handler.CheckBeforeQuery(sources2, true) // 只读操作
	if err == nil {
		fmt.Println("  ✓ 只读查询成功 (可以降级)")
	} else {
		fmt.Printf("  ✗ 查询失败: %v\n", err)
	}

	// 测试3: 写入操作
	fmt.Println("\n测试3: 写入操作 (要求MVCC支持)")
	_, err = handler.CheckBeforeWrite(sources2)
	if err == nil {
		fmt.Println("  ✓ 写入检查通过 (将降级执行)")
	} else {
		fmt.Printf("  ✗ 写入检查失败: %v\n", err)
	}

	// 测试4: 禁用自动降级
	fmt.Println("\n测试4: 禁用自动降级")
	handler.Disable()
	_, err = handler.CheckBeforeWrite(sources2)
	if err != nil {
		fmt.Printf("  ✓ 正确返回错误: %v\n", err)
	}

	fmt.Println()
}

// ============ 测试5: 版本可见性 ============
func test5_VersionVisibility() {
	fmt.Println("=== 测试5: 版本可见性 ===")

	// 创建数据源
	dataSource := mvcc.NewMemoryDataSource("test_db")
	clog := mvcc.NewTransactionLogStore()

	// 创建可见性检查器
	checker := mvcc.NewVisibilityChecker(clog)

	// 模拟多个版本
	key := "user:1"
	xid1 := mvcc.XID(1)
	xid2 := mvcc.XID(2)
	xid3 := mvcc.XID(3)

	// 创建版本
	version1 := mvcc.NewTupleVersion("Alice", xid1, key)
	version2 := mvcc.NewTupleVersion("Bob", xid2, key)
	version3 := mvcc.NewTupleVersion("Charlie", xid3, key)

	// 标记version1为已删除
	version1.MarkDeleted(xid2, 0)

	// 记录事务状态
	clog.Log(xid1, mvcc.TxnStatusCommitted)
	clog.Log(xid2, mvcc.TxnStatusCommitted)
	clog.Log(xid3, mvcc.TxnStatusCommitted)

	// 创建快照 (在xid3之后)
	snapshot := mvcc.NewSnapshot(1, xid3, []mvcc.XID{}, mvcc.RepeatableRead)

	fmt.Printf("快照: %s\n", snapshot)
	fmt.Println()

	// 检查可见性
	fmt.Println("版本可见性:")
	fmt.Printf("  version1 (xmin=%d, xmax=%d): %v\n",
		version1.Xmin, version1.Xmax, checker.IsVisible(version1, snapshot))
	fmt.Printf("  version2 (xmin=%d, xmax=%d): %v\n",
		version2.Xmin, version2.Xmax, checker.IsVisible(version2, snapshot))
	fmt.Printf("  version3 (xmin=%d, xmax=%d): %v\n",
		version3.Xmin, version3.Xmax, checker.IsVisible(version3, snapshot))

	fmt.Println()
}

// ============ 测试6: 事务冲突 ============
func test6_TransactionConflict() {
	fmt.Println("=== 测试6: 事务冲突 ===")

	// 创建MVCC管理器
	mgr := mvcc.NewManager(mvcc.DefaultConfig())
	dataSource := mvcc.NewMemoryDataSource("test_db")

	// 事务1: 写入用户
	txn1, _ := mgr.Begin(mvcc.RepeatableRead, dataSource.GetFeatures())
	fmt.Printf("事务1 (xid=%d) 开始\n", txn1.XID())

	err := txn1.Write("user:1", "Alice")
	if err != nil {
		log.Fatal(err)
	}

	// 事务2: 尝试写入同一用户
	txn2, _ := mgr.Begin(mvcc.RepeatableRead, dataSource.GetFeatures())
	fmt.Printf("事务2 (xid=%d) 开始\n", txn2.XID())

	err = txn2.Write("user:1", "Bob")
	if err != nil {
		// 预期会检测到冲突（当前实现简化版）
		fmt.Printf("事务2检测到冲突: %v\n", err)
	}

	// 提交事务1
	err = mgr.Commit(txn1)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("事务1 已提交\n")

	// 尝试提交事务2
	err = mgr.Commit(txn2)
	if err != nil {
		fmt.Printf("事务2 提交失败: %v\n", err)
	} else {
		fmt.Printf("事务2 已提交\n")
	}

	fmt.Println()
}

// ============ 辅助函数 ============

func printHeader(title string) {
	fmt.Printf("\n%s\n%s\n", title, "===============================================================================")
}

func printDivider(char string, length int) {
	fmt.Print(char)
	for i := 0; i < length-1; i++ {
		fmt.Print(char)
	}
	fmt.Println()
}
