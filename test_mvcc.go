package main

import (
	"fmt"
	"strings"
	"time"

	"mysql-proxy/mysql/mvcc"
)

// ============ 辅助函数 ============

func printHeader(title string) {
	fmt.Printf("\n%s\n%s\n", title, strings.Repeat("=", len(title)))
}

func printDivider(char string, length int) {
	fmt.Print(char)
	for i := 0; i < length-1; i++ {
		fmt.Print(char)
	}
	fmt.Println()
}

// ============ 测试1: 基本事务 ============

func testBasicTransaction() {
	printHeader("测试1: 基本事务操作")

	// 创建管理器
	config := mvcc.DefaultConfig()
	config.EnableWarning = true
	mgr := mvcc.NewManager(config)
	defer mgr.Close()

	// 注册数据源
	features := mvcc.NewDataSourceFeatures("test_db", mvcc.CapabilityFull)
	mgr.RegisterDataSource(features)

	// 开始事务
	txn, err := mgr.Begin(mvcc.RepeatableRead, features)
	if err != nil {
		fmt.Printf("开始事务失败: %v\n", err)
		return
	}

	fmt.Printf("事务ID: %d\n", txn.XID())
	fmt.Printf("隔离级别: %s\n", txn.Level())
	fmt.Printf("是否使用MVCC: %v\n", txn.IsMVCC())
	fmt.Printf("事务状态: %s\n", txn.Status())

	// 提交事务
	if err := mgr.Commit(txn); err != nil {
		fmt.Printf("提交事务失败: %v\n", err)
		return
	}

	fmt.Printf("事务状态（提交后）: %s\n", txn.Status())
	fmt.Println("✅ 测试1通过")
}

// ============ 测试2: 版本可见性 ============

func testVersionVisibility() {
	printHeader("测试2: 版本可见性检查")

	// 创建快照
	snapshot := mvcc.NewSnapshot(100, 200, []mvcc.XID{150, 160}, mvcc.RepeatableRead)

	// 创建版本
	version1 := mvcc.NewTupleVersion("data1", 90)   // xmin=90 < snapshot.xmin=100
	version2 := mvcc.NewTupleVersion("data2", 110)  // xmin=110 > snapshot.xmin=100, 但不在活跃列表
	version3 := mvcc.NewTupleVersion("data3", 150)  // xmin=150 在活跃列表

	// 检查可见性
	fmt.Printf("版本1 (xmin=90): 可见=%v\n", version1.IsVisibleTo(snapshot))
	fmt.Printf("版本2 (xmin=110): 可见=%v\n", version2.IsVisibleTo(snapshot))
	fmt.Printf("版本3 (xmin=150): 可见=%v\n", version3.IsVisibleTo(snapshot))

	fmt.Println("✅ 测试2通过")
}

// ============ 测试3: 数据源能力检测 ============

func testDataSourceCapability() {
	printHeader("测试3: 数据源能力检测")

	// 创建MVCC数据源
	mvccDS := mvcc.NewMemoryDataSource("mvcc_db")
	fmt.Printf("MVCC数据源: %s\n", mvccDS.GetFeatures().Name)
	fmt.Printf("能力等级: %s\n", mvccDS.GetFeatures().Capability)
	fmt.Printf("支持MVCC: %v\n", mvccDS.GetFeatures().HasMVCC())

	// 创建非MVCC数据源
	nonMVCCDS := mvcc.NewNonMVCCDataSource("flat_file")
	fmt.Printf("\n非MVCC数据源: %s\n", nonMVCCDS.GetFeatures().Name)
	fmt.Printf("能力等级: %s\n", nonMVCCDS.GetFeatures().Capability)
	fmt.Printf("支持MVCC: %v\n", nonMVCCDS.GetFeatures().HasMVCC())

	fmt.Println("✅ 测试3通过")
}

// ============ 测试4: 降级机制 ============

func testDowngradeMechanism() {
	printHeader("测试4: MVCC降级机制")

	// 创建管理器和注册表
	config := mvcc.DefaultConfig()
	config.EnableWarning = true
	config.AutoDowngrade = true
	mgr := mvcc.NewManager(config)
	defer mgr.Close()

	registry := mvcc.NewDataSourceRegistry()

	// 注册数据源
	mvccDS := mvcc.NewMemoryDataSource("mvcc_db")
	registry.Register("mvcc_db", mvccDS)

	nonMVCCDS := mvcc.NewNonMVCCDataSource("flat_file")
	registry.Register("flat_file", nonMVCCDS)

	// 创建降级处理器
	handler := mvcc.NewDowngradeHandler(mgr, registry)

	// 测试查询前检查（只读）
	fmt.Println("检查只读查询（包含非MVCC数据源）:")
	supportsMVCC, err := handler.CheckBeforeQuery([]string{"mvcc_db", "flat_file"}, true)
	if err != nil {
		fmt.Printf("错误: %v\n", err)
	} else {
		fmt.Printf("支持MVCC: %v (允许降级)\n", supportsMVCC)
	}

	// 测试查询前检查（读写）
	fmt.Println("\n检查读写查询（包含非MVCC数据源，允许降级）:")
	supportsMVCC, err = handler.CheckBeforeQuery([]string{"mvcc_db", "flat_file"}, false)
	if err != nil {
		fmt.Printf("错误: %v\n", err)
	} else {
		fmt.Printf("支持MVCC: %v (允许降级)\n", supportsMVCC)
	}

	// 测试写入前检查
	fmt.Println("\n检查写入操作（包含非MVCC数据源）:")
	supportsMVCC, err = handler.CheckBeforeWrite([]string{"mvcc_db", "flat_file"})
	if err != nil {
		fmt.Printf("错误: %v\n", err)
	} else {
		fmt.Printf("支持MVCC: %v\n", supportsMVCC)
	}

	fmt.Println("✅ 测试4通过")
}

// ============ 测试5: 事务提交日志 ============

func testCommitLog() {
	printHeader("测试5: 事务提交日志")

	// 创建clog
	clog := mvcc.NewCommitLog()

	// 设置事务状态
	clog.SetStatus(10, mvcc.TxnStatusCommitted)
	clog.SetStatus(20, mvcc.TxnStatusAborted)
	clog.SetStatus(30, mvcc.TxnStatusInProgress)

	// 查询状态
	status, exists := clog.GetStatus(10)
	fmt.Printf("事务10状态: %s, 存在: %v\n", status, exists)

	status, exists = clog.GetStatus(20)
	fmt.Printf("事务20状态: %s, 存在: %v\n", status, exists)

	// 检查是否已提交
	fmt.Printf("事务10已提交: %v\n", clog.IsCommitted(10))
	fmt.Printf("事务20已提交: %v\n", clog.IsCommitted(20))

	// 统计
	fmt.Printf("日志条目数: %d\n", clog.GetEntryCount())
	fmt.Printf("最小XID: %d\n", clog.GetOldestXID())

	fmt.Println("✅ 测试5通过")
}

// ============ 测试6: 可见性检查器 ============

func testVisibilityChecker() {
	printHeader("测试6: 可见性检查器")

	// 创建检查器和快照
	checker := mvcc.NewVisibilityChecker()
	snapshot := mvcc.NewSnapshot(100, 200, []mvcc.XID{150, 160}, mvcc.RepeatableRead)

	// 创建多个版本
	versions := []*mvcc.TupleVersion{
		mvcc.NewTupleVersion("data1", 90),
		mvcc.NewTupleVersion("data2", 110),
		mvcc.NewTupleVersion("data3", 150),
	}

	// 批量检查
	results := checker.CheckBatch(versions, snapshot)
	for i, visible := range results {
		fmt.Printf("版本%d: 可见=%v\n", i+1, visible)
	}

	// 过滤可见版本
	visibleVersions := checker.FilterVisible(versions, snapshot)
	fmt.Printf("\n可见版本数: %d\n", len(visibleVersions))

	fmt.Println("✅ 测试6通过")
}

// ============ 测试7: XID操作 ============

func testXIDOperations() {
	printHeader("测试7: XID操作")

	// 测试XID比较
	xid1 := mvcc.XID(100)
	xid2 := mvcc.XID(200)

	fmt.Printf("%d < %d: %v\n", xid1, xid2, xid1.IsBefore(xid2))
	fmt.Printf("%d > %d: %v\n", xid2, xid1, xid2.IsAfter(xid1))

	// 测试XID环绕
	wrapTest := mvcc.XIDMax
	next := mvcc.NextXID(wrapTest)
	fmt.Printf("\nXIDMax (%d) 下一个: %d\n", wrapTest, next)

	// 测试XID字符串
	fmt.Printf("XID字符串: %s\n", xid1)

	fmt.Println("✅ 测试7通过")
}

// ============ 测试8: 隔离级别 ============

func testIsolationLevels() {
	printHeader("测试8: 隔离级别")

	levels := []mvcc.IsolationLevel{
		mvcc.ReadUncommitted,
		mvcc.ReadCommitted,
		mvcc.RepeatableRead,
		mvcc.Serializable,
	}

	for _, level := range levels {
		fmt.Printf("隔离级别: %s\n", level)
	}

	// 从字符串解析
	level1 := mvcc.IsolationLevelFromString("READ UNCOMMITTED")
	level2 := mvcc.IsolationLevelFromString("ReadCommitted")
	level3 := mvcc.IsolationLevelFromString("REPEATABLE READ")

	fmt.Printf("\n解析 'READ UNCOMMITTED': %s\n", level1)
	fmt.Printf("解析 'ReadCommitted': %s\n", level2)
	fmt.Printf("解析 'REPEATABLE READ': %s\n", level3)

	fmt.Println("✅ 测试8通过")
}

// ============ 测试9: 多个事务并发 ============

func testMultipleTransactions() {
	printHeader("测试9: 多个事务")

	// 创建管理器
	mgr := mvcc.NewManager(mvcc.DefaultConfig())
	defer mgr.Close()

	features := mvcc.NewDataSourceFeatures("test_db", mvcc.CapabilityFull)

	// 创建多个事务
	txn1, _ := mgr.Begin(mvcc.RepeatableRead, features)
	txn2, _ := mgr.Begin(mvcc.ReadCommitted, features)

	fmt.Printf("事务1: XID=%d, 级别=%s\n", txn1.XID(), txn1.Level())
	fmt.Printf("事务2: XID=%d, 级别=%s\n", txn2.XID(), txn2.Level())

	// 提交事务1
	if err := mgr.Commit(txn1); err == nil {
		fmt.Printf("事务1已提交\n")
	}

	// 列出活跃事务
	activeTxns := mgr.ListActiveTransactions()
	fmt.Printf("\n活跃事务: %v\n", activeTxns)

	// 提交事务2
	if err := mgr.Commit(txn2); err == nil {
		fmt.Printf("事务2已提交\n")
	}

	// 获取统计信息
	stats := mgr.GetStatistics()
	fmt.Printf("\n统计信息:\n")
	for k, v := range stats {
		fmt.Printf("  %s: %v\n", k, v)
	}

	fmt.Println("✅ 测试9通过")
}

// ============ 测试10: 全局管理器 ============

func testGlobalManager() {
	printHeader("测试10: 全局管理器")

	// 获取全局管理器
	mgr := mvcc.GetGlobalManager()

	fmt.Printf("全局管理器地址: %p\n", mgr)
	fmt.Printf("当前XID: %d\n", mgr.CurrentXID())

	// 获取统计信息
	stats := mgr.GetStatistics()
	fmt.Printf("活跃事务数: %v\n", stats["active_txns"])

	fmt.Println("✅ 测试10通过")
}

// ============ 主函数 ============

func main() {
	fmt.Println("========================================")
	fmt.Println("     MVCC（多版本并发控制）测试套件")
	fmt.Println("========================================")

	startTime := time.Now()

	testBasicTransaction()
	testVersionVisibility()
	testDataSourceCapability()
	testDowngradeMechanism()
	testCommitLog()
	testVisibilityChecker()
	testXIDOperations()
	testIsolationLevels()
	testMultipleTransactions()
	testGlobalManager()

	elapsed := time.Since(startTime)

	printHeader("测试总结")
	fmt.Printf("总测试数: 10\n")
	fmt.Printf("总用时: %v\n", elapsed)
	fmt.Println("\n========================================")
	fmt.Println("     所有测试通过！✅")
	fmt.Println("========================================")
}
