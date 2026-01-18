package test

import (
	"testing"

	"github.com/kasuganosora/pkg/mvcc"
)

// TestBasicTransaction 测试基本事务操作
func TestBasicTransaction(t *testing.T) {
	config := mvcc.DefaultConfig()
	config.EnableWarning = true
	mgr := mvcc.NewManager(config)
	defer mgr.Close()

	features := mvcc.NewDataSourceFeatures("test_db", mvcc.CapabilityFull)
	mgr.RegisterDataSource(features)

	txn, err := mgr.Begin(mvcc.RepeatableRead, features)
	if err != nil {
		t.Fatalf("开始事务失败: %v", err)
	}

	if txn.XID() == 0 {
		t.Error("事务ID不应该为0")
	}

	if txn.Level() != mvcc.RepeatableRead {
		t.Errorf("期望隔离级别 %s, 实际 %s", mvcc.RepeatableRead, txn.Level())
	}

	if !txn.IsMVCC() {
		t.Error("应该使用MVCC")
	}

	if err := mgr.Commit(txn); err != nil {
		t.Fatalf("提交事务失败: %v", err)
	}
}

// TestVersionVisibility 测试版本可见性检查
func TestVersionVisibility(t *testing.T) {
	snapshot := mvcc.NewSnapshot(100, 200, []mvcc.XID{150, 160}, mvcc.RepeatableRead)

	version1 := mvcc.NewTupleVersion("data1", 90)
	version2 := mvcc.NewTupleVersion("data2", 110)
	version3 := mvcc.NewTupleVersion("data3", 150)

	// version1.xmin=90 < snapshot.xmin=100, 应该不可见
	if version1.IsVisibleTo(snapshot) {
		t.Error("版本1不应该可见")
	}

	// version2.xmin=110 > snapshot.xmin=100, 且不在活跃列表中, 应该可见
	if !version2.IsVisibleTo(snapshot) {
		t.Error("版本2应该可见")
	}

	// version3.xmin=150 在活跃列表中, 应该不可见
	if version3.IsVisibleTo(snapshot) {
		t.Error("版本3不应该可见")
	}
}

// TestDataSourceCapability 测试数据源能力检测
func TestDataSourceCapability(t *testing.T) {
	mvccDS := mvcc.NewMemoryDataSource("mvcc_db")
	features := mvccDS.GetFeatures()

	if features.Name != "mvcc_db" {
		t.Errorf("期望名称 mvcc_db, 实际 %s", features.Name)
	}

	if !features.HasMVCC() {
		t.Error("MVCC数据源应该支持MVCC")
	}

	nonMVCCDS := mvcc.NewNonMVCCDataSource("flat_file")
	nonMVCCFeatures := nonMVCCDS.GetFeatures()

	if nonMVCCFeatures.Name != "flat_file" {
		t.Errorf("期望名称 flat_file, 实际 %s", nonMVCCFeatures.Name)
	}

	if nonMVCCFeatures.HasMVCC() {
		t.Error("非MVCC数据源不应该支持MVCC")
	}
}

// TestDowngradeMechanism 测试MVCC降级机制
func TestDowngradeMechanism(t *testing.T) {
	config := mvcc.DefaultConfig()
	config.EnableWarning = true
	config.AutoDowngrade = true
	mgr := mvcc.NewManager(config)
	defer mgr.Close()

	registry := mvcc.NewDataSourceRegistry()

	mvccDS := mvcc.NewMemoryDataSource("mvcc_db")
	registry.Register("mvcc_db", mvccDS)

	nonMVCCDS := mvcc.NewNonMVCCDataSource("flat_file")
	registry.Register("flat_file", nonMVCCDS)

	handler := mvcc.NewDowngradeHandler(mgr, registry)

	// 测试只读查询（应该允许降级）
	supportsMVCC, err := handler.CheckBeforeQuery([]string{"mvcc_db", "flat_file"}, true)
	if err != nil {
		t.Errorf("只读查询检查失败: %v", err)
	}

	if !supportsMVCC {
		t.Error("混合数据源的只读查询应该允许降级")
	}

	// 测试读写查询（应该允许降级）
	supportsMVCC, err = handler.CheckBeforeQuery([]string{"mvcc_db", "flat_file"}, false)
	if err != nil {
		t.Errorf("读写查询检查失败: %v", err)
	}

	if !supportsMVCC {
		t.Error("混合数据源的读写查询应该允许降级")
	}

	// 测试写入操作（应该检测到降级）
	supportsMVCC, err = handler.CheckBeforeWrite([]string{"mvcc_db", "flat_file"})
	if err != nil {
		t.Errorf("写入操作检查失败: %v", err)
	}

	if supportsMVCC {
		t.Error("包含非MVCC数据源的写入不应该支持MVCC")
	}
}

// TestCommitLog 测试事务提交日志
func TestCommitLog(t *testing.T) {
	clog := mvcc.NewCommitLog()

	clog.SetStatus(10, mvcc.TxnStatusCommitted)
	clog.SetStatus(20, mvcc.TxnStatusAborted)
	clog.SetStatus(30, mvcc.TxnStatusInProgress)

	status, exists := clog.GetStatus(10)
	if !exists || status != mvcc.TxnStatusCommitted {
		t.Error("事务10应该是已提交状态")
	}

	status, exists = clog.GetStatus(20)
	if !exists || status != mvcc.TxnStatusAborted {
		t.Error("事务20应该是已中止状态")
	}

	if !clog.IsCommitted(10) {
		t.Error("事务10应该已提交")
	}

	if clog.IsCommitted(20) {
		t.Error("事务20不应该已提交")
	}

	if clog.GetEntryCount() != 3 {
		t.Errorf("期望3个日志条目, 实际 %d", clog.GetEntryCount())
	}
}

// TestVisibilityChecker 测试可见性检查器
func TestVisibilityChecker(t *testing.T) {
	checker := mvcc.NewVisibilityChecker()
	snapshot := mvcc.NewSnapshot(100, 200, []mvcc.XID{150, 160}, mvcc.RepeatableRead)

	versions := []*mvcc.TupleVersion{
		mvcc.NewTupleVersion("data1", 90),
		mvcc.NewTupleVersion("data2", 110),
		mvcc.NewTupleVersion("data3", 150),
	}

	results := checker.CheckBatch(versions, snapshot)
	if len(results) != 3 {
		t.Errorf("期望3个结果, 实际 %d", len(results))
	}

	visibleVersions := checker.FilterVisible(versions, snapshot)
	if len(visibleVersions) != 1 {
		t.Errorf("期望1个可见版本, 实际 %d", len(visibleVersions))
	}
}

// TestXIDOperations 测试XID操作
func TestXIDOperations(t *testing.T) {
	xid1 := mvcc.XID(100)
	xid2 := mvcc.XID(200)

	if !xid1.IsBefore(xid2) {
		t.Error("100应该在200之前")
	}

	if !xid2.IsAfter(xid1) {
		t.Error("200应该在100之后")
	}

	wrapTest := mvcc.XIDMax
	next := mvcc.NextXID(wrapTest)
	if next == wrapTest {
		t.Error("XIDMax下一个不应该相等")
	}
}

// TestIsolationLevels 测试隔离级别
func TestIsolationLevels(t *testing.T) {
	levels := []mvcc.IsolationLevel{
		mvcc.ReadUncommitted,
		mvcc.ReadCommitted,
		mvcc.RepeatableRead,
		mvcc.Serializable,
	}

	expectedLevels := []string{
		"READ UNCOMMITTED",
		"READ COMMITTED",
		"REPEATABLE READ",
		"SERIALIZABLE",
	}

	for i, level := range levels {
		if level.String() != expectedLevels[i] {
			t.Errorf("期望 %s, 实际 %s", expectedLevels[i], level.String())
		}
	}

	level1 := mvcc.IsolationLevelFromString("READ UNCOMMITTED")
	if level1 != mvcc.ReadUncommitted {
		t.Errorf("期望 ReadUncommitted, 实际 %v", level1)
	}

	level2 := mvcc.IsolationLevelFromString("ReadCommitted")
	if level2 != mvcc.ReadCommitted {
		t.Errorf("期望 ReadCommitted, 实际 %v", level2)
	}

	level3 := mvcc.IsolationLevelFromString("REPEATABLE READ")
	if level3 != mvcc.RepeatableRead {
		t.Errorf("期望 RepeatableRead, 实际 %v", level3)
	}
}

// TestMultipleTransactions 测试多个事务并发
func TestMultipleTransactions(t *testing.T) {
	mgr := mvcc.NewManager(mvcc.DefaultConfig())
	defer mgr.Close()

	features := mvcc.NewDataSourceFeatures("test_db", mvcc.CapabilityFull)

	txn1, err := mgr.Begin(mvcc.RepeatableRead, features)
	if err != nil {
		t.Fatalf("创建事务1失败: %v", err)
	}

	txn2, err := mgr.Begin(mvcc.ReadCommitted, features)
	if err != nil {
		t.Fatalf("创建事务2失败: %v", err)
	}

	if txn1.XID() == txn2.XID() {
		t.Error("两个事务的XID不应该相等")
	}

	if err := mgr.Commit(txn1); err != nil {
		t.Fatalf("提交事务1失败: %v", err)
	}

	activeTxns := mgr.ListActiveTransactions()
	if len(activeTxns) != 1 {
		t.Errorf("期望1个活跃事务, 实际 %d", len(activeTxns))
	}

	if err := mgr.Commit(txn2); err != nil {
		t.Fatalf("提交事务2失败: %v", err)
	}

	stats := mgr.GetStatistics()
	if stats == nil {
		t.Error("统计信息不应该为空")
	}
}

// TestGlobalManager 测试全局管理器
func TestGlobalManager(t *testing.T) {
	mgr := mvcc.GetGlobalManager()

	if mgr == nil {
		t.Fatal("全局管理器不应该为空")
	}

	currentXID := mgr.CurrentXID()
	if currentXID == 0 {
		t.Error("当前XID不应该为0")
	}

	stats := mgr.GetStatistics()
	if stats == nil {
		t.Error("统计信息不应该为空")
	}
}
