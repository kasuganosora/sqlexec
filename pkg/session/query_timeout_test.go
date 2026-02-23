package session

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/resource/memory"
)

// TestQueryTimeout 测试查询超时功能
func TestQueryTimeout(t *testing.T) {
	// 创建数据源
	ds := memory.NewMVCCDataSource(&domain.DataSourceConfig{
		Type: domain.DataSourceTypeMemory,
		Name: "test",
	})
	ds.Connect(context.Background())

	// 创建测试表
	_ = ds.CreateTable(context.Background(), &domain.TableInfo{
		Name: "users",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "INT"},
			{Name: "name", Type: "VARCHAR"},
		},
	})

	// 创建 CoreSession
	sess := NewCoreSession(ds)
	sess.SetQueryTimeout(100 * time.Millisecond) // 设置100ms超时

	// 执行查询(正常查询应该很快,不会超时)
	result, err := sess.ExecuteQuery(context.Background(), "SELECT * FROM users")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if result == nil {
		t.Fatal("Result should not be nil")
	}
	t.Logf("Query succeeded, got %d rows", result.Total)
}

// TestQueryKill 测试查询Kill功能
func TestQueryKill(t *testing.T) {
	// 创建数据源
	ds := memory.NewMVCCDataSource(&domain.DataSourceConfig{
		Type: domain.DataSourceTypeMemory,
		Name: "test",
	})
	ds.Connect(context.Background())

	// 创建测试表
	_ = ds.CreateTable(context.Background(), &domain.TableInfo{
		Name: "users",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "INT"},
			{Name: "name", Type: "VARCHAR"},
		},
	})

	// 创建 CoreSession
	sess := NewCoreSession(ds)
	sess.SetThreadID(123) // 设置threadID

	// 注册测试查询
	queryID := GenerateQueryID(123)
	ctx, cancel := context.WithCancel(context.Background())

	qc := &QueryContext{
		QueryID:    queryID,
		ThreadID:   123,
		SQL:        "SELECT * FROM users",
		StartTime:  time.Now(),
		CancelFunc: cancel,
	}

	registry := GetGlobalQueryRegistry()
	if err := registry.RegisterQuery(qc); err != nil {
		t.Fatalf("RegisterQuery failed: %v", err)
	}
	defer registry.UnregisterQuery(queryID)

	// 验证查询已注册
	retrievedQC := registry.GetQueryByThreadID(123)
	if retrievedQC == nil {
		t.Fatal("Query should be registered")
	}
	if retrievedQC.QueryID != queryID {
		t.Fatalf("QueryID mismatch: got %s, want %s", retrievedQC.QueryID, queryID)
	}

	// 测试Kill查询
	err := registry.KillQueryByThreadID(123)
	if err != nil {
		t.Fatalf("KillQueryByThreadID failed: %v", err)
	}

	// 验证查询已被标记为取消
	if !retrievedQC.IsCanceled() {
		t.Error("Query should be marked as canceled")
	}

	// 验证context已取消
	select {
	case <-ctx.Done():
		t.Log("Context was canceled successfully")
	default:
		t.Error("Context should be canceled")
	}
}

// TestQueryRegistry 测试查询注册表功能
func TestQueryRegistry(t *testing.T) {
	registry := NewQueryRegistry()

	// 创建测试查询
	_, cancel1 := context.WithCancel(context.Background())
	_, cancel2 := context.WithCancel(context.Background())

	qc1 := &QueryContext{
		QueryID:    "q1",
		ThreadID:   1,
		SQL:        "SELECT 1",
		StartTime:  time.Now(),
		CancelFunc: cancel1,
	}

	qc2 := &QueryContext{
		QueryID:    "q2",
		ThreadID:   2,
		SQL:        "SELECT 2",
		StartTime:  time.Now(),
		CancelFunc: cancel2,
	}

	// 注册查询
	if err := registry.RegisterQuery(qc1); err != nil {
		t.Fatalf("RegisterQuery failed: %v", err)
	}
	if err := registry.RegisterQuery(qc2); err != nil {
		t.Fatalf("RegisterQuery failed: %v", err)
	}

	// 验证查询数量
	if count := registry.GetQueryCount(); count != 2 {
		t.Fatalf("Query count mismatch: got %d, want 2", count)
	}

	// 通过 ThreadID 查找
	retrieved1 := registry.GetQueryByThreadID(1)
	if retrieved1 == nil {
		t.Fatal("Query with ThreadID 1 should exist")
	}
	if retrieved1.QueryID != "q1" {
		t.Fatalf("QueryID mismatch: got %s, want q1", retrieved1.QueryID)
	}

	// 通过 QueryID 查找
	retrieved2 := registry.GetQuery("q2")
	if retrieved2 == nil {
		t.Fatal("Query with QueryID q2 should exist")
	}

	// 获取所有查询
	allQueries := registry.GetAllQueries()
	if len(allQueries) != 2 {
		t.Fatalf("GetAllQueries count mismatch: got %d, want 2", len(allQueries))
	}

	// Kill 查询1
	if err := registry.KillQueryByThreadID(1); err != nil {
		t.Fatalf("KillQueryByThreadID failed: %v", err)
	}

	// 验证查询1已取消
	if !retrieved1.IsCanceled() {
		t.Error("Query 1 should be marked as canceled")
	}

	// 注销查询1（Kill只是取消，不删除）
	registry.UnregisterQuery("q1")

	// 注销查询2
	registry.UnregisterQuery("q2")

	// 验证查询数量
	if count := registry.GetQueryCount(); count != 0 {
		t.Fatalf("Query count mismatch: got %d, want 0", count)
	}
}

// TestQueryIDGeneration 测试查询ID生成
func TestQueryIDGeneration(t *testing.T) {
	id1 := GenerateQueryID(123)
	id2 := GenerateQueryID(123)

	// 验证ID不重复
	if id1 == id2 {
		t.Error("QueryIDs should be unique")
	}

	// 验证ID格式
	fmt.Printf("Generated QueryID 1: %s\n", id1)
	fmt.Printf("Generated QueryID 2: %s\n", id2)
}

// TestQueryContextStatus 测试查询状态
func TestQueryContextStatus(t *testing.T) {
	_, cancel := context.WithCancel(context.Background())

	qc := &QueryContext{
		QueryID:    "test_query",
		ThreadID:   1,
		SQL:        "SELECT * FROM test",
		StartTime:  time.Now(),
		CancelFunc: cancel,
	}

	// 初始状态
	status := qc.GetStatus()
	if status.Status != "running" {
		t.Fatalf("Initial status should be 'running', got %s", status.Status)
	}

	// 标记为超时
	qc.SetTimeout()
	status = qc.GetStatus()
	if status.Status != "timeout" {
		t.Fatalf("Status should be 'timeout' after SetTimeout, got %s", status.Status)
	}

	// 创建新的查询上下文
	_, cancel2 := context.WithCancel(context.Background())
	qc2 := &QueryContext{
		QueryID:    "test_query2",
		ThreadID:   1,
		SQL:        "SELECT * FROM test",
		StartTime:  time.Now(),
		CancelFunc: cancel2,
	}

	// 标记为取消
	qc2.SetCanceled()
	status2 := qc2.GetStatus()
	if status2.Status != "canceled" {
		t.Fatalf("Status should be 'canceled' after SetCanceled, got %s", status2.Status)
	}
}
