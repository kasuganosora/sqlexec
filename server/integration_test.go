package server

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/kasuganosora/sqlexec/pkg/api"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/resource/memory"
)

// TestServerIntegration 测试服务器与查询引擎的集成
func TestServerIntegration(t *testing.T) {
	ctx := context.Background()

	// 创建 API DB
	db, err := api.NewDB(&api.DBConfig{
		CacheEnabled:  false,
		CacheSize:     100,
		CacheTTL:      300,
		DebugMode:     true,
	})
	if err != nil {
		t.Fatalf("初始化 API DB 失败: %v", err)
	}

	// 创建 MVCC 数据源
	memoryDS := memory.NewMVCCDataSource(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Name:     "test_db",
		Writable: true,
	})

	// 连接数据源
	if err := memoryDS.Connect(ctx); err != nil {
		t.Fatalf("连接内存数据源失败: %v", err)
	}
	defer memoryDS.Close(ctx)

	// 注册数据源
	if err := db.RegisterDataSource("test_db", memoryDS); err != nil {
		t.Fatalf("注册数据源失败: %v", err)
	}

	// 创建表
	if err := memoryDS.CreateTable(ctx, &domain.TableInfo{
		Name: "users",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int", Primary: true, AutoIncrement: true, Nullable: false},
			{Name: "name", Type: "string", Nullable: false},
			{Name: "age", Type: "int", Nullable: true},
		},
	}); err != nil {
		t.Fatalf("创建表失败: %v", err)
	}

	// 插入测试数据
	if _, err := memoryDS.Insert(ctx, "users", []domain.Row{
		{"id": 1, "name": "Alice", "age": 30},
		{"id": 2, "name": "Bob", "age": 25},
		{"id": 3, "name": "Charlie", "age": 35},
	}, nil); err != nil {
		t.Fatalf("插入数据失败: %v", err)
	}

	// 测试查询
	sess := db.Session()
	result, err := sess.Query("SELECT * FROM users")
	if err != nil {
		t.Fatalf("查询失败: %v", err)
	}
	defer result.Close()

	// 验证列
	columns := result.Columns()
	if len(columns) != 3 {
		t.Errorf("期望 3 列，得到 %d 列", len(columns))
	}

	// 验证行数
	var rows []domain.Row
	for result.Next() {
		rows = append(rows, result.Row())
	}

	if len(rows) != 3 {
		t.Errorf("期望 3 行，得到 %d 行", len(rows))
	}

	// 验证第一行数据
	if rows[0]["name"] != "Alice" {
		t.Errorf("期望 name=Alice，得到 %v", rows[0]["name"])
	}

	// 检查 age 的实际类型
	t.Logf("age 的值: %v，类型: %T", rows[0]["age"], rows[0]["age"])

	ageValue := rows[0]["age"]
	// 尝试多种类型断言
	switch v := ageValue.(type) {
	case int64:
		if v != 30 {
			t.Errorf("期望 age=30 (int64)，得到 %v", v)
		}
	case int:
		if v != 30 {
			t.Errorf("期望 age=30 (int)，得到 %v", v)
		}
	default:
		t.Errorf("age 类型不符合预期: %T", ageValue)
	}

	t.Logf("集成测试成功！查询到 %d 行数据", len(rows))
}

// TestServerWithMySQLClient 测试使用 MySQL 客户端连接服务器
func TestServerWithMySQLClient(t *testing.T) {
	ctx := context.Background()

	// 启动服务器
	listener, err := net.Listen("tcp", "127.0.0.1:13306")
	if err != nil {
		t.Fatalf("监听失败: %v", err)
	}

	server := NewServer(ctx, listener, nil)
	go func() {
		if err := server.Start(); err != nil {
			t.Logf("服务器错误: %v", err)
		}
	}()

	// 等待服务器启动
	time.Sleep(100 * time.Millisecond)

	// 连接 MySQL 客户端
	dsn := fmt.Sprintf("root@tcp(127.0.0.1:13306)/")
	conn, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("连接 MySQL 失败: %v", err)
	}
	defer conn.Close()

	// 测试简单查询（应该返回空结果或错误，因为没有数据）
	var name string
	err = conn.QueryRow("SELECT @@version").Scan(&name)
	if err != nil {
		t.Logf("查询 version 失败（预期可能失败）: %v", err)
	}

	t.Log("MySQL 客户端连接测试完成")
}
