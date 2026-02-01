# MySQL 服务器测试指南

## 概述

本指南提供 MySQL 协议服务器的测试方法和最佳实践，帮助你在单元测试中启动服务器、复现问题并验证功能。

## 核心测试工具

### TestServer

`pkg/mysqltest.TestServer` 提供了一个可在测试中启动和停止的 MySQL 协议服务器。

```go
package tests

import (
    "database/sql"
    "testing"
    "github.com/kasuganosora/sqlexec/pkg/mysqltest"
    "github.com/stretchr/testify/assert"
)

func TestBasicConnection(t *testing.T) {
    // 创建测试服务器
    testServer := mysqltest.NewTestServer()
    
    // 启动服务器（使用随机端口）
    err := testServer.Start(13315)
    assert.NoError(t, err)
    defer testServer.Stop()  // 测试结束时自动停止
    
    // 使用 MySQL 客户端连接测试
    err = testServer.RunWithClient(func(conn *sql.DB) error {
        var result string
        err := conn.QueryRow("SELECT 1").Scan(&result)
        return err
    })
    
    assert.NoError(t, err)
}
```

## 测试类型

### 1. 协议层测试

测试 MySQL 协议包的正确序列化和反序列化。

```go
func TestErrorPacketSerialization(t *testing.T) {
    testServer := mysqltest.NewTestServer()
    err := testServer.Start(13320)
    assert.NoError(t, err)
    defer testServer.Stop()
    
    err = testServer.RunWithClient(func(conn *sql.DB) error {
        // 执行一个会产生错误的查询
        _, err := conn.Query("SELECT * FROM nonexistent_table")
        if err == nil {
            return errors.New("expected error but got none")
        }
        
        // 验证错误信息格式
        expectedMsg := "ERROR 1064 (42000): Table 'nonexistent_table' doesn't exist"
        if err.Error() != expectedMsg {
            return fmt.Errorf("error mismatch: got %s, want %s", err.Error(), expectedMsg)
        }
        
        return nil
    })
    
    assert.NoError(t, err)
}
```

### 2. 端到端测试 (End-to-End)

使用标准 MySQL 客户端测试完整的查询流程。

```go
func TestE2E_QueryExecution(t *testing.T) {
    testServer := mysqltest.NewTestServer()
    err := testServer.Start(13321)
    assert.NoError(t, err)
    defer testServer.Stop()
    
    err = testServer.RunWithClient(func(conn *sql.DB) error {
        // 创建测试表
        _, err := conn.Exec(`
            CREATE TABLE users (
                id INT PRIMARY KEY,
                name VARCHAR(100),
                age INT
            )
        `)
        if err != nil {
            return err
        }
        
        // 插入数据
        _, err = conn.Exec("INSERT INTO users VALUES (1, 'Alice', 30)")
        if err != nil {
            return err
        }
        
        // 查询数据
        rows, err := conn.Query("SELECT id, name, age FROM users")
        if err != nil {
            return err
        }
        defer rows.Close()
        
        count := 0
        for rows.Next() {
            var id int
            var name string
            var age int
            err := rows.Scan(&id, &name, &age)
            if err != nil {
                return err
            }
            count++
            
            // 验证数据
            assert.Equal(t, 1, id)
            assert.Equal(t, "Alice", name)
            assert.Equal(t, 30, age)
        }
        
        if count != 1 {
            return fmt.Errorf("expected 1 row, got %d", count)
        }
        
        return nil
    })
    
    assert.NoError(t, err)
}
```

### 3. 数据层测试

使用底层 API 直接测试数据存储和查询逻辑。

```go
func TestDataSourceOperations(t *testing.T) {
    ctx := context.Background()
    
    // 创建内存数据源
    ds := memory.NewMVCCDataSource(&domain.DataSourceConfig{
        Type:     domain.DataSourceTypeMemory,
        Name:     "test",
        Writable: true,
    })
    
    err := ds.Connect(ctx)
    assert.NoError(t, err)
    
    // 创建表
    schema := &domain.TableInfo{
        Name: "users",
        Columns: []domain.ColumnInfo{
            {Name: "id", Type: "int64", Primary: true},
            {Name: "name", Type: "string"},
        },
    }
    
    err = ds.CreateTable(ctx, schema)
    assert.NoError(t, err)
    
    // 插入数据
    rows := []domain.Row{
        {"id": int64(1), "name": "Alice"},
    }
    
    _, err = ds.Insert(ctx, "users", rows, nil)
    assert.NoError(t, err)
    
    // 查询验证
    result, err := ds.Query(ctx, "users", &domain.QueryOptions{})
    assert.NoError(t, err)
    assert.Len(t, result.Rows, 1)
    assert.Equal(t, "Alice", result.Rows[0]["name"])
}
```

## 问题复现和调试

### 使用日志输出

```go
func TestDebugIssue(t *testing.T) {
    testServer := mysqltest.NewTestServer()
    err := testServer.Start(13322)
    assert.NoError(t, err)
    defer testServer.Stop()
    
    t.Log("=== 开始复现问题 ===")
    t.Logf("服务器端口: %d", testServer.GetPort())
    
    err = testServer.RunWithClient(func(conn *sql.DB) error {
        t.Log("步骤 1: 连接成功")
        
        rows, err := conn.Query("SELECT 1")
        if err != nil {
            t.Logf("查询失败: %v", err)
            return err
        }
        
        t.Log("步骤 2: 查询成功")
        rows.Close()
        
        return nil
    })
    
    t.Logf("=== 测试结果: %v ===", err)
    assert.NoError(t, err)
}
```

### 并发测试

```go
func TestConcurrentQueries(t *testing.T) {
    testServer := mysqltest.NewTestServer()
    err := testServer.Start(13323)
    assert.NoError(t, err)
    defer testServer.Stop()
    
    var wg sync.WaitGroup
    errors := make(chan error, 10)
    
    for i := 0; i < 10; i++ {
        wg.Add(1)
        go func(id int) {
            defer wg.Done()
            
            err := testServer.RunWithClient(func(conn *sql.DB) error {
                var result string
                return conn.QueryRow("SELECT 1").Scan(&result)
            })
            
            if err != nil {
                errors <- fmt.Errorf("goroutine %d: %w", id, err)
            }
        }(i)
    }
    
    wg.Wait()
    close(errors)
    
    for err := range errors {
        assert.NoError(t, err)
    }
}
```

## 测试框架最佳实践

### 1. 测试命名规范

```go
// 功能测试
func TestFeatureName(t *testing.T) { }

// 边界条件测试
func TestFeatureName_EdgeCase(t *testing.T) { }

// 错误处理测试
func TestFeatureName_ErrorHandling(t *testing.T) { }

// 并发测试
func TestFeatureName_Concurrent(t *testing.T) { }
```

### 2. 测试结构

```go
func TestScenario(t *testing.T) {
    // Setup: 准备测试环境
    testServer := mysqltest.NewTestServer()
    err := testServer.Start(13324)
    assert.NoError(t, err)
    defer testServer.Stop()
    
    // Arrange: 准备测试数据
    t.Run("创建表", func(t *testing.T) {
        // ...
    })
    
    // Act: 执行被测试的操作
    t.Run("执行查询", func(t *testing.T) {
        // ...
    })
    
    // Assert: 验证结果
    t.Run("验证数据", func(t *testing.T) {
        // ...
    })
}
```

### 3. 表构建助手

```go
func setupTestTable(db *sql.DB, name string, schema string) error {
    _, err := db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", name))
    if err != nil {
        return err
    }
    _, err = db.Exec(fmt.Sprintf("CREATE TABLE %s (%s)", name, schema))
    return err
}

func TestWithHelper(t *testing.T) {
    testServer := mysqltest.NewTestServer()
    err := testServer.Start(13325)
    assert.NoError(t, err)
    defer testServer.Stop()
    
    err = testServer.RunWithClient(func(conn *sql.DB) error {
        // 使用助手函数
        err := setupTestTable(conn, "test_table", "id INT, value VARCHAR(50)")
        if err != nil {
            return err
        }
        
        // 测试逻辑...
        return nil
    })
    
    assert.NoError(t, err)
}
```

## 常见测试场景

### 1. NULL 值处理

```go
func TestNullHandling(t *testing.T) {
    testServer := mysqltest.NewTestServer()
    err := testServer.Start(13326)
    assert.NoError(t, err)
    defer testServer.Stop()
    
    err = testServer.RunWithClient(func(conn *sql.DB) error {
        _, err := conn.Exec("CREATE TABLE nullable_test (id INT, value VARCHAR(50))")
        if err != nil {
            return err
        }
        
        _, err = conn.Exec("INSERT INTO nullable_test VALUES (1, NULL)")
        if err != nil {
            return err
        }
        
        var value sql.NullString
        err = conn.QueryRow("SELECT value FROM nullable_test WHERE id = 1").Scan(&value)
        if err != nil {
            return err
        }
        
        if value.Valid {
            return errors.New("expected NULL value")
        }
        
        return nil
    })
    
    assert.NoError(t, err)
}
```

### 2. 错误返回测试

```go
func TestErrorMessages(t *testing.T) {
    testServer := mysqltest.NewTestServer()
    err := testServer.Start(13327)
    assert.NoError(t, err)
    defer testServer.Stop()
    
    err = testServer.RunWithClient(func(conn *sql.DB) error {
        // 测试语法错误
        _, err := conn.Query("SELEC * FROM table")  // 故意写错 SELECT
        if err == nil {
            return errors.New("expected syntax error")
        }
        
        // 验证错误代码
        if !strings.Contains(err.Error(), "ERROR 1064") {
            return fmt.Errorf("wrong error code: %s", err.Error())
        }
        
        return nil
    })
    
    assert.NoError(t, err)
}
```

### 3. 性能测试

```go
func TestPerformance(t *testing.T) {
    if testing.Short() {
        t.Skip("skipping performance test in short mode")
    }
    
    testServer := mysqltest.NewTestServer()
    err := testServer.Start(13328)
    assert.NoError(t, err)
    defer testServer.Stop()
    
    err = testServer.RunWithClient(func(conn *sql.DB) error {
        _, err := conn.Exec("CREATE TABLE perf_test (id INT, value INT)")
        if err != nil {
            return err
        }
        
        // 批量插入
        start := time.Now()
        stmt, _ := conn.Prepare("INSERT INTO perf_test VALUES (?, ?)")
        defer stmt.Close()
        
        for i := 0; i < 1000; i++ {
            _, err := stmt.Exec(i, i*2)
            if err != nil {
                return err
            }
        }
        duration := time.Since(start)
        
        t.Logf("插入 1000 行耗时: %v", duration)
        
        return nil
    })
    
    assert.NoError(t, err)
}
```

## 注意事项

### 1. 端口冲突

每个测试应该使用不同的端口号，避免冲突：

```go
func TestPort1(t *testing.T) {
    testServer := mysqltest.NewTestServer()
    err := testServer.Start(13330)  // 使用唯一端口
    // ...
}

func TestPort2(t *testing.T) {
    testServer := mysqltest.NewTestServer()
    err := testServer.Start(13331)  // 不同的端口
    // ...
}
```

### 2. 清理资源

总是使用 `defer` 确保服务器和连接被正确关闭：

```go
func TestCleanup(t *testing.T) {
    testServer := mysqltest.NewTestServer()
    err := testServer.Start(13332)
    assert.NoError(t, err)
    defer testServer.Stop()  // 确保清理
    
    err = testServer.RunWithClient(func(conn *sql.DB) error {
        rows, err := conn.Query("SELECT 1")
        if err != nil {
            return err
        }
        defer rows.Close()  // 确保关闭结果集
        // ...
    })
    
    assert.NoError(t, err)
}
```

### 3. 隔离性

每个测试应该独立运行，不依赖其他测试的状态：

```go
func TestIsolation(t *testing.T) {
    testServer := mysqltest.NewTestServer()
    err := testServer.Start(13333)
    assert.NoError(t, err)
    defer testServer.Stop()
    
    err = testServer.RunWithClient(func(conn *sql.DB) error {
        // 清理可能存在的旧表
        _, err := conn.Exec("DROP TABLE IF EXISTS test_table")
        if err != nil {
            return err
        }
        
        // 创建测试表
        _, err = conn.Exec("CREATE TABLE test_table (id INT)")
        // ...
    })
    
    assert.NoError(t, err)
}
```

### 4. 超时设置

为长时间运行的测试设置超时：

```go
func TestWithTimeout(t *testing.T) {
    testServer := mysqltest.NewTestServer()
    err := testServer.Start(13334)
    assert.NoError(t, err)
    defer testServer.Stop()
    
    done := make(chan error, 1)
    go func() {
        done <- testServer.RunWithClient(func(conn *sql.DB) error {
            // 测试逻辑
            return nil
        })
    }()
    
    select {
    case err := <-done:
        assert.NoError(t, err)
    case <-time.After(10 * time.Second):
        t.Fatal("test timeout")
    }
}
```

## 运行测试

```bash
# 运行所有测试
go test ./server/tests/... -v

# 运行特定测试
go test ./server/tests/... -run TestSimpleConnection -v

# 运行并显示覆盖率
go test ./server/tests/... -cover

# 运行性能测试
go test ./server/tests/... -bench=. -benchmem

# 跳过长时间测试
go test ./server/tests/... -short
```

## 总结

- 使用 `pkg/mysqltest.TestServer` 在测试中启动 MySQL 服务器
- 分层测试：协议层、端到端、数据层
- 关注隔离性、清理资源和错误处理
- 使用日志和子测试组织测试代码
- 确保每个测试独立且可重复
