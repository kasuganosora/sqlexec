# 集成测试 (Integration Tests)

集成测试使用真实的 MySQL 客户端连接，测试完整的请求-响应流程。

## 测试文件

### protocol_test.go

**测试内容：** 协议层端到端测试

**主要测试：**
- 连接和认证
- 简单 SELECT 查询
- 错误包序列化
- 空查询处理
- Ping 保活
- 错误返回格式验证

**端口范围：** 13300-13305

**运行方式：**
```bash
go test ./server/testing/integration -run TestProtocol
```

### end_to_end_test.go

**测试内容：** 端到端功能测试

**主要测试：**
- COM_INIT_DB / USE 命令
- SELECT DATABASE() 函数
- information_schema 查询
- 错误包端到端测试
- 多次数据库切换
- 连接恢复测试
- information_schema 实际数据测试
- 数据库上下文缓存
- SHOW DATABASES 命令

**端口范围：** 13306-13314

**运行方式：**
```bash
go test ./server/testing/integration -run TestE2E
```

### scenarios_test.go

**测试内容：** 场景测试

**主要测试：**
- 数据库切换流程
- 错误后连接恢复
- 不存在表查询
- 无效 SQL 语法
- Ping 保活
- 空查询
- 并发查询
- 连接池

**端口范围：** 13320-13327

**运行方式：**
```bash
go test ./server/testing/integration -run TestScenario
```

### com_init_db_test.go

**测试内容：** COM_INIT_DB 命令测试

**端口：** 13306

**运行方式：**
```bash
go test ./server/testing/integration -run TestCOMInitDB
```

### use_database_test.go

**测试内容：** USE 数据库命令测试

**端口：** 13306

**运行方式：**
```bash
go test ./server/testing/integration -run TestUseDatabase
```

### information_schema_test.go

**测试内容：** information_schema 查询测试

**端口范围：** 13308-13311

**运行方式：**
```bash
go test ./server/testing/integration -run TestInformationSchema
```

### privilege_test.go

**测试内容：** 权限表可见性测试

**端口：** 13307

**运行方式：**
```bash
go test ./server/testing/integration -run TestPrivilege
```

### show_processlist_test.go

**测试内容：** SHOW PROCESSLIST 命令测试

**运行方式：**
```bash
go test ./server/testing/integration -run TestShowProcesslist
```

### temporary_table_test.go

**测试内容：** 临时表完整功能测试

**运行方式：**
```bash
go test ./server/testing/integration -run TestTemporaryTables
```

### privileges_table_test.go

**测试内容：** 权限表测试（使用 ACL）

**运行方式：**
```bash
go test ./server/testing/integration -run TestPrivilegesTable
```

### privilege_tables_integration_test.go

**测试内容：** 权限表集成测试

**运行方式：**
```bash
go test ./server/testing/integration -run TestPrivilegeTablesIntegration
```

## 运行所有集成测试

```bash
# 运行所有集成测试
go test ./server/testing/integration

# 运行并显示详细输出
go test ./server/testing/integration -v

# 运行并显示覆盖率
go test ./server/testing/integration -cover
```

## 编写集成测试的指南

### 1. 使用 TestServer

```go
func TestExample(t *testing.T) {
    testServer := mysqltest.NewTestServer()

    // 启动服务器
    err := testServer.Start(13306)
    if err != nil {
        t.Fatalf("Failed to start test server: %v", err)
    }
    defer testServer.Stop()

    // 使用 MySQL 客户端连接
    err = testServer.RunWithClient(func(conn *sql.DB) error {
        // 测试代码
        _, err := conn.Exec("USE test_db")
        return err
    })

    assert.NoError(t, err)
}
```

### 2. 创建测试表

```go
func TestWithTable(t *testing.T) {
    testServer := mysqltest.NewTestServer()
    err := testServer.Start(13307)
    assert.NoError(t, err)
    defer testServer.Stop()

    // 创建测试表
    err = testServer.CreateTestTable("users", []domain.ColumnInfo{
        {Name: "id", Type: "int", Primary: true, AutoIncrement: true, Nullable: false},
        {Name: "name", Type: "string", Nullable: false},
    })
    assert.NoError(t, err)

    // 执行测试
    err = testServer.RunWithClient(func(conn *sql.DB) error {
        var name string
        return conn.QueryRow("SELECT name FROM users WHERE id = 1").Scan(&name)
    })

    assert.NoError(t, err)
}
```

### 3. 插入测试数据

```go
func TestWithData(t *testing.T) {
    testServer := mysqltest.NewTestServer()
    err := testServer.Start(13308)
    assert.NoError(t, err)
    defer testServer.Stop()

    // 插入测试数据
    err = testServer.InsertTestData("users", []domain.Row{
        {"id": int64(1), "name": "Alice"},
        {"id": int64(2), "name": "Bob"},
    })
    assert.NoError(t, err)

    // 执行测试
    err = testServer.RunWithClient(func(conn *sql.DB) error {
        rows, err := conn.Query("SELECT * FROM users")
        if err != nil {
            return err
        }
        defer rows.Close()

        var users []string
        for rows.Next() {
            var name string
            if err := rows.Scan(&name); err != nil {
                return err
            }
            users = append(users, name)
        }
        return nil
    })

    assert.NoError(t, err)
}
```

### 4. 测试错误场景

```go
func TestErrorHandling(t *testing.T) {
    testServer := mysqltest.NewTestServer()
    err := testServer.Start(13309)
    assert.NoError(t, err)
    defer testServer.Stop()

    err = testServer.RunWithClient(func(conn *sql.DB) error {
        // 测试查询不存在的表
        _, err := conn.Query("SELECT * FROM non_existent_table")
        if err == nil {
            return fmt.Errorf("expected error for non-existent table, got nil")
        }

        // 验证连接仍然有效
        err = conn.Ping()
        if err != nil {
            return fmt.Errorf("connection should still be active after error: %w", err)
        }

        return nil
    })

    assert.NoError(t, err)
}
```

### 5. 测试数据库切换

```go
func TestDatabaseSwitching(t *testing.T) {
    testServer := mysqltest.NewTestServer()
    err := testServer.Start(13310)
    assert.NoError(t, err)
    defer testServer.Stop()

    err = testServer.RunWithClient(func(conn *sql.DB) error {
        databases := []string{"db1", "db2", "information_schema"}

        for _, db := range databases {
            // 切换数据库
            _, err := conn.Exec(fmt.Sprintf("USE %s", db))
            if err != nil {
                return fmt.Errorf("USE %s failed: %w", db, err)
            }

            // 验证当前数据库
            var dbName string
            err = conn.QueryRow("SELECT DATABASE()").Scan(&dbName)
            if err != nil {
                return fmt.Errorf("SELECT DATABASE() failed: %w", err)
            }

            if dbName != db {
                return fmt.Errorf("expected '%s', got '%s'", db, dbName)
            }
        }

        return nil
    })

    assert.NoError(t, err)
}
```

## 端口分配

为了避免端口冲突，每个测试文件使用不同的端口范围：

| 测试文件 | 端口范围 |
|----------|----------|
| protocol_test.go | 13300-13305 |
| end_to_end_test.go | 13306-13314 |
| scenarios_test.go | 13320-13327 |
| com_init_db_test.go | 13306 |
| use_database_test.go | 13306 |
| information_schema_test.go | 13308-13311 |
| privilege_test.go | 13307 |

**注意：** 端口冲突需要解决，建议使用动态端口分配或统一端口管理。

## 注意事项

1. **包名** - 集成测试文件使用 `package tests` 包
2. **独立端口** - 每个测试应该使用独立的端口，避免冲突
3. **清理资源** - 使用 `defer testServer.Stop()` 确保服务器被停止
4. **错误处理** - 集成测试要验证错误场景和恢复
5. **并发测试** - 并发测试要验证线程安全和数据一致性
6. **测试隔离** - 每个测试应该独立，不依赖其他测试的状态
