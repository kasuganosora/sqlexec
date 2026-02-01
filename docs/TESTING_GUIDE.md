# 单元测试指南 - 不启动服务器进行完整测试

## 概述

本指南介绍如何在不启动 MySQL 服务器和客户端的情况下，对项目进行完整的单元测试。

## 测试架构

### 1. 纯逻辑测试（推荐）

这些测试完全不依赖网络、数据库或服务器，只测试业务逻辑。

#### 优点
- ✅ 执行速度快（毫秒级）
- ✅ 100% 可重复
- ✅ 易于调试
- ✅ 不依赖外部资源

#### 适用场景
- SQL 解析逻辑
- 查询优化逻辑
- 会话管理逻辑
- information_schema 虚拟表逻辑

## 测试工具

### 1. MemoryTestHelper

快速创建内存数据源进行测试。

```go
import "github.com/kasuganosora/sqlexec/pkg/testutils"

func TestMyFeature(t *testing.T) {
    // 创建测试辅助器
    helper := testutils.NewMemoryTestHelper(t)
    defer helper.Cleanup()

    // 创建表
    helper.CreateTable(t, &domain.TableInfo{
        Name: "users",
        Columns: []domain.ColumnInfo{
            {Name: "id", Type: "int64"},
            {Name: "name", Type: "string"},
        },
    })

    // 插入数据
    helper.InsertData(t, "users", []domain.Row{
        {"id": int64(1), "name": "Alice"},
        {"id": int64(2), "name": "Bob"},
    })

    // 执行测试...
    ds := helper.GetDataSource()
    result, err := ds.Query(ctx, "users", nil)
    require.NoError(t, err)
    assert.Equal(t, 2, len(result.Rows))
}
```

### 2. MockSessionExecutor

模拟查询执行器，不依赖真实优化器。

```go
import "github.com/kasuganosora/sqlexec/pkg/session"

func TestQueryLogic(t *testing.T) {
    ctx := context.Background()

    // 创建mock执行器
    mockExec := session.NewMockExecutor()

    // 设置预期结果
    mockExec.SetResult("SELECT * FROM users", &domain.QueryResult{
        Columns: []domain.ColumnInfo{
            {Name: "id", Type: "int64"},
            {Name: "name", Type: "string"},
        },
        Rows: []domain.Row{
            {"id": int64(1), "name": "Alice"},
        },
        Total: 1,
    })

    // 创建session并注入mock
    sess := &session.CoreSession{
        executor: mockExec,
        // ...
    }

    // 执行查询
    result, err := sess.ExecuteQuery(ctx, "SELECT * FROM users")
    require.NoError(t, err)
    assert.Equal(t, "Alice", result.Rows[0]["name"])

    // 验证调用了正确的SQL
    queries := mockExec.GetQueries()
    assert.Equal(t, 1, len(queries))
}
```

### 3. MockDataSource

模拟数据源，用于测试数据访问层。

```go
mockDS := &session.MockDataSource{
    Tables: map[string]*domain.TableInfo{
        "users": {
            Name: "users",
            Columns: []domain.ColumnInfo{
                {Name: "id", Type: "int64"},
                {Name: "name", Type: "string"},
            },
        },
    },
    Data: map[string][]domain.Row{
        "users": {
            {"id": int64(1), "name": "Alice"},
        },
    },
}

// 使用mock数据源
result, err := mockDS.Query(ctx, "users", nil)
```

## 测试示例

### 示例 1: 测试 SQL 解析

```go
func TestSQLParser(t *testing.T) {
    adapter := parser.NewSQLAdapter()

    sql := "SELECT id, name FROM users WHERE id = 1"
    stmt, err := adapter.Parse(sql)
    require.NoError(t, err)

    assert.Equal(t, parser.SQLTypeSelect, stmt.Type)
    assert.Equal(t, 2, len(stmt.Select.Columns))
    assert.Equal(t, "users", stmt.Select.From)
}
```

### 示例 2: 测试查询执行逻辑

```go
func TestQueryExecution(t *testing.T) {
    ctx := context.Background()

    // 创建mock执行器
    mockExec := executor.NewMockExecutor()
    mockExec.SetResult("SELECT * FROM users", &domain.QueryResult{
        Columns: []domain.ColumnInfo{
            {Name: "id", Type: "int64"},
            {Name: "name", Type: "string"},
        },
        Rows: []domain.Row{
            {"id": int64(1), "name": "Alice"},
        },
        Total: 1,
    })

    // 执行查询
    result, err := mockExec.Execute(ctx, "SELECT * FROM users")
    require.NoError(t, err)

    assert.Equal(t, 1, len(result.Rows))
    assert.Equal(t, "Alice", result.Rows[0]["name"])
}
```

### 示例 3: 测试会话管理

```go
func TestSessionManagement(t *testing.T) {
    ctx := context.Background()

    // 创建mock数据源和执行器
    mockDS := &session.MockDataSource{}
    mockExec := session.NewMockExecutor()

    // 创建session
    sess := &session.CoreSession{
        dataSource: mockDS,
        executor:   mockExec,
        adapter:    parser.NewSQLAdapter(),
        currentDB:  "test_db",
    }

    // 设置当前数据库
    sess.SetCurrentDB("information_schema")
    assert.Equal(t, "information_schema", sess.GetCurrentDB())

    // 执行查询
    mockExec.SetResult("SELECT DATABASE()", &domain.QueryResult{
        Columns: []domain.ColumnInfo{{Name: "DATABASE()", Type: "string"}},
        Rows:    []domain.Row{{"DATABASE()": "information_schema"}},
        Total:   1,
    })

    result, err := sess.ExecuteQuery(ctx, "SELECT DATABASE()")
    require.NoError(t, err)
    assert.Equal(t, "information_schema", result.Rows[0]["DATABASE()"])
}
```

### 示例 4: 测试错误处理

```go
func TestErrorHandling(t *testing.T) {
    ctx := context.Background()

    mockDS := &session.MockDataSource{}
    mockExec := session.NewMockExecutor()

    sess := &session.CoreSession{
        dataSource: mockDS,
        executor:   mockExec,
    }

    // 设置错误
    mockExec.SetError("INVALID SQL",
        domain.NewError("syntax error", nil))

    // 执行会失败的查询
    _, err := sess.ExecuteQuery(ctx, "INVALID SQL")

    // 验证错误
    assert.Error(t, err)
    assert.Contains(t, err.Error(), "syntax error")
}
```

### 示例 5: 测试事务逻辑

```go
func TestTransactionLogic(t *testing.T) {
    ctx := context.Background()

    mockDS := &session.MockDataSource{
        Transactions: make(map[string]*session.MockTransaction),
    }
    mockExec := session.NewMockExecutor()

    sess := &session.CoreSession{
        dataSource: mockDS,
        executor:   mockExec,
    }

    // 开始事务
    txn, err := mockDS.BeginTransaction(ctx, &domain.TransactionOptions{})
    require.NoError(t, err)
    sess.SetTransaction(txn)

    // 验证事务状态
    assert.True(t, sess.InTx())
    assert.NotNil(t, sess.Transaction())

    // 执行事务内操作
    mockExec.SetResult("INSERT INTO users", &domain.QueryResult{
        Columns: []domain.ColumnInfo{},
        Rows:    []domain.Row{},
        Total:   1,
    })

    _, err = sess.ExecuteInsert(ctx, "INSERT INTO users", []domain.Row{
        {"id": int64(1), "name": "Alice"},
    })
    require.NoError(t, err)

    // 提交事务
    err = sess.CommitTx(ctx)
    require.NoError(t, err)

    // 验证事务已关闭
    assert.False(t, sess.InTx())
    assert.Nil(t, sess.Transaction())
}
```

## 运行测试

### 运行所有单元测试

```bash
# 运行所有测试
go test ./...

# 运行特定包的测试
go test ./pkg/session
go test ./pkg/optimizer
go test ./pkg/parser

# 运行特定测试函数
go test ./pkg/session -run TestCoreSession_WithoutServer
go test ./pkg/executor -run TestQueryExecutor_SimpleSelect

# 查看详细输出
go test ./pkg/session -v

# 查看测试覆盖率
go test ./pkg/session -cover
go test ./pkg/session -coverprofile=coverage.out
go tool cover -html=coverage.out
```

### 运行纯逻辑测试（不涉及网络）

```bash
# 只运行以 _unit_test.go 结尾的测试文件
go test -run "Unit" ./pkg/session
go test -run "Unit" ./pkg/executor
```

## 测试最佳实践

### 1. 使用 Table-Driven Tests

```go
func TestSQLParser(t *testing.T) {
    tests := []struct {
        name    string
        sql     string
        want    *parser.SQLStatement
        wantErr bool
    }{
        {
            name: "simple select",
            sql:  "SELECT * FROM users",
            wantErr: false,
        },
        {
            name: "select with where",
            sql:  "SELECT * FROM users WHERE id = 1",
            wantErr: false,
        },
        {
            name: "invalid sql",
            sql:  "INVALID",
            wantErr: true,
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            adapter := parser.NewSQLAdapter()
            stmt, err := adapter.Parse(tt.sql)

            if tt.wantErr {
                assert.Error(t, err)
            } else {
                assert.NoError(t, err)
                assert.NotNil(t, stmt)
            }
        })
    }
}
```

### 2. 使用 require 处理前置条件

```go
func TestMyFeature(t *testing.T) {
    ctx := context.Background()

    // 前置条件：必须成功
    helper := testutils.NewMemoryTestHelper(t)
    defer helper.Cleanup()

    helper.CreateTable(t, &domain.TableInfo{
        Name: "users",
        Columns: []domain.ColumnInfo{
            {Name: "id", Type: "int64"},
            {Name: "name", Type: "string"},
        },
    })

    // 执行被测试的逻辑
    ds := helper.GetDataSource()
    result, err := ds.Query(ctx, "users", nil)

    // 验证结果
    require.NoError(t, err)  // 如果出错，立即停止
    assert.Equal(t, 2, len(result.Rows))  // 如果不相等，继续执行
}
```

### 3. 测试边界条件

```go
func TestEdgeCases(t *testing.T) {
    tests := []struct {
        name     string
        input    string
        expected string
    }{
        {"empty string", "", ""},
        {"very long string", string(make([]byte, 10000)), "..."},
        {"special characters", "!@#$%^&*()", "!@#$%^&*()"},
        {"unicode", "你好世界", "你好世界"},
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            result := process(tt.input)
            assert.Equal(t, tt.expected, result)
        })
    }
}
```

### 4. Mock 验证

```go
func TestMockVerification(t *testing.T) {
    ctx := context.Background()

    mockExec := session.NewMockExecutor()
    sess := &session.CoreSession{executor: mockExec}

    // 执行操作
    _, err := sess.ExecuteQuery(ctx, "SELECT * FROM users")
    require.NoError(t, err)

    // 验证调用了正确的SQL
    queries := mockExec.GetQueries()
    assert.Equal(t, 1, len(queries))
    assert.Equal(t, "SELECT * FROM users", queries[0])
}
```

## 文件组织

```
pkg/
├── session/
│   ├── core.go              # 核心实现
│   ├── core_unit_test.go    # 纯逻辑单元测试 ✅
│   ├── mock_executor.go     # Mock执行器 ✅
│   └── session_test.go      # 集成测试
├── executor/
│   ├── executor.go
│   ├── executor_test.go     # 执行器测试 ✅
├── testutils/
│   └── helper.go            # 测试辅助工具 ✅
└── parser/
    ├── adapter.go
    └── parser_test.go       # 解析器测试
```

## 测试覆盖率目标

| 模块 | 目标覆盖率 | 当前覆盖率 |
|-----|----------|----------|
| pkg/parser | > 90% | ✅ 已达标 |
| pkg/session | > 80% | 🔄 进行中 |
| pkg/optimizer | > 80% | 🔄 进行中 |
| pkg/api | > 70% | 🔄 进行中 |
| pkg/information_schema | > 70% | 🔄 进行中 |

## 总结

通过使用 `MemoryTestHelper`、`MockSessionExecutor` 和 `MockDataSource`，我们可以在不启动服务器和客户端的情况下，对以下内容进行完整的单元测试：

1. ✅ SQL 解析逻辑
2. ✅ 查询优化逻辑
3. ✅ 会话管理逻辑
4. ✅ 事务处理逻辑
5. ✅ 错误处理逻辑
6. ✅ 表操作逻辑
7. ✅ 数据库切换逻辑

所有测试都是纯逻辑测试，执行速度快，易于调试，不依赖任何外部资源。
