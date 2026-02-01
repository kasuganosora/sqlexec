---
name: server-test
description: 提供MySQL Server测试用例创建和管理的专门指导，包括协议测试、端对端测试、错误处理、并发和性能测试
---

# MySQL Server测试用例创建Skill

这个skill提供了创建、运行和管理MySQL Server测试用例的完整指导，涵盖协议层、业务逻辑、错误处理和性能测试。

## 使用场景

当用户需要以下任务时使用此skill：

- **创建新的server测试用例** - 针对特定功能或场景编写测试
- **测试MySQL协议功能** - 握手、认证、命令序列号、数据包处理
- **端对端功能测试** - 使用Go的database/sql库进行真实的协议交互测试
- **错误处理测试** - 不存在的表、语法错误、边界条件
- **并发和性能测试** - 多连接、高并发、压力测试
- **修复测试失败** - 根据错误日志定位问题并修复

## Skill内容

此skill包含以下可重用资源：

- **SKILL.md** (本文件) - 核心指导文档
- **scripts/** - 测试生成脚本和工具
- **references/** - 测试最佳实践和参考文档
- **assets/** - 测试模板和代码片段

## 快速开始

### 创建基础测试用例

使用测试模板快速开始：

```bash
# 使用交互式工具
python .codebuddy/skills/server-test/scripts/generate_test.py --type protocol --name test_handshake

# 或手动创建测试文件
复制 .codebuddy/skills/server-test/assets/test_template.go 到 integration/目录
```

### 运行现有测试

```bash
# 运行所有server测试
go test -v -run "TestServer_" ./server

# 运行协议测试
go test -v -run "TestProtocol_" ./server/protocol

# 运行端对端测试
go test -v -run "TestE2E_" ./integration
```

## 测试类型

### 1. 协议层测试 (Protocol Tests)

**目标**: 验证MySQL协议实现的正确性

**测试范围**:
- 握手包 (HandshakeV10Packet) - 序列号、字段完整性
- 认证流程 - 握手响应、OK包发送
- 序列号管理 - 握手阶段、命令阶段、重置逻辑
- 数据包编解码 - Marshal/Unmarshal正确性
- 命令包处理 - COM_PING、COM_QUERY、COM_INIT_DB等

**关键文件**:
- `server/protocol/packet.go` - 数据包定义和编解码
- `server/protocol/com_init_db_test.go` - COM_INIT_DB测试
- `server/server.go` - 协议处理逻辑

**最佳实践**:
```go
func TestProtocol_SequenceNumberReset(t *testing.T) {
    // 验证握手后的序列号重置
    testServer := mysqltest.NewTestServer()
    testServer.Start(13306)
    defer testServer.Stop()
    
    testServer.RunWithClient(func(conn *sql.DB) error {
        // 握手后第一个命令序列号应为0
        conn.Exec("PING")
        
        // 第二个命令序列号应为0（已重置）
        conn.Exec("SELECT 1")
        
        // 验证连接保持活跃
        conn.Ping()
        return nil
    })
}
```

### 2. 端对端测试 (End-to-End Tests)

**目标**: 验证完整的客户端-服务器交互

**测试范围**:
- 连接建立和认证
- 数据库切换 (USE命令)
- 查询执行 (SELECT、INSERT、UPDATE、DELETE)
- information_schema查询
- 错误包发送
- 连接保活

**关键文件**:
- `integration/end_to_end_test.go` - 主要的E2E测试集合
- `integration/scenarios_test.go` - 用户场景测试

**测试结构**:
```go
func TestE2E_CompleteWorkflow(t *testing.T) {
    testServer := mysqltest.NewTestServer()
    testServer.Start(13306)
    defer testServer.Stop()
    
    testServer.RunWithClient(func(conn *sql.DB) error {
        // 1. 认证和连接
        err := conn.Ping()
        require.NoError(t, err)
        
        // 2. 创建和查询数据
        conn.Exec("CREATE TABLE test (id INT, name VARCHAR(50))")
        
        // 3. 切换数据库
        conn.Exec("USE test_db")
        
        // 4. 验证当前数据库
        var dbName string
        conn.QueryRow("SELECT DATABASE()").Scan(&dbName)
        assert.Equal(t, "test_db", dbName)
        
        // 5. 查询information_schema
        var count int
        conn.QueryRow("SELECT COUNT(*) FROM information_schema.schemata").Scan(&count)
        assert.Greater(t, count, 0)
        
        return nil
    })
}
```

### 3. 错误处理测试 (Error Handling Tests)

**目标**: 验证server正确处理错误情况

**测试范围**:
- 不存在的表/列
- 无效的SQL语法
- 权限错误
- 数据类型不匹配
- 边界条件（空查询、超大包等）

**错误验证要点**:
```go
func TestError_NonExistentTable(t *testing.T) {
    testServer := mysqltest.NewTestServer()
    testServer.Start(13307)
    defer testServer.Stop()
    
    testServer.RunWithClient(func(conn *sql.DB) error {
        // 查询不存在的表
        _, err := conn.Query("SELECT * FROM non_existent_table")
        
        // 验证返回错误
        assert.Error(t, err)
        
        // 验证错误信息包含关键信息
        assert.Contains(t, err.Error(), "not found")
        
        // 验证连接仍然有效（不是致命错误）
        err = conn.Ping()
        assert.NoError(t, err)
        
        return nil
    })
}
```

### 4. 并发和性能测试 (Concurrency & Performance Tests)

**目标**: 验证server在高并发下的稳定性

**测试范围**:
- 并发查询
- 连接池压力
- 长时间运行的查询
- 内存泄漏检测
- 死锁检测

**并发测试模式**:
```go
func TestConcurrency_10ConcurrentQueries(t *testing.T) {
    testServer := mysqltest.NewTestServer()
    testServer.Start(13308)
    defer testServer.Stop()
    
    testServer.RunWithClient(func(conn *sql.DB) error {
        errChan := make(chan error, 10)
        
        // 启动10个并发查询
        for i := 0; i < 10; i++ {
            go func(id int) {
                var dbName string
                errChan <- conn.QueryRow("SELECT DATABASE()").Scan(&dbName)
            }(i)
        }
        
        // 等待所有查询完成
        successCount := 0
        for i := 0; i < 10; i++ {
            if <-errChan == nil {
                successCount++
            }
        }
        
        // 验证所有查询都成功
        assert.Equal(t, 10, successCount)
        return nil
    })
}
```

## 测试工具和脚本

### 使用TestServer工具

所有server测试应使用`pkg/mysqltest/test_server.go`中提供的`TestServer`工具：

```go
import "github.com/kasuganosora/sqlexec/pkg/mysqltest"

func TestExample(t *testing.T) {
    testServer := mysqltest.NewTestServer()
    
    // 启动服务器（自动选择可用端口）
    err := testServer.Start(0) // 0表示自动选择端口
    require.NoError(t, err)
    defer testServer.Stop()
    
    // 获取服务器端口
    port := testServer.GetPort()
    t.Logf("Server running on port %d", port)
    
    // 使用客户端连接
    testServer.RunWithClient(func(conn *sql.DB) error {
        // 执行测试逻辑
        conn.Exec("SHOW DATABASES")
        return nil
    })
}
```

### 创建测试表和数据

```go
// 创建测试表
err := testServer.CreateTestTable("users", []domain.ColumnInfo{
    {Name: "id", Type: "int", Primary: true, AutoIncrement: true, Nullable: false},
    {Name: "name", Type: "string", Nullable: false},
    {Name: "email", Type: "string", Nullable: false},
})
require.NoError(t, err)

// 插入测试数据
err = testServer.InsertTestData("users", []domain.Row{
    {"id": 1, "name": "Alice", "email": "alice@example.com"},
    {"id": 2, "name": "Bob", "email": "bob@example.com"},
})
require.NoError(t, err)
```

## 常见问题和解决方案

### 问题1: 测试超时或挂起

**症状**: 测试运行超时或无限等待

**解决方法**:
- 添加超时设置
- 使用context.WithTimeout
- 检查goroutine泄漏

```go
ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()

err := conn.QueryContext(ctx, "SELECT * FROM large_table")
if errors.Is(err, context.DeadlineExceeded) {
    t.Fatal("Test timed out")
}
```

### 问题2: 端口冲突

**症状**: "Address already in use"错误

**解决方法**:
- 使用端口0自动选择可用端口
- 确保defer Stop()被调用
- 使用不同端口运行并发测试

```go
// 方式1: 自动选择端口
testServer := mysqltest.NewTestServer()
testServer.Start(0) // 0 = 自动选择

// 方式2: 指定端口
testServer.Start(13306)
defer testServer.Stop() // 确保释放端口
```

### 问题3: 序列号不匹配

**症状**: "Packets out of order"或"unexpected sequence"错误

**解决方法**:
- 参考MariaDB源码中的序列号规则
- 握手: 0, 认证响应: 1, OK: 2
- 命令阶段: 每个命令开始时重置为0

```go
// 参考: server/server.go
func (s *Server) handleCommand(...) {
    // 每个新命令开始时重置序列号
    sess.ResetSequenceID()
    
    // 处理命令
    switch commandType {
    case COM_PING:
        return s.handleComPing(...)
    }
}
```

## 测试覆盖检查清单

创建测试时，确保覆盖以下方面：

### 协议层
- [ ] 握手包序列号为0
- [ ] 认证响应处理
- [ ] 认证OK包序列号为2
- [ ] 命令阶段序列号重置为0
- [ ] 所有命令类型 (COM_QUERY, COM_PING, COM_INIT_DB等)
- [ ] 错误包格式正确

### 功能层
- [ ] 数据库切换 (USE命令)
- [ ] SELECT DATABASE()函数
- [ ] information_schema查询（带前缀和不带前缀）
- [ ] CRUD操作 (INSERT, UPDATE, DELETE)
- [ ] SHOW命令

### 错误处理
- [ ] 不存在的表/列
- [ ] SQL语法错误
- [ ] 权限错误
- [ ] 连接断开后的处理
- [ ] 错误后连接仍然有效

### 并发和性能
- [ ] 并发查询 (10+个goroutine)
- [ ] 连接池管理
- [ ] 长时间运行查询
- [ ] 大数据集查询 (1000+行)
- [ ] 内存泄漏检测

### 边界条件
- [ ] 空查询
- [ ] 超大SQL语句
- [ ] 特殊字符处理
- [ ] Unicode和emoji支持
- [ ] Null值处理

## 测试文件命名规范

使用一致的命名约定便于查找和维护：

- 协议测试: `TestProtocol_*`
- E2E测试: `TestE2E_*`
- 场景测试: `TestScenario_*`
- 错误测试: `TestError_*`
- 并发测试: `TestConcurrency_*`
- 性能测试: `TestPerformance_*`

示例:
```
✓ TestProtocol_HandshakeSequence
✓ TestE2E_SelectDatabase
✓ TestScenario_DatabaseSwitching
✓ TestError_NonExistentTable
✓ TestConcurrency_10ConcurrentQueries
✓ TestPerformance_LargeResultSet
```

## 运行和调试

### 运行测试

```bash
# 运行单个测试
go test -v -run TestProtocol_HandshakeSequence ./server/protocol

# 运行所有协议测试
go test -v -run TestProtocol_ ./server/protocol

# 运行所有integration测试
go test -v ./integration

# 查看测试覆盖率
go test -cover ./integration > coverage.txt
go tool cover -html=coverage.txt
```

### 调试失败的测试

```bash
# 1. 运行测试并查看详细输出
go test -v -run TestFailingTest ./server

# 2. 查看服务器日志
# Server日志会输出到stdout，查看错误信息
go test -v 2>&1 | grep -i error

# 3. 使用Delve调试
dlv test ./server -test.run TestFailingTest

# 4. 检查race condition
go test -race ./integration
```

## 参考资源

### 相关文档和示例
- `integration/end_to_end_test.go` - E2E测试参考实现
- `integration/scenarios_test.go` - 场景测试参考实现
- `server/protocol/com_init_db_test.go` - 协议测试示例
- `pkg/mysqltest/test_server.go` - TestServer工具文档

### MariaDB协议参考
参考MariaDB源码理解正确的协议实现：
- 握手和认证流程
- 序列号管理规则
- 数据包格式定义
- 错误码处理

相关源码文件：
- `mariadb-server/sql/net_serv.cc` - 网络层实现
- `mariadb-server/sql/sql_connect.cc` - 连接处理
- `mariadb-server/sql/sql_acl.cc` - 认证逻辑

## 技巧和最佳实践

### 1. 测试隔离

每个测试应该：
- 独立运行（不依赖其他测试的状态）
- 使用defer确保资源清理
- 使用不同的端口避免冲突
- 创建独立的测试表和数据

### 2. 断言使用

- 使用require.NoError/fatal验证设置条件
- 使用assert.Equal/Contains验证结果
- 添加清晰的错误消息帮助调试
```go
require.NoError(t, err, "Failed to start test server")
assert.Equal(t, expected, actual, "Database name should match")
assert.Contains(t, err.Error(), "not found", "Error message should indicate table not found")
```

### 3. 日志记录

- 使用t.Log记录重要步骤
- 使用t.Logf记录详细信息
- 避免在循环中过度记录
```go
t.Logf("Starting test with %d rows", rowCount)
t.Log("Executing query:", query)
```

### 4. 表驱动测试

使用子测试减少重复代码：
```go
func testCases := []struct {
    name     string
    input    string
    expected  string
}{
    {"simple query", "SELECT 1", "1"},
    {"empty db", "SELECT DATABASE()", ""},
    // ... more test cases
}

for _, tc := range testCases {
    t.Run(tc.name, func(t *testing.T) {
        result := executeQuery(tc.input)
        assert.Equal(t, tc.expected, result)
    })
}
```

### 5. Mock和Real测试

- 使用`mysqltest.NewTestServer()`进行真实协议测试
- 使用Mock测试业务逻辑（无需完整协议栈）
- 区分单元测试和集成测试

```go
// 集成测试 - 使用真实server
func TestIntegration_RealServer(t *testing.T) {
    testServer := mysqltest.NewTestServer()
    testServer.Start(13306)
    defer testServer.Stop()
    // ... test with real protocol
}

// 单元测试 - 使用mock
func TestUnit_WithMock(t *testing.T) {
    mockSession := &MockSession{}
    // ... test business logic only
}
```

## 扩展这个Skill

要扩展此skill，可以：

1. **添加新的测试生成脚本** - 在`scripts/`目录中
2. **创建新的测试模板** - 在`assets/`目录中
3. **更新最佳实践文档** - 在`references/`目录中
4. **添加新的测试类型指南** - 在SKILL.md中

扩展时，保持相同的结构和命名约定。
