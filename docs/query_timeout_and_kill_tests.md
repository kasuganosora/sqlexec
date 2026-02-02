# 查询超时和Kill功能测试说明

## 测试文件概述

本项目为查询超时和Kill功能编写了完整的测试用例,覆盖 API 层和 Server 层。

## 测试文件列表

### API 层测试 (`pkg/api/`)

#### 1. `query_timeout_test.go`
API 层的测试,包含:
- DBConfig 超时配置测试
- SessionOptions 超时配置测试
- Session 超时优先级测试
- ThreadID 功能测试
- 错误码测试

#### 2. `session/query_timeout_test.go`
CoreSession 层的测试,包含:
- 查询超时功能测试
- Kill 查询功能测试
- 查询注册表功能测试
- 查询 ID 生成测试
- 查询状态测试

### Server 层测试 (`server/tests/`)

#### 1. `query_kill_test.go`
Server 层的测试,包含:
- COM_PROCESS_KILL 命令处理测试
- Kill 查询功能测试
- 查询追踪测试
- 并发安全测试
- 查询状态测试

## 测试覆盖范围

### 配置相关测试

```go
TestDBConfig_QueryTimeout          // 测试DBConfig超时配置
TestSessionOptions_QueryTimeout   // 测试SessionOptions超时配置
TestNewDBWithQueryTimeout      // 测试创建带超时的DB
```

**测试场景**:
- 默认配置(不限制超时)
- 设置短超时(100ms)
- 设置正常超时(30秒)
- 设置长超时(1小时)

### 超时优先级测试

```go
TestSession_QueryTimeoutPriority  // 测试Session超时优先级
TestSession_QueryTimeoutOverride  // 测试Session覆盖DB超时
```

**测试场景**:
- DB和Session都设置 → 优先Session
- DB设置,Session不设置 → 使用DB
- DB不设置,Session设置 → 使用Session
- 都不设置 → 不限制

### ThreadID 相关测试

```go
TestSession_ThreadID  // 测试Session的ThreadID功能
```

**测试场景**:
- ThreadID=0
- ThreadID=1
- ThreadID=123
- ThreadID=MAX_UINT32

### Kill 查询测试

```go
TestKillQueryByThreadID  // 测试通过ThreadID终止查询
```

**测试场景**:
- Kill存在的查询
- Kill不存在的查询
- Kill ThreadID=0

### 查询注册表测试

```go
TestQueryRegistry       // 测试查询注册表
TestQueryIDGeneration  // 测试查询ID生成
```

**测试场景**:
- 注册查询
- 注销查询
- 通过ThreadID查找
- 通过QueryID查找
- 获取所有查询
- 验证ID唯一性

### 查询状态测试

```go
TestQueryStatus             // 测试查询状态
TestQueryDurationTracking  // 测试查询时长跟踪
TestQueryStatusTransitions // 测试查询状态转换
```

**测试场景**:
- 初始状态: running
- 被Kill: canceled
- 超时: timeout
- 时长跟踪: 持续增加

### 并发测试

```go
TestServer_ConcurrentKillQueries     // 测试并发Kill查询
TestServer_QueryRegistryConcurrency  // 测试查询注册表并发安全
```

**测试场景**:
- 并发注册10个查询
- 并发Kill所有查询
- 验证并发安全

### 错误处理测试

```go
TestErrorCode_Timeout       // 测试超时错误码
TestServer_ErrorResponseOnKill  // 测试Kill失败的错误响应
TestServer_KillNonExistentQuery  // 测试Kill不存在的查询
```

## 运行测试

### 运行所有测试

```bash
# 运行 API 层测试
cd d:/code/db
go test ./pkg/api -v -run "QueryTimeout"

# 运行 Server 层测试
go test ./server/tests -v -run "QueryKill"
```

### 运行特定测试

```bash
# 只运行DB配置测试
go test ./pkg/api -v -run "TestDBConfig_QueryTimeout"

# 只运行Kill测试
go test ./server/tests -v -run "TestServer_KillQuery"
```

### 运行所有测试并显示覆盖率

```bash
# API 层
go test ./pkg/api -v -cover

# Server 层
go test ./server/tests -v -cover
```

## 测试数据说明

### 查询 ID 格式

```
{ThreadID}_{timestamp}_{sequence}

示例: 123_1704067200000000_1
```

- **ThreadID**: 会话线程ID
- **timestamp**: 纳秒时间戳
- **sequence**: 原子递增序列号

### 查询状态

| 状态 | 说明 | 触发条件 |
|------|------|----------|
| running | 查询正在执行 | 初始状态 |
| canceled | 查询被Kill | 调用 KillQueryByThreadID |
| timeout | 查询超时 | context.DeadlineExceeded |

### 超时错误码

| 错误码 | 字符串值 | 说明 |
|---------|----------|------|
| ErrCodeTimeout | TIMEOUT | 查询超时或被Kill |
| ErrCodeQueryKilled | QUERY_KILLED | 查询被外部终止 |

## 测试最佳实践

### 1. 单元测试结构

```go
func Test<FunctionName>(t *testing.T) {
	tests := []struct {
		name      string
		input     <inputType>
		expected  <expectedType>
	}{
		// 测试用例...
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 测试逻辑
			// 断言验证
		})
	}
}
```

### 2. 断言使用

```go
assert.Equal(t, expected, actual)      // 相等
assert.NoError(t, err)                // 无错误
assert.Error(t, err)                  // 有错误
assert.True(t, condition)               // 条件为真
assert.Contains(t, str, substr)        // 包含子串
assert.Greater(t, actual, threshold)   // 大于阈值
```

### 3. 辅助函数

使用辅助函数简化测试:
- `setupTestQuery()` - 设置测试查询
- `cleanupTestQuery()` - 清理测试查询

## 测试场景示例

### 场景1: 正常查询(不超时)

```go
db, _ := api.NewDB(&api.DBConfig{QueryTimeout: 30 * time.Second})
session := db.Session()

result, err := session.Query("SELECT 1")
assert.NoError(t, err)
assert.NotNil(t, result)
```

### 场景2: Kill 查询

```go
// 注册查询
queryID := session.GenerateQueryID(123)
// ... 设置查询上下文 ...

// Kill 查询
err := session.KillQueryByThreadID(123)
assert.NoError(t, err)

// 验证查询被取消
qc := session.GetQueryByThreadID(123)
assert.True(t, qc.IsCanceled())
```

### 场景3: 超时查询

```go
// 创建带超时的Session
session := db.SessionWithOptions(&api.SessionOptions{
    QueryTimeout: 100 * time.Millisecond,
})

// 执行长时间查询(会超时)
result, err := session.Query("SELECT * FROM large_table")
if err != nil && api.IsErrorCode(err, api.ErrCodeTimeout) {
    // 查询超时
}
```

## 测试覆盖率目标

| 组件 | 目标覆盖率 |
|--------|----------|
| API 层 (DBConfig/SessionOptions) | 100% |
| API 层 (Session) | 100% |
| CoreSession 层 | 100% |
| QueryRegistry | 100% |
| Server 层 (KILL处理) | 100% |

## 已知限制

1. **集成测试**: 部分测试需要实际的 MySQL 服务器连接
2. **Mock 数据源**: API 层测试使用 mock 数据源,无法验证实际查询
3. **时序测试**: 超时测试依赖实际时间流逝,可能不稳定

## 下一步测试

### 1. 集成测试

创建端到端测试,模拟真实使用场景:
- 客户端连接服务器
- 执行长时间查询
- 从另一个连接发送 KILL 命令
- 验证查询被终止

### 2. 性能测试

测试查询注册表的性能:
- 大量并发查询注册
- 高频 Kill 操作
- 内存占用测试

### 3. 压力测试

长时间运行测试:
- 1000+ 次查询/Kill 操作
- 检测内存泄漏
- 验证资源清理

## 故障排查

### 问题: 测试超时

**原因**: 实际查询执行时间超过超时设置

**解决**: 调整测试中的超时时间
```go
// 从100ms调整到1秒
QueryTimeout: 1 * time.Second
```

### 问题: Kill 不生效

**原因**: 查询已经完成

**解决**: 确保在查询执行期间调用 Kill
```go
// 立即Kill
setupTestQuery(t, threadID)
defer session.KillQueryByThreadID(threadID)
```

### 问题: 测试不稳定

**原因**: 依赖系统时间流逝

**解决**: 增加测试容差
```go
// 预留足够时间
time.Sleep(50 * time.Millisecond)
```

## 总结

测试覆盖了查询超时和Kill功能的所有关键场景:
- ✅ 配置管理(DB/Session两级)
- ✅ 超时机制(context.WithTimeout)
- ✅ Kill 机制(context.WithCancel)
- ✅ 查询追踪(全局注册表)
- ✅ ThreadID 管理
- ✅ 错误处理
- ✅ 并发安全
- ✅ MySQL KILL 协议兼容

所有测试均无编译错误,可以直接运行。
