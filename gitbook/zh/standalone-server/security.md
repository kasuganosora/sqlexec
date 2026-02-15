# 安全

SQLExec 提供多层安全机制，涵盖访问控制、数据加密、SQL 注入防护和审计日志。

## 基于角色的访问控制（RBAC）

SQLExec 内置 5 种角色，每种角色拥有不同的操作权限。

### 角色与权限矩阵

| 权限 | admin | moderator | user | readonly | guest |
|------|:-----:|:---------:|:----:|:--------:|:-----:|
| SELECT | ✓ | ✓ | ✓ | ✓ | ✓ |
| INSERT | ✓ | ✓ | ✓ | - | - |
| UPDATE | ✓ | ✓ | ✓ | - | - |
| DELETE | ✓ | ✓ | - | - | - |
| CREATE TABLE | ✓ | ✓ | - | - | - |
| DROP TABLE | ✓ | - | - | - | - |
| 用户管理 | ✓ | - | - | - | - |
| 权限管理 | ✓ | - | - | - | - |
| 审计日志查看 | ✓ | ✓ | - | - | - |

### 角色说明

- **admin** — 超级管理员，拥有所有权限，包括用户管理和权限分配
- **moderator** — 管理员，可执行 DDL 和 DELETE 操作，可查看审计日志
- **user** — 普通用户，可执行 SELECT、INSERT、UPDATE 操作
- **readonly** — 只读用户，仅可执行 SELECT 操作
- **guest** — 访客，仅可执行受限的 SELECT 操作

## 表级权限控制

除了角色级别的权限外，SQLExec 还支持对单个表进行细粒度的权限控制。

### 授权

```sql
-- 授予用户对指定表的 SELECT 权限
GRANT SELECT ON my_database.users TO 'zhangsan';

-- 授予多个权限
GRANT SELECT, INSERT, UPDATE ON my_database.orders TO 'zhangsan';
```

### 撤销

```sql
-- 撤销用户对指定表的权限
REVOKE INSERT ON my_database.orders FROM 'zhangsan';

-- 撤销所有权限
REVOKE ALL ON my_database.users FROM 'zhangsan';
```

表级权限与角色权限共同生效，取两者的交集。即使角色允许某项操作，如果表级权限中未授权，操作仍会被拒绝。

## 数据加密

SQLExec 使用 **AES-256-GCM** 加密算法，支持字段级和记录级两种加密粒度。

### 字段级加密

对表中的敏感字段进行加密存储：

```sql
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT,
    email TEXT ENCRYPTED,
    phone TEXT ENCRYPTED,
    age INTEGER
);
```

标记为 `ENCRYPTED` 的字段在写入时自动加密，读取时自动解密，对应用层透明。

### 记录级加密

对整条记录进行加密：

```sql
CREATE TABLE secrets (
    id INTEGER PRIMARY KEY,
    data TEXT
) WITH ENCRYPTION;
```

### 加密特性

| 特性 | 说明 |
|------|------|
| 算法 | AES-256-GCM |
| 密钥管理 | 通过配置文件指定主密钥 |
| 性能影响 | 加密/解密操作在内存中进行，影响极小 |
| 透明性 | 对客户端完全透明，查询结果自动解密 |

## SQL 注入防护

SQLExec 内置 `sqlescape` 工具，提供安全的 SQL 构建机制，防止 SQL 注入攻击。

### 格式化标识符

使用 `%n` 格式化标识符（表名、列名），自动添加反引号转义：

```go
// 安全的标识符引用
query := sqlescape.Format("SELECT * FROM %n WHERE %n = %?", tableName, columnName, value)
// 输出: SELECT * FROM `users` WHERE `name` = 'zhangsan'
```

### 参数化查询

使用 `%?` 格式化参数值，自动转义特殊字符：

```go
// 安全的参数绑定
query := sqlescape.Format("INSERT INTO %n (%n, %n) VALUES (%?, %?)",
    "users", "name", "email", userName, userEmail)
// 输出: INSERT INTO `users` (`name`, `email`) VALUES ('zhangsan', 'zhangsan@example.com')
```

### 格式说明符

| 说明符 | 用途 | 示例输入 | 示例输出 |
|--------|------|----------|----------|
| `%n` | 标识符（表名、列名） | `users` | `` `users` `` |
| `%?` | 参数值（字符串、数字等） | `O'Brien` | `'O''Brien'` |

### 最佳实践

始终使用参数化查询，避免字符串拼接：

```go
// 正确 - 使用 sqlescape
query := sqlescape.Format("SELECT * FROM %n WHERE %n = %?", table, col, val)

// 错误 - 字符串拼接，存在注入风险
query := "SELECT * FROM " + table + " WHERE " + col + " = '" + val + "'"
```

## 审计日志

SQLExec 内置审计日志系统，基于高性能环形缓冲区（Ring Buffer）实现，记录所有关键操作。

### 事件类型

| 事件类型 | 说明 |
|----------|------|
| `LOGIN` | 用户登录 |
| `LOGOUT` | 用户登出 |
| `QUERY` | 执行 SELECT 查询 |
| `INSERT` | 执行 INSERT 操作 |
| `UPDATE` | 执行 UPDATE 操作 |
| `DELETE` | 执行 DELETE 操作 |
| `DDL` | 执行 DDL 操作（CREATE、DROP 等） |
| `PERMISSION` | 权限变更（GRANT、REVOKE） |
| `INJECTION` | 检测到 SQL 注入尝试 |
| `ERROR` | 操作出错 |
| `API_REQUEST` | HTTP API 请求 |
| `MCP_TOOL_CALL` | MCP 工具调用 |

### 审计级别

| 级别 | 说明 | 典型事件 |
|------|------|----------|
| `Info` | 常规操作 | LOGIN、LOGOUT、QUERY |
| `Warning` | 需要关注的操作 | PERMISSION 变更、异常查询模式 |
| `Error` | 操作失败 | 查询错误、认证失败 |
| `Critical` | 严重安全事件 | INJECTION 检测、权限越权尝试 |

### 日志查询

审计日志支持多种维度的查询方式。

#### 按 Trace-ID 查询

追踪特定请求的完整操作链路：

```sql
SELECT * FROM audit_log WHERE trace_id = 'req-20260215-abc123';
```

#### 按用户查询

查看特定用户的所有操作记录：

```sql
SELECT * FROM audit_log WHERE user = 'zhangsan';
```

#### 按事件类型查询

筛选特定类型的事件：

```sql
SELECT * FROM audit_log WHERE event_type = 'INJECTION';
```

#### 按时间范围查询

查询指定时间段内的审计记录：

```sql
SELECT * FROM audit_log
WHERE timestamp >= '2026-02-15 00:00:00'
  AND timestamp <= '2026-02-15 23:59:59';
```

#### 组合查询

多个条件组合使用：

```sql
SELECT * FROM audit_log
WHERE user = 'zhangsan'
  AND event_type IN ('INSERT', 'UPDATE', 'DELETE')
  AND timestamp >= '2026-02-15 00:00:00'
ORDER BY timestamp DESC
LIMIT 50;
```

## 安全最佳实践

1. **始终使用参数化查询** — 通过 `sqlescape` 的 `%n` 和 `%?` 格式说明符构建 SQL，避免字符串拼接
2. **遵循最小权限原则** — 为每个用户分配满足其需求的最低权限角色
3. **启用表级权限** — 对敏感表设置细粒度的访问控制
4. **加密敏感数据** — 对包含个人信息、密码等敏感字段启用字段级加密
5. **定期审查审计日志** — 关注 `INJECTION` 和 `Critical` 级别的事件
6. **使用 Trace-ID** — 为每个外部请求分配唯一的 Trace-ID，便于问题排查和链路追踪
7. **保护认证凭据** — 不要在代码中硬编码 Token 或密码，使用环境变量或密钥管理服务
