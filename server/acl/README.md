# Server ACL (Access Control List) 系统

本文档说明了 Server 层的 ACL 和用户管理机制。

## 概述

Server ACL 系统提供了一个 MySQL/MariaDB 兼容的权限管理功能，包括：

- 用户认证（MySQL native password 协议）
- 权限管理（全局、数据库级、表级、列级）
- JSON 文件持久化（`users.json` 和 `permissions.json`）
- Information Schema 权限视图
- SQL 语句支持（CREATE USER, GRANT, REVOKE 等）

## 架构

```
Server 层
├── ACL Manager (acl/manager.go)
│   ├── User Manager (acl/user_manager.go)
│   ├── Permission Manager (acl/permission_manager.go)
│   └── Authenticator (acl/authenticator.go)
├── MySQL Schema (acl/mysql_schema.go)
│   ├── mysql.user
│   ├── mysql.db
│   ├── mysql.tables_priv
│   └── mysql.columns_priv
└── Information Schema Views (pkg/information_schema/privileges.go)
    ├── USER_PRIVILEGES
    ├── SCHEMA_PRIVILEGES
    ├── TABLE_PRIVILEGES
    └── COLUMN_PRIVILEGES
```

## 数据存储

### users.json

存储用户信息和全局权限：

```json
{
  "users": [
    {
      "host": "%",
      "user": "root",
      "password": "",
      "select_priv": "Y",
      "insert_priv": "Y",
      "update_priv": "Y",
      "delete_priv": "Y",
      "create_priv": "Y",
      "drop_priv": "Y",
      "grant_priv": "Y",
      ...
    }
  ]
}
```

### permissions.json

存储数据库级、表级、列级权限：

```json
{
  "db": [
    {
      "host": "%",
      "db": "test_db",
      "user": "testuser",
      "select_priv": "Y",
      "insert_priv": "Y",
      ...
    }
  ],
  "tables_priv": [],
  "columns_priv": []
}
```

## 权限模型

### 权限级别

1. **全局权限** (Global)
   - 作用范围：所有数据库和所有对象
   - 存储位置：`users.json` (mysql.user 表)
   - 示例：`GRANT ALL ON *.* TO 'user'@'%'`

2. **数据库级权限** (Database)
   - 作用范围：特定数据库的所有对象
   - 存储位置：`permissions.json` (mysql.db 表)
   - 示例：`GRANT SELECT ON testdb.* TO 'user'@'%'`

3. **表级权限** (Table)
   - 作用范围：特定数据库中的特定表
   - 存储位置：`permissions.json` (mysql.tables_priv 表)
   - 示例：`GRANT INSERT ON testdb.users TO 'user'@'%'`

4. **列级权限** (Column)
   - 作用范围：特定表中的特定列
   - 存储位置：`permissions.json` (mysql.columns_priv 表)
   - 示例：`GRANT UPDATE (name) ON testdb.users TO 'user'@'%'`

### 权限类型

支持的标准 MySQL 权限：

- SELECT
- INSERT
- UPDATE
- DELETE
- CREATE
- DROP
- GRANT OPTION (允许授予其他用户权限)
- SUPER
- CREATE USER
- RELOAD
- SHUTDOWN
- PROCESS
- FILE
- REFERENCES
- INDEX
- ALTER
- SHOW DATABASES
- CREATE TEMPORARY TABLES
- LOCK TABLES
- EXECUTE
- REPLICATION SLAVE
- REPLICATION CLIENT
- CREATE VIEW
- SHOW VIEW
- CREATE ROUTINE
- ALTER ROUTINE
- CREATE TABLESPACE
- EVENT
- TRIGGER

## SQL 语法

### CREATE USER

创建新用户：

```sql
CREATE USER 'username'@'host' IDENTIFIED BY 'password';
CREATE USER IF NOT EXISTS 'username'@'host' IDENTIFIED BY 'password';
```

**说明：**
- `host` 可以是主机名、IP 地址或 `%`（任意主机）
- `IDENTIFIED BY` 指定密码（使用 MySQL native password 算法哈希）
- `IF NOT EXISTS` 避免重复创建错误

**示例：**
```sql
CREATE USER 'testuser'@'%' IDENTIFIED BY 'testpass';
```

### DROP USER

删除用户：

```sql
DROP USER 'username'@'host';
DROP USER IF EXISTS 'username'@'host';
```

**示例：**
```sql
DROP USER 'testuser'@'%';
```

### GRANT

授予权限：

```sql
GRANT privilege_type ON database.table TO 'username'@'host' [WITH GRANT OPTION];
GRANT ALL PRIVILEGES ON *.* TO 'root'@'%';
GRANT SELECT, INSERT ON testdb.* TO 'testuser'@'%';
GRANT SELECT (name, email) ON testdb.users TO 'readonly'@'%';
```

**说明：**
- `privilege_type`：权限类型（用逗号分隔多个权限）
- `database.table`：权限范围（`*.*` 表示全局，`db.*` 表示数据库级，`db.table` 表示表级）
- `WITH GRANT OPTION`：允许用户授予其他用户权限

**示例：**
```sql
-- 授予所有全局权限
GRANT ALL PRIVILEGES ON *.* TO 'admin'@'%';

-- 授予数据库级 SELECT 和 INSERT 权限
GRANT SELECT, INSERT ON myapp.* TO 'appuser'@'%';

-- 授予表级特定列的 UPDATE 权限
GRANT UPDATE (name, email) ON myapp.users TO 'editor'@'%';
```

### REVOKE

撤销权限：

```sql
REVOKE privilege_type ON database.table FROM 'username'@'host';
REVOKE ALL PRIVILEGES ON *.* FROM 'testuser'@'%';
REVOKE INSERT ON mydb.* FROM 'appuser'@'%';
```

**示例：**
```sql
-- 撤销所有权限
REVOKE ALL PRIVILEGES ON *.* FROM 'testuser'@'%';

-- 撤销特定权限
REVOKE INSERT, DELETE ON mydb.* FROM 'appuser'@'%';
```

### SET PASSWORD

修改用户密码：

```sql
SET PASSWORD FOR 'username'@'host' = PASSWORD('newpassword');
SET PASSWORD = PASSWORD('newpassword'); -- 为当前用户设置密码
```

**示例：**
```sql
SET PASSWORD FOR 'testuser'@'%' = PASSWORD('newpass123');
```

## Information Schema 表

### USER_PRIVILEGES

显示用户全局权限：

```sql
SELECT * FROM information_schema.USER_PRIVILEGES;
```

**列：**
- `GRANTEE`: 用户标识（格式：`'user'@'host'`）
- `TABLE_CATALOG`: 总是 `'def'`
- `PRIVILEGE_TYPE`: 权限类型
- `IS_GRANTABLE`: 是否可以授予该权限给其他用户（`'YES'`/`'NO'`）

**说明：** 只有拥有 `GRANT OPTION` 的用户才能查询此表。

### SCHEMA_PRIVILEGES

显示数据库级权限：

```sql
SELECT * FROM information_schema.SCHEMA_PRIVILEGES;
```

**列：**
- `GRANTEE`: 用户标识
- `TABLE_CATALOG`: 总是 `'def'`
- `TABLE_SCHEMA`: 数据库名
- `PRIVILEGE_TYPE`: 权限类型
- `IS_GRANTABLE`: 是否可以授予该权限

**说明：** 只有拥有 `GRANT OPTION` 的用户才能查询此表。

### TABLE_PRIVILEGES

显示表级权限：

```sql
SELECT * FROM information_schema.TABLE_PRIVILEGES;
```

**列：**
- `GRANTEE`: 用户标识
- `TABLE_CATALOG`: 总是 `'def'`
- `TABLE_SCHEMA`: 数据库名
- `TABLE_NAME`: 表名
- `PRIVILEGE_TYPE`: 权限类型
- `IS_GRANTABLE`: 是否可以授予该权限

**说明：** 只有拥有 `GRANT OPTION` 的用户才能查询此表。

### COLUMN_PRIVILEGES

显示列级权限：

```sql
SELECT * FROM information_schema.COLUMN_PRIVILEGES;
```

**列：**
- `GRANTEE`: 用户标识
- `TABLE_CATALOG`: 总是 `'def'`
- `TABLE_SCHEMA`: 数据库名
- `TABLE_NAME`: 表名
- `COLUMN_NAME`: 列名
- `PRIVILEGE_TYPE`: 权限类型
- `IS_GRANTABLE`: 是否可以授予该权限

**说明：** 只有拥有 `GRANT OPTION` 的用户才能查询此表。

## 初始化

首次启动服务器时，如果 `users.json` 和 `permissions.json` 不存在，系统会自动创建：

1. **users.json**：创建默认的 `root` 用户
   - 用户名：`root`
   - 主机：`%`（任意主机）
   - 密码：空（无密码）
   - 权限：所有全局权限（包括 GRANT OPTION）

2. **permissions.json**：创建空权限结构
   - `db`: 空数组
   - `tables_priv`: 空数组
   - `columns_priv`: 空数组

## 认证流程

1. **握手阶段**：
   - 服务器发送握手包（包含随机 salt）
   - 客户端发送认证响应（包含密码哈希）

2. **认证验证**：
   - 使用 MySQL native password 算法验证密码
   - 检查用户是否存在
   - 检查主机是否匹配

3. **会话建立**：
   - 认证成功后，会话中保存用户信息
   - 后续 SQL 操作都会进行权限检查

## 权限检查

权限检查遵循以下优先级：

1. **全局权限检查** (mysql.user)
   - 检查用户是否有全局权限
   - 如果有，直接通过

2. **数据库级权限检查** (mysql.db)
   - 如果全局权限为 N，检查数据库级权限
   - 检查用户对特定数据库的权限

3. **表级权限检查** (mysql.tables_priv)
   - 如果数据库级权限为 N，检查表级权限
   - 检查用户对特定表的权限

4. **列级权限检查** (mysql.columns_priv)
   - 如果表级权限为 N，检查列级权限
   - 检查用户对特定列的权限

5. **拒绝访问**：
   - 所有级别都返回 N 时，拒绝访问
   - 返回 MySQL 标准错误码 1227

## 可见性控制

权限表（USER_PRIVILEGES, SCHEMA_PRIVILEGES, TABLE_PRIVILEGES, COLUMN_PRIVILEGES）只对特权用户可见：

- **特权用户定义**：拥有 `GRANT OPTION` 权限的用户
- **普通用户**：
  - 执行 `SHOW TABLES FROM information_schema` 时，权限表不会出现
  - 直接查询权限表时返回权限不足错误

## 安全建议

1. **默认用户**：首次启动后，立即修改 `root` 用户的密码
2. **最小权限原则**：只授予用户所需的最低权限
3. **限制主机**：使用具体的主机名或 IP 地址，避免使用 `%`
4. **定期审计**：定期检查权限表，撤销不必要的权限
5. **密码安全**：使用强密码，避免空密码

## 配置选项

### 数据目录

默认情况下，ACL 文件存储在服务器启动目录：

```
./users.json
./permissions.json
```

可以通过修改 `server/server.go` 中的 `dataDir` 参数来更改。

### 禁用 ACL

如果不需要权限控制，可以在 `NewServer` 中将 `aclManager` 设置为 `nil`：

```go
s := &Server{
    ...
    aclManager: nil,  // 禁用 ACL
}
```

**注意：** 禁用 ACL 后，所有用户都可以无限制访问数据库。

## API 层 vs Server 层

- **Server 层**：实现完整的 ACL 系统，包括认证和权限检查
- **API 层**：作为内嵌库使用，不包含 ACL 功能

设计上，API 层保持简单，专注于数据库操作；Server 层负责认证和授权。

## 故障排除

### 认证失败

**错误：** `Access denied for user 'user'@'host'`

**原因：**
1. 用户不存在
2. 密码错误
3. 主机不匹配

**解决方法：**
1. 检查 `users.json` 中是否存在用户
2. 确认密码正确
3. 确认主机匹配（包括通配符 `%`）

### 权限不足

**错误：** `Access denied; you need the SUPER privilege`

**原因：** 用户没有执行操作所需的权限

**解决方法：**
1. 使用 `GRANT` 授予所需权限
2. 使用特权用户（如 `root`）执行操作

### 权限表不可见

**问题：** `SHOW TABLES FROM information_schema` 不显示权限表

**原因：** 用户没有 `GRANT OPTION` 权限

**解决方法：** 使用 `GRANT ALL ON *.* TO 'user'@'%' WITH GRANT OPTION`

## 示例场景

### 场景 1：创建只读用户

```sql
-- 1. 创建用户
CREATE USER 'readonly'@'%' IDENTIFIED BY 'readonly_pass';

-- 2. 授予 SELECT 权限到特定数据库
GRANT SELECT ON myapp.* TO 'readonly'@'%';
```

### 场景 2：创建应用用户

```sql
-- 1. 创建用户
CREATE USER 'appuser'@'192.168.1.%' IDENTIFIED BY 'app_pass';

-- 2. 授予应用所需的权限
GRANT SELECT, INSERT, UPDATE, DELETE ON myapp.* TO 'appuser'@'192.168.1.%';
```

### 场景 3：创建管理员用户

```sql
-- 1. 创建用户
CREATE USER 'admin'@'%' IDENTIFIED BY 'admin_pass';

-- 2. 授予所有权限（包括 GRANT OPTION）
GRANT ALL PRIVILEGES ON *.* TO 'admin'@'%' WITH GRANT OPTION;
```

### 场景 4：撤销权限

```sql
-- 撤销用户的 DROP 权限
REVOKE DROP ON myapp.* FROM 'appuser'@'192.168.1.%';
```

### 场景 5：修改密码

```sql
-- 修改用户密码
SET PASSWORD FOR 'admin'@'%' = PASSWORD('new_admin_pass');
```

## 性能考虑

- **权限缓存**：ACL Manager 在内存中缓存用户和权限数据
- **延迟写入**：权限修改后批量写回 JSON 文件
- **索引优化**：使用 map 数据结构快速查找用户和权限

## 兼容性

- **MySQL**: 5.7, 8.0+
- **MariaDB**: 10.x, 11.x
- **权限模型**: 完全兼容 MySQL 标准权限系统

## 限制

1. **认证协议**: 当前实现简化认证，完整 MySQL native password 认证需要进一步完善
2. **角色支持**: 暂不支持 MySQL 8.0 的角色功能
3. **密码插件**: 仅支持 mysql_native_password 插件
4. **代理用户**: 暂不支持代理用户功能

## 未来改进

- [ ] 完整的 MySQL native password 认证协议
- [ ] 支持角色 (Roles)
- [ ] 支持密码强度检查
- [ ] 支持密码过期策略
- [ ] 审计日志记录
- [ ] 权限继承机制
