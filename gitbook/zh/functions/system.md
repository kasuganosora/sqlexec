# 系统函数

SQLExec 提供了一组系统函数，用于类型检测、唯一标识符生成和运行环境信息查询。

## 函数列表

| 函数 | 说明 | 示例 |
|------|------|------|
| `TYPEOF(val)` | 返回值的数据类型名称 | `SELECT TYPEOF(42);` -- `'INTEGER'` |
| `COLUMN_TYPE(col)` | 返回列的声明类型 | `SELECT COLUMN_TYPE(name) FROM users LIMIT 1;` |
| `UUID()` | 生成标准 UUID（v4） | `SELECT UUID();` -- `'550e8400-e29b-41d4-a716-446655440000'` |
| `UUID_SHORT()` | 生成短格式的唯一标识符 | `SELECT UUID_SHORT();` -- `'1a2b3c4d5e6f'` |
| `DATABASE()` | 返回当前数据库名称 | `SELECT DATABASE();` -- `'mydb'` |
| `USER()` | 返回当前用户名 | `SELECT USER();` -- `'admin'` |
| `VERSION()` | 返回 SQLExec 版本号 | `SELECT VERSION();` -- `'1.0.0'` |

## 详细说明

### TYPEOF -- 类型检测

返回表达式的运行时数据类型名称。用于调试和数据验证。

```sql
-- 检查不同值的类型
SELECT TYPEOF(42);           -- 'INTEGER'
SELECT TYPEOF(3.14);         -- 'FLOAT'
SELECT TYPEOF('hello');      -- 'TEXT'
SELECT TYPEOF(true);         -- 'BOOLEAN'
SELECT TYPEOF(NULL);         -- 'NULL'

-- 检查表中数据的实际类型
SELECT id, value, TYPEOF(value) AS value_type
FROM raw_data;

-- 筛选特定类型的数据
SELECT * FROM dynamic_table
WHERE TYPEOF(data) = 'TEXT';
```

### COLUMN_TYPE -- 列类型查询

返回表中某列的声明类型（DDL 中定义的类型），与 `TYPEOF` 返回运行时类型不同。

```sql
-- 查看列的声明类型
SELECT COLUMN_TYPE(name) FROM users LIMIT 1;     -- 'VARCHAR(255)'
SELECT COLUMN_TYPE(age) FROM users LIMIT 1;      -- 'INT'
SELECT COLUMN_TYPE(balance) FROM users LIMIT 1;  -- 'DECIMAL(10,2)'

-- 数据字典查询
SELECT COLUMN_TYPE(id) AS id_type,
       COLUMN_TYPE(name) AS name_type,
       COLUMN_TYPE(created_at) AS created_at_type
FROM users
LIMIT 1;
```

### UUID -- 生成唯一标识符

生成符合 RFC 4122 标准的 v4 UUID（128 位随机标识符），格式为 `xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx`。

```sql
-- 生成新的 UUID
SELECT UUID();
-- '550e8400-e29b-41d4-a716-446655440000'

-- 作为记录的主键
INSERT INTO events (id, event_type, data)
VALUES (UUID(), 'user_login', '{"user_id": 123}');

-- 批量生成 UUID
SELECT UUID() AS new_id FROM generate_series(1, 10);
```

### UUID_SHORT -- 短格式唯一标识符

生成较短的唯一标识符，适合对长度有要求的场景。

```sql
-- 生成短 UUID
SELECT UUID_SHORT();
-- '1a2b3c4d5e6f'

-- 生成短链接标识
SELECT UUID_SHORT() AS short_code;
```

### DATABASE -- 当前数据库

返回当前连接的数据库名称。

```sql
-- 查看当前数据库
SELECT DATABASE();
-- 'mydb'

-- 在日志中记录数据库信息
SELECT DATABASE() AS db_name, NOW() AS query_time;
```

### USER -- 当前用户

返回当前连接的用户名称。

```sql
-- 查看当前用户
SELECT USER();
-- 'admin'

-- 审计日志记录
INSERT INTO audit_log (user_name, action, timestamp)
VALUES (USER(), 'data_export', NOW());
```

### VERSION -- 版本信息

返回 SQLExec 引擎的版本号。

```sql
-- 查看版本
SELECT VERSION();
-- '1.0.0'

-- 检查版本兼容性
SELECT IF(VERSION() >= '1.0.0', '支持', '不支持') AS feature_support;
```

## 使用示例

### 数据调试与诊断

```sql
-- 诊断数据类型问题
SELECT id, value,
       TYPEOF(value) AS runtime_type,
       COLUMN_TYPE(value) AS declared_type
FROM problematic_table
WHERE TYPEOF(value) != 'INTEGER'
LIMIT 20;

-- 系统环境信息
SELECT DATABASE() AS current_db,
       USER() AS current_user,
       VERSION() AS engine_version;
```

### 唯一标识符应用

```sql
-- 创建具有 UUID 主键的记录
INSERT INTO documents (id, title, content, created_by, created_at)
VALUES (UUID(), '新文档', '文档内容...', USER(), NOW());

-- 生成分布式追踪 ID
SELECT UUID() AS trace_id, UUID_SHORT() AS span_id;
```

### 数据治理

```sql
-- 记录数据变更日志
INSERT INTO change_log (change_id, db_name, user_name, table_name, action, changed_at)
VALUES (UUID(), DATABASE(), USER(), 'products', 'UPDATE', NOW());
```
