# 数据源概述

SQLExec 提供统一的 `DataSource` 接口，允许你使用标准 SQL 查询多种异构数据源。无论底层是内存表、关系型数据库还是文件，查询语法保持一致。

## 支持的数据源类型

| 类型 | 标识符 | 读写 | 说明 |
|------|--------|------|------|
| Memory | `memory` | 读写 | 默认内存数据源，支持 MVCC 事务 |
| MySQL | `mysql` | 读写 | 连接外部 MySQL 5.7+ 数据库 |
| PostgreSQL | `postgresql` | 读写 | 连接外部 PostgreSQL 12+ 数据库 |
| CSV | `csv` | 可配置 | 加载 CSV 文件并以 SQL 查询 |
| JSON | `json` | 可配置 | 加载 JSON 数组文件 |
| JSONL | `jsonl` | 可配置 | 加载 JSON Lines 文件 |
| Excel | `excel` | 只读 | 加载 XLS/XLSX 文件 |
| Parquet | `parquet` | 只读 | 加载 Apache Parquet 列式文件 |
| HTTP | `http` | 只读 | 查询远程 HTTP/REST API |

## 架构

SQLExec 的数据源管理由三个核心组件构成：

```
DataSourceFactory → Registry → DataSourceManager
```

- **DataSourceFactory**：根据类型标识符创建对应的数据源实例。
- **Registry**：维护所有已注册的数据源工厂，支持自定义扩展。
- **DataSourceManager**：管理数据源的生命周期，包括连接、切换和关闭。

## 通用配置字段

所有数据源共享以下通用配置字段：

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `name` | string | 是 | **数据源标识符**，即数据库名称。使用 `USE <name>` 切换到该数据源 |
| `type` | string | 是 | 数据源类型标识（`memory`、`mysql`、`csv` 等） |
| `host` | string | 视类型 | 服务器地址（MySQL、PostgreSQL、HTTP 使用） |
| `port` | int | 否 | 端口号（MySQL、PostgreSQL 使用） |
| `username` | string | 视类型 | 用户名（MySQL、PostgreSQL 使用） |
| `password` | string | 视类型 | 密码（MySQL、PostgreSQL 使用） |
| `database` | string | 视类型 | 数据库名或文件路径（含义因类型而异，见下表） |
| `writable` | bool | 否 | 是否允许写入操作 |
| `options` | object | 否 | 类型特定的高级选项 |

### `database` 字段的含义

`database` 字段在不同类型的数据源中有不同的含义：

| 数据源类型 | `database` 字段含义 | 示例 |
|-----------|-------------------|------|
| MySQL | 远程 MySQL 数据库名称 | `"myapp"` |
| PostgreSQL | 远程 PostgreSQL 数据库名称 | `"analytics"` |
| CSV | CSV 文件路径 | `"/data/logs.csv"` |
| JSON | JSON 文件路径 | `"/data/users.json"` |
| JSONL | JSONL 文件路径 | `"/data/events.jsonl"` |
| Excel | Excel 文件路径 | `"/data/report.xlsx"` |
| Parquet | Parquet 文件路径 | `"/data/events.parquet"` |
| HTTP | _不使用_ | — |
| Memory | _不使用_ | — |

> **重要**：`name` 字段决定了该数据源在 SQLExec 中作为"数据库"的名称。通过 `USE <name>` 切换活动数据源后，后续的所有 SQL 操作都在该数据源上执行。

## 配置方式

SQLExec 支持三种数据源配置方式。

### 方式一：datasources.json（服务器模式）

适用于独立部署的 SQLExec 服务器。在配置目录创建 `datasources.json` 文件：

```json
{
  "datasources": [
    {
      "name": "default",
      "type": "memory",
      "writable": true
    },
    {
      "name": "mydb",
      "type": "mysql",
      "host": "localhost",
      "port": 3306,
      "username": "root",
      "password": "secret",
      "database": "myapp"
    },
    {
      "name": "logs",
      "type": "csv",
      "database": "/data/access_logs.csv"
    }
  ]
}
```

### 方式二：db.RegisterDataSource()（嵌入模式）

适用于将 SQLExec 作为 Go 库嵌入到应用程序中：

```go
package main

import (
    "github.com/kasuganosora/sqlexec/pkg/api"
)

func main() {
    db, _ := api.NewDB(&api.DBConfig{})

    // 注册内存数据源
    db.RegisterDataSource("default", &api.DataSourceConfig{
        Type:     "memory",
        Writable: true,
    })

    // 注册 MySQL 数据源
    db.RegisterDataSource("mydb", &api.DataSourceConfig{
        Type:     "mysql",
        Host:     "localhost",
        Port:     3306,
        Username: "root",
        Password: "secret",
        Database: "myapp",
    })
}
```

### 方式三：SQL 管理（运行时动态配置）

SQLExec 提供了一个名为 `config` 的虚拟数据库，其中包含 `datasource` 虚拟表，允许通过标准 SQL 语句在运行时动态管理数据源。

#### 查看所有数据源

```sql
SELECT * FROM config.datasource;
```

返回的列：

| 列名 | 类型 | 说明 |
|------|------|------|
| `name` | varchar(64) | 数据源名称（主键） |
| `type` | varchar(32) | 数据源类型 |
| `host` | varchar(255) | 服务器地址 |
| `port` | int | 端口号 |
| `username` | varchar(64) | 用户名 |
| `password` | varchar(128) | 密码（显示为 `****`） |
| `database_name` | varchar(128) | 数据库名/文件路径 |
| `writable` | boolean | 是否可写 |
| `options` | text | JSON 格式的选项 |
| `status` | varchar(16) | 连接状态（`connected` / `disconnected`） |

#### 添加数据源

```sql
-- 添加 MySQL 数据源
INSERT INTO config.datasource (name, type, host, port, username, password, database_name, writable)
VALUES ('production', 'mysql', 'db.example.com', 3306, 'app_user', 'secret', 'myapp', true);

-- 添加 CSV 数据源
INSERT INTO config.datasource (name, type, database_name)
VALUES ('logs', 'csv', '/data/access_logs.csv');

-- 添加 PostgreSQL 数据源（带选项）
INSERT INTO config.datasource (name, type, host, port, username, password, database_name, options)
VALUES ('analytics', 'postgresql', 'pg.example.com', 5432, 'analyst', 'pass', 'analytics_db',
        '{"schema": "public", "ssl_mode": "require"}');
```

数据源添加后会**立即生效**：自动创建连接并注册到系统中，同时持久化到 `datasources.json` 文件。

#### 修改数据源

```sql
-- 修改连接地址和端口
UPDATE config.datasource
SET host = 'new-db.example.com', port = 3307
WHERE name = 'production';

-- 修改密码
UPDATE config.datasource
SET password = 'new_password'
WHERE name = 'production';
```

修改后数据源会**自动重连**，使用新的配置参数。

#### 删除数据源

```sql
DELETE FROM config.datasource WHERE name = 'production';
```

删除操作会断开连接、从系统中注销，并从配置文件中移除。

#### 按条件查询

```sql
-- 查看所有 MySQL 类型的数据源
SELECT name, host, port, status FROM config.datasource WHERE type = 'mysql';

-- 查看所有已连接的数据源
SELECT name, type, status FROM config.datasource WHERE status = 'connected';

-- 模糊搜索
SELECT * FROM config.datasource WHERE name LIKE 'prod%';
```

> **注意**：通过 SQL 管理数据源的操作会自动持久化到 `datasources.json` 文件，服务器重启后配置仍然有效。

## 多数据源查询

使用 `USE` 语句切换当前活动数据源，后续查询将在该数据源上执行：

```sql
-- 切换到 MySQL 数据源
USE mydb;

-- 在 MySQL 上执行查询
SELECT * FROM users WHERE status = 'active';

-- 切换到 CSV 数据源
USE logs;

-- 查询 CSV 文件中的数据
SELECT ip, COUNT(*) AS cnt FROM csv_data GROUP BY ip ORDER BY cnt DESC LIMIT 10;

-- 切换回默认内存数据源
USE default;
```

## DataSource 接口

所有数据源都实现了统一的 `DataSource` 接口：

| 方法 | 说明 |
|------|------|
| `Connect()` | 建立与数据源的连接 |
| `Close()` | 关闭连接并释放资源 |
| `Query(sql)` | 执行查询语句，返回结果集 |
| `Insert(table, rows)` | 插入数据行 |
| `Update(table, updates, where)` | 更新符合条件的数据 |
| `Delete(table, where)` | 删除符合条件的数据 |
| `CreateTable(name, schema)` | 创建新表 |
| `DropTable(name)` | 删除表 |
| `GetTables()` | 获取所有表名列表 |
| `GetTableInfo(name)` | 获取表的结构信息（列名、类型等） |
| `Execute(sql)` | 执行非查询语句（DDL、DML） |
| `IsConnected()` | 检查当前连接是否有效 |
| `IsWritable()` | 检查数据源是否支持写入操作 |

不同类型的数据源对接口方法的支持程度不同。只读数据源（如 Parquet）调用写入方法时会返回错误。
