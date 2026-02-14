# Config 虚拟数据库设计文档

## 概述

`config` 是一个类似 `information_schema` 的虚拟数据库，用于通过标准 SQL 接口管理系统配置。与 `information_schema`（只读）不同，`config` 数据库支持完整的 CRUD 操作——对虚拟表的修改会持久化到对应的 JSON 配置文件，JSON 文件的修改也会实时反映到查询结果中。

## 架构

```
┌─────────────────────────────────────────────────────┐
│  SQL 层 (SELECT/INSERT/UPDATE/DELETE)                │
├─────────────────────────────────────────────────────┤
│  OptimizedExecutor (查询路由)                        │
│  - isConfigQuery() 检测                              │
│  - getConfigDataSource() 获取可写虚拟数据源           │
├─────────────────────────────────────────────────────┤
│  WritableVirtualDataSource (扩展 VirtualDataSource)  │
│  - 实现完整 DataSource 接口（含写操作）              │
│  - 委托给 WritableVirtualTable                      │
├─────────────────────────────────────────────────────┤
│  ConfigProvider (实现 VirtualTableProvider)          │
│  - 管理所有 config 虚拟表                            │
│  - 注册 datasource 表等                              │
├─────────────────────────────────────────────────────┤
│  WritableVirtualTable 接口 (扩展 VirtualTable)      │
│  - Insert / Update / Delete 方法                     │
├─────────────────────────────────────────────────────┤
│  DatasourceTable (实现 WritableVirtualTable)         │
│  - 读: 从 datasources.json 加载并返回数据源列表      │
│  - 写: 修改后持久化到 datasources.json               │
│  - 写后触发 DataSourceManager 热重载                 │
└─────────────────────────────────────────────────────┘
```

## 权限控制

- 仅特权用户（拥有 GRANT OPTION 权限的用户）可以访问 `config` 数据库
- 非特权用户执行 `USE config` 或查询 `config.*` 时返回 "Access denied"
- `SHOW DATABASES` 中仅对特权用户显示 `config`

## 虚拟表: datasource

### Schema

```sql
CREATE TABLE config.datasource (
    name          VARCHAR(64)  NOT NULL PRIMARY KEY,  -- 数据源名称
    type          VARCHAR(32)  NOT NULL,              -- 类型: memory, mysql, csv, json, excel, sqlite, parquet
    host          VARCHAR(255),                       -- 主机地址
    port          INT,                                -- 端口号
    username      VARCHAR(64),                        -- 用户名
    password      VARCHAR(128),                       -- 密码（查询时显示 ****）
    database_name VARCHAR(128),                       -- 数据库名
    writable      BOOLEAN DEFAULT TRUE,               -- 是否可写
    options       TEXT,                               -- 额外选项 (JSON 格式)
    status        VARCHAR(16)                         -- 状态: connected, disconnected, error
);
```

### 使用示例

```sql
-- 查看所有数据源
USE config;
SELECT * FROM datasource;

-- 或者不切换数据库
SELECT * FROM config.datasource;

-- 添加新数据源
INSERT INTO config.datasource (name, type, host, port, username, password, database_name)
VALUES ('mydb', 'mysql', '127.0.0.1', 3306, 'root', '123456', 'testdb');

-- 修改数据源
UPDATE config.datasource SET host = '192.168.1.100' WHERE name = 'mydb';

-- 删除数据源
DELETE FROM config.datasource WHERE name = 'mydb';
```

### 特殊行为

- **密码保护**: `SELECT` 查询时 `password` 字段显示为 `****`，不暴露原始密码
- **实时状态**: `status` 字段从 DataSourceManager 实时获取连接状态
- **文件同步**: 每次 `SELECT` 都从 `datasources.json` 重新读取，确保与文件修改同步
- **热重载**: `INSERT/UPDATE/DELETE` 操作后立即更新运行时的 DataSourceManager

## JSON 文件格式

`datasources.json` 文件位于配置目录下:

```json
[
  {
    "name": "mydb",
    "type": "mysql",
    "host": "127.0.0.1",
    "port": 3306,
    "username": "root",
    "password": "123456",
    "database": "testdb",
    "writable": true,
    "options": {}
  }
]
```

## 扩展性

`config` 数据库设计为可扩展的框架。未来可以轻松添加新的配置表:

- `config.server` — 服务器配置 (host, port, version 等)
- `config.cache` — 缓存配置
- `config.optimizer` — 优化器配置
- `config.session` — 会话配置
- `config.acl` — 权限配置

每个新表只需实现 `WritableVirtualTable` 接口并在 `ConfigProvider` 中注册即可。
