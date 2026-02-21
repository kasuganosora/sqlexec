# XML 持久化存储

XML 持久化存储允许将单个表的数据持久化为磁盘上的 XML 文件。与其他数据源不同，XML 持久化并不是一个独立的数据源类型，而是作为**内存数据源之上的持久化层** —— 数据存储在内存中以实现快速查询，每次 DML 操作后自动写回到 XML 文件。

当数据库重启并执行 `USE` 命令时，持久化的表会自动从磁盘加载恢复。

## 工作原理

1. `USE mydb` 创建内存数据源（行为不变）
2. `CREATE TABLE ... ENGINE=xml` 在内存中创建表，**同时**将 schema 和数据写入 XML 文件
3. DML 操作（INSERT、UPDATE、DELETE）先在内存中执行，然后写回 XML
4. 重启后，`USE mydb` 扫描 `./database/mydb/` 目录，自动恢复持久化的表及其数据和索引

## 快速开始

```sql
USE mydb;

-- 创建持久化表（默认 file_per_row 模式）
CREATE TABLE users (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(100),
    email VARCHAR(200) UNIQUE
) ENGINE=xml;

-- 每次 DML 操作后数据自动持久化
INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com');
INSERT INTO users (name, email) VALUES ('Bob', 'bob@example.com');

-- 创建索引（同样会被持久化）
CREATE INDEX idx_name ON users (name);

-- 查询正常进行（从内存中读取）
SELECT * FROM users WHERE name = 'Alice';
```

## 存储模式

支持两种存储模式，通过 `CREATE TABLE` 的 `COMMENT` 子句配置。

### file_per_row（默认）

每行数据存储为一个单独的 XML 文件，以主键值命名：

```sql
CREATE TABLE users (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(100)
) ENGINE=xml;
-- 或显式指定：
-- ENGINE=xml COMMENT 'xml_mode=file_per_row'
```

磁盘结构：
```
./database/mydb/users/
├── __schema__.xml     -- 表结构定义
├── __meta__.xml       -- 索引元数据
├── 1.xml              -- <Row id="1" name="Alice" />
└── 2.xml              -- <Row id="2" name="Bob" />
```

### single_file

所有行存储在一个 `data.xml` 文件中：

```sql
CREATE TABLE logs (
    id INT PRIMARY KEY AUTO_INCREMENT,
    msg TEXT,
    level VARCHAR(10)
) ENGINE=xml COMMENT 'xml_mode=single_file';
```

磁盘结构：
```
./database/mydb/logs/
├── __schema__.xml
├── __meta__.xml
└── data.xml           -- 所有行存储在一个文件中
```

`data.xml` 内容示例：
```xml
<?xml version="1.0" encoding="UTF-8"?>
<Rows>
  <Row id="1" msg="Server started" level="INFO" />
  <Row id="2" msg="Connection timeout" level="WARN" />
</Rows>
```

## 文件格式

### Schema 文件（`__schema__.xml`）

存储表结构，包括列名、类型、约束和存储模式：

```xml
<?xml version="1.0" encoding="UTF-8"?>
<TableSchema name="users" engine="xml" rootTag="Row" storageMode="file_per_row">
  <Column name="id" type="INT" nullable="false" primary="true" autoIncrement="true"/>
  <Column name="name" type="VARCHAR" nullable="true"/>
  <Column name="email" type="VARCHAR" nullable="true" unique="true"/>
</TableSchema>
```

### 索引元数据文件（`__meta__.xml`）

存储索引定义，以便在重启时重建索引：

```xml
<?xml version="1.0" encoding="UTF-8"?>
<IndexMeta>
  <Index name="idx_name" table="users" type="btree" unique="false">
    <Column>name</Column>
  </Index>
</IndexMeta>
```

### 行数据文件

每行存储为一个 XML 元素，列值作为属性：

```xml
<Row id="1" name="Alice" email="alice@example.com" />
```

值中的特殊字符（`<`、`>`、`&`、`"`）会被正确转义。

## 配置

### 数据库目录

持久化数据的基础目录可在 `config.json` 中配置：

```json
{
  "database": {
    "database_dir": "./database"
  }
}
```

默认值：`./database`。表存储在 `<database_dir>/<数据库名>/<表名>/` 路径下。

### 嵌入式使用

```go
db, _ := api.NewDB(&api.DBConfig{
    DatabaseDir: "./my_data",
})

session := db.Session()
defer session.Close()

session.Execute("USE mydb")
session.Execute("CREATE TABLE t (id INT PRIMARY KEY, val TEXT) ENGINE=xml")
session.Execute("INSERT INTO t (id, val) VALUES (1, 'hello')")

// 数据已持久化到 ./my_data/mydb/t/
```

## 索引持久化

在持久化表上创建的索引会自动保存到 `__meta__.xml`。重启后，索引会根据元数据自动重建。

支持的索引类型：
- B-Tree 索引
- Hash 索引
- Unique 索引

```sql
-- 对 ENGINE=xml 表的所有索引操作都会被持久化
CREATE INDEX idx_email ON users (email);
CREATE UNIQUE INDEX idx_username ON users (name);
DROP INDEX idx_email ON users;
```

## DROP TABLE

当删除持久化表时，其整个目录（schema、数据、索引）都会从磁盘移除：

```sql
DROP TABLE users;
-- 移除 ./database/mydb/users/ 及其所有文件
```

## 存储模式选择

| 选择依据 | file_per_row | single_file |
|---------|-------------|-------------|
| 适用场景 | 频繁单行更新的表 | 追加写入的表、小表 |
| 磁盘文件 | 每行一个 XML 文件 | 所有行在一个 `data.xml` 中 |
| 可读性 | 方便检查单行数据 | 紧凑、单文件 |
| 写入模式 | 仅重写修改的行文件 | 任何更改都重写整个文件 |

## 注意事项

- XML 持久化基于内存数据源运行。查询性能与纯内存表相同。
- 所有 DML 操作会写回整个表数据（非增量写入）。对于频繁写入的大表，请注意性能影响。
- `ENGINE=xml` 子句从 `CREATE TABLE` 选项中解析。目前仅支持 `xml` 作为持久化引擎。
- 未指定 `ENGINE=xml` 的表仍为纯内存表（默认行为不变）。
- NULL 值在 XML 属性中被省略（不会被持久化为空字符串）。
