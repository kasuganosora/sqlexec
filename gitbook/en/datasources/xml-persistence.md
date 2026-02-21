# XML Persistent Storage

XML persistent storage allows individual tables to be persisted as XML files on disk. Unlike other data sources, XML persistence is not a standalone data source type. Instead, it works as a **persistence layer on top of the memory data source** — data is stored in memory for fast queries, and automatically written back to XML files after every DML operation.

When the database is restarted and the `USE` command is executed, persisted tables are automatically loaded from disk.

## How It Works

1. `USE mydb` creates a memory data source (unchanged behavior)
2. `CREATE TABLE ... ENGINE=xml` creates the table in memory **and** writes schema/data to XML files
3. DML operations (INSERT, UPDATE, DELETE) execute in memory first, then write back to XML
4. On restart, `USE mydb` scans `./database/mydb/` and restores persisted tables with their data and indexes

## Quick Start

```sql
USE mydb;

-- Create a persistent table (default: file_per_row mode)
CREATE TABLE users (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(100),
    email VARCHAR(200) UNIQUE
) ENGINE=xml;

-- Data is automatically persisted after each DML
INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com');
INSERT INTO users (name, email) VALUES ('Bob', 'bob@example.com');

-- Create an index (also persisted)
CREATE INDEX idx_name ON users (name);

-- Query works normally (from memory)
SELECT * FROM users WHERE name = 'Alice';
```

## Storage Modes

Two storage modes are available, configured via the `COMMENT` clause on `CREATE TABLE`.

### file_per_row (Default)

Each row is stored as a separate XML file, named by primary key value:

```sql
CREATE TABLE users (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(100)
) ENGINE=xml;
-- Or explicitly:
-- ENGINE=xml COMMENT 'xml_mode=file_per_row'
```

Disk structure:
```
./database/mydb/users/
├── __schema__.xml     -- Table schema definition
├── __meta__.xml       -- Index metadata
├── 1.xml              -- <Row id="1" name="Alice" />
└── 2.xml              -- <Row id="2" name="Bob" />
```

### single_file

All rows are stored in a single `data.xml` file:

```sql
CREATE TABLE logs (
    id INT PRIMARY KEY AUTO_INCREMENT,
    msg TEXT,
    level VARCHAR(10)
) ENGINE=xml COMMENT 'xml_mode=single_file';
```

Disk structure:
```
./database/mydb/logs/
├── __schema__.xml
├── __meta__.xml
└── data.xml           -- All rows in one file
```

`data.xml` content example:
```xml
<?xml version="1.0" encoding="UTF-8"?>
<Rows>
  <Row id="1" msg="Server started" level="INFO" />
  <Row id="2" msg="Connection timeout" level="WARN" />
</Rows>
```

## File Formats

### Schema File (`__schema__.xml`)

Stores table structure including column names, types, constraints, and storage mode:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<TableSchema name="users" engine="xml" rootTag="Row" storageMode="file_per_row">
  <Column name="id" type="INT" nullable="false" primary="true" autoIncrement="true"/>
  <Column name="name" type="VARCHAR" nullable="true"/>
  <Column name="email" type="VARCHAR" nullable="true" unique="true"/>
</TableSchema>
```

### Index Metadata File (`__meta__.xml`)

Stores index definitions so they can be rebuilt on restart:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<IndexMeta>
  <Index name="idx_name" table="users" type="btree" unique="false">
    <Column>name</Column>
  </Index>
</IndexMeta>
```

### Row Data Files

Each row is stored as an XML element with column values as attributes:

```xml
<Row id="1" name="Alice" email="alice@example.com" />
```

Special characters in values (`<`, `>`, `&`, `"`) are properly XML-escaped.

## Configuration

### Database Directory

The base directory for persisted data is configurable in `config.json`:

```json
{
  "database": {
    "database_dir": "./database"
  }
}
```

Default: `./database`. Tables are stored under `<database_dir>/<db_name>/<table_name>/`.

### Embedded Usage

```go
db, _ := api.NewDB(&api.DBConfig{
    DatabaseDir: "./my_data",
})

session := db.Session()
defer session.Close()

session.Execute("USE mydb")
session.Execute("CREATE TABLE t (id INT PRIMARY KEY, val TEXT) ENGINE=xml")
session.Execute("INSERT INTO t (id, val) VALUES (1, 'hello')")

// Data is now persisted to ./my_data/mydb/t/
```

## Index Persistence

Indexes created on persisted tables are automatically saved to `__meta__.xml`. On restart, indexes are rebuilt from the metadata.

Supported index types:
- B-Tree indexes
- Hash indexes
- Unique indexes

```sql
-- All index operations on ENGINE=xml tables are persisted
CREATE INDEX idx_email ON users (email);
CREATE UNIQUE INDEX idx_username ON users (name);
DROP INDEX idx_email ON users;
```

## DROP TABLE

When a persisted table is dropped, its entire directory (schema, data, indexes) is removed from disk:

```sql
DROP TABLE users;
-- Removes ./database/mydb/users/ and all files within
```

## Choosing a Storage Mode

| Criterion | file_per_row | single_file |
|-----------|-------------|-------------|
| Best for | Tables with frequent single-row updates | Append-only tables, small tables |
| Disk files | One XML file per row | One `data.xml` for all rows |
| Readability | Easy to inspect individual rows | Compact, single file |
| Write pattern | Only modified row files are rewritten | Entire file rewritten on any change |

## Notes

- XML persistence works on top of the memory data source. Query performance is the same as memory tables.
- All DML operations write back the entire table data (not incremental). For large tables with frequent writes, consider the performance impact.
- The `ENGINE=xml` clause is parsed from `CREATE TABLE` options. Only `xml` is supported as a persistence engine currently.
- Tables without `ENGINE=xml` remain as pure in-memory tables (default behavior unchanged).
- NULL values are omitted from XML attributes (not persisted as empty strings).
