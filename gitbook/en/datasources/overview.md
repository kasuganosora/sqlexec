# Data Sources Overview

SQLExec provides a unified `DataSource` interface that allows you to query multiple heterogeneous data sources using standard SQL. Whether the underlying source is an in-memory table, a relational database, or a file, the query syntax remains consistent.

## Supported Data Source Types

| Type | Identifier | Read/Write | Description |
|------|-----------|------------|-------------|
| Memory | `memory` | Read/Write | Default in-memory data source with MVCC transaction support |
| MySQL | `mysql` | Read/Write | Connect to external MySQL 5.7+ databases |
| PostgreSQL | `postgresql` | Read/Write | Connect to external PostgreSQL 12+ databases |
| CSV | `csv` | Configurable | Load CSV files and query with SQL |
| JSON | `json` | Configurable | Load JSON array files |
| JSONL | `jsonl` | Configurable | Load JSON Lines files |
| Excel | `excel` | Read-only | Load XLS/XLSX files |
| Parquet | `parquet` | Read-only | Load Apache Parquet columnar files |
| HTTP | `http` | Read-only | Query remote HTTP/REST APIs |
| XML Persistence | `ENGINE=xml` | Read/Write | Per-table XML file persistence with automatic data recovery |
| Badger | `badger` | Read/Write | Embedded persistent storage based on Badger KV |
| Hybrid | `hybrid` | Read/Write | Memory + Badger hybrid storage with per-table persistence |
| Slice | `slice` | Configurable | Wrap Go `[]struct` or `[]map` as SQL tables |

## Architecture

SQLExec's data source management is composed of three core components:

```
DataSourceFactory → Registry → DataSourceManager
```

- **DataSourceFactory**: Creates the corresponding data source instance based on the type identifier.
- **Registry**: Maintains all registered data source factories and supports custom extensions.
- **DataSourceManager**: Manages the lifecycle of data sources, including connection, switching, and shutdown.

## Common Configuration Fields

All data sources share the following common configuration fields:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | Yes | **Data source identifier**, i.e. the database name. Use `USE <name>` to switch to this data source |
| `type` | string | Yes | Data source type identifier (`memory`, `mysql`, `csv`, etc.) |
| `host` | string | Depends | Server address (used by MySQL, PostgreSQL, HTTP) |
| `port` | int | No | Port number (used by MySQL, PostgreSQL) |
| `username` | string | Depends | Username (used by MySQL, PostgreSQL) |
| `password` | string | Depends | Password (used by MySQL, PostgreSQL) |
| `database` | string | Depends | Database name the data source belongs to (meaning varies by type, see table below) |
| `writable` | bool | No | Whether write operations are allowed |
| `options` | object | No | Type-specific advanced options |

### Meaning of the `database` Field

The `database` field defines which database the data source belongs to:

| Data Source Type | `database` Field Meaning | Example |
|-----------------|------------------------|---------|
| MySQL | Remote MySQL database name | `"myapp"` |
| PostgreSQL | Remote PostgreSQL database name | `"analytics"` |
| CSV / JSON / JSONL / Excel / Parquet | Database name it belongs to (optional) | `"analytics"` |
| HTTP | Database name it belongs to (optional) | `"api_db"` |
| Memory | _Not used_ | — |

> **Important**:
> - The `name` field determines the "database" name for this data source within SQLExec. After switching the active data source with `USE <name>`, all subsequent SQL operations execute against that data source.
> - For file-based data sources (CSV, JSON, JSONL, Excel, Parquet), the **file path** should be specified via the `path` field in `options`, not in the `database` field.

## Configuration Methods

SQLExec supports three ways to configure data sources.

### Method 1: datasources.json (Server Mode)

Suitable for standalone SQLExec server deployments. Create a `datasources.json` file in the config directory:

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
      "options": {
        "path": "/data/access_logs.csv"
      }
    }
  ]
}
```

### Method 2: db.RegisterDataSource() (Embedded Mode)

Suitable for embedding SQLExec as a Go library in your application:

```go
package main

import (
    "github.com/kasuganosora/sqlexec/pkg/api"
)

func main() {
    db, _ := api.NewDB(&api.DBConfig{})

    // Register in-memory data source
    db.RegisterDataSource("default", &api.DataSourceConfig{
        Type:     "memory",
        Writable: true,
    })

    // Register MySQL data source
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

### Method 3: SQL Management (Runtime Dynamic Configuration)

SQLExec provides a virtual database called `config` that contains a `datasource` virtual table, allowing you to dynamically manage data sources at runtime using standard SQL statements.

#### View All Data Sources

```sql
SELECT * FROM config.datasource;
```

Returned columns:

| Column | Type | Description |
|--------|------|-------------|
| `name` | varchar(64) | Data source name (primary key) |
| `type` | varchar(32) | Data source type |
| `host` | varchar(255) | Server address |
| `port` | int | Port number |
| `username` | varchar(64) | Username |
| `password` | varchar(128) | Password (displayed as `****`) |
| `database_name` | varchar(128) | Database name it belongs to |
| `writable` | boolean | Whether writable |
| `options` | text | Options in JSON format |
| `status` | varchar(16) | Connection status (`connected` / `disconnected`) |

#### Add a Data Source

```sql
-- Add a MySQL data source
INSERT INTO config.datasource (name, type, host, port, username, password, database_name, writable)
VALUES ('production', 'mysql', 'db.example.com', 3306, 'app_user', 'secret', 'myapp', true);

-- Add a CSV data source (file path goes in options)
INSERT INTO config.datasource (name, type, options)
VALUES ('logs', 'csv', '{"path": "/data/access_logs.csv"}');

-- Add a PostgreSQL data source with options
INSERT INTO config.datasource (name, type, host, port, username, password, database_name, options)
VALUES ('analytics', 'postgresql', 'pg.example.com', 5432, 'analyst', 'pass', 'analytics_db',
        '{"schema": "public", "ssl_mode": "require"}');
```

Newly added data sources take effect **immediately**: a connection is automatically created and registered in the system, and the configuration is persisted to the `datasources.json` file.

#### Modify a Data Source

```sql
-- Change connection address and port
UPDATE config.datasource
SET host = 'new-db.example.com', port = 3307
WHERE name = 'production';

-- Change password
UPDATE config.datasource
SET password = 'new_password'
WHERE name = 'production';
```

After modification, the data source **automatically reconnects** using the new configuration.

#### Delete a Data Source

```sql
DELETE FROM config.datasource WHERE name = 'production';
```

The delete operation disconnects, unregisters from the system, and removes from the configuration file.

#### Conditional Queries

```sql
-- View all MySQL-type data sources
SELECT name, host, port, status FROM config.datasource WHERE type = 'mysql';

-- View all connected data sources
SELECT name, type, status FROM config.datasource WHERE status = 'connected';

-- Fuzzy search
SELECT * FROM config.datasource WHERE name LIKE 'prod%';
```

> **Note**: Data source management operations via SQL are automatically persisted to the `datasources.json` file and remain effective after server restart.

## Multi-Data-Source Queries

Use the `USE` statement to switch the currently active data source. Subsequent queries will be executed against that data source:

```sql
-- Switch to the MySQL data source
USE mydb;

-- Execute a query on MySQL
SELECT * FROM users WHERE status = 'active';

-- Switch to the CSV data source
USE logs;

-- Query data from the CSV file
SELECT ip, COUNT(*) AS cnt FROM csv_data GROUP BY ip ORDER BY cnt DESC LIMIT 10;

-- Switch back to the default in-memory data source
USE default;
```

## DataSource Interface

All data sources implement the unified `DataSource` interface:

| Method | Description |
|--------|-------------|
| `Connect()` | Establish a connection to the data source |
| `Close()` | Close the connection and release resources |
| `Query(sql)` | Execute a query statement and return a result set |
| `Insert(table, rows)` | Insert data rows |
| `Update(table, updates, where)` | Update data matching the conditions |
| `Delete(table, where)` | Delete data matching the conditions |
| `CreateTable(name, schema)` | Create a new table |
| `DropTable(name)` | Drop a table |
| `GetTables()` | Get a list of all table names |
| `GetTableInfo(name)` | Get table schema information (column names, types, etc.) |
| `Execute(sql)` | Execute a non-query statement (DDL, DML) |
| `IsConnected()` | Check if the current connection is valid |
| `IsWritable()` | Check if the data source supports write operations |

Different data source types have varying levels of support for interface methods. Calling write methods on a read-only data source (e.g., Parquet) will return an error.
