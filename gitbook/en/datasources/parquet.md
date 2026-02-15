# Parquet Data Source

The Parquet data source supports loading Apache Parquet columnar storage files, suitable for analyzing large-scale structured data. The Parquet format features efficient compression and encoding schemes and is widely used in big data ecosystems.

## Basic Configuration

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `name` | string | Yes | Data source name, used as the database identifier (`USE <name>` to switch) |
| `type` | string | Yes | Fixed value `parquet` |
| `database` | string | No | Database name this data source belongs to |

## Options

| Option | Default | Description |
|--------|---------|-------------|
| `path` | _(required)_ | Parquet file path |

## Table Name

After loading, the Parquet data source uses a fixed table name of `parquet_data`.

```sql
SELECT * FROM parquet_data;
```

## Read/Write Mode

The Parquet data source is **read-only** and does not support INSERT, UPDATE, DELETE, or other write operations.

## Type Mapping

Parquet files contain complete schema information. SQLExec automatically maps Parquet types to internal types:

| Parquet Type | SQLExec Type |
|-------------|-------------|
| `INT32` | `INT` |
| `INT64` | `INT` |
| `FLOAT` | `FLOAT` |
| `DOUBLE` | `FLOAT` |
| `BOOLEAN` | `BOOLEAN` |
| `BYTE_ARRAY` (UTF8) | `TEXT` |
| `FIXED_LEN_BYTE_ARRAY` | `TEXT` |
| `INT96` (Timestamp) | `TEXT` |

## Configuration Examples

### datasources.json

```json
{
  "datasources": [
    {
      "name": "analytics",
      "type": "parquet",
      "options": {
        "path": "/data/warehouse/events_2025.parquet"
      }
    }
  ]
}
```

### Query Examples

```sql
-- Switch to the Parquet data source
USE analytics;

-- Query data
SELECT * FROM parquet_data LIMIT 10;

-- Aggregation analysis
SELECT
    event_type,
    COUNT(*) AS event_count,
    AVG(duration_ms) AS avg_duration
FROM parquet_data
WHERE event_date >= '2025-01-01'
GROUP BY event_type
ORDER BY event_count DESC;

-- Conditional filtering
SELECT user_id, event_type, event_date
FROM parquet_data
WHERE country = 'CN' AND event_type = 'purchase'
ORDER BY event_date DESC
LIMIT 100;
```

## Notes

- The Parquet data source is read-only; executing write operations will return an error.
- The file is loaded into memory during connection; be mindful of memory usage with large files.
- The table name is fixed as `parquet_data` and cannot be customized.
- Parquet files carry their own schema information, so no type inference is needed.
- Common compression formats such as Snappy, Gzip, and LZ4 are supported.
- Nested types (e.g., `LIST`, `MAP`, `STRUCT`) may not be fully supported; flat structures are recommended.
