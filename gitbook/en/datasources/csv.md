# CSV Data Source

The CSV data source loads CSV files into memory, allowing you to query them using standard SQL. It is suitable for quickly analyzing log files, exported data, and similar scenarios.

## Basic Configuration

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `name` | string | Yes | Data source name, used as the database identifier (`USE <name>` to switch) |
| `type` | string | Yes | Fixed value `csv` |
| `database` | string | Yes | CSV file path |

## Options

| Option | Default | Description |
|--------|---------|-------------|
| `delimiter` | `,` | Field delimiter; can be set to `\t` (tab), `;` (semicolon), etc. |
| `header` | `true` | Whether the first row is a column header |
| `writable` | `false` | Whether to allow write operations |

## Table Name

After loading, the CSV data source uses a fixed table name of `csv_data`. All queries use this table name:

```sql
SELECT * FROM csv_data;
```

## Type Inference

All fields in a CSV file are inherently strings. SQLExec automatically samples the first 100 rows to infer data types:

| Inferred Type | Matching Rule |
|--------------|---------------|
| `INT` | All sampled values are integers |
| `FLOAT` | All sampled values are numeric (including decimals) |
| `BOOLEAN` | All sampled values are `true`/`false` |
| `TEXT` | All other cases |

## Configuration Examples

### datasources.json

```json
{
  "datasources": [
    {
      "name": "access_logs",
      "type": "csv",
      "database": "/data/logs/access_log.csv",
      "options": {
        "delimiter": ",",
        "header": "true",
        "writable": "false"
      }
    },
    {
      "name": "tsv_data",
      "type": "csv",
      "database": "/data/export.tsv",
      "options": {
        "delimiter": "\t",
        "header": "true"
      }
    }
  ]
}
```

### Query Examples

```sql
-- Switch to the CSV data source
USE access_logs;

-- Query all data
SELECT * FROM csv_data LIMIT 10;

-- Aggregation analysis
SELECT status_code, COUNT(*) AS cnt
FROM csv_data
GROUP BY status_code
ORDER BY cnt DESC;

-- Conditional filtering
SELECT ip, path, response_time
FROM csv_data
WHERE response_time > 1000
ORDER BY response_time DESC
LIMIT 20;
```

## Writable Mode

When `writable` is set to `true`, you can perform insert, update, and delete operations on the CSV data. All modifications are made in memory, and when the data source is closed (by calling `Close()`), changes are written back to the original CSV file.

```json
{
  "name": "editable_csv",
  "type": "csv",
  "database": "/data/products.csv",
  "options": {
    "writable": "true"
  }
}
```

```sql
USE editable_csv;

-- Insert a new row
INSERT INTO csv_data (name, price, category) VALUES ('New Product', 99.99, 'Electronics');

-- Update data
UPDATE csv_data SET price = 89.99 WHERE name = 'New Product';

-- Delete data
DELETE FROM csv_data WHERE category = 'Discontinued';
```

## Notes

- The CSV file is loaded into memory all at once during connection; be mindful of memory usage with large files.
- Type inference is based on the first 100 rows of samples; inconsistent data types in subsequent rows may cause errors.
- The table name is fixed as `csv_data` and cannot be customized.
- In writable mode, changes are only written back to the file on `Close()`; modifications will be lost if the process exits abnormally.
