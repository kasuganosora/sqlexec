# JSON Data Source

The JSON data source loads JSON array files into memory, allowing you to query them using standard SQL. The file format must be a JSON array where each element is an object.

## File Format

The JSON data source requires the file content to be a JSON array, with each element being a flat JSON object:

```json
[
  {"id": 1, "name": "Zhang San", "age": 28, "city": "Beijing"},
  {"id": 2, "name": "Li Si", "age": 35, "city": "Shanghai"},
  {"id": 3, "name": "Wang Wu", "age": 22, "city": "Guangzhou"}
]
```

## Basic Configuration

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `name` | string | Yes | Data source name, used as the database identifier (`USE <name>` to switch) |
| `type` | string | Yes | Fixed value `json` |
| `database` | string | Yes | JSON file path |

## Options

| Option | Default | Description |
|--------|---------|-------------|
| `path` | _(empty)_ | JSON path expression for extracting an array from a nested structure |
| `writable` | `false` | Whether to allow write operations |

## Table Name

After loading, the JSON data source uses a fixed table name of `json_data`.

## Type Inference

SQLExec infers the data type of each field from sampled rows:

| JSON Type | Inferred Result |
|-----------|----------------|
| `number` (integer) | `INT` |
| `number` (floating point) | `FLOAT` |
| `boolean` | `BOOLEAN` |
| `string` | `TEXT` |
| `null` | Inferred from other rows |

## Configuration Examples

### datasources.json

```json
{
  "datasources": [
    {
      "name": "users",
      "type": "json",
      "database": "/data/users.json",
      "options": {
        "writable": "false"
      }
    },
    {
      "name": "api_response",
      "type": "json",
      "database": "/data/response.json",
      "options": {
        "path": "data.items"
      }
    }
  ]
}
```

For nested JSON structures, you can use the `path` option to specify the location of the data array:

```json
{
  "status": "ok",
  "data": {
    "items": [
      {"id": 1, "name": "item1"},
      {"id": 2, "name": "item2"}
    ]
  }
}
```

Setting `"path": "data.items"` will extract the `items` array as the data table.

### Query Examples

```sql
-- Switch to the JSON data source
USE users;

-- Query all data
SELECT * FROM json_data;

-- Conditional query
SELECT name, age, city
FROM json_data
WHERE age >= 25
ORDER BY age DESC;

-- Aggregation query
SELECT city, COUNT(*) AS cnt, AVG(age) AS avg_age
FROM json_data
GROUP BY city;
```

## Writable Mode

When `writable` is set to `true`, write operations can be performed. SQLExec uses an atomic write-back mechanism: it first writes to a temporary file, and upon success, renames it to replace the original file, preventing data corruption.

```json
{
  "name": "editable_json",
  "type": "json",
  "database": "/data/config.json",
  "options": {
    "writable": "true"
  }
}
```

```sql
USE editable_json;

-- Insert data
INSERT INTO json_data (id, name, value) VALUES (4, 'New Config Item', 'some_value');

-- Update data
UPDATE json_data SET value = 'updated_value' WHERE id = 4;

-- Delete data
DELETE FROM json_data WHERE id = 4;
```

Write-back process:

1. Write in-memory data to a temporary file (in the same directory).
2. After a successful write, rename the temporary file to the original file name.
3. The rename is an atomic operation, ensuring file integrity.

## Notes

- The file is loaded into memory all at once during connection; be mindful of memory usage with large files.
- The table name is fixed as `json_data` and cannot be customized.
- Only flat JSON object arrays are supported; deeply nested fields cannot be directly mapped to columns.
- In writable mode, changes are written back to the file atomically on `Close()`.
