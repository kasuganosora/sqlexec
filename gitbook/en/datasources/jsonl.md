# JSONL Data Source

The JSONL (JSON Lines) data source loads JSON Lines format files, with one independent JSON object per line. Compared to standard JSON, JSONL is better suited for processing large-scale data and streaming logs.

## File Format

Each line in a JSON Lines file is an independent JSON object, separated by newline characters:

```
{"id": 1, "event": "login", "user": "Zhang San", "timestamp": "2025-06-01T10:00:00Z"}
{"id": 2, "event": "purchase", "user": "Li Si", "timestamp": "2025-06-01T10:05:00Z"}
{"id": 3, "event": "logout", "user": "Zhang San", "timestamp": "2025-06-01T10:30:00Z"}
```

## Basic Configuration

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `name` | string | Yes | Data source name, used as the database identifier (`USE <name>` to switch) |
| `type` | string | Yes | Fixed value `jsonl` |
| `database` | string | Yes | JSONL file path |

## Options

| Option | Default | Description |
|--------|---------|-------------|
| `path` | _(empty)_ | JSON path expression |
| `writable` | `false` | Whether to allow write operations |
| `skip_errors` | `false` | Whether to skip malformed lines |

## Table Name

After loading, the JSONL data source uses a fixed table name of `jsonl_data`.

## skip_errors Option

When `skip_errors` is set to `true`, malformed lines encountered during parsing will be skipped instead of aborting the entire loading process. This is useful for log files and other scenarios that may contain irregular data:

```
{"id": 1, "status": "ok"}
this is not valid json
{"id": 2, "status": "ok"}
```

- `skip_errors=false` (default): Errors out on the second line and aborts loading.
- `skip_errors=true`: Skips the second line and continues loading subsequent data.

## Parsing Mechanism

JSONL files are parsed line by line using `bufio.Scanner`, with a maximum line size of **10MB**. Lines exceeding this limit will cause a parsing error.

## Configuration Examples

### datasources.json

```json
{
  "datasources": [
    {
      "name": "events",
      "type": "jsonl",
      "database": "/data/logs/events.jsonl",
      "options": {
        "skip_errors": "true",
        "writable": "false"
      }
    }
  ]
}
```

### Query Examples

```sql
-- Switch to the JSONL data source
USE events;

-- Query all events
SELECT * FROM jsonl_data LIMIT 20;

-- Count by event type
SELECT event, COUNT(*) AS cnt
FROM jsonl_data
GROUP BY event
ORDER BY cnt DESC;

-- Filter by time range
SELECT user, event, timestamp
FROM jsonl_data
WHERE timestamp >= '2025-06-01' AND timestamp < '2025-07-01'
ORDER BY timestamp;
```

## Writable Mode

Like the JSON data source, the JSONL data source's writable mode uses an atomic write-back mechanism:

```json
{
  "name": "writable_events",
  "type": "jsonl",
  "database": "/data/events.jsonl",
  "options": {
    "writable": "true"
  }
}
```

```sql
USE writable_events;

-- Insert a new event
INSERT INTO jsonl_data (id, event, user, timestamp)
VALUES (100, 'signup', 'Zhao Liu', '2025-06-15T09:00:00Z');

-- Update an event
UPDATE jsonl_data SET event = 'register' WHERE id = 100;

-- Delete an event
DELETE FROM jsonl_data WHERE id = 100;
```

## JSON vs JSONL Comparison

| Feature | JSON | JSONL |
|---------|------|-------|
| File format | Single JSON array `[{...}, {...}]` | One JSON object per line |
| Table name | `json_data` | `jsonl_data` |
| Parsing method | Parsed as a whole | Parsed line by line |
| Memory usage | Requires loading the entire array at once | Loaded line by line, lower peak memory |
| Error tolerance | Any format error causes complete failure | Supports `skip_errors` to skip malformed lines |
| Max line size limit | None (limited by memory) | 10MB per line |
| Use cases | Structured configs, API responses | Log files, streaming data, large-scale datasets |
| Append writes | Requires rewriting the entire file | Naturally supports appending |

## Notes

- The file is loaded line by line into memory during connection, but all data ultimately resides in memory.
- Maximum line size is 10MB; lines exceeding this limit will cause a parsing error.
- The table name is fixed as `jsonl_data` and cannot be customized.
- `skip_errors` is useful for handling log data that is not fully controlled, but skipped lines will not be queryable.
- In writable mode, changes are written back to the file atomically on `Close()`.
