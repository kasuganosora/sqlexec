# System Functions

SQLExec provides a set of system functions for type detection, unique identifier generation, and runtime environment information queries.

## Function List

| Function | Description | Example |
|----------|-------------|---------|
| `TYPEOF(val)` | Return the data type name of a value | `SELECT TYPEOF(42);` -- `'INTEGER'` |
| `COLUMN_TYPE(col)` | Return the declared type of a column | `SELECT COLUMN_TYPE(name) FROM users LIMIT 1;` |
| `UUID()` | Generate a standard UUID (v4) | `SELECT UUID();` -- `'550e8400-e29b-41d4-a716-446655440000'` |
| `UUID_SHORT()` | Generate a short-format unique identifier | `SELECT UUID_SHORT();` -- `'1a2b3c4d5e6f'` |
| `DATABASE()` | Return the current database name | `SELECT DATABASE();` -- `'mydb'` |
| `USER()` | Return the current username | `SELECT USER();` -- `'admin'` |
| `VERSION()` | Return the SQLExec version number | `SELECT VERSION();` -- `'1.0.0'` |

## Detailed Description

### TYPEOF -- Type Detection

Returns the runtime data type name of an expression. Useful for debugging and data validation.

```sql
-- Check the type of different values
SELECT TYPEOF(42);           -- 'INTEGER'
SELECT TYPEOF(3.14);         -- 'FLOAT'
SELECT TYPEOF('hello');      -- 'TEXT'
SELECT TYPEOF(true);         -- 'BOOLEAN'
SELECT TYPEOF(NULL);         -- 'NULL'

-- Check the actual types of data in a table
SELECT id, value, TYPEOF(value) AS value_type
FROM raw_data;

-- Filter data by specific type
SELECT * FROM dynamic_table
WHERE TYPEOF(data) = 'TEXT';
```

### COLUMN_TYPE -- Column Type Query

Returns the declared type of a column in a table (the type defined in the DDL), unlike `TYPEOF` which returns the runtime type.

```sql
-- View the declared type of a column
SELECT COLUMN_TYPE(name) FROM users LIMIT 1;     -- 'VARCHAR(255)'
SELECT COLUMN_TYPE(age) FROM users LIMIT 1;      -- 'INT'
SELECT COLUMN_TYPE(balance) FROM users LIMIT 1;  -- 'DECIMAL(10,2)'

-- Data dictionary query
SELECT COLUMN_TYPE(id) AS id_type,
       COLUMN_TYPE(name) AS name_type,
       COLUMN_TYPE(created_at) AS created_at_type
FROM users
LIMIT 1;
```

### UUID -- Generate Unique Identifiers

Generates an RFC 4122 compliant v4 UUID (128-bit random identifier) in the format `xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx`.

```sql
-- Generate a new UUID
SELECT UUID();
-- '550e8400-e29b-41d4-a716-446655440000'

-- Use as a primary key for records
INSERT INTO events (id, event_type, data)
VALUES (UUID(), 'user_login', '{"user_id": 123}');

-- Batch generate UUIDs
SELECT UUID() AS new_id FROM generate_series(1, 10);
```

### UUID_SHORT -- Short-Format Unique Identifier

Generates a shorter unique identifier, suitable for scenarios with length constraints.

```sql
-- Generate a short UUID
SELECT UUID_SHORT();
-- '1a2b3c4d5e6f'

-- Generate short link identifiers
SELECT UUID_SHORT() AS short_code;
```

### DATABASE -- Current Database

Returns the name of the currently connected database.

```sql
-- View the current database
SELECT DATABASE();
-- 'mydb'

-- Record database information in logs
SELECT DATABASE() AS db_name, NOW() AS query_time;
```

### USER -- Current User

Returns the username of the current connection.

```sql
-- View the current user
SELECT USER();
-- 'admin'

-- Audit log recording
INSERT INTO audit_log (user_name, action, timestamp)
VALUES (USER(), 'data_export', NOW());
```

### VERSION -- Version Information

Returns the version number of the SQLExec engine.

```sql
-- View the version
SELECT VERSION();
-- '1.0.0'

-- Check version compatibility
SELECT IF(VERSION() >= '1.0.0', 'Supported', 'Not supported') AS feature_support;
```

## Usage Examples

### Data Debugging and Diagnostics

```sql
-- Diagnose data type issues
SELECT id, value,
       TYPEOF(value) AS runtime_type,
       COLUMN_TYPE(value) AS declared_type
FROM problematic_table
WHERE TYPEOF(value) != 'INTEGER'
LIMIT 20;

-- System environment information
SELECT DATABASE() AS current_db,
       USER() AS current_user,
       VERSION() AS engine_version;
```

### Unique Identifier Applications

```sql
-- Create a record with a UUID primary key
INSERT INTO documents (id, title, content, created_by, created_at)
VALUES (UUID(), 'New Document', 'Document content...', USER(), NOW());

-- Generate distributed trace IDs
SELECT UUID() AS trace_id, UUID_SHORT() AS span_id;
```

### Data Governance

```sql
-- Record data change logs
INSERT INTO change_log (change_id, db_name, user_name, table_name, action, changed_at)
VALUES (UUID(), DATABASE(), USER(), 'products', 'UPDATE', NOW());
```
