# JSON Functions

SQLExec provides a powerful set of JSON functions for extracting values from JSON data, constructing JSON objects and arrays, and modifying and querying JSON content.

## JSON Path Syntax

JSON functions use path expressions to locate elements within JSON:

| Syntax | Description | Example |
|--------|-------------|---------|
| `$` | Root element | `$` |
| `$.key` | A key of an object | `$.name` |
| `$.key1.key2` | Nested key | `$.address.city` |
| `$.array[n]` | The nth element of an array (0-based) | `$.items[0]` |
| `$.array[*]` | All elements of an array | `$.tags[*]` |

## Function List

| Function | Description | Example |
|----------|-------------|---------|
| `JSON_EXTRACT(json, path)` | Extract a JSON value by path | `SELECT JSON_EXTRACT('{"name":"Alice"}', '$.name');` -- `'"Alice"'` |
| `JSON_EXTRACT_SCALAR(json, path)` | Extract a scalar value by path (returns a string) | `SELECT JSON_EXTRACT_SCALAR('{"age":30}', '$.age');` -- `'30'` |
| `JSON_SET(json, path, val)` | Set the value at a path (update if exists, insert if not) | `SELECT JSON_SET('{"a":1}', '$.b', 2);` -- `'{"a":1,"b":2}'` |
| `JSON_INSERT(json, path, val)` | Insert a value only if the path does not exist | `SELECT JSON_INSERT('{"a":1}', '$.a', 99);` -- `'{"a":1}'` |
| `JSON_REPLACE(json, path, val)` | Replace a value only if the path exists | `SELECT JSON_REPLACE('{"a":1}', '$.a', 99);` -- `'{"a":99}'` |
| `JSON_ARRAY(val1, val2, ...)` | Construct a JSON array | `SELECT JSON_ARRAY(1, 'two', 3);` -- `'[1,"two",3]'` |
| `JSON_OBJECT(k1, v1, k2, v2, ...)` | Construct a JSON object | `SELECT JSON_OBJECT('name', 'Alice', 'age', 30);` -- `'{"name":"Alice","age":30}'` |
| `JSON_KEYS(json)` | Return all keys of a JSON object | `SELECT JSON_KEYS('{"a":1,"b":2}');` -- `'["a","b"]'` |
| `JSON_LENGTH(json)` | Return the number of elements in a JSON array or object | `SELECT JSON_LENGTH('[1,2,3]');` -- `3` |
| `JSON_TYPE(json)` | Return the type of a JSON value | `SELECT JSON_TYPE('{"a":1}');` -- `'OBJECT'` |
| `JSON_CONTAINS(json, val)` | Check if a JSON value contains a specified value | `SELECT JSON_CONTAINS('[1,2,3]', '2');` -- `true` |
| `JSON_SEARCH(json, mode, val)` | Search for a value in JSON and return the path | `SELECT JSON_SEARCH('{"a":"hello"}', 'one', 'hello');` -- `'$.a'` |

## Usage Examples

### Extracting JSON Data

```sql
-- Extract fields from a JSON column
SELECT id,
       JSON_EXTRACT(profile, '$.name') AS name,
       JSON_EXTRACT(profile, '$.address.city') AS city
FROM users;

-- Extract as scalar values (without quotes)
SELECT id,
       JSON_EXTRACT_SCALAR(metadata, '$.version') AS version,
       JSON_EXTRACT_SCALAR(metadata, '$.count') AS count
FROM configs;

-- Extract array elements
SELECT JSON_EXTRACT(tags, '$[0]') AS first_tag
FROM articles;
```

### Constructing JSON Data

```sql
-- Construct a JSON object
SELECT JSON_OBJECT(
    'id', id,
    'name', name,
    'email', email
) AS user_json
FROM users;

-- Construct a JSON array
SELECT JSON_ARRAY(name, age, city) AS user_array
FROM users;
```

### Modifying JSON Data

```sql
-- Set or update a value
SELECT JSON_SET(config, '$.theme', 'dark') AS updated_config
FROM user_settings;

-- Insert a new key only (do not overwrite existing values)
SELECT JSON_INSERT(config, '$.language', 'zh-CN') AS updated_config
FROM user_settings;

-- Replace only existing values
SELECT JSON_REPLACE(config, '$.theme', 'light') AS updated_config
FROM user_settings;
```

### Querying and Inspecting

```sql
-- Get the list of keys in a JSON object
SELECT JSON_KEYS(metadata) AS keys
FROM documents;

-- Check JSON array length
SELECT * FROM articles
WHERE JSON_LENGTH(JSON_EXTRACT(data, '$.tags')) > 3;

-- Determine the JSON value type
SELECT JSON_TYPE(data) AS data_type
FROM raw_records;

-- Search for JSON containing a specific value
SELECT * FROM products
WHERE JSON_CONTAINS(categories, '"electronics"');

-- Find the path of a value in JSON
SELECT JSON_SEARCH(config, 'one', 'admin') AS path
FROM settings;
```

### Comprehensive Examples

```sql
-- Extract and analyze from JSON logs
SELECT JSON_EXTRACT_SCALAR(log_data, '$.level') AS level,
       COUNT(*) AS count
FROM logs
WHERE JSON_EXTRACT_SCALAR(log_data, '$.level') IN ('ERROR', 'WARN')
GROUP BY JSON_EXTRACT_SCALAR(log_data, '$.level');

-- Dynamically construct API responses
SELECT JSON_OBJECT(
    'status', 'success',
    'data', JSON_OBJECT(
        'user', JSON_OBJECT('id', id, 'name', name),
        'total_orders', order_count
    )
) AS response
FROM user_summary;
```
