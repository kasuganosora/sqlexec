# JSON 函数

SQLExec 提供了一组强大的 JSON 函数，用于从 JSON 数据中提取值、构造 JSON 对象与数组、以及修改和查询 JSON 内容。

## JSON 路径语法

JSON 函数使用路径表达式定位 JSON 中的元素：

| 语法 | 说明 | 示例 |
|------|------|------|
| `$` | 根元素 | `$` |
| `$.key` | 对象的某个键 | `$.name` |
| `$.key1.key2` | 嵌套键 | `$.address.city` |
| `$.array[n]` | 数组的第 n 个元素（从 0 开始） | `$.items[0]` |
| `$.array[*]` | 数组的所有元素 | `$.tags[*]` |

## 函数列表

| 函数 | 说明 | 示例 |
|------|------|------|
| `JSON_EXTRACT(json, path)` | 按路径提取 JSON 值 | `SELECT JSON_EXTRACT('{"name":"Alice"}', '$.name');` -- `'"Alice"'` |
| `JSON_EXTRACT_SCALAR(json, path)` | 按路径提取标量值（返回字符串） | `SELECT JSON_EXTRACT_SCALAR('{"age":30}', '$.age');` -- `'30'` |
| `JSON_SET(json, path, val)` | 设置路径对应的值（存在则更新，不存在则插入） | `SELECT JSON_SET('{"a":1}', '$.b', 2);` -- `'{"a":1,"b":2}'` |
| `JSON_INSERT(json, path, val)` | 仅在路径不存在时插入值 | `SELECT JSON_INSERT('{"a":1}', '$.a', 99);` -- `'{"a":1}'` |
| `JSON_REPLACE(json, path, val)` | 仅在路径存在时替换值 | `SELECT JSON_REPLACE('{"a":1}', '$.a', 99);` -- `'{"a":99}'` |
| `JSON_ARRAY(val1, val2, ...)` | 构造 JSON 数组 | `SELECT JSON_ARRAY(1, 'two', 3);` -- `'[1,"two",3]'` |
| `JSON_OBJECT(k1, v1, k2, v2, ...)` | 构造 JSON 对象 | `SELECT JSON_OBJECT('name', 'Alice', 'age', 30);` -- `'{"name":"Alice","age":30}'` |
| `JSON_KEYS(json)` | 返回 JSON 对象的所有键 | `SELECT JSON_KEYS('{"a":1,"b":2}');` -- `'["a","b"]'` |
| `JSON_LENGTH(json)` | 返回 JSON 数组或对象的元素数量 | `SELECT JSON_LENGTH('[1,2,3]');` -- `3` |
| `JSON_TYPE(json)` | 返回 JSON 值的类型 | `SELECT JSON_TYPE('{"a":1}');` -- `'OBJECT'` |
| `JSON_CONTAINS(json, val)` | 判断 JSON 是否包含指定值 | `SELECT JSON_CONTAINS('[1,2,3]', '2');` -- `true` |
| `JSON_SEARCH(json, mode, val)` | 在 JSON 中搜索值并返回路径 | `SELECT JSON_SEARCH('{"a":"hello"}', 'one', 'hello');` -- `'$.a'` |

## 使用示例

### 提取 JSON 数据

```sql
-- 从 JSON 列提取字段
SELECT id,
       JSON_EXTRACT(profile, '$.name') AS name,
       JSON_EXTRACT(profile, '$.address.city') AS city
FROM users;

-- 提取为标量值（不带引号）
SELECT id,
       JSON_EXTRACT_SCALAR(metadata, '$.version') AS version,
       JSON_EXTRACT_SCALAR(metadata, '$.count') AS count
FROM configs;

-- 提取数组元素
SELECT JSON_EXTRACT(tags, '$[0]') AS first_tag
FROM articles;
```

### 构造 JSON 数据

```sql
-- 构造 JSON 对象
SELECT JSON_OBJECT(
    'id', id,
    'name', name,
    'email', email
) AS user_json
FROM users;

-- 构造 JSON 数组
SELECT JSON_ARRAY(name, age, city) AS user_array
FROM users;
```

### 修改 JSON 数据

```sql
-- 设置或更新值
SELECT JSON_SET(config, '$.theme', 'dark') AS updated_config
FROM user_settings;

-- 仅插入新键（不覆盖已有值）
SELECT JSON_INSERT(config, '$.language', 'zh-CN') AS updated_config
FROM user_settings;

-- 仅替换已有值
SELECT JSON_REPLACE(config, '$.theme', 'light') AS updated_config
FROM user_settings;
```

### 查询与检查

```sql
-- 获取 JSON 对象的键列表
SELECT JSON_KEYS(metadata) AS keys
FROM documents;

-- 检查 JSON 数组长度
SELECT * FROM articles
WHERE JSON_LENGTH(JSON_EXTRACT(data, '$.tags')) > 3;

-- 判断 JSON 值类型
SELECT JSON_TYPE(data) AS data_type
FROM raw_records;

-- 搜索包含特定值的 JSON
SELECT * FROM products
WHERE JSON_CONTAINS(categories, '"electronics"');

-- 查找值在 JSON 中的路径
SELECT JSON_SEARCH(config, 'one', 'admin') AS path
FROM settings;
```

### 综合应用

```sql
-- 从 JSON 日志中提取并分析
SELECT JSON_EXTRACT_SCALAR(log_data, '$.level') AS level,
       COUNT(*) AS count
FROM logs
WHERE JSON_EXTRACT_SCALAR(log_data, '$.level') IN ('ERROR', 'WARN')
GROUP BY JSON_EXTRACT_SCALAR(log_data, '$.level');

-- 动态构造 API 响应
SELECT JSON_OBJECT(
    'status', 'success',
    'data', JSON_OBJECT(
        'user', JSON_OBJECT('id', id, 'name', name),
        'total_orders', order_count
    )
) AS response
FROM user_summary;
```
