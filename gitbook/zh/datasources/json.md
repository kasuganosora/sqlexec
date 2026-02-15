# JSON 数据源

JSON 数据源将 JSON 数组文件加载到内存中，允许你使用标准 SQL 进行查询。文件格式要求为 JSON 数组，每个元素是一个对象。

## 文件格式

JSON 数据源要求文件内容为一个 JSON 数组，每个元素为一个扁平的 JSON 对象：

```json
[
  {"id": 1, "name": "张三", "age": 28, "city": "北京"},
  {"id": 2, "name": "李四", "age": 35, "city": "上海"},
  {"id": 3, "name": "王五", "age": 22, "city": "广州"}
]
```

## 基本配置

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `name` | string | 是 | 数据源名称，作为数据库标识符（`USE <name>` 切换） |
| `type` | string | 是 | 固定值 `json` |
| `database` | string | 否 | 所属数据库名称 |

## 选项

| 选项 | 默认值 | 说明 |
|------|--------|------|
| `path` | _(必填)_ | JSON 文件路径 |
| `array_root` | _(空)_ | JSON 路径表达式，用于从嵌套结构中提取数组 |
| `writable` | `false` | 是否允许写入操作 |

## 表名

JSON 数据源加载后，数据表名固定为 `json_data`。

## 类型推断

SQLExec 会从采样行中推断每个字段的数据类型：

| JSON 类型 | 推断结果 |
|-----------|----------|
| `number`（整数） | `INT` |
| `number`（浮点数） | `FLOAT` |
| `boolean` | `BOOLEAN` |
| `string` | `TEXT` |
| `null` | 根据其他行推断 |

## 配置示例

### datasources.json

```json
{
  "datasources": [
    {
      "name": "users",
      "type": "json",
      "options": {
        "path": "/data/users.json",
        "writable": "false"
      }
    },
    {
      "name": "api_response",
      "type": "json",
      "options": {
        "path": "/data/response.json",
        "array_root": "data.items"
      }
    }
  ]
}
```

对于嵌套 JSON 结构，可以使用 `array_root` 选项指定数据数组的位置：

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

配置 `"array_root": "data.items"` 将提取 `items` 数组作为数据表。

### 查询示例

```sql
-- 切换到 JSON 数据源
USE users;

-- 查询所有数据
SELECT * FROM json_data;

-- 条件查询
SELECT name, age, city
FROM json_data
WHERE age >= 25
ORDER BY age DESC;

-- 聚合查询
SELECT city, COUNT(*) AS cnt, AVG(age) AS avg_age
FROM json_data
GROUP BY city;
```

## 可写模式

当 `writable` 设置为 `true` 时，可以执行写入操作。SQLExec 使用原子写回机制：先写入临时文件，成功后通过重命名替换原文件，避免数据损坏。

```json
{
  "name": "editable_json",
  "type": "json",
  "options": {
    "path": "/data/config.json",
    "writable": "true"
  }
}
```

```sql
USE editable_json;

-- 插入数据
INSERT INTO json_data (id, name, value) VALUES (4, '新配置项', 'some_value');

-- 更新数据
UPDATE json_data SET value = 'updated_value' WHERE id = 4;

-- 删除数据
DELETE FROM json_data WHERE id = 4;
```

写回流程：

1. 将内存中的数据写入临时文件（同目录下）。
2. 写入成功后，将临时文件重命名为原文件名。
3. 重命名是原子操作，确保文件完整性。

## 注意事项

- 文件在连接时一次性加载到内存，大文件需注意内存占用。
- 表名固定为 `json_data`，无法自定义。
- 仅支持扁平 JSON 对象数组，不支持深层嵌套字段直接映射为列。
- 可写模式下，变更在 `Close()` 时通过原子操作写回文件。
