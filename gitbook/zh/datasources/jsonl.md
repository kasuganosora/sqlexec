# JSONL 数据源

JSONL（JSON Lines）数据源加载 JSON Lines 格式的文件，每行一个独立的 JSON 对象。相比标准 JSON，JSONL 更适合处理大规模数据和流式日志。

## 文件格式

JSON Lines 文件中每一行是一个独立的 JSON 对象，行与行之间用换行符分隔：

```
{"id": 1, "event": "login", "user": "张三", "timestamp": "2025-06-01T10:00:00Z"}
{"id": 2, "event": "purchase", "user": "李四", "timestamp": "2025-06-01T10:05:00Z"}
{"id": 3, "event": "logout", "user": "张三", "timestamp": "2025-06-01T10:30:00Z"}
```

## 基本配置

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `name` | string | 是 | 数据源名称，作为数据库标识符（`USE <name>` 切换） |
| `type` | string | 是 | 固定值 `jsonl` |
| `database` | string | 否 | 所属数据库名称 |

## 选项

| 选项 | 默认值 | 说明 |
|------|--------|------|
| `path` | _(必填)_ | JSONL 文件路径 |
| `writable` | `false` | 是否允许写入操作 |
| `skip_errors` | `false` | 是否跳过格式错误的行 |

## 表名

JSONL 数据源加载后，数据表名固定为 `jsonl_data`。

## skip_errors 选项

当 `skip_errors` 设置为 `true` 时，解析过程中遇到格式错误的行将被跳过，而不是中止整个加载过程。适用于日志文件等可能包含不规范数据的场景：

```
{"id": 1, "status": "ok"}
this is not valid json
{"id": 2, "status": "ok"}
```

- `skip_errors=false`（默认）：遇到第二行时报错并终止加载。
- `skip_errors=true`：跳过第二行，继续加载后续数据。

## 解析机制

JSONL 文件使用 `bufio.Scanner` 逐行解析，每行最大支持 **10MB**。超过此限制的行将导致解析错误。

## 配置示例

### datasources.json

```json
{
  "datasources": [
    {
      "name": "events",
      "type": "jsonl",
      "options": {
        "path": "/data/logs/events.jsonl",
        "skip_errors": "true",
        "writable": "false"
      }
    }
  ]
}
```

### 查询示例

```sql
-- 切换到 JSONL 数据源
USE events;

-- 查询所有事件
SELECT * FROM jsonl_data LIMIT 20;

-- 按事件类型统计
SELECT event, COUNT(*) AS cnt
FROM jsonl_data
GROUP BY event
ORDER BY cnt DESC;

-- 按时间范围过滤
SELECT user, event, timestamp
FROM jsonl_data
WHERE timestamp >= '2025-06-01' AND timestamp < '2025-07-01'
ORDER BY timestamp;
```

## 可写模式

与 JSON 数据源相同，JSONL 数据源的可写模式使用原子写回机制：

```json
{
  "name": "writable_events",
  "type": "jsonl",
  "options": {
    "path": "/data/events.jsonl",
    "writable": "true"
  }
}
```

```sql
USE writable_events;

-- 插入新事件
INSERT INTO jsonl_data (id, event, user, timestamp)
VALUES (100, 'signup', '赵六', '2025-06-15T09:00:00Z');

-- 更新事件
UPDATE jsonl_data SET event = 'register' WHERE id = 100;

-- 删除事件
DELETE FROM jsonl_data WHERE id = 100;
```

## JSON vs JSONL 对比

| 特性 | JSON | JSONL |
|------|------|-------|
| 文件格式 | 单个 JSON 数组 `[{...}, {...}]` | 每行一个 JSON 对象 |
| 表名 | `json_data` | `jsonl_data` |
| 解析方式 | 整体解析 | 逐行解析 |
| 内存占用 | 需要一次性加载整个数组 | 逐行加载，峰值内存更低 |
| 容错性 | 任何格式错误导致整体失败 | 支持 `skip_errors` 跳过错误行 |
| 单行大小限制 | 无（受内存限制） | 最大 10MB/行 |
| 适用场景 | 结构化配置、API 响应 | 日志文件、流式数据、大规模数据集 |
| 追加写入 | 需要重写整个文件 | 天然支持追加 |

## 注意事项

- 文件在连接时逐行加载到内存，但最终所有数据仍存在于内存中。
- 单行最大 10MB，超过此限制的行将导致解析错误。
- 表名固定为 `jsonl_data`，无法自定义。
- `skip_errors` 适合处理不完全可控的日志数据，但跳过的行将无法被查询。
- 可写模式下，变更在 `Close()` 时通过原子操作写回文件。
