# Parquet 数据源

Parquet 数据源支持加载 Apache Parquet 列式存储文件，适用于分析大规模结构化数据。Parquet 格式具有高效的压缩和编码方案，广泛用于大数据生态系统。

## 基本配置

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `name` | string | 是 | 数据源名称，作为数据库标识符（`USE <name>` 切换） |
| `type` | string | 是 | 固定值 `parquet` |
| `database` | string | 否 | 所属数据库名称 |

## 选项

| 选项 | 默认值 | 说明 |
|------|--------|------|
| `path` | _(必填)_ | Parquet 文件路径 |

## 表名

Parquet 数据源加载后，数据表名固定为 `parquet_data`。

```sql
SELECT * FROM parquet_data;
```

## 读写模式

Parquet 数据源为**只读**模式，不支持 INSERT、UPDATE、DELETE 等写入操作。

## 类型映射

Parquet 文件包含完整的 schema 信息，SQLExec 会将 Parquet 类型自动映射为内部类型：

| Parquet 类型 | SQLExec 类型 |
|-------------|-------------|
| `INT32` | `INT` |
| `INT64` | `INT` |
| `FLOAT` | `FLOAT` |
| `DOUBLE` | `FLOAT` |
| `BOOLEAN` | `BOOLEAN` |
| `BYTE_ARRAY`（UTF8） | `TEXT` |
| `FIXED_LEN_BYTE_ARRAY` | `TEXT` |
| `INT96`（Timestamp） | `TEXT` |

## 配置示例

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

### 查询示例

```sql
-- 切换到 Parquet 数据源
USE analytics;

-- 查询数据
SELECT * FROM parquet_data LIMIT 10;

-- 聚合分析
SELECT
    event_type,
    COUNT(*) AS event_count,
    AVG(duration_ms) AS avg_duration
FROM parquet_data
WHERE event_date >= '2025-01-01'
GROUP BY event_type
ORDER BY event_count DESC;

-- 条件过滤
SELECT user_id, event_type, event_date
FROM parquet_data
WHERE country = 'CN' AND event_type = 'purchase'
ORDER BY event_date DESC
LIMIT 100;
```

## 注意事项

- Parquet 数据源为只读模式，执行写入操作将返回错误。
- 文件在连接时加载到内存，大文件需注意内存占用。
- 表名固定为 `parquet_data`，无法自定义。
- Parquet 文件自带 schema 信息，无需类型推断。
- 支持 Snappy、Gzip、LZ4 等常见压缩格式。
- 嵌套类型（如 `LIST`、`MAP`、`STRUCT`）可能不完全支持，建议使用扁平结构。
