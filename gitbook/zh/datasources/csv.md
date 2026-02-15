# CSV 数据源

CSV 数据源将 CSV 文件加载到内存中，允许你使用标准 SQL 进行查询。适用于快速分析日志文件、导出数据等场景。

## 基本配置

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `name` | string | 是 | 数据源名称，作为数据库标识符（`USE <name>` 切换） |
| `type` | string | 是 | 固定值 `csv` |
| `database` | string | 否 | 所属数据库名称 |

## 选项

| 选项 | 默认值 | 说明 |
|------|--------|------|
| `path` | _(必填)_ | CSV 文件路径 |
| `delimiter` | `,` | 字段分隔符，可设为 `\t`（制表符）、`;`（分号）等 |
| `header` | `true` | 首行是否为列标题 |
| `writable` | `false` | 是否允许写入操作 |

## 表名

CSV 数据源加载后，数据表名固定为 `csv_data`。所有查询都使用此表名：

```sql
SELECT * FROM csv_data;
```

## 类型推断

CSV 文件中所有字段本质上都是字符串。SQLExec 会自动采样前 100 行数据进行类型推断：

| 推断类型 | 匹配规则 |
|----------|----------|
| `INT` | 所有采样值均为整数 |
| `FLOAT` | 所有采样值均为数值（含小数） |
| `BOOLEAN` | 所有采样值为 `true`/`false` |
| `TEXT` | 其他情况 |

## 配置示例

### datasources.json

```json
{
  "datasources": [
    {
      "name": "access_logs",
      "type": "csv",
      "options": {
        "path": "/data/logs/access_log.csv",
        "delimiter": ",",
        "header": "true",
        "writable": "false"
      }
    },
    {
      "name": "tsv_data",
      "type": "csv",
      "options": {
        "path": "/data/export.tsv",
        "delimiter": "\t",
        "header": "true"
      }
    }
  ]
}
```

### 查询示例

```sql
-- 切换到 CSV 数据源
USE access_logs;

-- 查询所有数据
SELECT * FROM csv_data LIMIT 10;

-- 聚合分析
SELECT status_code, COUNT(*) AS cnt
FROM csv_data
GROUP BY status_code
ORDER BY cnt DESC;

-- 条件过滤
SELECT ip, path, response_time
FROM csv_data
WHERE response_time > 1000
ORDER BY response_time DESC
LIMIT 20;
```

## 可写模式

当 `writable` 设置为 `true` 时，可以对 CSV 数据执行插入、更新和删除操作。所有修改都在内存中进行，当数据源关闭（调用 `Close()`）时，变更将写回原始 CSV 文件。

```json
{
  "name": "editable_csv",
  "type": "csv",
  "options": {
    "path": "/data/products.csv",
    "writable": "true"
  }
}
```

```sql
USE editable_csv;

-- 插入新行
INSERT INTO csv_data (name, price, category) VALUES ('新产品', 99.99, '电子产品');

-- 更新数据
UPDATE csv_data SET price = 89.99 WHERE name = '新产品';

-- 删除数据
DELETE FROM csv_data WHERE category = '已下架';
```

## 注意事项

- CSV 文件在连接时一次性加载到内存，大文件需注意内存占用。
- 类型推断基于前 100 行采样，如果后续数据类型不一致可能导致错误。
- 表名固定为 `csv_data`，无法自定义。
- 可写模式下，变更在 `Close()` 时才写回文件，进程异常退出会丢失修改。
