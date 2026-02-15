# Excel 数据源

Excel 数据源支持加载 XLS 和 XLSX 格式的电子表格文件，允许你使用 SQL 查询 Excel 数据。

## 基本配置

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `name` | string | 是 | 数据源名称，作为数据库标识符（`USE <name>` 切换） |
| `type` | string | 是 | 固定值 `excel` |
| `database` | string | 是 | Excel 文件路径（.xls 或 .xlsx） |

## 选项

| 选项 | 默认值 | 说明 |
|------|--------|------|
| `sheet` | _(第一个工作表)_ | 工作表名称或索引（从 0 开始） |

## 表名

Excel 数据源的表名基于工作表名称。例如，工作表名为 `Sheet1`，则表名为 `Sheet1`。

```sql
SELECT * FROM Sheet1;
```

如果工作表名包含空格或特殊字符，需要使用反引号包裹：

```sql
SELECT * FROM `销售数据 2025`;
```

## 数据解析规则

- **首行作为列标题**：工作表的第一行将被解析为列名。
- **类型推断**：SQLExec 根据单元格内容自动推断列的数据类型。

| Excel 单元格类型 | 推断结果 |
|------------------|----------|
| 数值（整数） | `INT` |
| 数值（小数） | `FLOAT` |
| 布尔值 | `BOOLEAN` |
| 文本 | `TEXT` |
| 日期 | `TEXT`（格式化为字符串） |
| 空值 | 根据其他行推断 |

## 配置示例

### datasources.json

```json
{
  "datasources": [
    {
      "name": "sales",
      "type": "excel",
      "database": "/data/reports/sales_2025.xlsx",
      "options": {
        "sheet": "Q1数据"
      }
    },
    {
      "name": "inventory",
      "type": "excel",
      "database": "/data/inventory.xls"
    }
  ]
}
```

### 查询示例

```sql
-- 切换到 Excel 数据源
USE sales;

-- 查询指定工作表
SELECT * FROM Q1数据 LIMIT 10;

-- 按产品类别统计销售额
SELECT category, SUM(amount) AS total_sales
FROM Q1数据
GROUP BY category
ORDER BY total_sales DESC;

-- 条件过滤
SELECT product_name, quantity, unit_price
FROM Q1数据
WHERE quantity > 100 AND unit_price < 50.00;
```

### 多工作表查询

通过配置多个数据源指向同一文件的不同工作表：

```json
{
  "datasources": [
    {
      "name": "sales_q1",
      "type": "excel",
      "database": "/data/reports/annual_2025.xlsx",
      "options": {
        "sheet": "Q1"
      }
    },
    {
      "name": "sales_q2",
      "type": "excel",
      "database": "/data/reports/annual_2025.xlsx",
      "options": {
        "sheet": "Q2"
      }
    }
  ]
}
```

## 注意事项

- Excel 数据源默认为只读模式，不支持写入操作。
- 文件在连接时一次性加载到内存，大文件需注意内存占用。
- 首行必须为列标题，不支持无标题的工作表。
- 合并单元格、公式等 Excel 高级特性可能无法正确解析。
- 日期类型的单元格将被格式化为文本字符串。
- 支持通过工作表名称或索引号（从 0 开始）指定工作表。
