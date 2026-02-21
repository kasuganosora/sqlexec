# 函数系统概述

SQLExec 内置了丰富的函数库，涵盖 13 个类别，满足数据查询、转换、分析等各类场景的需求。

## 函数类别

| 类别 | 说明 | 链接 |
|------|------|------|
| 字符串函数 | 字符串拼接、截取、查找、转换等操作 | [查看详情](string.md) |
| 数学函数 | 数值计算、三角函数、取整、随机数等 | [查看详情](math.md) |
| 日期时间函数 | 日期解析、格式化、运算、提取等 | [查看详情](datetime.md) |
| 聚合函数 | 分组统计、求和、平均、计数等 | [查看详情](aggregate.md) |
| JSON 函数 | JSON 数据的提取、构造、查询、修改 | [查看详情](json-functions.md) |
| 控制流函数 | 条件判断、空值处理、分支逻辑 | [查看详情](control.md) |
| 编码与哈希函数 | Base64、Hex 编解码及常用哈希算法 | [查看详情](encoding.md) |
| 相似度函数 | 字符串相似度、余弦相似度计算 | [查看详情](similarity.md) |
| 向量函数 | 向量距离与相似度计算，支持向量检索 | [查看详情](vector.md) |
| 金融函数 | 净现值、年金、利率等金融计算 | [查看详情](financial.md) |
| 位运算函数 | 按位与、或、异或、移位等操作 | [查看详情](bitwise.md) |
| 空间函数 | 几何体构造、距离、面积、包含、相交等地理空间计算 | [查看详情](spatial.md) |
| 系统函数 | 类型检测、UUID 生成、环境信息查询 | [查看详情](system.md) |

## 函数类型

SQLExec 支持三种类型的函数：

### 标量函数（Scalar Functions）

标量函数对每一行输入返回一个值。大多数内置函数属于此类型。

```sql
SELECT UPPER(name), LENGTH(name) FROM users;
```

### 聚合函数（Aggregate Functions）

聚合函数对一组行进行计算，返回单个汇总值。通常与 `GROUP BY` 配合使用。

```sql
SELECT department, COUNT(*), AVG(salary)
FROM employees
GROUP BY department;
```

### 窗口函数（Window Functions）

窗口函数在一组与当前行相关的行上执行计算，不会将多行折叠为一行。使用 `OVER()` 子句定义窗口。

```sql
SELECT name, salary,
       RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS rank
FROM employees;
```

## 基本用法

函数可以在 `SELECT`、`WHERE`、`HAVING`、`ORDER BY` 等子句中使用：

```sql
-- 在 SELECT 中使用
SELECT CONCAT(first_name, ' ', last_name) AS full_name FROM users;

-- 在 WHERE 中使用
SELECT * FROM orders WHERE YEAR(created_at) = 2025;

-- 在 ORDER BY 中使用
SELECT * FROM products ORDER BY LOWER(name);

-- 函数嵌套
SELECT UPPER(TRIM(name)) FROM users;
```

## 用户自定义函数（UDF）

除内置函数外，SQLExec 还支持用户自定义函数（User-Defined Functions）。你可以使用 `CREATE FUNCTION` 语句注册自定义的标量函数或聚合函数，以扩展系统的计算能力。

```sql
-- 注册自定义函数示例
CREATE FUNCTION double_value(x INT) RETURNS INT
AS 'return x * 2';

-- 使用自定义函数
SELECT double_value(price) FROM products;
```

详细的 UDF 创建与管理方法请参阅 [用户自定义函数](../advanced/udf.md) 章节。
