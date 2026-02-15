# 聚合与窗口函数

聚合函数将多行数据汇总为单个值，窗口函数在保留每行数据的同时计算跨行的聚合结果。

## GROUP BY 分组

将数据按指定列分组，配合聚合函数进行汇总统计：

```sql
SELECT category, COUNT(*) AS product_count
FROM products
GROUP BY category;
```

```sql
SELECT
  status,
  COUNT(*) AS order_count,
  SUM(total_price) AS total_amount,
  AVG(total_price) AS avg_amount
FROM orders
GROUP BY status;
```

### 多列分组

```sql
SELECT
  category,
  brand,
  COUNT(*) AS count,
  AVG(price) AS avg_price
FROM products
GROUP BY category, brand;
```

## HAVING 过滤

`HAVING` 用于对分组后的聚合结果进行过滤（`WHERE` 在分组前过滤，`HAVING` 在分组后过滤）：

```sql
-- 查询订单数超过 5 的用户
SELECT user_id, COUNT(*) AS order_count
FROM orders
GROUP BY user_id
HAVING COUNT(*) > 5;
```

```sql
-- 查询平均价格超过 500 且商品数不少于 3 的分类
SELECT
  category,
  COUNT(*) AS product_count,
  AVG(price) AS avg_price
FROM products
GROUP BY category
HAVING AVG(price) > 500 AND COUNT(*) >= 3;
```

## 聚合函数

### 基本聚合函数

| 函数 | 说明 | 示例 |
|------|------|------|
| `COUNT(*)` | 计算行数 | `SELECT COUNT(*) FROM users` |
| `COUNT(col)` | 计算非 NULL 值的个数 | `SELECT COUNT(email) FROM users` |
| `SUM(col)` | 求和 | `SELECT SUM(amount) FROM payments` |
| `AVG(col)` | 求平均值 | `SELECT AVG(score) FROM exams` |
| `MIN(col)` | 最小值 | `SELECT MIN(price) FROM products` |
| `MAX(col)` | 最大值 | `SELECT MAX(price) FROM products` |

### 扩展聚合函数

| 函数 | 说明 | 示例 |
|------|------|------|
| `GROUP_CONCAT(col)` | 将组内的值连接为字符串 | `SELECT GROUP_CONCAT(name) FROM users GROUP BY dept` |
| `STDDEV(col)` | 标准差 | `SELECT STDDEV(score) FROM exams` |
| `VARIANCE(col)` | 方差 | `SELECT VARIANCE(price) FROM products` |
| `MEDIAN(col)` | 中位数 | `SELECT MEDIAN(salary) FROM employees` |
| `MODE(col)` | 众数 | `SELECT MODE(category) FROM products` |
| `PERCENTILE(col, p)` | 百分位数 | `SELECT PERCENTILE(score, 0.95) FROM exams` |

### GROUP_CONCAT 示例

```sql
-- 将每个分类下的商品名连接为逗号分隔的字符串
SELECT
  category,
  GROUP_CONCAT(name) AS product_names,
  COUNT(*) AS count
FROM products
GROUP BY category;
```

### 统计函数示例

```sql
SELECT
  department,
  COUNT(*) AS 人数,
  AVG(salary) AS 平均薪资,
  MEDIAN(salary) AS 中位数薪资,
  STDDEV(salary) AS 薪资标准差,
  PERCENTILE(salary, 0.90) AS P90薪资
FROM employees
GROUP BY department;
```

## 窗口函数

窗口函数在不折叠行的情况下，对一组相关行（窗口）执行计算。

### ROW_NUMBER()

为窗口中的每行分配唯一的递增序号：

```sql
SELECT
  name,
  department,
  salary,
  ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) AS rank_in_dept
FROM employees;
```

### RANK() 和 DENSE_RANK()

- `RANK()`：有并列时跳号（如 1, 2, 2, 4）
- `DENSE_RANK()`：有并列时不跳号（如 1, 2, 2, 3）

```sql
SELECT
  name,
  score,
  RANK() OVER (ORDER BY score DESC) AS rank,
  DENSE_RANK() OVER (ORDER BY score DESC) AS dense_rank
FROM students;
```

### LAG() 和 LEAD()

访问窗口中当前行之前或之后的行：

```sql
SELECT
  month,
  revenue,
  LAG(revenue, 1) OVER (ORDER BY month) AS prev_month_revenue,
  LEAD(revenue, 1) OVER (ORDER BY month) AS next_month_revenue,
  revenue - LAG(revenue, 1) OVER (ORDER BY month) AS month_over_month
FROM monthly_revenue;
```

### FIRST_VALUE() 和 LAST_VALUE()

获取窗口中第一行或最后一行的值：

```sql
SELECT
  name,
  department,
  salary,
  FIRST_VALUE(name) OVER (PARTITION BY department ORDER BY salary DESC) AS highest_paid,
  LAST_VALUE(name) OVER (
    PARTITION BY department ORDER BY salary DESC
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
  ) AS lowest_paid
FROM employees;
```

### OVER 子句详解

`OVER` 子句定义窗口的范围和排序：

```sql
function() OVER (
  PARTITION BY col1, col2    -- 分区：将数据分为多个窗口
  ORDER BY col3 DESC         -- 排序：窗口内的行排列顺序
  ROWS BETWEEN ...           -- 帧：指定窗口内的行范围
)
```

帧规范（Frame Specification）：

| 帧定义 | 说明 |
|--------|------|
| `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` | 从窗口第一行到当前行（默认） |
| `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING` | 整个窗口 |
| `ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING` | 前一行到后一行 |
| `ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING` | 当前行到窗口末尾 |

## 综合示例

```sql
-- 分析每位员工在部门内的薪资排名和占比
SELECT
  name AS 姓名,
  department AS 部门,
  salary AS 薪资,
  RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS 部门排名,
  SUM(salary) OVER (PARTITION BY department) AS 部门总薪资,
  ROUND(salary * 100.0 / SUM(salary) OVER (PARTITION BY department), 2) AS 薪资占比
FROM employees
ORDER BY department, salary DESC;
```

```sql
-- 计算销售额的 7 天移动平均
SELECT
  sale_date,
  amount,
  AVG(amount) OVER (
    ORDER BY sale_date
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ) AS moving_avg_7d
FROM daily_sales;
```
