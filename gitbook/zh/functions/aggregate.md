# 聚合函数

聚合函数对一组行进行计算，返回单个汇总值。通常与 `GROUP BY` 子句配合使用，也可以对整个结果集进行聚合。

## 函数列表

| 函数 | 说明 | 示例 |
|------|------|------|
| `COUNT(*)` | 统计行数 | `SELECT COUNT(*) FROM users;` |
| `COUNT(col)` | 统计指定列的非 NULL 值个数 | `SELECT COUNT(email) FROM users;` |
| `SUM(col)` | 计算数值列的总和 | `SELECT SUM(amount) FROM orders;` |
| `AVG(col)` | 计算数值列的平均值 | `SELECT AVG(score) FROM exam_results;` |
| `MIN(col)` | 返回列的最小值 | `SELECT MIN(price) FROM products;` |
| `MAX(col)` | 返回列的最大值 | `SELECT MAX(price) FROM products;` |
| `GROUP_CONCAT(col, sep)` | 将分组中的值拼接为字符串 | `SELECT GROUP_CONCAT(name, ', ') FROM users GROUP BY dept;` |
| `STDDEV(col)` | 计算标准差 | `SELECT STDDEV(salary) FROM employees;` |
| `VARIANCE(col)` | 计算方差 | `SELECT VARIANCE(salary) FROM employees;` |
| `MEDIAN(col)` | 计算中位数 | `SELECT MEDIAN(response_time) FROM requests;` |
| `MODE(col)` | 返回出现次数最多的值 | `SELECT MODE(category) FROM products;` |
| `PERCENTILE(col, p)` | 返回百分位值（p 为 0-1） | `SELECT PERCENTILE(score, 0.95) FROM results;` |

## 基本用法

### 对整个结果集聚合

不使用 `GROUP BY` 时，聚合函数对整个结果集计算一个汇总值：

```sql
-- 统计用户总数
SELECT COUNT(*) AS total_users FROM users;

-- 计算订单总金额和平均金额
SELECT SUM(amount) AS total_amount,
       AVG(amount) AS avg_amount
FROM orders;

-- 获取价格范围
SELECT MIN(price) AS min_price,
       MAX(price) AS max_price
FROM products;
```

### 配合 GROUP BY 分组聚合

```sql
-- 按部门统计员工数和平均工资
SELECT department,
       COUNT(*) AS employee_count,
       AVG(salary) AS avg_salary
FROM employees
GROUP BY department;

-- 按年月统计销售额
SELECT YEAR(order_date) AS year,
       MONTH(order_date) AS month,
       SUM(amount) AS monthly_sales,
       COUNT(*) AS order_count
FROM orders
GROUP BY YEAR(order_date), MONTH(order_date)
ORDER BY year, month;
```

### 使用 HAVING 过滤分组

```sql
-- 查找订单数超过 10 的客户
SELECT customer_id,
       COUNT(*) AS order_count,
       SUM(amount) AS total_spent
FROM orders
GROUP BY customer_id
HAVING COUNT(*) > 10;

-- 查找平均工资高于 10000 的部门
SELECT department, AVG(salary) AS avg_salary
FROM employees
GROUP BY department
HAVING AVG(salary) > 10000;
```

## 使用示例

### COUNT 的不同用法

```sql
-- COUNT(*) 统计所有行，包括 NULL
SELECT COUNT(*) AS total_rows FROM users;

-- COUNT(col) 只统计非 NULL 值
SELECT COUNT(email) AS users_with_email FROM users;

-- COUNT(DISTINCT col) 统计不同值的个数
SELECT COUNT(DISTINCT city) AS city_count FROM users;
```

### GROUP_CONCAT 拼接值

```sql
-- 每个部门的员工名单
SELECT department,
       GROUP_CONCAT(name, ', ') AS members
FROM employees
GROUP BY department;

-- 每个订单的商品列表
SELECT order_id,
       GROUP_CONCAT(product_name, ' | ') AS products
FROM order_items
GROUP BY order_id;
```

### 统计分析

```sql
-- 综合统计分析
SELECT department,
       COUNT(*) AS count,
       AVG(salary) AS avg_salary,
       MEDIAN(salary) AS median_salary,
       STDDEV(salary) AS stddev_salary,
       MIN(salary) AS min_salary,
       MAX(salary) AS max_salary
FROM employees
GROUP BY department;

-- 百分位分析
SELECT PERCENTILE(response_time, 0.50) AS p50,
       PERCENTILE(response_time, 0.90) AS p90,
       PERCENTILE(response_time, 0.95) AS p95,
       PERCENTILE(response_time, 0.99) AS p99
FROM api_requests;
```

### 众数分析

```sql
-- 查找最常见的商品类别
SELECT MODE(category) AS most_common_category
FROM products;

-- 按地区查找最常用的支付方式
SELECT region,
       MODE(payment_method) AS preferred_payment
FROM orders
GROUP BY region;
```
