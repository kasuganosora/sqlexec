# 子查询与 CTE

子查询（Subquery）是嵌套在其他 SQL 语句中的查询。CTE（Common Table Expression，公用表表达式）提供了一种更清晰的方式来组织复杂查询。

## WHERE 子句中的子查询

### IN 子查询

```sql
-- 查询有过订单的用户
SELECT * FROM users
WHERE id IN (SELECT DISTINCT user_id FROM orders);
```

```sql
-- 查询没有订单的用户
SELECT * FROM users
WHERE id NOT IN (SELECT DISTINCT user_id FROM orders);
```

### 比较运算符子查询

```sql
-- 查询价格高于平均价格的商品
SELECT * FROM products
WHERE price > (SELECT AVG(price) FROM products);
```

## FROM 子句中的子查询（派生表）

子查询结果作为一张临时表使用，必须指定别名：

```sql
-- 查询每个分类的平均价格，筛选平均价格超过 100 的分类
SELECT * FROM (
  SELECT category, AVG(price) AS avg_price, COUNT(*) AS product_count
  FROM products
  GROUP BY category
) AS category_stats
WHERE avg_price > 100
ORDER BY avg_price DESC;
```

```sql
-- 查询每位用户的订单统计
SELECT
  u.name,
  order_stats.order_count,
  order_stats.total_amount
FROM users u
INNER JOIN (
  SELECT user_id, COUNT(*) AS order_count, SUM(total_price) AS total_amount
  FROM orders
  GROUP BY user_id
) AS order_stats ON u.id = order_stats.user_id;
```

## SELECT 列表中的子查询（标量子查询）

标量子查询必须返回单个值（一行一列）：

```sql
SELECT
  name,
  price,
  (SELECT AVG(price) FROM products) AS avg_price,
  price - (SELECT AVG(price) FROM products) AS diff_from_avg
FROM products;
```

```sql
SELECT
  u.name,
  (SELECT COUNT(*) FROM orders o WHERE o.user_id = u.id) AS order_count
FROM users u;
```

## EXISTS / NOT EXISTS

`EXISTS` 判断子查询是否返回至少一行结果：

```sql
-- 查询有过订单的用户
SELECT * FROM users u
WHERE EXISTS (
  SELECT 1 FROM orders o WHERE o.user_id = u.id
);
```

```sql
-- 查询没有任何订单的用户
SELECT * FROM users u
WHERE NOT EXISTS (
  SELECT 1 FROM orders o WHERE o.user_id = u.id
);
```

`EXISTS` 通常比 `IN` 子查询效率更高，因为它在找到第一个匹配后就会停止搜索。

## WITH CTE（公用表表达式）

CTE 使用 `WITH` 关键字定义临时命名结果集，使复杂查询更易读：

### 基本 CTE

```sql
WITH active_users AS (
  SELECT id, name, email
  FROM users
  WHERE active = TRUE
)
SELECT
  au.name,
  COUNT(o.id) AS order_count
FROM active_users au
LEFT JOIN orders o ON au.id = o.user_id
GROUP BY au.name;
```

### 多个 CTE

可以定义多个 CTE，后面的 CTE 可以引用前面定义的 CTE：

```sql
WITH
user_orders AS (
  SELECT user_id, COUNT(*) AS order_count, SUM(total_price) AS total_amount
  FROM orders
  GROUP BY user_id
),
vip_users AS (
  SELECT user_id
  FROM user_orders
  WHERE order_count >= 10 OR total_amount >= 10000
)
SELECT
  u.name,
  uo.order_count,
  uo.total_amount
FROM users u
INNER JOIN vip_users v ON u.id = v.user_id
INNER JOIN user_orders uo ON u.id = uo.user_id
ORDER BY uo.total_amount DESC;
```

### 递归 CTE

递归 CTE 可以引用自身，适用于处理树形结构和层级数据：

```sql
WITH RECURSIVE category_tree AS (
  -- 基础情况：顶级分类
  SELECT id, name, parent_id, 1 AS level
  FROM categories
  WHERE parent_id IS NULL

  UNION ALL

  -- 递归部分：子分类
  SELECT c.id, c.name, c.parent_id, ct.level + 1
  FROM categories c
  INNER JOIN category_tree ct ON c.parent_id = ct.id
)
SELECT * FROM category_tree ORDER BY level, name;
```

## UNION / UNION ALL

将多个查询的结果合并为一个结果集。

### UNION（去重）

```sql
-- 合并两个来源的用户，自动去除重复行
SELECT name, email FROM internal_users
UNION
SELECT name, email FROM external_users;
```

### UNION ALL（保留重复）

```sql
-- 合并所有日志，保留重复行（性能更好）
SELECT timestamp, message FROM app_logs
UNION ALL
SELECT timestamp, message FROM system_logs
ORDER BY timestamp DESC;
```

{% hint style="info" %}
`UNION ALL` 不执行去重操作，因此比 `UNION` 性能更好。如果不需要去重，优先使用 `UNION ALL`。
{% endhint %}

## 综合示例

```sql
-- 使用 CTE 和子查询进行复杂的销售分析
WITH
monthly_sales AS (
  SELECT
    user_id,
    DATE_FORMAT(created_at, '%Y-%m') AS month,
    SUM(total_price) AS monthly_total
  FROM orders
  WHERE status = 'completed'
  GROUP BY user_id, DATE_FORMAT(created_at, '%Y-%m')
),
user_avg AS (
  SELECT user_id, AVG(monthly_total) AS avg_monthly
  FROM monthly_sales
  GROUP BY user_id
)
SELECT
  u.name AS 用户名,
  ms.month AS 月份,
  ms.monthly_total AS 当月销售额,
  ua.avg_monthly AS 月均销售额,
  CASE
    WHEN ms.monthly_total > ua.avg_monthly THEN '高于平均'
    ELSE '低于平均'
  END AS 表现
FROM monthly_sales ms
INNER JOIN users u ON ms.user_id = u.id
INNER JOIN user_avg ua ON ms.user_id = ua.user_id
ORDER BY u.name, ms.month;
```
