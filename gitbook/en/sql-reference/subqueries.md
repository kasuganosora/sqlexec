# Subqueries and CTEs

A subquery is a query nested inside another SQL statement. CTEs (Common Table Expressions) provide a cleaner way to organize complex queries.

## Subqueries in the WHERE Clause

### IN Subqueries

```sql
-- Query users who have placed orders
SELECT * FROM users
WHERE id IN (SELECT DISTINCT user_id FROM orders);
```

```sql
-- Query users who have no orders
SELECT * FROM users
WHERE id NOT IN (SELECT DISTINCT user_id FROM orders);
```

### Comparison Operator Subqueries

```sql
-- Query products with a price higher than the average price
SELECT * FROM products
WHERE price > (SELECT AVG(price) FROM products);
```

## Subqueries in the FROM Clause (Derived Tables)

The subquery result is used as a temporary table and must have an alias:

```sql
-- Query the average price per category, filtering categories with an average price over 100
SELECT * FROM (
  SELECT category, AVG(price) AS avg_price, COUNT(*) AS product_count
  FROM products
  GROUP BY category
) AS category_stats
WHERE avg_price > 100
ORDER BY avg_price DESC;
```

```sql
-- Query order statistics for each user
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

## Subqueries in the SELECT List (Scalar Subqueries)

Scalar subqueries must return a single value (one row, one column):

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

`EXISTS` checks whether a subquery returns at least one row:

```sql
-- Query users who have placed orders
SELECT * FROM users u
WHERE EXISTS (
  SELECT 1 FROM orders o WHERE o.user_id = u.id
);
```

```sql
-- Query users who have no orders at all
SELECT * FROM users u
WHERE NOT EXISTS (
  SELECT 1 FROM orders o WHERE o.user_id = u.id
);
```

`EXISTS` is generally more efficient than `IN` subqueries because it stops searching as soon as the first match is found.

## WITH CTE (Common Table Expressions)

CTEs use the `WITH` keyword to define temporary named result sets, making complex queries more readable:

### Basic CTE

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

### Multiple CTEs

You can define multiple CTEs, and later CTEs can reference previously defined ones:

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

### Recursive CTEs

Recursive CTEs can reference themselves and are useful for handling tree structures and hierarchical data:

```sql
WITH RECURSIVE category_tree AS (
  -- Base case: top-level categories
  SELECT id, name, parent_id, 1 AS level
  FROM categories
  WHERE parent_id IS NULL

  UNION ALL

  -- Recursive part: child categories
  SELECT c.id, c.name, c.parent_id, ct.level + 1
  FROM categories c
  INNER JOIN category_tree ct ON c.parent_id = ct.id
)
SELECT * FROM category_tree ORDER BY level, name;
```

## UNION / UNION ALL

Combine the results of multiple queries into a single result set.

### UNION (Deduplicated)

```sql
-- Merge users from two sources, automatically removing duplicate rows
SELECT name, email FROM internal_users
UNION
SELECT name, email FROM external_users;
```

### UNION ALL (Preserving Duplicates)

```sql
-- Merge all logs, preserving duplicate rows (better performance)
SELECT timestamp, message FROM app_logs
UNION ALL
SELECT timestamp, message FROM system_logs
ORDER BY timestamp DESC;
```

{% hint style="info" %}
`UNION ALL` does not perform deduplication, so it has better performance than `UNION`. If deduplication is not needed, prefer `UNION ALL`.
{% endhint %}

## Comprehensive Example

```sql
-- Complex sales analysis using CTEs and subqueries
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
  u.name AS user_name,
  ms.month AS month,
  ms.monthly_total AS monthly_sales_amount,
  ua.avg_monthly AS avg_monthly_sales,
  CASE
    WHEN ms.monthly_total > ua.avg_monthly THEN 'above average'
    ELSE 'below average'
  END AS performance
FROM monthly_sales ms
INNER JOIN users u ON ms.user_id = u.id
INNER JOIN user_avg ua ON ms.user_id = ua.user_id
ORDER BY u.name, ms.month;
```
