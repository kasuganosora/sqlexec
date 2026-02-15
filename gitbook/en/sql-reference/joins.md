# JOIN Operations

JOIN is used to combine data from two or more tables based on related conditions.

## INNER JOIN

Returns rows that have matching values in both tables. Only rows with matches on both sides appear in the result.

```sql
SELECT
  o.id AS order_id,
  u.name AS user_name,
  o.total_price AS amount
FROM orders o
INNER JOIN users u ON o.user_id = u.id;
```

The `INNER` keyword in `INNER JOIN` can be omitted. Writing `JOIN` alone is equivalent to `INNER JOIN`:

```sql
SELECT o.id, u.name
FROM orders o
JOIN users u ON o.user_id = u.id;
```

## LEFT JOIN

Returns all rows from the left table. If there is no matching row in the right table, the corresponding columns are filled with NULL.

```sql
SELECT
  u.name AS user_name,
  o.id AS order_id,
  o.total_price AS amount
FROM users u
LEFT JOIN orders o ON u.id = o.user_id;
```

Example result:

| user_name | order_id | amount |
|-----------|----------|--------|
| Zhang San | 1001 | 299.00 |
| Zhang San | 1002 | 58.50 |
| Li Si | 1003 | 1200.00 |
| Wang Wu | NULL | NULL |

In the example above, Wang Wu has no orders, but because `LEFT JOIN` is used, he still appears in the result with NULL values for the order-related columns.

## RIGHT JOIN

Returns all rows from the right table. If there is no matching row in the left table, the corresponding columns are filled with NULL.

```sql
SELECT
  u.name AS user_name,
  p.name AS product_name,
  p.category AS category
FROM users u
RIGHT JOIN products p ON u.favorite_product_id = p.id;
```

A `RIGHT JOIN` produces the same result as swapping the left and right tables and using a `LEFT JOIN`.

## CROSS JOIN

Returns the Cartesian product of two tables, combining every row from the left table with every row from the right table.

```sql
SELECT
  c.name AS color,
  s.label AS size
FROM colors c
CROSS JOIN sizes s;
```

If `colors` has 3 rows and `sizes` has 4 rows, the result will have 3 x 4 = 12 rows.

{% hint style="warning" %}
Cross joins produce a large number of result rows. Use with caution, especially on large tables.
{% endhint %}

## Self-Join

Joins a table with itself. This is commonly used for hierarchical data (such as organizational structures, category trees, etc.).

```sql
-- Query employees and their direct managers
SELECT
  e.name AS employee,
  m.name AS manager
FROM employees e
LEFT JOIN employees m ON e.manager_id = m.id;
```

```sql
-- Find pairs of users in the same city
SELECT
  a.name AS user_a,
  b.name AS user_b,
  a.city AS city
FROM users a
INNER JOIN users b ON a.city = b.city AND a.id < b.id;
```

## Multi-Table Joins

You can chain multiple tables together:

```sql
SELECT
  o.id AS order_id,
  u.name AS user_name,
  p.name AS product_name,
  oi.quantity AS quantity,
  oi.unit_price AS unit_price
FROM orders o
INNER JOIN users u ON o.user_id = u.id
INNER JOIN order_items oi ON o.id = oi.order_id
INNER JOIN products p ON oi.product_id = p.id
WHERE o.status = 'completed'
ORDER BY o.id;
```

## Compound Join Conditions

Join conditions can include multiple conditions connected with `AND`:

```sql
SELECT
  s.student_name,
  c.course_name,
  sc.score
FROM student_courses sc
INNER JOIN students s ON sc.student_id = s.id
INNER JOIN courses c ON sc.course_id = c.id AND c.active = TRUE
WHERE sc.score >= 60;
```

## Comprehensive Example

```sql
-- Query each user's recent orders with product details
SELECT
  u.name AS user_name,
  u.email AS email,
  o.id AS order_id,
  o.created_at AS order_time,
  p.name AS product_name,
  p.category AS category,
  oi.quantity AS quantity,
  oi.unit_price * oi.quantity AS subtotal
FROM users u
LEFT JOIN orders o ON u.id = o.user_id
LEFT JOIN order_items oi ON o.id = oi.order_id
LEFT JOIN products p ON oi.product_id = p.id
WHERE u.active = TRUE
ORDER BY u.name, o.created_at DESC;
```
