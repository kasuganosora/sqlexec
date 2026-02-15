# SELECT Queries

`SELECT` is the most commonly used SQL statement for retrieving data from tables.

## Basic Queries

Query specific columns:

```sql
SELECT name, age FROM users;
```

Query all columns:

```sql
SELECT * FROM users;
```

## WHERE Conditional Filtering

### Comparison Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `=` | Equal to | `WHERE age = 18` |
| `!=` or `<>` | Not equal to | `WHERE status != 'inactive'` |
| `<` | Less than | `WHERE price < 100` |
| `>` | Greater than | `WHERE score > 90` |
| `<=` | Less than or equal to | `WHERE quantity <= 0` |
| `>=` | Greater than or equal to | `WHERE rating >= 4.5` |

### LIKE Pattern Matching

```sql
-- % matches any number of characters
SELECT * FROM users WHERE name LIKE '张%';

-- _ matches a single character
SELECT * FROM users WHERE code LIKE 'A_01';
```

### IN Range Matching

```sql
SELECT * FROM products WHERE category IN ('电子', '图书', '服装');
```

### BETWEEN Range Queries

```sql
SELECT * FROM orders WHERE amount BETWEEN 100 AND 500;
```

### NULL Value Checks

```sql
-- Check for NULL
SELECT * FROM users WHERE email IS NULL;

-- Check for NOT NULL
SELECT * FROM users WHERE phone IS NOT NULL;
```

## Logical Operators

Use `AND`, `OR`, `NOT` to combine multiple conditions:

```sql
SELECT * FROM products
WHERE category = '电子'
  AND price < 1000
  AND brand IS NOT NULL;
```

```sql
SELECT * FROM users
WHERE age >= 18 OR guardian_approved = TRUE;
```

```sql
SELECT * FROM orders
WHERE NOT (status = 'cancelled' OR status = 'refunded');
```

## ORDER BY Sorting

### Ascending Order (Default)

```sql
SELECT * FROM users ORDER BY created_at ASC;
```

### Descending Order

```sql
SELECT * FROM products ORDER BY price DESC;
```

### Multi-Column Sorting

```sql
SELECT * FROM students ORDER BY grade DESC, name ASC;
```

## LIMIT and OFFSET Pagination

```sql
-- Return the first 10 records
SELECT * FROM logs ORDER BY created_at DESC LIMIT 10;

-- Skip the first 20 records and return the next 10 (page 3)
SELECT * FROM logs ORDER BY created_at DESC LIMIT 10 OFFSET 20;
```

## DISTINCT Deduplication

```sql
SELECT DISTINCT category FROM products;
```

```sql
SELECT DISTINCT city, province FROM addresses;
```

## Column Aliases

Use `AS` to assign aliases to columns:

```sql
SELECT
  name AS user_name,
  age AS user_age,
  score * 1.1 AS weighted_score
FROM students;
```

The `AS` keyword can be omitted:

```sql
SELECT name user_name, age user_age FROM students;
```

## Expressions in the SELECT List

You can use various expressions in the SELECT list:

```sql
SELECT
  name,
  price,
  quantity,
  price * quantity AS total,
  CASE
    WHEN price * quantity > 1000 THEN 'large'
    WHEN price * quantity > 100  THEN 'medium'
    ELSE 'small'
  END AS order_level
FROM order_items;
```

## Comprehensive Example

```sql
-- Query active users, sorted by registration time in descending order, with pagination
SELECT
  id,
  name AS user_name,
  email AS email_address,
  created_at AS registration_time
FROM users
WHERE status = 'active'
  AND created_at >= '2025-01-01'
  AND email IS NOT NULL
ORDER BY created_at DESC
LIMIT 20 OFFSET 0;
```

```sql
-- Fuzzy search for products
SELECT DISTINCT
  name,
  category,
  price
FROM products
WHERE name LIKE '%手机%'
  AND price BETWEEN 1000 AND 5000
  AND category IN ('电子', '通讯')
ORDER BY price ASC
LIMIT 50;
```
