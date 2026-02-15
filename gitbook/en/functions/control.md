# Control Flow Functions

Control flow functions are used to implement conditional evaluation, null handling, and branching logic. They are essential tools for building complex queries.

## Function List

| Function | Description | Example |
|----------|-------------|---------|
| `IF(cond, true_val, false_val)` | Return true_val if condition is true, otherwise false_val | `SELECT IF(score >= 60, 'Pass', 'Fail');` |
| `CASE WHEN ... THEN ... ELSE ... END` | Multi-branch conditional expression | See detailed description below |
| `COALESCE(v1, v2, ...)` | Return the first non-NULL value from the argument list | `SELECT COALESCE(nickname, username, 'anonymous');` |
| `IFNULL(expr, default)` | Return default if expr is NULL | `SELECT IFNULL(phone, 'N/A');` |
| `NULLIF(a, b)` | Return NULL if a equals b, otherwise return a | `SELECT NULLIF(value, 0);` |

## Detailed Description

### IF Function

The `IF` function is the simplest conditional evaluation, suitable for binary choice scenarios.

```sql
-- Basic usage
SELECT name,
       score,
       IF(score >= 60, 'Pass', 'Fail') AS result
FROM students;

-- Nested IF
SELECT name,
       IF(age >= 18, IF(age >= 65, 'Senior', 'Adult'), 'Minor') AS age_group
FROM users;

-- Using IF with aggregation
SELECT COUNT(IF(status = 'active', 1, NULL)) AS active_count,
       COUNT(IF(status = 'inactive', 1, NULL)) AS inactive_count
FROM users;
```

### CASE WHEN Expression

`CASE WHEN` supports multiple branch conditions and is suitable for complex conditional logic.

**Searched CASE:**

```sql
-- Assign grades based on scores
SELECT name, score,
       CASE
           WHEN score >= 90 THEN 'A'
           WHEN score >= 80 THEN 'B'
           WHEN score >= 70 THEN 'C'
           WHEN score >= 60 THEN 'D'
           ELSE 'F'
       END AS grade
FROM students;
```

**Simple CASE:**

```sql
-- Convert status codes to descriptive text
SELECT order_id,
       CASE status
           WHEN 'pending' THEN 'Pending'
           WHEN 'shipped' THEN 'Shipped'
           WHEN 'delivered' THEN 'Delivered'
           WHEN 'cancelled' THEN 'Cancelled'
           ELSE 'Unknown'
       END AS status_text
FROM orders;
```

**Using CASE in aggregation and sorting:**

```sql
-- Conditional aggregation
SELECT department,
       SUM(CASE WHEN gender = 'M' THEN 1 ELSE 0 END) AS male_count,
       SUM(CASE WHEN gender = 'F' THEN 1 ELSE 0 END) AS female_count
FROM employees
GROUP BY department;

-- Custom sorting
SELECT * FROM tasks
ORDER BY CASE priority
    WHEN 'critical' THEN 1
    WHEN 'high' THEN 2
    WHEN 'medium' THEN 3
    WHEN 'low' THEN 4
    ELSE 5
END;
```

### COALESCE Function

`COALESCE` returns the first non-NULL value from the argument list. It is commonly used for setting default values and merging multiple potentially null fields.

```sql
-- Use default values
SELECT COALESCE(nickname, username, email, 'anonymous') AS display_name
FROM users;

-- Merge multiple address fields
SELECT COALESCE(shipping_address, billing_address, default_address) AS address
FROM customers;

-- Handle potentially NULL calculations
SELECT product_id,
       COALESCE(discount_price, original_price) AS final_price
FROM products;
```

### IFNULL Function

`IFNULL` is a two-argument shorthand for `COALESCE` that returns a default value when the value is NULL.

```sql
-- Replace null values
SELECT name,
       IFNULL(phone, 'Not provided') AS phone,
       IFNULL(email, 'Not provided') AS email
FROM contacts;

-- Handle NULL in calculations
SELECT order_id,
       subtotal + IFNULL(shipping_fee, 0) + IFNULL(tax, 0) AS total
FROM orders;
```

### NULLIF Function

`NULLIF` returns NULL when two values are equal. It is commonly used to avoid division-by-zero errors or to convert specific values to NULL.

```sql
-- Avoid division by zero
SELECT revenue / NULLIF(cost, 0) AS roi
FROM financials;

-- Convert empty strings to NULL
SELECT NULLIF(description, '') AS description
FROM products;

-- Convert placeholder values to NULL
SELECT NULLIF(phone, '000-0000-0000') AS phone
FROM users;
```

## Comprehensive Examples

```sql
-- Multiple condition combination
SELECT name,
       COALESCE(
           IF(vip_level > 0,
              CASE vip_level
                  WHEN 1 THEN 'Silver Member'
                  WHEN 2 THEN 'Gold Member'
                  WHEN 3 THEN 'Diamond Member'
              END,
              NULL),
           'Regular User'
       ) AS user_type
FROM users;
```
