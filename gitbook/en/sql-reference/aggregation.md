# Aggregation and Window Functions

Aggregate functions summarize multiple rows into a single value, while window functions compute aggregated results across rows while preserving each individual row.

## GROUP BY

Group data by specified columns and use aggregate functions for summary statistics:

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

### Multi-Column Grouping

```sql
SELECT
  category,
  brand,
  COUNT(*) AS count,
  AVG(price) AS avg_price
FROM products
GROUP BY category, brand;
```

## HAVING Filtering

`HAVING` is used to filter aggregated results after grouping (`WHERE` filters before grouping, `HAVING` filters after grouping):

```sql
-- Query users with more than 5 orders
SELECT user_id, COUNT(*) AS order_count
FROM orders
GROUP BY user_id
HAVING COUNT(*) > 5;
```

```sql
-- Query categories with an average price over 500 and at least 3 products
SELECT
  category,
  COUNT(*) AS product_count,
  AVG(price) AS avg_price
FROM products
GROUP BY category
HAVING AVG(price) > 500 AND COUNT(*) >= 3;
```

## Aggregate Functions

### Basic Aggregate Functions

| Function | Description | Example |
|----------|-------------|---------|
| `COUNT(*)` | Count rows | `SELECT COUNT(*) FROM users` |
| `COUNT(col)` | Count non-NULL values | `SELECT COUNT(email) FROM users` |
| `SUM(col)` | Sum | `SELECT SUM(amount) FROM payments` |
| `AVG(col)` | Average | `SELECT AVG(score) FROM exams` |
| `MIN(col)` | Minimum value | `SELECT MIN(price) FROM products` |
| `MAX(col)` | Maximum value | `SELECT MAX(price) FROM products` |

### Extended Aggregate Functions

| Function | Description | Example |
|----------|-------------|---------|
| `GROUP_CONCAT(col)` | Concatenate values within a group into a string | `SELECT GROUP_CONCAT(name) FROM users GROUP BY dept` |
| `STDDEV(col)` | Standard deviation | `SELECT STDDEV(score) FROM exams` |
| `VARIANCE(col)` | Variance | `SELECT VARIANCE(price) FROM products` |
| `MEDIAN(col)` | Median | `SELECT MEDIAN(salary) FROM employees` |
| `MODE(col)` | Mode | `SELECT MODE(category) FROM products` |
| `PERCENTILE(col, p)` | Percentile | `SELECT PERCENTILE(score, 0.95) FROM exams` |

### GROUP_CONCAT Example

```sql
-- Concatenate product names within each category into a comma-separated string
SELECT
  category,
  GROUP_CONCAT(name) AS product_names,
  COUNT(*) AS count
FROM products
GROUP BY category;
```

### Statistical Functions Example

```sql
SELECT
  department,
  COUNT(*) AS headcount,
  AVG(salary) AS avg_salary,
  MEDIAN(salary) AS median_salary,
  STDDEV(salary) AS salary_stddev,
  PERCENTILE(salary, 0.90) AS p90_salary
FROM employees
GROUP BY department;
```

## Window Functions

Window functions perform calculations across a set of related rows (a window) without collapsing the rows.

### ROW_NUMBER()

Assigns a unique, incrementing number to each row within a window:

```sql
SELECT
  name,
  department,
  salary,
  ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) AS rank_in_dept
FROM employees;
```

### RANK() and DENSE_RANK()

- `RANK()`: Skips numbers on ties (e.g., 1, 2, 2, 4)
- `DENSE_RANK()`: Does not skip numbers on ties (e.g., 1, 2, 2, 3)

```sql
SELECT
  name,
  score,
  RANK() OVER (ORDER BY score DESC) AS rank,
  DENSE_RANK() OVER (ORDER BY score DESC) AS dense_rank
FROM students;
```

### LAG() and LEAD()

Access the row before or after the current row in the window:

```sql
SELECT
  month,
  revenue,
  LAG(revenue, 1) OVER (ORDER BY month) AS prev_month_revenue,
  LEAD(revenue, 1) OVER (ORDER BY month) AS next_month_revenue,
  revenue - LAG(revenue, 1) OVER (ORDER BY month) AS month_over_month
FROM monthly_revenue;
```

### FIRST_VALUE() and LAST_VALUE()

Get the value from the first or last row in the window:

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

### OVER Clause Details

The `OVER` clause defines the scope and ordering of the window:

```sql
function() OVER (
  PARTITION BY col1, col2    -- Partition: divide data into multiple windows
  ORDER BY col3 DESC         -- Order: row ordering within the window
  ROWS BETWEEN ...           -- Frame: specify the range of rows within the window
)
```

Frame Specification:

| Frame Definition | Description |
|-----------------|-------------|
| `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` | From the first row of the window to the current row (default) |
| `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING` | The entire window |
| `ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING` | From one row before to one row after |
| `ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING` | From the current row to the end of the window |

## Comprehensive Examples

```sql
-- Analyze each employee's salary ranking and proportion within their department
SELECT
  name,
  department,
  salary,
  RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS dept_rank,
  SUM(salary) OVER (PARTITION BY department) AS dept_total_salary,
  ROUND(salary * 100.0 / SUM(salary) OVER (PARTITION BY department), 2) AS salary_proportion
FROM employees
ORDER BY department, salary DESC;
```

```sql
-- Calculate 7-day moving average of sales
SELECT
  sale_date,
  amount,
  AVG(amount) OVER (
    ORDER BY sale_date
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ) AS moving_avg_7d
FROM daily_sales;
```
