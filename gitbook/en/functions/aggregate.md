# Aggregate Functions

Aggregate functions perform calculations on a set of rows and return a single summary value. They are typically used with the `GROUP BY` clause, but can also aggregate over the entire result set.

## Function List

| Function | Description | Example |
|----------|-------------|---------|
| `COUNT(*)` | Count the number of rows | `SELECT COUNT(*) FROM users;` |
| `COUNT(col)` | Count the number of non-NULL values in a column | `SELECT COUNT(email) FROM users;` |
| `SUM(col)` | Calculate the sum of a numeric column | `SELECT SUM(amount) FROM orders;` |
| `AVG(col)` | Calculate the average of a numeric column | `SELECT AVG(score) FROM exam_results;` |
| `MIN(col)` | Return the minimum value of a column | `SELECT MIN(price) FROM products;` |
| `MAX(col)` | Return the maximum value of a column | `SELECT MAX(price) FROM products;` |
| `GROUP_CONCAT(col, sep)` | Concatenate values within a group into a string | `SELECT GROUP_CONCAT(name, ', ') FROM users GROUP BY dept;` |
| `STDDEV(col)` | Calculate the standard deviation | `SELECT STDDEV(salary) FROM employees;` |
| `VARIANCE(col)` | Calculate the variance | `SELECT VARIANCE(salary) FROM employees;` |
| `MEDIAN(col)` | Calculate the median | `SELECT MEDIAN(response_time) FROM requests;` |
| `MODE(col)` | Return the most frequently occurring value | `SELECT MODE(category) FROM products;` |
| `PERCENTILE(col, p)` | Return the percentile value (p ranges from 0 to 1) | `SELECT PERCENTILE(score, 0.95) FROM results;` |

## Basic Usage

### Aggregating Over the Entire Result Set

When `GROUP BY` is not used, aggregate functions compute a single summary value over the entire result set:

```sql
-- Count total users
SELECT COUNT(*) AS total_users FROM users;

-- Calculate total and average order amounts
SELECT SUM(amount) AS total_amount,
       AVG(amount) AS avg_amount
FROM orders;

-- Get the price range
SELECT MIN(price) AS min_price,
       MAX(price) AS max_price
FROM products;
```

### Grouping with GROUP BY

```sql
-- Count employees and average salary by department
SELECT department,
       COUNT(*) AS employee_count,
       AVG(salary) AS avg_salary
FROM employees
GROUP BY department;

-- Monthly sales statistics
SELECT YEAR(order_date) AS year,
       MONTH(order_date) AS month,
       SUM(amount) AS monthly_sales,
       COUNT(*) AS order_count
FROM orders
GROUP BY YEAR(order_date), MONTH(order_date)
ORDER BY year, month;
```

### Filtering Groups with HAVING

```sql
-- Find customers with more than 10 orders
SELECT customer_id,
       COUNT(*) AS order_count,
       SUM(amount) AS total_spent
FROM orders
GROUP BY customer_id
HAVING COUNT(*) > 10;

-- Find departments with average salary above 10000
SELECT department, AVG(salary) AS avg_salary
FROM employees
GROUP BY department
HAVING AVG(salary) > 10000;
```

## Usage Examples

### Different Uses of COUNT

```sql
-- COUNT(*) counts all rows, including NULLs
SELECT COUNT(*) AS total_rows FROM users;

-- COUNT(col) only counts non-NULL values
SELECT COUNT(email) AS users_with_email FROM users;

-- COUNT(DISTINCT col) counts distinct values
SELECT COUNT(DISTINCT city) AS city_count FROM users;
```

### Concatenating Values with GROUP_CONCAT

```sql
-- List of employees per department
SELECT department,
       GROUP_CONCAT(name, ', ') AS members
FROM employees
GROUP BY department;

-- Product list per order
SELECT order_id,
       GROUP_CONCAT(product_name, ' | ') AS products
FROM order_items
GROUP BY order_id;
```

### Statistical Analysis

```sql
-- Comprehensive statistical analysis
SELECT department,
       COUNT(*) AS count,
       AVG(salary) AS avg_salary,
       MEDIAN(salary) AS median_salary,
       STDDEV(salary) AS stddev_salary,
       MIN(salary) AS min_salary,
       MAX(salary) AS max_salary
FROM employees
GROUP BY department;

-- Percentile analysis
SELECT PERCENTILE(response_time, 0.50) AS p50,
       PERCENTILE(response_time, 0.90) AS p90,
       PERCENTILE(response_time, 0.95) AS p95,
       PERCENTILE(response_time, 0.99) AS p99
FROM api_requests;
```

### Mode Analysis

```sql
-- Find the most common product category
SELECT MODE(category) AS most_common_category
FROM products;

-- Find the most popular payment method by region
SELECT region,
       MODE(payment_method) AS preferred_payment
FROM orders
GROUP BY region;
```
