# Function System Overview

SQLExec comes with a rich built-in function library covering 12 categories to meet the needs of data querying, transformation, analysis, and more.

## Function Categories

| Category | Description | Link |
|----------|-------------|------|
| String Functions | String concatenation, extraction, searching, conversion, etc. | [Details](string.md) |
| Math Functions | Numeric calculations, trigonometric functions, rounding, random numbers, etc. | [Details](math.md) |
| Date & Time Functions | Date parsing, formatting, arithmetic, extraction, etc. | [Details](datetime.md) |
| Aggregate Functions | Group statistics, sum, average, count, etc. | [Details](aggregate.md) |
| JSON Functions | JSON data extraction, construction, querying, modification | [Details](json-functions.md) |
| Control Flow Functions | Conditional evaluation, null handling, branching logic | [Details](control.md) |
| Encoding & Hash Functions | Base64, Hex encoding/decoding and common hash algorithms | [Details](encoding.md) |
| Similarity Functions | String similarity, cosine similarity calculation | [Details](similarity.md) |
| Vector Functions | Vector distance and similarity calculation, supporting vector search | [Details](vector.md) |
| Financial Functions | Net present value, annuity, interest rate, and other financial calculations | [Details](financial.md) |
| Bitwise Functions | Bitwise AND, OR, XOR, shift operations, etc. | [Details](bitwise.md) |
| System Functions | Type detection, UUID generation, environment information queries | [Details](system.md) |

## Function Types

SQLExec supports three types of functions:

### Scalar Functions

Scalar functions return a single value for each input row. Most built-in functions belong to this type.

```sql
SELECT UPPER(name), LENGTH(name) FROM users;
```

### Aggregate Functions

Aggregate functions perform calculations on a set of rows and return a single summary value. They are typically used with `GROUP BY`.

```sql
SELECT department, COUNT(*), AVG(salary)
FROM employees
GROUP BY department;
```

### Window Functions

Window functions perform calculations across a set of rows related to the current row without collapsing multiple rows into one. The window is defined using the `OVER()` clause.

```sql
SELECT name, salary,
       RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS rank
FROM employees;
```

## Basic Usage

Functions can be used in `SELECT`, `WHERE`, `HAVING`, `ORDER BY`, and other clauses:

```sql
-- In SELECT
SELECT CONCAT(first_name, ' ', last_name) AS full_name FROM users;

-- In WHERE
SELECT * FROM orders WHERE YEAR(created_at) = 2025;

-- In ORDER BY
SELECT * FROM products ORDER BY LOWER(name);

-- Function nesting
SELECT UPPER(TRIM(name)) FROM users;
```

## User-Defined Functions (UDF)

In addition to built-in functions, SQLExec also supports User-Defined Functions. You can use the `CREATE FUNCTION` statement to register custom scalar or aggregate functions to extend the system's computational capabilities.

```sql
-- Register a custom function example
CREATE FUNCTION double_value(x INT) RETURNS INT
AS 'return x * 2';

-- Use the custom function
SELECT double_value(price) FROM products;
```

For detailed instructions on creating and managing UDFs, see the [User-Defined Functions](../advanced/udf.md) chapter.
