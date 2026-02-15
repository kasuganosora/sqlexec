# Date & Time Functions

SQLExec provides a rich set of date and time functions for retrieving the current time, extracting date components, formatting, date arithmetic, and more.

## Function List

| Function | Description | Example |
|----------|-------------|---------|
| `NOW()` | Return the current date and time | `SELECT NOW();` -- `'2025-06-15 14:30:00'` |
| `CURRENT_TIMESTAMP` | Alias for NOW | `SELECT CURRENT_TIMESTAMP;` -- `'2025-06-15 14:30:00'` |
| `DATE(expr)` | Extract the date part | `SELECT DATE('2025-06-15 14:30:00');` -- `'2025-06-15'` |
| `TIME(expr)` | Extract the time part | `SELECT TIME('2025-06-15 14:30:00');` -- `'14:30:00'` |
| `YEAR(date)` | Extract the year | `SELECT YEAR('2025-06-15');` -- `2025` |
| `MONTH(date)` | Extract the month (1-12) | `SELECT MONTH('2025-06-15');` -- `6` |
| `DAY(date)` | Extract the day of the month (1-31) | `SELECT DAY('2025-06-15');` -- `15` |
| `HOUR(time)` | Extract the hour (0-23) | `SELECT HOUR('14:30:00');` -- `14` |
| `MINUTE(time)` | Extract the minute (0-59) | `SELECT MINUTE('14:30:00');` -- `30` |
| `SECOND(time)` | Extract the second (0-59) | `SELECT SECOND('14:30:45');` -- `45` |
| `WEEK(date)` | Return the week number of the year | `SELECT WEEK('2025-06-15');` -- `24` |
| `WEEKDAY(date)` | Return the day of the week (0=Monday, 6=Sunday) | `SELECT WEEKDAY('2025-06-15');` -- `6` |
| `DAYOFWEEK(date)` | Return the day of the week (1=Sunday, 7=Saturday) | `SELECT DAYOFWEEK('2025-06-15');` -- `1` |
| `DAYOFYEAR(date)` | Return the day of the year | `SELECT DAYOFYEAR('2025-06-15');` -- `166` |
| `DATE_FORMAT(date, fmt)` | Format a date using a format template | `SELECT DATE_FORMAT(NOW(), '%Y年%m月%d日');` -- `'2025年06月15日'` |
| `STR_TO_DATE(s, fmt)` | Parse a date string using a format template | `SELECT STR_TO_DATE('2025/06/15', '%Y/%m/%d');` -- `'2025-06-15'` |
| `DATE_ADD(date, INTERVAL n unit)` | Add a specified interval to a date | `SELECT DATE_ADD('2025-06-15', INTERVAL 30 DAY);` -- `'2025-07-15'` |
| `DATE_SUB(date, INTERVAL n unit)` | Subtract a specified interval from a date | `SELECT DATE_SUB('2025-06-15', INTERVAL 1 MONTH);` -- `'2025-05-15'` |
| `DATEDIFF(d1, d2)` | Return the number of days between two dates | `SELECT DATEDIFF('2025-12-31', '2025-01-01');` -- `364` |
| `TIMESTAMPDIFF(unit, d1, d2)` | Return the difference between two dates in the specified unit | `SELECT TIMESTAMPDIFF(MONTH, '2025-01-01', '2025-06-15');` -- `5` |
| `FROM_UNIXTIME(ts)` | Convert a Unix timestamp to a datetime | `SELECT FROM_UNIXTIME(1718448000);` -- `'2025-06-15 12:00:00'` |
| `UNIX_TIMESTAMP(date)` | Convert a datetime to a Unix timestamp | `SELECT UNIX_TIMESTAMP('2025-06-15 12:00:00');` -- `1718448000` |
| `EXTRACT(unit FROM date)` | Extract a specific part from a date | `SELECT EXTRACT(YEAR FROM '2025-06-15');` -- `2025` |

## Format Specifiers

`DATE_FORMAT` and `STR_TO_DATE` support the following format specifiers:

| Specifier | Description | Example |
|-----------|-------------|---------|
| `%Y` | Four-digit year | `2025` |
| `%y` | Two-digit year | `25` |
| `%m` | Month (01-12) | `06` |
| `%d` | Day (01-31) | `15` |
| `%H` | Hour (00-23) | `14` |
| `%i` | Minute (00-59) | `30` |
| `%s` | Second (00-59) | `45` |
| `%W` | Weekday name | `Sunday` |
| `%w` | Weekday number (0=Sunday) | `0` |
| `%j` | Day of the year (001-366) | `166` |

## Interval Units

`DATE_ADD`, `DATE_SUB`, `TIMESTAMPDIFF`, and `EXTRACT` support the following interval units:

- `SECOND` -- Seconds
- `MINUTE` -- Minutes
- `HOUR` -- Hours
- `DAY` -- Days
- `WEEK` -- Weeks
- `MONTH` -- Months
- `YEAR` -- Years

## Usage Examples

### Getting Current Time Information

```sql
-- Current date and time
SELECT NOW() AS current_time;

-- Extract current year, month, and day
SELECT YEAR(NOW()) AS year,
       MONTH(NOW()) AS month,
       DAY(NOW()) AS day;
```

### Date Arithmetic

```sql
-- Query orders from the last 7 days
SELECT * FROM orders
WHERE created_at >= DATE_SUB(NOW(), INTERVAL 7 DAY);

-- Calculate the number of days since an order was placed
SELECT order_id, DATEDIFF(NOW(), created_at) AS days_ago
FROM orders;

-- Calculate expiration date
SELECT contract_id,
       start_date,
       DATE_ADD(start_date, INTERVAL duration_months MONTH) AS end_date
FROM contracts;
```

### Date Formatting

```sql
-- Format as a localized date string
SELECT DATE_FORMAT(created_at, '%Y年%m月%d日 %H:%i') AS formatted_date
FROM articles;

-- Parse a custom-formatted date string
SELECT STR_TO_DATE('15/06/2025', '%d/%m/%Y') AS parsed_date;
```

### Aggregating by Time Dimensions

```sql
-- Monthly sales statistics
SELECT YEAR(order_date) AS year,
       MONTH(order_date) AS month,
       SUM(amount) AS total_sales
FROM orders
GROUP BY YEAR(order_date), MONTH(order_date)
ORDER BY year, month;

-- Order count by day of week
SELECT DAYOFWEEK(created_at) AS day_of_week,
       COUNT(*) AS order_count
FROM orders
GROUP BY DAYOFWEEK(created_at);
```

### Unix Timestamp Conversion

```sql
-- Timestamp to datetime
SELECT FROM_UNIXTIME(created_ts) AS created_at
FROM events;

-- Datetime to timestamp
SELECT UNIX_TIMESTAMP(NOW()) AS current_ts;
```
