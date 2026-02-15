# 日期时间函数

SQLExec 提供了丰富的日期时间函数，用于获取当前时间、提取日期组件、格式化、日期运算等操作。

## 函数列表

| 函数 | 说明 | 示例 |
|------|------|------|
| `NOW()` | 返回当前日期和时间 | `SELECT NOW();` -- `'2025-06-15 14:30:00'` |
| `CURRENT_TIMESTAMP` | NOW 的别名 | `SELECT CURRENT_TIMESTAMP;` -- `'2025-06-15 14:30:00'` |
| `DATE(expr)` | 提取日期部分 | `SELECT DATE('2025-06-15 14:30:00');` -- `'2025-06-15'` |
| `TIME(expr)` | 提取时间部分 | `SELECT TIME('2025-06-15 14:30:00');` -- `'14:30:00'` |
| `YEAR(date)` | 提取年份 | `SELECT YEAR('2025-06-15');` -- `2025` |
| `MONTH(date)` | 提取月份（1-12） | `SELECT MONTH('2025-06-15');` -- `6` |
| `DAY(date)` | 提取日期中的天（1-31） | `SELECT DAY('2025-06-15');` -- `15` |
| `HOUR(time)` | 提取小时（0-23） | `SELECT HOUR('14:30:00');` -- `14` |
| `MINUTE(time)` | 提取分钟（0-59） | `SELECT MINUTE('14:30:00');` -- `30` |
| `SECOND(time)` | 提取秒（0-59） | `SELECT SECOND('14:30:45');` -- `45` |
| `WEEK(date)` | 返回一年中的第几周 | `SELECT WEEK('2025-06-15');` -- `24` |
| `WEEKDAY(date)` | 返回星期几（0=周一, 6=周日） | `SELECT WEEKDAY('2025-06-15');` -- `6` |
| `DAYOFWEEK(date)` | 返回星期几（1=周日, 7=周六） | `SELECT DAYOFWEEK('2025-06-15');` -- `1` |
| `DAYOFYEAR(date)` | 返回一年中的第几天 | `SELECT DAYOFYEAR('2025-06-15');` -- `166` |
| `DATE_FORMAT(date, fmt)` | 按格式模板格式化日期 | `SELECT DATE_FORMAT(NOW(), '%Y年%m月%d日');` -- `'2025年06月15日'` |
| `STR_TO_DATE(s, fmt)` | 按格式模板解析日期字符串 | `SELECT STR_TO_DATE('2025/06/15', '%Y/%m/%d');` -- `'2025-06-15'` |
| `DATE_ADD(date, INTERVAL n unit)` | 日期加上指定间隔 | `SELECT DATE_ADD('2025-06-15', INTERVAL 30 DAY);` -- `'2025-07-15'` |
| `DATE_SUB(date, INTERVAL n unit)` | 日期减去指定间隔 | `SELECT DATE_SUB('2025-06-15', INTERVAL 1 MONTH);` -- `'2025-05-15'` |
| `DATEDIFF(d1, d2)` | 返回两个日期之间的天数差 | `SELECT DATEDIFF('2025-12-31', '2025-01-01');` -- `364` |
| `TIMESTAMPDIFF(unit, d1, d2)` | 返回两个日期之间指定单位的差 | `SELECT TIMESTAMPDIFF(MONTH, '2025-01-01', '2025-06-15');` -- `5` |
| `FROM_UNIXTIME(ts)` | 将 Unix 时间戳转换为日期时间 | `SELECT FROM_UNIXTIME(1718448000);` -- `'2025-06-15 12:00:00'` |
| `UNIX_TIMESTAMP(date)` | 将日期时间转换为 Unix 时间戳 | `SELECT UNIX_TIMESTAMP('2025-06-15 12:00:00');` -- `1718448000` |
| `EXTRACT(unit FROM date)` | 从日期中提取指定部分 | `SELECT EXTRACT(YEAR FROM '2025-06-15');` -- `2025` |

## 格式化符号

`DATE_FORMAT` 和 `STR_TO_DATE` 支持以下格式化符号：

| 符号 | 说明 | 示例 |
|------|------|------|
| `%Y` | 四位年份 | `2025` |
| `%y` | 两位年份 | `25` |
| `%m` | 月份（01-12） | `06` |
| `%d` | 日期（01-31） | `15` |
| `%H` | 小时（00-23） | `14` |
| `%i` | 分钟（00-59） | `30` |
| `%s` | 秒（00-59） | `45` |
| `%W` | 星期名称 | `Sunday` |
| `%w` | 星期数字（0=周日） | `0` |
| `%j` | 年中第几天（001-366） | `166` |

## 时间间隔单位

`DATE_ADD`、`DATE_SUB`、`TIMESTAMPDIFF`、`EXTRACT` 支持以下间隔单位：

- `SECOND` -- 秒
- `MINUTE` -- 分钟
- `HOUR` -- 小时
- `DAY` -- 天
- `WEEK` -- 周
- `MONTH` -- 月
- `YEAR` -- 年

## 使用示例

### 获取当前时间信息

```sql
-- 当前日期和时间
SELECT NOW() AS current_time;

-- 提取当前年月日
SELECT YEAR(NOW()) AS year,
       MONTH(NOW()) AS month,
       DAY(NOW()) AS day;
```

### 日期运算

```sql
-- 查询最近 7 天的订单
SELECT * FROM orders
WHERE created_at >= DATE_SUB(NOW(), INTERVAL 7 DAY);

-- 计算订单距今天数
SELECT order_id, DATEDIFF(NOW(), created_at) AS days_ago
FROM orders;

-- 计算到期日
SELECT contract_id,
       start_date,
       DATE_ADD(start_date, INTERVAL duration_months MONTH) AS end_date
FROM contracts;
```

### 日期格式化

```sql
-- 格式化为中文日期
SELECT DATE_FORMAT(created_at, '%Y年%m月%d日 %H:%i') AS formatted_date
FROM articles;

-- 解析自定义格式的日期字符串
SELECT STR_TO_DATE('15/06/2025', '%d/%m/%Y') AS parsed_date;
```

### 按时间维度统计

```sql
-- 按月统计销售额
SELECT YEAR(order_date) AS year,
       MONTH(order_date) AS month,
       SUM(amount) AS total_sales
FROM orders
GROUP BY YEAR(order_date), MONTH(order_date)
ORDER BY year, month;

-- 按星期几统计订单量
SELECT DAYOFWEEK(created_at) AS day_of_week,
       COUNT(*) AS order_count
FROM orders
GROUP BY DAYOFWEEK(created_at);
```

### Unix 时间戳转换

```sql
-- 时间戳转日期
SELECT FROM_UNIXTIME(created_ts) AS created_at
FROM events;

-- 日期转时间戳
SELECT UNIX_TIMESTAMP(NOW()) AS current_ts;
```
