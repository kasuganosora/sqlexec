# 数学函数

SQLExec 提供了完整的数学计算函数，涵盖基本运算、取整、三角函数、对数、随机数等。

## 函数列表

| 函数 | 说明 | 示例 |
|------|------|------|
| `ABS(x)` | 返回绝对值 | `SELECT ABS(-15.7);` -- `15.7` |
| `CEIL(x)` | 向上取整 | `SELECT CEIL(4.2);` -- `5` |
| `CEILING(x)` | CEIL 的别名 | `SELECT CEILING(4.2);` -- `5` |
| `FLOOR(x)` | 向下取整 | `SELECT FLOOR(4.8);` -- `4` |
| `ROUND(x, d)` | 四舍五入到 d 位小数 | `SELECT ROUND(3.14159, 2);` -- `3.14` |
| `SQRT(x)` | 返回平方根 | `SELECT SQRT(16);` -- `4.0` |
| `POWER(x, y)` | 返回 x 的 y 次幂 | `SELECT POWER(2, 10);` -- `1024` |
| `POW(x, y)` | POWER 的别名 | `SELECT POW(3, 3);` -- `27` |
| `EXP(x)` | 返回 e 的 x 次幂 | `SELECT EXP(1);` -- `2.718281828...` |
| `LOG(x)` | 返回自然对数（以 e 为底） | `SELECT LOG(2.718281828);` -- `1.0` |
| `LN(x)` | LOG 的别名 | `SELECT LN(10);` -- `2.302585...` |
| `LOG10(x)` | 返回以 10 为底的对数 | `SELECT LOG10(1000);` -- `3.0` |
| `LOG2(x)` | 返回以 2 为底的对数 | `SELECT LOG2(8);` -- `3.0` |
| `SIN(x)` | 返回正弦值（x 为弧度） | `SELECT SIN(PI() / 2);` -- `1.0` |
| `COS(x)` | 返回余弦值（x 为弧度） | `SELECT COS(0);` -- `1.0` |
| `TAN(x)` | 返回正切值（x 为弧度） | `SELECT TAN(PI() / 4);` -- `1.0` |
| `ASIN(x)` | 返回反正弦值（弧度） | `SELECT ASIN(1);` -- `1.570796...` |
| `ACOS(x)` | 返回反余弦值（弧度） | `SELECT ACOS(1);` -- `0.0` |
| `ATAN(x)` | 返回反正切值（弧度） | `SELECT ATAN(1);` -- `0.785398...` |
| `ATAN2(y, x)` | 返回 y/x 的反正切值（弧度） | `SELECT ATAN2(1, 1);` -- `0.785398...` |
| `RADIANS(x)` | 将角度转换为弧度 | `SELECT RADIANS(180);` -- `3.141592...` |
| `DEGREES(x)` | 将弧度转换为角度 | `SELECT DEGREES(PI());` -- `180.0` |
| `SIGN(x)` | 返回符号（-1、0 或 1） | `SELECT SIGN(-42);` -- `-1` |
| `RANDOM()` | 返回 0 到 1 之间的随机数 | `SELECT RANDOM();` -- `0.738291...` |
| `RAND()` | RANDOM 的别名 | `SELECT RAND();` -- `0.521873...` |
| `LEAST(a, b, ...)` | 返回最小值 | `SELECT LEAST(10, 3, 7, 1);` -- `1` |
| `GREATEST(a, b, ...)` | 返回最大值 | `SELECT GREATEST(10, 3, 7, 1);` -- `10` |
| `MOD(x, y)` | 返回取模结果 | `SELECT MOD(17, 5);` -- `2` |
| `TRUNCATE(x, d)` | 截断到 d 位小数（不四舍五入） | `SELECT TRUNCATE(3.14159, 2);` -- `3.14` |
| `PI()` | 返回圆周率 | `SELECT PI();` -- `3.141592653589793` |

## 使用示例

### 基本数值运算

```sql
-- 计算商品折后价格，四舍五入到两位小数
SELECT name, price, ROUND(price * 0.85, 2) AS discounted_price
FROM products;

-- 取绝对值计算偏差
SELECT ABS(actual - expected) AS deviation
FROM measurements;
```

### 取整操作

```sql
-- 向上取整计算所需页数
SELECT CEIL(total_count * 1.0 / page_size) AS total_pages
FROM pagination_config;

-- 截断到整数
SELECT TRUNCATE(score, 0) AS int_score
FROM exam_results;
```

### 三角函数与角度转换

```sql
-- 根据经纬度计算两点距离（Haversine 公式）
SELECT 6371 * 2 * ASIN(SQRT(
    POWER(SIN(RADIANS(lat2 - lat1) / 2), 2) +
    COS(RADIANS(lat1)) * COS(RADIANS(lat2)) *
    POWER(SIN(RADIANS(lon2 - lon1) / 2), 2)
)) AS distance_km
FROM locations;
```

### 对数与指数运算

```sql
-- 计算复利
SELECT principal * POWER(1 + rate, years) AS future_value
FROM investments;

-- 对数缩放
SELECT name, LOG10(population) AS log_population
FROM cities;
```

### 随机数与比较

```sql
-- 随机排序
SELECT * FROM questions ORDER BY RANDOM() LIMIT 10;

-- 限制数值范围（clamp）
SELECT GREATEST(0, LEAST(100, raw_score)) AS clamped_score
FROM results;
```
