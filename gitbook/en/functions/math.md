# Math Functions

SQLExec provides a comprehensive set of math functions covering basic arithmetic, rounding, trigonometric functions, logarithms, random numbers, and more.

## Function List

| Function | Description | Example |
|----------|-------------|---------|
| `ABS(x)` | Return the absolute value | `SELECT ABS(-15.7);` -- `15.7` |
| `CEIL(x)` | Round up to the nearest integer | `SELECT CEIL(4.2);` -- `5` |
| `CEILING(x)` | Alias for CEIL | `SELECT CEILING(4.2);` -- `5` |
| `FLOOR(x)` | Round down to the nearest integer | `SELECT FLOOR(4.8);` -- `4` |
| `ROUND(x, d)` | Round to d decimal places | `SELECT ROUND(3.14159, 2);` -- `3.14` |
| `SQRT(x)` | Return the square root | `SELECT SQRT(16);` -- `4.0` |
| `POWER(x, y)` | Return x raised to the power of y | `SELECT POWER(2, 10);` -- `1024` |
| `POW(x, y)` | Alias for POWER | `SELECT POW(3, 3);` -- `27` |
| `EXP(x)` | Return e raised to the power of x | `SELECT EXP(1);` -- `2.718281828...` |
| `LOG(x)` | Return the natural logarithm (base e) | `SELECT LOG(2.718281828);` -- `1.0` |
| `LN(x)` | Alias for LOG | `SELECT LN(10);` -- `2.302585...` |
| `LOG10(x)` | Return the base-10 logarithm | `SELECT LOG10(1000);` -- `3.0` |
| `LOG2(x)` | Return the base-2 logarithm | `SELECT LOG2(8);` -- `3.0` |
| `SIN(x)` | Return the sine (x in radians) | `SELECT SIN(PI() / 2);` -- `1.0` |
| `COS(x)` | Return the cosine (x in radians) | `SELECT COS(0);` -- `1.0` |
| `TAN(x)` | Return the tangent (x in radians) | `SELECT TAN(PI() / 4);` -- `1.0` |
| `ASIN(x)` | Return the arc sine (in radians) | `SELECT ASIN(1);` -- `1.570796...` |
| `ACOS(x)` | Return the arc cosine (in radians) | `SELECT ACOS(1);` -- `0.0` |
| `ATAN(x)` | Return the arc tangent (in radians) | `SELECT ATAN(1);` -- `0.785398...` |
| `ATAN2(y, x)` | Return the arc tangent of y/x (in radians) | `SELECT ATAN2(1, 1);` -- `0.785398...` |
| `RADIANS(x)` | Convert degrees to radians | `SELECT RADIANS(180);` -- `3.141592...` |
| `DEGREES(x)` | Convert radians to degrees | `SELECT DEGREES(PI());` -- `180.0` |
| `SIGN(x)` | Return the sign (-1, 0, or 1) | `SELECT SIGN(-42);` -- `-1` |
| `RANDOM()` | Return a random number between 0 and 1 | `SELECT RANDOM();` -- `0.738291...` |
| `RAND()` | Alias for RANDOM | `SELECT RAND();` -- `0.521873...` |
| `LEAST(a, b, ...)` | Return the minimum value | `SELECT LEAST(10, 3, 7, 1);` -- `1` |
| `GREATEST(a, b, ...)` | Return the maximum value | `SELECT GREATEST(10, 3, 7, 1);` -- `10` |
| `MOD(x, y)` | Return the modulo result | `SELECT MOD(17, 5);` -- `2` |
| `TRUNCATE(x, d)` | Truncate to d decimal places (no rounding) | `SELECT TRUNCATE(3.14159, 2);` -- `3.14` |
| `PI()` | Return the value of pi | `SELECT PI();` -- `3.141592653589793` |

## Usage Examples

### Basic Numeric Operations

```sql
-- Calculate discounted product price, rounded to two decimal places
SELECT name, price, ROUND(price * 0.85, 2) AS discounted_price
FROM products;

-- Calculate deviation using absolute value
SELECT ABS(actual - expected) AS deviation
FROM measurements;
```

### Rounding Operations

```sql
-- Round up to calculate the number of pages needed
SELECT CEIL(total_count * 1.0 / page_size) AS total_pages
FROM pagination_config;

-- Truncate to integer
SELECT TRUNCATE(score, 0) AS int_score
FROM exam_results;
```

### Trigonometric Functions and Angle Conversion

```sql
-- Calculate distance between two points using latitude and longitude (Haversine formula)
SELECT 6371 * 2 * ASIN(SQRT(
    POWER(SIN(RADIANS(lat2 - lat1) / 2), 2) +
    COS(RADIANS(lat1)) * COS(RADIANS(lat2)) *
    POWER(SIN(RADIANS(lon2 - lon1) / 2), 2)
)) AS distance_km
FROM locations;
```

### Logarithmic and Exponential Operations

```sql
-- Calculate compound interest
SELECT principal * POWER(1 + rate, years) AS future_value
FROM investments;

-- Logarithmic scaling
SELECT name, LOG10(population) AS log_population
FROM cities;
```

### Random Numbers and Comparisons

```sql
-- Random ordering
SELECT * FROM questions ORDER BY RANDOM() LIMIT 10;

-- Clamp a value to a range
SELECT GREATEST(0, LEAST(100, raw_score)) AS clamped_score
FROM results;
```
