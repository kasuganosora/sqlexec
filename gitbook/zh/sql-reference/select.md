# SELECT 查询

`SELECT` 是最常用的 SQL 语句，用于从表中检索数据。

## 基本查询

查询指定列：

```sql
SELECT name, age FROM users;
```

查询所有列：

```sql
SELECT * FROM users;
```

## WHERE 条件过滤

### 比较运算符

| 运算符 | 说明 | 示例 |
|--------|------|------|
| `=` | 等于 | `WHERE age = 18` |
| `!=` 或 `<>` | 不等于 | `WHERE status != 'inactive'` |
| `<` | 小于 | `WHERE price < 100` |
| `>` | 大于 | `WHERE score > 90` |
| `<=` | 小于等于 | `WHERE quantity <= 0` |
| `>=` | 大于等于 | `WHERE rating >= 4.5` |

### LIKE 模糊匹配

```sql
-- % 匹配任意多个字符
SELECT * FROM users WHERE name LIKE '张%';

-- _ 匹配单个字符
SELECT * FROM users WHERE code LIKE 'A_01';
```

### IN 范围匹配

```sql
SELECT * FROM products WHERE category IN ('电子', '图书', '服装');
```

### BETWEEN 区间查询

```sql
SELECT * FROM orders WHERE amount BETWEEN 100 AND 500;
```

### NULL 值判断

```sql
-- 判断为 NULL
SELECT * FROM users WHERE email IS NULL;

-- 判断不为 NULL
SELECT * FROM users WHERE phone IS NOT NULL;
```

## 逻辑运算符

使用 `AND`、`OR`、`NOT` 组合多个条件：

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

## ORDER BY 排序

### 升序排序（默认）

```sql
SELECT * FROM users ORDER BY created_at ASC;
```

### 降序排序

```sql
SELECT * FROM products ORDER BY price DESC;
```

### 多列排序

```sql
SELECT * FROM students ORDER BY grade DESC, name ASC;
```

## LIMIT 与 OFFSET 分页

```sql
-- 返回前 10 条记录
SELECT * FROM logs ORDER BY created_at DESC LIMIT 10;

-- 跳过前 20 条，返回接下来的 10 条（第 3 页）
SELECT * FROM logs ORDER BY created_at DESC LIMIT 10 OFFSET 20;
```

## DISTINCT 去重

```sql
SELECT DISTINCT category FROM products;
```

```sql
SELECT DISTINCT city, province FROM addresses;
```

## 列别名

使用 `AS` 为列指定别名：

```sql
SELECT
  name AS 姓名,
  age AS 年龄,
  score * 1.1 AS 加权分数
FROM students;
```

`AS` 关键字可以省略：

```sql
SELECT name 姓名, age 年龄 FROM students;
```

## SELECT 列表中的表达式

可以在 SELECT 列表中使用各种表达式：

```sql
SELECT
  name,
  price,
  quantity,
  price * quantity AS total,
  CASE
    WHEN price * quantity > 1000 THEN '大额'
    WHEN price * quantity > 100  THEN '中额'
    ELSE '小额'
  END AS order_level
FROM order_items;
```

## 综合示例

```sql
-- 查询活跃用户，按注册时间倒序排列，分页返回
SELECT
  id,
  name AS 用户名,
  email AS 邮箱,
  created_at AS 注册时间
FROM users
WHERE status = 'active'
  AND created_at >= '2025-01-01'
  AND email IS NOT NULL
ORDER BY created_at DESC
LIMIT 20 OFFSET 0;
```

```sql
-- 模糊搜索商品
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
