# JOIN 操作

JOIN 用于将两张或多张表的数据按照关联条件进行合并查询。

## INNER JOIN 内连接

返回两张表中满足连接条件的匹配行。只有两侧都存在匹配的行才会出现在结果中。

```sql
SELECT
  o.id AS 订单号,
  u.name AS 用户名,
  o.total_price AS 金额
FROM orders o
INNER JOIN users u ON o.user_id = u.id;
```

`INNER JOIN` 中的 `INNER` 关键字可以省略，直接写 `JOIN` 等价于 `INNER JOIN`：

```sql
SELECT o.id, u.name
FROM orders o
JOIN users u ON o.user_id = u.id;
```

## LEFT JOIN 左连接

返回左表的所有行。如果右表中没有匹配的行，对应列填充 NULL。

```sql
SELECT
  u.name AS 用户名,
  o.id AS 订单号,
  o.total_price AS 金额
FROM users u
LEFT JOIN orders o ON u.id = o.user_id;
```

结果示例：

| 用户名 | 订单号 | 金额 |
|--------|--------|------|
| 张三 | 1001 | 299.00 |
| 张三 | 1002 | 58.50 |
| 李四 | 1003 | 1200.00 |
| 王五 | NULL | NULL |

上例中，王五没有订单，但因为使用了 `LEFT JOIN`，他仍然出现在结果中，订单相关列为 NULL。

## RIGHT JOIN 右连接

返回右表的所有行。如果左表中没有匹配的行，对应列填充 NULL。

```sql
SELECT
  u.name AS 用户名,
  p.name AS 商品名,
  p.category AS 分类
FROM users u
RIGHT JOIN products p ON u.favorite_product_id = p.id;
```

`RIGHT JOIN` 的效果与交换左右表后使用 `LEFT JOIN` 等价。

## CROSS JOIN 交叉连接

返回两张表的笛卡尔积，即左表的每一行与右表的每一行进行组合。

```sql
SELECT
  c.name AS 颜色,
  s.label AS 尺码
FROM colors c
CROSS JOIN sizes s;
```

如果 `colors` 有 3 行，`sizes` 有 4 行，结果将有 3 x 4 = 12 行。

{% hint style="warning" %}
交叉连接会产生大量结果行，请谨慎使用，尤其是在大表上。
{% endhint %}

## 自连接（Self-Join）

将表与自身进行连接，常用于处理层级关系数据（如组织架构、分类树等）。

```sql
-- 查询员工及其直属经理
SELECT
  e.name AS 员工,
  m.name AS 经理
FROM employees e
LEFT JOIN employees m ON e.manager_id = m.id;
```

```sql
-- 查找同一城市的用户对
SELECT
  a.name AS 用户A,
  b.name AS 用户B,
  a.city AS 城市
FROM users a
INNER JOIN users b ON a.city = b.city AND a.id < b.id;
```

## 多表连接

可以链式连接多张表：

```sql
SELECT
  o.id AS 订单号,
  u.name AS 用户名,
  p.name AS 商品名,
  oi.quantity AS 数量,
  oi.unit_price AS 单价
FROM orders o
INNER JOIN users u ON o.user_id = u.id
INNER JOIN order_items oi ON o.id = oi.order_id
INNER JOIN products p ON oi.product_id = p.id
WHERE o.status = 'completed'
ORDER BY o.id;
```

## 复合连接条件

连接条件可以包含多个条件，使用 `AND` 连接：

```sql
SELECT
  s.student_name,
  c.course_name,
  sc.score
FROM student_courses sc
INNER JOIN students s ON sc.student_id = s.id
INNER JOIN courses c ON sc.course_id = c.id AND c.active = TRUE
WHERE sc.score >= 60;
```

## 综合示例

```sql
-- 查询每位用户的最近订单及商品详情
SELECT
  u.name AS 用户名,
  u.email AS 邮箱,
  o.id AS 订单号,
  o.created_at AS 下单时间,
  p.name AS 商品名,
  p.category AS 分类,
  oi.quantity AS 数量,
  oi.unit_price * oi.quantity AS 小计
FROM users u
LEFT JOIN orders o ON u.id = o.user_id
LEFT JOIN order_items oi ON o.id = oi.order_id
LEFT JOIN products p ON oi.product_id = p.id
WHERE u.active = TRUE
ORDER BY u.name, o.created_at DESC;
```
