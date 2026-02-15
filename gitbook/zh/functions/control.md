# 控制流函数

控制流函数用于实现条件判断、空值处理和分支逻辑，是构建复杂查询的重要工具。

## 函数列表

| 函数 | 说明 | 示例 |
|------|------|------|
| `IF(cond, true_val, false_val)` | 条件为真返回 true_val，否则返回 false_val | `SELECT IF(score >= 60, '及格', '不及格');` |
| `CASE WHEN ... THEN ... ELSE ... END` | 多分支条件表达式 | 见下方详细说明 |
| `COALESCE(v1, v2, ...)` | 返回参数列表中第一个非 NULL 值 | `SELECT COALESCE(nickname, username, 'anonymous');` |
| `IFNULL(expr, default)` | 若 expr 为 NULL 则返回 default | `SELECT IFNULL(phone, 'N/A');` |
| `NULLIF(a, b)` | 若 a 等于 b 则返回 NULL，否则返回 a | `SELECT NULLIF(value, 0);` |

## 详细说明

### IF 函数

`IF` 函数是最简单的条件判断，适用于二选一的场景。

```sql
-- 基本用法
SELECT name,
       score,
       IF(score >= 60, '及格', '不及格') AS result
FROM students;

-- 嵌套 IF
SELECT name,
       IF(age >= 18, IF(age >= 65, '老年', '成年'), '未成年') AS age_group
FROM users;

-- 在聚合中使用 IF
SELECT COUNT(IF(status = 'active', 1, NULL)) AS active_count,
       COUNT(IF(status = 'inactive', 1, NULL)) AS inactive_count
FROM users;
```

### CASE WHEN 表达式

`CASE WHEN` 支持多个分支条件，适用于复杂的条件逻辑。

**搜索型 CASE：**

```sql
-- 按分数划分等级
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

**简单型 CASE：**

```sql
-- 状态码转中文描述
SELECT order_id,
       CASE status
           WHEN 'pending' THEN '待处理'
           WHEN 'shipped' THEN '已发货'
           WHEN 'delivered' THEN '已送达'
           WHEN 'cancelled' THEN '已取消'
           ELSE '未知状态'
       END AS status_text
FROM orders;
```

**在聚合和排序中使用 CASE：**

```sql
-- 条件聚合
SELECT department,
       SUM(CASE WHEN gender = 'M' THEN 1 ELSE 0 END) AS male_count,
       SUM(CASE WHEN gender = 'F' THEN 1 ELSE 0 END) AS female_count
FROM employees
GROUP BY department;

-- 自定义排序
SELECT * FROM tasks
ORDER BY CASE priority
    WHEN 'critical' THEN 1
    WHEN 'high' THEN 2
    WHEN 'medium' THEN 3
    WHEN 'low' THEN 4
    ELSE 5
END;
```

### COALESCE 函数

`COALESCE` 返回参数列表中第一个非 NULL 值，常用于设置默认值和合并多个可能为空的字段。

```sql
-- 使用默认值
SELECT COALESCE(nickname, username, email, 'anonymous') AS display_name
FROM users;

-- 合并多个地址字段
SELECT COALESCE(shipping_address, billing_address, default_address) AS address
FROM customers;

-- 处理可能的 NULL 计算
SELECT product_id,
       COALESCE(discount_price, original_price) AS final_price
FROM products;
```

### IFNULL 函数

`IFNULL` 是 `COALESCE` 的两参数简化版，当值为 NULL 时返回默认值。

```sql
-- 空值替换
SELECT name,
       IFNULL(phone, '未填写') AS phone,
       IFNULL(email, '未填写') AS email
FROM contacts;

-- 计算中处理 NULL
SELECT order_id,
       subtotal + IFNULL(shipping_fee, 0) + IFNULL(tax, 0) AS total
FROM orders;
```

### NULLIF 函数

`NULLIF` 在两个值相等时返回 NULL，常用于避免除零错误或将特定值转换为 NULL。

```sql
-- 避免除零错误
SELECT revenue / NULLIF(cost, 0) AS roi
FROM financials;

-- 将空字符串转为 NULL
SELECT NULLIF(description, '') AS description
FROM products;

-- 将占位值转为 NULL
SELECT NULLIF(phone, '000-0000-0000') AS phone
FROM users;
```

## 综合应用

```sql
-- 多重条件组合
SELECT name,
       COALESCE(
           IF(vip_level > 0,
              CASE vip_level
                  WHEN 1 THEN '银卡会员'
                  WHEN 2 THEN '金卡会员'
                  WHEN 3 THEN '钻石会员'
              END,
              NULL),
           '普通用户'
       ) AS user_type
FROM users;
```
