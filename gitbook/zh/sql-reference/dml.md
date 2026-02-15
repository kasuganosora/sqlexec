# INSERT / UPDATE / DELETE

DML（数据操作语言）语句用于对表中的数据进行插入、更新和删除操作。

## INSERT 插入数据

### 基本插入

指定列名插入单条记录：

```sql
INSERT INTO users (name, email, age) VALUES ('张三', 'zhangsan@example.com', 28);
```

### 省略列名

当插入所有列的值时，可以省略列名（需按表定义的列顺序提供值）：

```sql
INSERT INTO users VALUES (1, '张三', 'zhangsan@example.com', 28, '2026-01-15');
```

### 批量插入

一次插入多条记录，使用逗号分隔多个值元组：

```sql
INSERT INTO products (name, category, price) VALUES
  ('无线鼠标', '外设', 79.00),
  ('机械键盘', '外设', 299.00),
  ('显示器', '显示', 1599.00),
  ('USB-C 线', '配件', 19.90);
```

批量插入比逐条插入效率更高，推荐用于大量数据写入场景。

### 插入含默认值的记录

如果列定义了 `DEFAULT` 值，可以省略该列：

```sql
-- status 列有默认值 'active'，无需显式指定
INSERT INTO users (name, email) VALUES ('李四', 'lisi@example.com');
```

## UPDATE 更新数据

### 基本更新

```sql
UPDATE users SET email = 'new_email@example.com' WHERE id = 1;
```

### 更新多列

```sql
UPDATE users
SET name = '王五',
    email = 'wangwu@example.com',
    age = 30
WHERE id = 2;
```

### 使用表达式更新

```sql
-- 价格上调 10%
UPDATE products SET price = price * 1.1 WHERE category = '电子';
```

### 条件更新

```sql
UPDATE orders
SET status = 'expired'
WHERE status = 'pending'
  AND created_at < '2025-01-01';
```

{% hint style="warning" %}
**注意：** 不带 `WHERE` 子句的 `UPDATE` 会更新表中所有行，请谨慎使用。
{% endhint %}

## DELETE 删除数据

### 基本删除

```sql
DELETE FROM users WHERE id = 100;
```

### 条件删除

```sql
DELETE FROM logs WHERE created_at < '2024-01-01';
```

### 组合条件删除

```sql
DELETE FROM sessions
WHERE expired = TRUE
  AND last_active < '2025-06-01';
```

{% hint style="warning" %}
**注意：** 不带 `WHERE` 子句的 `DELETE` 会删除表中所有行。如需清空整张表，建议使用 `TRUNCATE TABLE`，效率更高。
{% endhint %}

## 返回值

DML 语句的执行结果包含以下信息：

| 语句 | 返回值 | 说明 |
|------|--------|------|
| `INSERT` | affected rows | 成功插入的行数 |
| `INSERT` | last insert ID | 自增列生成的最后一个 ID |
| `UPDATE` | affected rows | 实际被修改的行数 |
| `DELETE` | affected rows | 被删除的行数 |

### 示例：获取返回值

```go
// INSERT 返回 last insert ID
result, err := db.Exec("INSERT INTO users (name) VALUES (?)", "张三")
lastID, _ := result.LastInsertId()
affected, _ := result.RowsAffected()
fmt.Printf("插入成功: ID=%d, 影响行数=%d\n", lastID, affected)

// UPDATE 返回受影响行数
result, err = db.Exec("UPDATE users SET age = ? WHERE name = ?", 30, "张三")
affected, _ = result.RowsAffected()
fmt.Printf("更新成功: 影响行数=%d\n", affected)

// DELETE 返回受影响行数
result, err = db.Exec("DELETE FROM users WHERE status = ?", "inactive")
affected, _ = result.RowsAffected()
fmt.Printf("删除成功: 影响行数=%d\n", affected)
```
