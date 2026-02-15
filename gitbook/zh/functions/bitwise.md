# 位运算函数

SQLExec 提供了一组位运算函数，用于对整数值进行按位操作，适用于权限管理、标志位处理、底层数据操作等场景。

## 函数列表

| 函数 | 说明 | 示例 |
|------|------|------|
| `BITWISE_AND(a, b)` | 按位与 | `SELECT BITWISE_AND(12, 10);` -- `8` |
| `BITWISE_OR(a, b)` | 按位或 | `SELECT BITWISE_OR(12, 10);` -- `14` |
| `BITWISE_XOR(a, b)` | 按位异或 | `SELECT BITWISE_XOR(12, 10);` -- `6` |
| `BITWISE_NOT(a)` | 按位取反 | `SELECT BITWISE_NOT(0);` -- `-1` |
| `LSHIFT(a, n)` | 左移 n 位 | `SELECT LSHIFT(1, 3);` -- `8` |
| `RSHIFT(a, n)` | 右移 n 位 | `SELECT RSHIFT(16, 2);` -- `4` |

## 运算规则

### BITWISE_AND -- 按位与

对两个整数的每一位执行与运算。只有两位都为 1 时结果才为 1。

```
  12 = 1100
  10 = 1010
  ----------
AND  = 1000 = 8
```

```sql
SELECT BITWISE_AND(12, 10);  -- 8
```

### BITWISE_OR -- 按位或

对两个整数的每一位执行或运算。任一位为 1 时结果即为 1。

```
  12 = 1100
  10 = 1010
  ----------
OR   = 1110 = 14
```

```sql
SELECT BITWISE_OR(12, 10);  -- 14
```

### BITWISE_XOR -- 按位异或

对两个整数的每一位执行异或运算。两位不同时结果为 1，相同时为 0。

```
  12 = 1100
  10 = 1010
  ----------
XOR  = 0110 = 6
```

```sql
SELECT BITWISE_XOR(12, 10);  -- 6
```

### BITWISE_NOT -- 按位取反

对整数的每一位取反，0 变为 1，1 变为 0。

```sql
SELECT BITWISE_NOT(0);    -- -1
SELECT BITWISE_NOT(255);  -- -256
```

### LSHIFT -- 左移

将整数的二进制位向左移动指定位数，右侧补 0。等价于乘以 2 的 n 次方。

```sql
SELECT LSHIFT(1, 0);  -- 1   (0001)
SELECT LSHIFT(1, 1);  -- 2   (0010)
SELECT LSHIFT(1, 2);  -- 4   (0100)
SELECT LSHIFT(1, 3);  -- 8   (1000)
SELECT LSHIFT(5, 2);  -- 20  (10100)
```

### RSHIFT -- 右移

将整数的二进制位向右移动指定位数。等价于整除 2 的 n 次方。

```sql
SELECT RSHIFT(16, 1);  -- 8
SELECT RSHIFT(16, 2);  -- 4
SELECT RSHIFT(16, 3);  -- 2
SELECT RSHIFT(16, 4);  -- 1
```

## 使用示例

### 权限管理

使用位标志管理用户权限，每一位代表一项权限：

```sql
-- 权限定义：
-- 第 0 位 (1)  = 读取权限
-- 第 1 位 (2)  = 写入权限
-- 第 2 位 (4)  = 删除权限
-- 第 3 位 (8)  = 管理权限

-- 检查用户是否有写入权限
SELECT * FROM users
WHERE BITWISE_AND(permissions, 2) > 0;

-- 检查用户是否同时有读取和写入权限 (1 + 2 = 3)
SELECT * FROM users
WHERE BITWISE_AND(permissions, 3) = 3;

-- 为用户添加删除权限
SELECT BITWISE_OR(permissions, 4) AS new_permissions
FROM users
WHERE id = 1;

-- 移除用户的管理权限
SELECT BITWISE_AND(permissions, BITWISE_NOT(8)) AS new_permissions
FROM users
WHERE id = 1;

-- 切换权限（有则移除，无则添加）
SELECT BITWISE_XOR(permissions, 4) AS toggled_permissions
FROM users
WHERE id = 1;
```

### 标志位处理

```sql
-- 检查第 n 位是否设置
SELECT id, name,
       IF(BITWISE_AND(flags, LSHIFT(1, 0)) > 0, '是', '否') AS flag_0,
       IF(BITWISE_AND(flags, LSHIFT(1, 1)) > 0, '是', '否') AS flag_1,
       IF(BITWISE_AND(flags, LSHIFT(1, 2)) > 0, '是', '否') AS flag_2
FROM records;

-- 统计设置了特定标志位的记录数
SELECT COUNT(*) AS count
FROM records
WHERE BITWISE_AND(flags, LSHIFT(1, 5)) > 0;
```

### 数据压缩与提取

```sql
-- 将 RGB 颜色值打包为单个整数
SELECT BITWISE_OR(
    BITWISE_OR(LSHIFT(red, 16), LSHIFT(green, 8)),
    blue
) AS color_int
FROM colors;

-- 从打包的颜色整数中提取 RGB 分量
SELECT BITWISE_AND(RSHIFT(color_int, 16), 255) AS red,
       BITWISE_AND(RSHIFT(color_int, 8), 255) AS green,
       BITWISE_AND(color_int, 255) AS blue
FROM colors;
```

## 注意事项

- 位运算函数仅适用于整数类型，对浮点数或字符串使用会产生错误。
- `BITWISE_NOT` 的结果取决于整数的位宽（通常为 64 位有符号整数）。
- 左移操作可能导致溢出，请注意移位位数不要超过整数位宽。
