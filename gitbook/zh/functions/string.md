# 字符串函数

SQLExec 提供了丰富的字符串处理函数，用于字符串的拼接、截取、查找、转换等操作。

## 函数列表

| 函数 | 说明 | 示例 |
|------|------|------|
| `CONCAT(s1, s2, ...)` | 拼接多个字符串 | `SELECT CONCAT('Hello', ' ', 'World');` -- `'Hello World'` |
| `CONCAT_WS(sep, s1, s2, ...)` | 使用指定分隔符拼接字符串 | `SELECT CONCAT_WS('-', '2025', '01', '15');` -- `'2025-01-15'` |
| `LENGTH(s)` | 返回字符串的字节长度 | `SELECT LENGTH('Hello');` -- `5` |
| `CHAR_LENGTH(s)` | 返回字符串的字符数 | `SELECT CHAR_LENGTH('你好');` -- `2` |
| `UPPER(s)` | 将字符串转换为大写 | `SELECT UPPER('hello');` -- `'HELLO'` |
| `LOWER(s)` | 将字符串转换为小写 | `SELECT LOWER('HELLO');` -- `'hello'` |
| `TRIM(s)` | 去除字符串首尾空白字符 | `SELECT TRIM('  hello  ');` -- `'hello'` |
| `LTRIM(s)` | 去除字符串左侧空白字符 | `SELECT LTRIM('  hello');` -- `'hello'` |
| `RTRIM(s)` | 去除字符串右侧空白字符 | `SELECT RTRIM('hello  ');` -- `'hello'` |
| `LPAD(s, len, pad)` | 在字符串左侧填充至指定长度 | `SELECT LPAD('42', 5, '0');` -- `'00042'` |
| `RPAD(s, len, pad)` | 在字符串右侧填充至指定长度 | `SELECT RPAD('hi', 5, '!');` -- `'hi!!!'` |
| `SUBSTRING(s, pos, len)` | 截取子字符串 | `SELECT SUBSTRING('Hello World', 7, 5);` -- `'World'` |
| `SUBSTR(s, pos, len)` | SUBSTRING 的别名 | `SELECT SUBSTR('Hello World', 1, 5);` -- `'Hello'` |
| `LEFT(s, n)` | 返回字符串左侧 n 个字符 | `SELECT LEFT('Hello', 3);` -- `'Hel'` |
| `RIGHT(s, n)` | 返回字符串右侧 n 个字符 | `SELECT RIGHT('Hello', 3);` -- `'llo'` |
| `POSITION(sub IN s)` | 返回子字符串首次出现的位置 | `SELECT POSITION('World' IN 'Hello World');` -- `7` |
| `LOCATE(sub, s)` | 返回子字符串首次出现的位置 | `SELECT LOCATE('lo', 'Hello');` -- `4` |
| `INSTR(s, sub)` | 返回子字符串首次出现的位置 | `SELECT INSTR('Hello', 'lo');` -- `4` |
| `REPLACE(s, from, to)` | 替换字符串中的指定内容 | `SELECT REPLACE('Hello World', 'World', 'SQLExec');` -- `'Hello SQLExec'` |
| `REPEAT(s, n)` | 将字符串重复 n 次 | `SELECT REPEAT('ab', 3);` -- `'ababab'` |
| `REVERSE(s)` | 反转字符串 | `SELECT REVERSE('Hello');` -- `'olleH'` |
| `TRANSLATE(s, from, to)` | 按字符映射逐字替换 | `SELECT TRANSLATE('hello', 'el', 'ip');` -- `'hippo'` |
| `STARTS_WITH(s, prefix)` | 判断字符串是否以指定前缀开头 | `SELECT STARTS_WITH('Hello', 'He');` -- `true` |
| `ENDS_WITH(s, suffix)` | 判断字符串是否以指定后缀结尾 | `SELECT ENDS_WITH('Hello', 'lo');` -- `true` |
| `CONTAINS(s, sub)` | 判断字符串是否包含指定子串 | `SELECT CONTAINS('Hello World', 'World');` -- `true` |
| `FORMAT(fmt, args...)` | 按格式模板生成字符串 | `SELECT FORMAT('Name: %s, Age: %d', name, age) FROM users;` |
| `ASCII(s)` | 返回第一个字符的 ASCII 码 | `SELECT ASCII('A');` -- `65` |
| `CHR(n)` | 返回 ASCII 码对应的字符 | `SELECT CHR(65);` -- `'A'` |
| `URL_ENCODE(s)` | 对字符串进行 URL 编码 | `SELECT URL_ENCODE('hello world');` -- `'hello%20world'` |
| `URL_DECODE(s)` | 对字符串进行 URL 解码 | `SELECT URL_DECODE('hello%20world');` -- `'hello world'` |

## 使用示例

### 字符串拼接与格式化

```sql
-- 拼接姓名
SELECT CONCAT(last_name, first_name) AS full_name
FROM users;

-- 使用分隔符拼接
SELECT CONCAT_WS(', ', city, province, country) AS address
FROM locations;
```

### 字符串查找与截取

```sql
-- 提取邮箱域名
SELECT SUBSTRING(email, POSITION('@' IN email) + 1) AS domain
FROM users;

-- 获取文件扩展名
SELECT RIGHT(filename, LENGTH(filename) - POSITION('.' IN REVERSE(filename))) AS ext
FROM files;
```

### 字符串清洗与转换

```sql
-- 清洗并标准化数据
SELECT TRIM(LOWER(email)) AS clean_email
FROM users;

-- 手机号脱敏
SELECT CONCAT(LEFT(phone, 3), '****', RIGHT(phone, 4)) AS masked_phone
FROM users;
```

### 字符串判断

```sql
-- 筛选以指定前缀开头的记录
SELECT * FROM products
WHERE STARTS_WITH(sku, 'PRD-');

-- 筛选包含关键字的记录
SELECT * FROM articles
WHERE CONTAINS(title, '数据库');
```
