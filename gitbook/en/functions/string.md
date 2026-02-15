# String Functions

SQLExec provides a rich set of string processing functions for concatenation, extraction, searching, conversion, and more.

## Function List

| Function | Description | Example |
|----------|-------------|---------|
| `CONCAT(s1, s2, ...)` | Concatenate multiple strings | `SELECT CONCAT('Hello', ' ', 'World');` -- `'Hello World'` |
| `CONCAT_WS(sep, s1, s2, ...)` | Concatenate strings with a specified separator | `SELECT CONCAT_WS('-', '2025', '01', '15');` -- `'2025-01-15'` |
| `LENGTH(s)` | Return the byte length of a string | `SELECT LENGTH('Hello');` -- `5` |
| `CHAR_LENGTH(s)` | Return the character count of a string | `SELECT CHAR_LENGTH('你好');` -- `2` |
| `UPPER(s)` | Convert a string to uppercase | `SELECT UPPER('hello');` -- `'HELLO'` |
| `LOWER(s)` | Convert a string to lowercase | `SELECT LOWER('HELLO');` -- `'hello'` |
| `TRIM(s)` | Remove leading and trailing whitespace | `SELECT TRIM('  hello  ');` -- `'hello'` |
| `LTRIM(s)` | Remove leading whitespace | `SELECT LTRIM('  hello');` -- `'hello'` |
| `RTRIM(s)` | Remove trailing whitespace | `SELECT RTRIM('hello  ');` -- `'hello'` |
| `LPAD(s, len, pad)` | Left-pad a string to the specified length | `SELECT LPAD('42', 5, '0');` -- `'00042'` |
| `RPAD(s, len, pad)` | Right-pad a string to the specified length | `SELECT RPAD('hi', 5, '!');` -- `'hi!!!'` |
| `SUBSTRING(s, pos, len)` | Extract a substring | `SELECT SUBSTRING('Hello World', 7, 5);` -- `'World'` |
| `SUBSTR(s, pos, len)` | Alias for SUBSTRING | `SELECT SUBSTR('Hello World', 1, 5);` -- `'Hello'` |
| `LEFT(s, n)` | Return the leftmost n characters | `SELECT LEFT('Hello', 3);` -- `'Hel'` |
| `RIGHT(s, n)` | Return the rightmost n characters | `SELECT RIGHT('Hello', 3);` -- `'llo'` |
| `POSITION(sub IN s)` | Return the position of the first occurrence of a substring | `SELECT POSITION('World' IN 'Hello World');` -- `7` |
| `LOCATE(sub, s)` | Return the position of the first occurrence of a substring | `SELECT LOCATE('lo', 'Hello');` -- `4` |
| `INSTR(s, sub)` | Return the position of the first occurrence of a substring | `SELECT INSTR('Hello', 'lo');` -- `4` |
| `REPLACE(s, from, to)` | Replace specified content in a string | `SELECT REPLACE('Hello World', 'World', 'SQLExec');` -- `'Hello SQLExec'` |
| `REPEAT(s, n)` | Repeat a string n times | `SELECT REPEAT('ab', 3);` -- `'ababab'` |
| `REVERSE(s)` | Reverse a string | `SELECT REVERSE('Hello');` -- `'olleH'` |
| `TRANSLATE(s, from, to)` | Replace characters by character-level mapping | `SELECT TRANSLATE('hello', 'el', 'ip');` -- `'hippo'` |
| `STARTS_WITH(s, prefix)` | Check if a string starts with the specified prefix | `SELECT STARTS_WITH('Hello', 'He');` -- `true` |
| `ENDS_WITH(s, suffix)` | Check if a string ends with the specified suffix | `SELECT ENDS_WITH('Hello', 'lo');` -- `true` |
| `CONTAINS(s, sub)` | Check if a string contains the specified substring | `SELECT CONTAINS('Hello World', 'World');` -- `true` |
| `FORMAT(fmt, args...)` | Generate a string from a format template | `SELECT FORMAT('Name: %s, Age: %d', name, age) FROM users;` |
| `ASCII(s)` | Return the ASCII code of the first character | `SELECT ASCII('A');` -- `65` |
| `CHR(n)` | Return the character for an ASCII code | `SELECT CHR(65);` -- `'A'` |
| `URL_ENCODE(s)` | URL-encode a string | `SELECT URL_ENCODE('hello world');` -- `'hello%20world'` |
| `URL_DECODE(s)` | URL-decode a string | `SELECT URL_DECODE('hello%20world');` -- `'hello world'` |

## Usage Examples

### String Concatenation and Formatting

```sql
-- Concatenate names
SELECT CONCAT(last_name, first_name) AS full_name
FROM users;

-- Concatenate with separator
SELECT CONCAT_WS(', ', city, province, country) AS address
FROM locations;
```

### String Searching and Extraction

```sql
-- Extract email domain
SELECT SUBSTRING(email, POSITION('@' IN email) + 1) AS domain
FROM users;

-- Get file extension
SELECT RIGHT(filename, LENGTH(filename) - POSITION('.' IN REVERSE(filename))) AS ext
FROM files;
```

### String Cleaning and Transformation

```sql
-- Clean and normalize data
SELECT TRIM(LOWER(email)) AS clean_email
FROM users;

-- Mask phone numbers
SELECT CONCAT(LEFT(phone, 3), '****', RIGHT(phone, 4)) AS masked_phone
FROM users;
```

### String Matching

```sql
-- Filter records starting with a specific prefix
SELECT * FROM products
WHERE STARTS_WITH(sku, 'PRD-');

-- Filter records containing a keyword
SELECT * FROM articles
WHERE CONTAINS(title, '数据库');
```
