# 编码与哈希函数

SQLExec 提供了常用的编码/解码函数和加密哈希函数，用于数据转换、校验和安全处理。

## 函数列表

| 函数 | 说明 | 示例 |
|------|------|------|
| `BASE64_ENCODE(s)` | 将字符串编码为 Base64 | `SELECT BASE64_ENCODE('Hello World');` -- `'SGVsbG8gV29ybGQ='` |
| `BASE64_DECODE(s)` | 将 Base64 字符串解码 | `SELECT BASE64_DECODE('SGVsbG8gV29ybGQ=');` -- `'Hello World'` |
| `HEX_ENCODE(s)` | 将字符串编码为十六进制 | `SELECT HEX_ENCODE('Hello');` -- `'48656c6c6f'` |
| `HEX_DECODE(s)` | 将十六进制字符串解码 | `SELECT HEX_DECODE('48656c6c6f');` -- `'Hello'` |
| `MD5(s)` | 计算 MD5 哈希值（128 位） | `SELECT MD5('Hello');` -- `'8b1a9953c4611296a827abf8c47804d7'` |
| `SHA1(s)` | 计算 SHA-1 哈希值（160 位） | `SELECT SHA1('Hello');` -- `'f7ff9e8b7bb2e09b70935a5d785e0cc5d9d0abf0'` |
| `SHA256(s)` | 计算 SHA-256 哈希值（256 位） | `SELECT SHA256('Hello');` -- `'185f8db32271fe25f561a6fc938b2e2...'` |
| `SHA512(s)` | 计算 SHA-512 哈希值（512 位） | `SELECT SHA512('Hello');` -- `'3615f80c9d293ed7402687f94b22d58...'` |
| `HMAC_SHA256(data, key)` | 使用密钥计算 HMAC-SHA256 签名 | `SELECT HMAC_SHA256('message', 'secret_key');` |

## 使用示例

### Base64 编解码

```sql
-- 编码二进制数据为 Base64 文本
SELECT id, BASE64_ENCODE(content) AS encoded_content
FROM binary_data;

-- 解码 Base64 数据
SELECT id, BASE64_DECODE(encoded) AS decoded_content
FROM encoded_records;

-- 在数据传输中使用
SELECT JSON_OBJECT(
    'id', id,
    'avatar', BASE64_ENCODE(avatar_data)
) AS response
FROM users;
```

### 十六进制编解码

```sql
-- 将数据转为十六进制表示
SELECT HEX_ENCODE(raw_bytes) AS hex_value
FROM sensor_data;

-- 解码十六进制字符串
SELECT HEX_DECODE(hex_string) AS original_data
FROM hex_records;
```

### 哈希计算

```sql
-- 计算数据指纹用于去重
SELECT MD5(content) AS content_hash, COUNT(*) AS count
FROM documents
GROUP BY MD5(content)
HAVING COUNT(*) > 1;

-- 使用 SHA-256 进行安全哈希
SELECT id, SHA256(CONCAT(email, salt)) AS email_hash
FROM users;

-- 数据完整性校验
SELECT filename,
       SHA256(file_content) AS checksum
FROM files;
```

### HMAC 签名

```sql
-- 生成 API 请求签名
SELECT HMAC_SHA256(
    CONCAT(method, '&', path, '&', timestamp),
    api_secret
) AS signature
FROM api_configs;

-- 验证消息签名
SELECT * FROM webhooks
WHERE HMAC_SHA256(payload, secret) = received_signature;
```

### 综合应用

```sql
-- 数据脱敏与编码组合
SELECT id,
       LEFT(MD5(email), 8) AS email_id,
       BASE64_ENCODE(
           JSON_OBJECT('user_id', id, 'role', role)
       ) AS token
FROM users;

-- 多重哈希用于安全场景
SELECT SHA256(CONCAT(password, salt)) AS password_hash
FROM credentials;
```

## 注意事项

- `MD5` 和 `SHA1` 已不推荐用于安全场景（如密码存储），建议使用 `SHA256` 或 `SHA512`。
- 哈希函数返回的是十六进制字符串。
- `HMAC_SHA256` 需要提供密钥参数，适用于消息认证和签名场景。
- 编解码函数处理的是字符串数据，确保输入格式正确以避免解码错误。
