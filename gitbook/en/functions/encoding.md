# Encoding & Hash Functions

SQLExec provides commonly used encoding/decoding functions and cryptographic hash functions for data transformation, verification, and security processing.

## Function List

| Function | Description | Example |
|----------|-------------|---------|
| `BASE64_ENCODE(s)` | Encode a string to Base64 | `SELECT BASE64_ENCODE('Hello World');` -- `'SGVsbG8gV29ybGQ='` |
| `BASE64_DECODE(s)` | Decode a Base64 string | `SELECT BASE64_DECODE('SGVsbG8gV29ybGQ=');` -- `'Hello World'` |
| `HEX_ENCODE(s)` | Encode a string to hexadecimal | `SELECT HEX_ENCODE('Hello');` -- `'48656c6c6f'` |
| `HEX_DECODE(s)` | Decode a hexadecimal string | `SELECT HEX_DECODE('48656c6c6f');` -- `'Hello'` |
| `MD5(s)` | Compute the MD5 hash (128-bit) | `SELECT MD5('Hello');` -- `'8b1a9953c4611296a827abf8c47804d7'` |
| `SHA1(s)` | Compute the SHA-1 hash (160-bit) | `SELECT SHA1('Hello');` -- `'f7ff9e8b7bb2e09b70935a5d785e0cc5d9d0abf0'` |
| `SHA256(s)` | Compute the SHA-256 hash (256-bit) | `SELECT SHA256('Hello');` -- `'185f8db32271fe25f561a6fc938b2e2...'` |
| `SHA512(s)` | Compute the SHA-512 hash (512-bit) | `SELECT SHA512('Hello');` -- `'3615f80c9d293ed7402687f94b22d58...'` |
| `HMAC_SHA256(data, key)` | Compute an HMAC-SHA256 signature using a key | `SELECT HMAC_SHA256('message', 'secret_key');` |

## Usage Examples

### Base64 Encoding/Decoding

```sql
-- Encode binary data to Base64 text
SELECT id, BASE64_ENCODE(content) AS encoded_content
FROM binary_data;

-- Decode Base64 data
SELECT id, BASE64_DECODE(encoded) AS decoded_content
FROM encoded_records;

-- Use in data transfer
SELECT JSON_OBJECT(
    'id', id,
    'avatar', BASE64_ENCODE(avatar_data)
) AS response
FROM users;
```

### Hexadecimal Encoding/Decoding

```sql
-- Convert data to hexadecimal representation
SELECT HEX_ENCODE(raw_bytes) AS hex_value
FROM sensor_data;

-- Decode hexadecimal strings
SELECT HEX_DECODE(hex_string) AS original_data
FROM hex_records;
```

### Hash Computation

```sql
-- Compute data fingerprints for deduplication
SELECT MD5(content) AS content_hash, COUNT(*) AS count
FROM documents
GROUP BY MD5(content)
HAVING COUNT(*) > 1;

-- Use SHA-256 for secure hashing
SELECT id, SHA256(CONCAT(email, salt)) AS email_hash
FROM users;

-- Data integrity verification
SELECT filename,
       SHA256(file_content) AS checksum
FROM files;
```

### HMAC Signatures

```sql
-- Generate API request signatures
SELECT HMAC_SHA256(
    CONCAT(method, '&', path, '&', timestamp),
    api_secret
) AS signature
FROM api_configs;

-- Verify message signatures
SELECT * FROM webhooks
WHERE HMAC_SHA256(payload, secret) = received_signature;
```

### Comprehensive Examples

```sql
-- Data masking and encoding combination
SELECT id,
       LEFT(MD5(email), 8) AS email_id,
       BASE64_ENCODE(
           JSON_OBJECT('user_id', id, 'role', role)
       ) AS token
FROM users;

-- Multi-layer hashing for security scenarios
SELECT SHA256(CONCAT(password, salt)) AS password_hash
FROM credentials;
```

## Notes

- `MD5` and `SHA1` are no longer recommended for security scenarios (such as password storage). Use `SHA256` or `SHA512` instead.
- Hash functions return hexadecimal strings.
- `HMAC_SHA256` requires a key parameter and is suitable for message authentication and signing scenarios.
- Encoding/decoding functions operate on string data. Ensure the input format is correct to avoid decoding errors.
