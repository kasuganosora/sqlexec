# Security

SQLExec provides multiple layers of security mechanisms, covering access control, data encryption, SQL injection prevention, and audit logging.

## Role-Based Access Control (RBAC)

SQLExec includes 5 built-in roles, each with different operation permissions.

### Role and Permission Matrix

| Permission | admin | moderator | user | readonly | guest |
|------------|:-----:|:---------:|:----:|:--------:|:-----:|
| SELECT | ✓ | ✓ | ✓ | ✓ | ✓ |
| INSERT | ✓ | ✓ | ✓ | - | - |
| UPDATE | ✓ | ✓ | ✓ | - | - |
| DELETE | ✓ | ✓ | - | - | - |
| CREATE TABLE | ✓ | ✓ | - | - | - |
| DROP TABLE | ✓ | - | - | - | - |
| User Management | ✓ | - | - | - | - |
| Permission Management | ✓ | - | - | - | - |
| Audit Log Viewing | ✓ | ✓ | - | - | - |

### Role Descriptions

- **admin** -- Super administrator with all permissions, including user management and permission assignment
- **moderator** -- Administrator who can execute DDL and DELETE operations, and view audit logs
- **user** -- Regular user who can execute SELECT, INSERT, and UPDATE operations
- **readonly** -- Read-only user who can only execute SELECT operations
- **guest** -- Guest with access to limited SELECT operations only

## Table-Level Permission Control

In addition to role-level permissions, SQLExec supports fine-grained permission control on individual tables.

### Granting Permissions

```sql
-- Grant SELECT permission on a specific table to a user
GRANT SELECT ON my_database.users TO 'zhangsan';

-- Grant multiple permissions
GRANT SELECT, INSERT, UPDATE ON my_database.orders TO 'zhangsan';
```

### Revoking Permissions

```sql
-- Revoke a user's permission on a specific table
REVOKE INSERT ON my_database.orders FROM 'zhangsan';

-- Revoke all permissions
REVOKE ALL ON my_database.users FROM 'zhangsan';
```

Table-level permissions work in conjunction with role permissions, taking the intersection of both. Even if a role allows a certain operation, the operation will be denied if it is not authorized at the table level.

## Data Encryption

SQLExec uses **AES-256-GCM** encryption and supports two levels of encryption granularity: field-level and record-level.

### Field-Level Encryption

Encrypt sensitive fields in a table for storage:

```sql
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT,
    email TEXT ENCRYPTED,
    phone TEXT ENCRYPTED,
    age INTEGER
);
```

Fields marked as `ENCRYPTED` are automatically encrypted on write and decrypted on read, transparent to the application layer.

### Record-Level Encryption

Encrypt entire records:

```sql
CREATE TABLE secrets (
    id INTEGER PRIMARY KEY,
    data TEXT
) WITH ENCRYPTION;
```

### Encryption Features

| Feature | Description |
|---------|-------------|
| Algorithm | AES-256-GCM |
| Key Management | Master key specified via configuration file |
| Performance Impact | Encryption/decryption is performed in memory with minimal overhead |
| Transparency | Fully transparent to clients; query results are automatically decrypted |

## SQL Injection Prevention

SQLExec includes a built-in `sqlescape` utility that provides safe SQL construction mechanisms to prevent SQL injection attacks.

### Formatting Identifiers

Use `%n` to format identifiers (table names, column names), which automatically adds backtick escaping:

```go
// Safe identifier quoting
query := sqlescape.Format("SELECT * FROM %n WHERE %n = %?", tableName, columnName, value)
// Output: SELECT * FROM `users` WHERE `name` = 'zhangsan'
```

### Parameterized Queries

Use `%?` to format parameter values, which automatically escapes special characters:

```go
// Safe parameter binding
query := sqlescape.Format("INSERT INTO %n (%n, %n) VALUES (%?, %?)",
    "users", "name", "email", userName, userEmail)
// Output: INSERT INTO `users` (`name`, `email`) VALUES ('zhangsan', 'zhangsan@example.com')
```

### Format Specifiers

| Specifier | Purpose | Example Input | Example Output |
|-----------|---------|---------------|----------------|
| `%n` | Identifier (table name, column name) | `users` | `` `users` `` |
| `%?` | Parameter value (string, number, etc.) | `O'Brien` | `'O''Brien'` |

### Best Practices

Always use parameterized queries and avoid string concatenation:

```go
// Correct - using sqlescape
query := sqlescape.Format("SELECT * FROM %n WHERE %n = %?", table, col, val)

// Wrong - string concatenation, vulnerable to injection
query := "SELECT * FROM " + table + " WHERE " + col + " = '" + val + "'"
```

## Audit Logging

SQLExec includes a built-in audit logging system implemented with a high-performance ring buffer that records all critical operations.

### Event Types

| Event Type | Description |
|------------|-------------|
| `LOGIN` | User login |
| `LOGOUT` | User logout |
| `QUERY` | SELECT query executed |
| `INSERT` | INSERT operation executed |
| `UPDATE` | UPDATE operation executed |
| `DELETE` | DELETE operation executed |
| `DDL` | DDL operation executed (CREATE, DROP, etc.) |
| `PERMISSION` | Permission change (GRANT, REVOKE) |
| `INJECTION` | SQL injection attempt detected |
| `ERROR` | Operation error |
| `API_REQUEST` | HTTP API request |
| `MCP_TOOL_CALL` | MCP tool call |

### Audit Levels

| Level | Description | Typical Events |
|-------|-------------|----------------|
| `Info` | Routine operations | LOGIN, LOGOUT, QUERY |
| `Warning` | Operations requiring attention | PERMISSION changes, unusual query patterns |
| `Error` | Failed operations | Query errors, authentication failures |
| `Critical` | Serious security events | INJECTION detection, privilege escalation attempts |

### Log Queries

Audit logs support querying across multiple dimensions.

#### Query by Trace-ID

Trace the complete operation chain of a specific request:

```sql
SELECT * FROM audit_log WHERE trace_id = 'req-20260215-abc123';
```

#### Query by User

View all operation records for a specific user:

```sql
SELECT * FROM audit_log WHERE user = 'zhangsan';
```

#### Query by Event Type

Filter events of a specific type:

```sql
SELECT * FROM audit_log WHERE event_type = 'INJECTION';
```

#### Query by Time Range

Query audit records within a specific time period:

```sql
SELECT * FROM audit_log
WHERE timestamp >= '2026-02-15 00:00:00'
  AND timestamp <= '2026-02-15 23:59:59';
```

#### Combined Queries

Combine multiple conditions:

```sql
SELECT * FROM audit_log
WHERE user = 'zhangsan'
  AND event_type IN ('INSERT', 'UPDATE', 'DELETE')
  AND timestamp >= '2026-02-15 00:00:00'
ORDER BY timestamp DESC
LIMIT 50;
```

## Security Best Practices

1. **Always use parameterized queries** -- Build SQL using `sqlescape`'s `%n` and `%?` format specifiers to avoid string concatenation
2. **Follow the principle of least privilege** -- Assign each user the lowest-privilege role that meets their needs
3. **Enable table-level permissions** -- Set fine-grained access controls on sensitive tables
4. **Encrypt sensitive data** -- Enable field-level encryption for fields containing personal information, passwords, and other sensitive data
5. **Regularly review audit logs** -- Pay attention to `INJECTION` events and `Critical` level entries
6. **Use Trace-IDs** -- Assign a unique Trace-ID to each external request for easier troubleshooting and request chain tracing
7. **Protect authentication credentials** -- Never hard-code tokens or passwords in your code; use environment variables or a secrets management service
