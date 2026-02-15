# Bitwise Functions

SQLExec provides a set of bitwise operation functions for performing bit-level operations on integer values, suitable for permission management, flag handling, low-level data manipulation, and other scenarios.

## Function List

| Function | Description | Example |
|----------|-------------|---------|
| `BITWISE_AND(a, b)` | Bitwise AND | `SELECT BITWISE_AND(12, 10);` -- `8` |
| `BITWISE_OR(a, b)` | Bitwise OR | `SELECT BITWISE_OR(12, 10);` -- `14` |
| `BITWISE_XOR(a, b)` | Bitwise XOR | `SELECT BITWISE_XOR(12, 10);` -- `6` |
| `BITWISE_NOT(a)` | Bitwise NOT | `SELECT BITWISE_NOT(0);` -- `-1` |
| `LSHIFT(a, n)` | Left shift by n bits | `SELECT LSHIFT(1, 3);` -- `8` |
| `RSHIFT(a, n)` | Right shift by n bits | `SELECT RSHIFT(16, 2);` -- `4` |

## Operation Rules

### BITWISE_AND -- Bitwise AND

Performs an AND operation on each bit of two integers. The result bit is 1 only when both bits are 1.

```
  12 = 1100
  10 = 1010
  ----------
AND  = 1000 = 8
```

```sql
SELECT BITWISE_AND(12, 10);  -- 8
```

### BITWISE_OR -- Bitwise OR

Performs an OR operation on each bit of two integers. The result bit is 1 when either bit is 1.

```
  12 = 1100
  10 = 1010
  ----------
OR   = 1110 = 14
```

```sql
SELECT BITWISE_OR(12, 10);  -- 14
```

### BITWISE_XOR -- Bitwise XOR

Performs an XOR operation on each bit of two integers. The result bit is 1 when the two bits are different, and 0 when they are the same.

```
  12 = 1100
  10 = 1010
  ----------
XOR  = 0110 = 6
```

```sql
SELECT BITWISE_XOR(12, 10);  -- 6
```

### BITWISE_NOT -- Bitwise NOT

Inverts each bit of an integer: 0 becomes 1, and 1 becomes 0.

```sql
SELECT BITWISE_NOT(0);    -- -1
SELECT BITWISE_NOT(255);  -- -256
```

### LSHIFT -- Left Shift

Shifts the binary representation of an integer to the left by the specified number of bits, padding with 0s on the right. Equivalent to multiplying by 2 to the power of n.

```sql
SELECT LSHIFT(1, 0);  -- 1   (0001)
SELECT LSHIFT(1, 1);  -- 2   (0010)
SELECT LSHIFT(1, 2);  -- 4   (0100)
SELECT LSHIFT(1, 3);  -- 8   (1000)
SELECT LSHIFT(5, 2);  -- 20  (10100)
```

### RSHIFT -- Right Shift

Shifts the binary representation of an integer to the right by the specified number of bits. Equivalent to integer division by 2 to the power of n.

```sql
SELECT RSHIFT(16, 1);  -- 8
SELECT RSHIFT(16, 2);  -- 4
SELECT RSHIFT(16, 3);  -- 2
SELECT RSHIFT(16, 4);  -- 1
```

## Usage Examples

### Permission Management

Use bit flags to manage user permissions, where each bit represents a permission:

```sql
-- Permission definitions:
-- Bit 0 (1)  = Read permission
-- Bit 1 (2)  = Write permission
-- Bit 2 (4)  = Delete permission
-- Bit 3 (8)  = Admin permission

-- Check if a user has write permission
SELECT * FROM users
WHERE BITWISE_AND(permissions, 2) > 0;

-- Check if a user has both read and write permissions (1 + 2 = 3)
SELECT * FROM users
WHERE BITWISE_AND(permissions, 3) = 3;

-- Grant delete permission to a user
SELECT BITWISE_OR(permissions, 4) AS new_permissions
FROM users
WHERE id = 1;

-- Revoke admin permission from a user
SELECT BITWISE_AND(permissions, BITWISE_NOT(8)) AS new_permissions
FROM users
WHERE id = 1;

-- Toggle a permission (remove if present, add if absent)
SELECT BITWISE_XOR(permissions, 4) AS toggled_permissions
FROM users
WHERE id = 1;
```

### Flag Handling

```sql
-- Check if the nth bit is set
SELECT id, name,
       IF(BITWISE_AND(flags, LSHIFT(1, 0)) > 0, 'Yes', 'No') AS flag_0,
       IF(BITWISE_AND(flags, LSHIFT(1, 1)) > 0, 'Yes', 'No') AS flag_1,
       IF(BITWISE_AND(flags, LSHIFT(1, 2)) > 0, 'Yes', 'No') AS flag_2
FROM records;

-- Count records with a specific flag bit set
SELECT COUNT(*) AS count
FROM records
WHERE BITWISE_AND(flags, LSHIFT(1, 5)) > 0;
```

### Data Packing and Extraction

```sql
-- Pack RGB color values into a single integer
SELECT BITWISE_OR(
    BITWISE_OR(LSHIFT(red, 16), LSHIFT(green, 8)),
    blue
) AS color_int
FROM colors;

-- Extract RGB components from a packed color integer
SELECT BITWISE_AND(RSHIFT(color_int, 16), 255) AS red,
       BITWISE_AND(RSHIFT(color_int, 8), 255) AS green,
       BITWISE_AND(color_int, 255) AS blue
FROM colors;
```

## Notes

- Bitwise functions only work with integer types. Using them with floating-point numbers or strings will result in an error.
- The result of `BITWISE_NOT` depends on the integer's bit width (typically a 64-bit signed integer).
- Left shift operations may cause overflow. Ensure the shift count does not exceed the integer's bit width.
