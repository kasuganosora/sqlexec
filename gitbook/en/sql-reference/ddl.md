# CREATE / ALTER / DROP

DDL (Data Definition Language) statements are used to create, modify, and drop database objects (tables, indexes, etc.).

{% hint style="info" %}
**Note:** DDL is fully supported on the memory datasource. For file-based datasources, DDL support may be limited depending on the underlying storage format.
{% endhint %}

## CREATE TABLE

### Basic Syntax

```sql
CREATE TABLE users (
  id     BIGINT AUTO_INCREMENT PRIMARY KEY,
  name   VARCHAR(100) NOT NULL,
  email  VARCHAR(255),
  age    INT DEFAULT 0,
  active BOOL DEFAULT TRUE,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
```

### Column Constraints

| Constraint | Description | Example |
|------------|-------------|---------|
| `PRIMARY KEY` | Primary key, uniquely identifies each row | `id BIGINT PRIMARY KEY` |
| `NOT NULL` | Does not allow NULL values | `name VARCHAR(100) NOT NULL` |
| `DEFAULT` | Specifies a default value | `status VARCHAR(20) DEFAULT 'active'` |
| `AUTO_INCREMENT` | Auto-increment column, automatically generates increasing values | `id BIGINT AUTO_INCREMENT` |

### Complete Table Creation Example

```sql
CREATE TABLE orders (
  id          BIGINT AUTO_INCREMENT PRIMARY KEY,
  user_id     BIGINT NOT NULL,
  product_id  BIGINT NOT NULL,
  quantity    INT NOT NULL DEFAULT 1,
  total_price DECIMAL(10, 2) NOT NULL,
  status      VARCHAR(20) DEFAULT 'pending',
  note        TEXT,
  created_at  DATETIME DEFAULT CURRENT_TIMESTAMP
);
```

### Generated Columns

The value of a generated column is automatically computed from an expression and does not need to be manually assigned:

```sql
CREATE TABLE products (
  id          BIGINT AUTO_INCREMENT PRIMARY KEY,
  name        VARCHAR(200) NOT NULL,
  price       DECIMAL(10, 2) NOT NULL,
  quantity    INT NOT NULL DEFAULT 0,
  total_value DECIMAL(12, 2) GENERATED ALWAYS AS (price * quantity) STORED
);
```

Generated columns use the `GENERATED ALWAYS AS (expr) STORED` syntax, and their values are automatically computed and stored when data is written.

### Vector Columns

SQLExec supports the vector data type for storing high-dimensional vectors (such as text embeddings, image features, etc.):

```sql
CREATE TABLE documents (
  id        BIGINT AUTO_INCREMENT PRIMARY KEY,
  title     VARCHAR(500) NOT NULL,
  content   TEXT,
  embedding VECTOR(768)
);
```

The `dim` in `VECTOR(dim)` specifies the vector dimension. For example, `VECTOR(768)` represents a 768-dimensional vector.

## ALTER TABLE

### Adding a Column

```sql
ALTER TABLE users ADD COLUMN phone VARCHAR(20);
```

```sql
ALTER TABLE users ADD COLUMN updated_at DATETIME DEFAULT CURRENT_TIMESTAMP;
```

### Dropping a Column

```sql
ALTER TABLE users DROP COLUMN phone;
```

### Adding an Index

```sql
ALTER TABLE users ADD INDEX idx_email (email);
```

```sql
ALTER TABLE orders ADD INDEX idx_user_status (user_id, status);
```

### Dropping an Index

```sql
ALTER TABLE users DROP INDEX idx_email;
```

## DROP TABLE

Drop an entire table and all its data:

```sql
DROP TABLE users;
```

{% hint style="warning" %}
`DROP TABLE` is irreversible. All data in the table will be permanently lost.
{% endhint %}

## TRUNCATE TABLE

Clear all data in a table while preserving the table structure:

```sql
TRUNCATE TABLE logs;
```

Compared to `DELETE FROM table`, `TRUNCATE` is more efficient because it does not delete rows one by one but instead resets the table data directly.

## Comprehensive Example

```sql
-- Create a table with vector columns and generated columns
CREATE TABLE articles (
  id          BIGINT AUTO_INCREMENT PRIMARY KEY,
  title       VARCHAR(500) NOT NULL,
  content     TEXT NOT NULL,
  author      VARCHAR(100),
  word_count  INT GENERATED ALWAYS AS (LENGTH(content)) STORED,
  embedding   VECTOR(1536),
  published   BOOL DEFAULT FALSE,
  created_at  DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Add indexes for commonly queried columns
ALTER TABLE articles ADD INDEX idx_author (author);
ALTER TABLE articles ADD INDEX idx_published (published);
```
