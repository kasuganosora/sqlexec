# CREATE / ALTER / DROP

DDL（数据定义语言）语句用于创建、修改和删除数据库对象（表、索引等）。

{% hint style="info" %}
**注意：** DDL 在内存数据源（memory datasource）上完全支持。对于文件类数据源，DDL 支持可能有限制，具体取决于底层存储格式。
{% endhint %}

## CREATE TABLE 创建表

### 基本语法

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

### 列约束

| 约束 | 说明 | 示例 |
|------|------|------|
| `PRIMARY KEY` | 主键，唯一标识每行记录 | `id BIGINT PRIMARY KEY` |
| `NOT NULL` | 不允许 NULL 值 | `name VARCHAR(100) NOT NULL` |
| `DEFAULT` | 指定默认值 | `status VARCHAR(20) DEFAULT 'active'` |
| `AUTO_INCREMENT` | 自增列，自动生成递增值 | `id BIGINT AUTO_INCREMENT` |

### 完整建表示例

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

### 生成列（Generated Columns）

生成列的值由表达式自动计算，无需手动赋值：

```sql
CREATE TABLE products (
  id          BIGINT AUTO_INCREMENT PRIMARY KEY,
  name        VARCHAR(200) NOT NULL,
  price       DECIMAL(10, 2) NOT NULL,
  quantity    INT NOT NULL DEFAULT 0,
  total_value DECIMAL(12, 2) GENERATED ALWAYS AS (price * quantity) STORED
);
```

生成列使用 `GENERATED ALWAYS AS (expr) STORED` 语法，其值在数据写入时自动计算并存储。

### 向量列（Vector Columns）

SQLExec 支持向量数据类型，用于存储高维向量（如文本嵌入、图像特征等）：

```sql
CREATE TABLE documents (
  id        BIGINT AUTO_INCREMENT PRIMARY KEY,
  title     VARCHAR(500) NOT NULL,
  content   TEXT,
  embedding VECTOR(768)
);
```

`VECTOR(dim)` 中的 `dim` 指定向量维度。例如 `VECTOR(768)` 表示 768 维的向量。

## ALTER TABLE 修改表

### 添加列

```sql
ALTER TABLE users ADD COLUMN phone VARCHAR(20);
```

```sql
ALTER TABLE users ADD COLUMN updated_at DATETIME DEFAULT CURRENT_TIMESTAMP;
```

### 删除列

```sql
ALTER TABLE users DROP COLUMN phone;
```

### 添加索引

```sql
ALTER TABLE users ADD INDEX idx_email (email);
```

```sql
ALTER TABLE orders ADD INDEX idx_user_status (user_id, status);
```

### 删除索引

```sql
ALTER TABLE users DROP INDEX idx_email;
```

## DROP TABLE 删除表

删除整张表及其所有数据：

```sql
DROP TABLE users;
```

{% hint style="warning" %}
`DROP TABLE` 操作不可逆，表中的所有数据将永久丢失。
{% endhint %}

## TRUNCATE TABLE 清空表

清空表中所有数据，但保留表结构：

```sql
TRUNCATE TABLE logs;
```

与 `DELETE FROM table` 相比，`TRUNCATE` 效率更高，因为它不会逐行删除，而是直接重置表数据。

## 综合示例

```sql
-- 创建一张带向量列和生成列的表
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

-- 为常用查询列添加索引
ALTER TABLE articles ADD INDEX idx_author (author);
ALTER TABLE articles ADD INDEX idx_published (published);
```
