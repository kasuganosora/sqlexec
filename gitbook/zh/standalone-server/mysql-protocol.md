# MySQL 协议接入

SQLExec 实现了 MySQL Wire Protocol Version 10，兼容 MariaDB，允许使用任何标准 MySQL 客户端或驱动程序直接连接。

## 协议支持

### Wire Protocol

- **协议版本**: MySQL Protocol Version 10
- **兼容性**: MySQL 5.7+、MariaDB 10.x+
- **认证插件**: `mysql_native_password`

### 支持的 COM 命令

| 命令 | 说明 |
|------|------|
| `COM_QUIT` | 关闭连接 |
| `COM_INIT_DB` | 切换数据库（等同于 `USE` 语句） |
| `COM_QUERY` | 执行 SQL 语句 |
| `COM_FIELD_LIST` | 获取表的字段列表 |
| `COM_PING` | 检测连接是否存活 |
| `COM_PROCESS_KILL` | 终止指定连接 |
| `COM_STATISTICS` | 获取服务器统计信息 |

## 支持的 SQL 语句

### 数据查询与操作

| 语句 | 说明 |
|------|------|
| `SELECT` | 数据查询 |
| `INSERT` | 插入数据 |
| `UPDATE` | 更新数据 |
| `DELETE` | 删除数据 |

### 数据定义

| 语句 | 说明 |
|------|------|
| `CREATE TABLE` | 创建表 |
| `DROP TABLE` | 删除表 |

### 元数据与管理

| 语句 | 说明 |
|------|------|
| `SHOW DATABASES` | 列出所有已注册的数据源 |
| `SHOW TABLES` | 列出当前数据库中的所有表 |
| `DESCRIBE` / `DESC` | 查看表结构 |
| `EXPLAIN` | 查看查询执行计划 |
| `USE` | 切换当前数据库 |
| `SET` | 设置会话变量 |

## 客户端连接

### mysql CLI

```bash
mysql -h 127.0.0.1 -P 3306 -u root -p
```

连接后切换数据库：

```sql
USE my_database;
SHOW TABLES;
SELECT * FROM users LIMIT 10;
```

### Go (go-sql-driver/mysql)

```go
import (
    "database/sql"
    _ "github.com/go-sql-driver/mysql"
)

db, err := sql.Open("mysql", "root:password@tcp(127.0.0.1:3306)/my_database")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

rows, err := db.Query("SELECT id, name FROM users WHERE age > ?", 18)
if err != nil {
    log.Fatal(err)
}
defer rows.Close()

for rows.Next() {
    var id int
    var name string
    rows.Scan(&id, &name)
    fmt.Printf("id=%d, name=%s\n", id, name)
}
```

### Python (PyMySQL)

```python
import pymysql

conn = pymysql.connect(
    host='127.0.0.1',
    port=3306,
    user='root',
    password='password',
    database='my_database'
)

with conn.cursor() as cursor:
    cursor.execute("SELECT id, name FROM users WHERE age > %s", (18,))
    for row in cursor.fetchall():
        print(f"id={row[0]}, name={row[1]}")

conn.close()
```

### Java (JDBC)

```java
import java.sql.*;

String url = "jdbc:mysql://127.0.0.1:3306/my_database";
String user = "root";
String password = "password";

try (Connection conn = DriverManager.getConnection(url, user, password);
     PreparedStatement stmt = conn.prepareStatement(
         "SELECT id, name FROM users WHERE age > ?")) {

    stmt.setInt(1, 18);
    ResultSet rs = stmt.executeQuery();

    while (rs.next()) {
        System.out.printf("id=%d, name=%s%n",
            rs.getInt("id"), rs.getString("name"));
    }
}
```

### Node.js (mysql2)

```javascript
const mysql = require('mysql2/promise');

async function main() {
    const conn = await mysql.createConnection({
        host: '127.0.0.1',
        port: 3306,
        user: 'root',
        password: 'password',
        database: 'my_database'
    });

    const [rows] = await conn.execute(
        'SELECT id, name FROM users WHERE age > ?',
        [18]
    );

    for (const row of rows) {
        console.log(`id=${row.id}, name=${row.name}`);
    }

    await conn.end();
}

main();
```

## 多数据库切换

SQLExec 将每个注册的数据源映射为一个 MySQL 数据库。使用 `USE` 语句在不同数据源之间切换：

```sql
-- 查看所有可用数据源
SHOW DATABASES;

-- 切换到指定数据源
USE analytics_db;

-- 当前数据源下的操作
SHOW TABLES;
SELECT COUNT(*) FROM events;

-- 切换到另一个数据源
USE user_db;
SELECT * FROM profiles LIMIT 5;
```

## 会话变量

通过 `SET` 语句设置会话级别的变量，用于请求追踪和审计：

```sql
-- 设置 trace_id 用于请求追踪
SET @trace_id = 'req-20260215-abc123';

-- 此后该会话内的所有查询都会关联此 trace_id
SELECT * FROM orders WHERE status = 'pending';

-- 可以随时更新 trace_id
SET @trace_id = 'req-20260215-def456';
```

审计日志中会自动记录 `trace_id`，方便后续排查问题时关联上下游请求。
