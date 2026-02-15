# MySQL Protocol Access

SQLExec implements MySQL Wire Protocol Version 10, compatible with MariaDB, allowing direct connections from any standard MySQL client or driver.

## Protocol Support

### Wire Protocol

- **Protocol Version**: MySQL Protocol Version 10
- **Compatibility**: MySQL 5.7+, MariaDB 10.x+
- **Authentication Plugin**: `mysql_native_password`

### Supported COM Commands

| Command | Description |
|---------|-------------|
| `COM_QUIT` | Close the connection |
| `COM_INIT_DB` | Switch database (equivalent to the `USE` statement) |
| `COM_QUERY` | Execute an SQL statement |
| `COM_FIELD_LIST` | Retrieve the field list of a table |
| `COM_PING` | Check if the connection is alive |
| `COM_PROCESS_KILL` | Terminate a specified connection |
| `COM_STATISTICS` | Retrieve server statistics |

## Supported SQL Statements

### Data Query and Manipulation

| Statement | Description |
|-----------|-------------|
| `SELECT` | Query data |
| `INSERT` | Insert data |
| `UPDATE` | Update data |
| `DELETE` | Delete data |

### Data Definition

| Statement | Description |
|-----------|-------------|
| `CREATE TABLE` | Create a table |
| `DROP TABLE` | Drop a table |

### Metadata and Administration

| Statement | Description |
|-----------|-------------|
| `SHOW DATABASES` | List all registered data sources |
| `SHOW TABLES` | List all tables in the current database |
| `DESCRIBE` / `DESC` | View table structure |
| `EXPLAIN` | View query execution plan |
| `USE` | Switch the current database |
| `SET` | Set session variables |

## Client Connections

### mysql CLI

```bash
mysql -h 127.0.0.1 -P 3306 -u root -p
```

After connecting, switch databases:

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

## Multi-Database Switching

SQLExec maps each registered data source as a MySQL database. Use the `USE` statement to switch between different data sources:

```sql
-- View all available data sources
SHOW DATABASES;

-- Switch to a specific data source
USE analytics_db;

-- Operations on the current data source
SHOW TABLES;
SELECT COUNT(*) FROM events;

-- Switch to another data source
USE user_db;
SELECT * FROM profiles LIMIT 5;
```

## Session Variables

Use the `SET` statement to configure session-level variables for request tracing and auditing:

```sql
-- Set a trace_id for request tracing
SET @trace_id = 'req-20260215-abc123';

-- All subsequent queries in this session will be associated with this trace_id
SELECT * FROM orders WHERE status = 'pending';

-- You can update the trace_id at any time
SET @trace_id = 'req-20260215-def456';
```

The `trace_id` is automatically recorded in audit logs, making it easy to correlate upstream and downstream requests when troubleshooting issues.
