# 快速开始

## 场景一：独立服务器

### 1. 启动服务器

```bash
# 编译并启动（默认监听 3306 端口）
go build -o sqlexec ./cmd/service
./sqlexec
```

### 2. 连接数据库

使用任意 MySQL 客户端连接：

```bash
mysql -h 127.0.0.1 -P 3306 -u root
```

### 3. 执行 SQL

```sql
-- 创建表
CREATE TABLE users (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(100),
    age INT
);

-- 插入数据
INSERT INTO users (name, age) VALUES ('Alice', 30), ('Bob', 25);

-- 查询
SELECT * FROM users WHERE age > 20 ORDER BY name;

-- 聚合
SELECT AVG(age) as avg_age, COUNT(*) as total FROM users;
```

## 场景二：嵌入式库

### 1. 创建数据库实例

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/kasuganosora/sqlexec/pkg/api"
    "github.com/kasuganosora/sqlexec/pkg/resource/domain"
    "github.com/kasuganosora/sqlexec/pkg/resource/memory"
)

func main() {
    // 创建数据库实例
    db, err := api.NewDB(nil)
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // 注册内存数据源
    memDS := memory.NewMVCCDataSource(&domain.DataSourceConfig{
        Type:     domain.DataSourceTypeMemory,
        Name:     "default",
        Writable: true,
    })
    memDS.Connect(context.Background())
    db.RegisterDataSource("default", memDS)

    // 创建会话
    session := db.Session()
    defer session.Close()

    // 创建表
    session.Execute("CREATE TABLE users (id INT, name VARCHAR(100), age INT)")

    // 插入数据
    session.Execute("INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 25)")

    // 查询
    rows, err := session.QueryAll("SELECT * FROM users WHERE age > ?", 20)
    if err != nil {
        log.Fatal(err)
    }

    for _, row := range rows {
        fmt.Printf("ID: %v, Name: %v, Age: %v\n", row["id"], row["name"], row["age"])
    }
}
```

### 2. 使用文件数据源

```go
// 查询 CSV 文件
csvDS, _ := csv.NewCSVFactory().Create(&domain.DataSourceConfig{
    Type:     domain.DataSourceTypeCSV,
    Name:     "sales",
    Database: "/data/sales.csv",
})
csvDS.Connect(context.Background())
db.RegisterDataSource("sales", csvDS)

session.Execute("USE sales")
rows, _ := session.QueryAll("SELECT product, SUM(amount) FROM csv_data GROUP BY product")
```

### 3. 使用 GORM

```go
import (
    sqlexecgorm "github.com/kasuganosora/sqlexec/pkg/api/gorm"
    "gorm.io/gorm"
)

// 创建 GORM 连接
gormDB, err := gorm.Open(
    sqlexecgorm.NewDialector(db.Session()),
    &gorm.Config{SkipDefaultTransaction: true},
)

type User struct {
    ID   uint
    Name string
    Age  int
}

gormDB.AutoMigrate(&User{})
gormDB.Create(&User{Name: "Alice", Age: 30})

var users []User
gormDB.Where("age > ?", 18).Find(&users)
```

## 下一步

- [配置详解](configuration.md) — 了解所有配置选项
- [独立服务器](../standalone-server/overview.md) — 部署生产服务器
- [嵌入式使用](../embedded/overview.md) — 深入 API 使用
- [数据源](../datasources/overview.md) — 连接各种数据源
