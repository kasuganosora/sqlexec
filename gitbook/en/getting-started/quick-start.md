# Quick Start

## Scenario 1: Standalone Server

### 1. Start the Server

```bash
# Build and start (listens on port 3306 by default)
go build -o sqlexec ./cmd/service
./sqlexec
```

### 2. Connect to the Database

Connect using any MySQL client:

```bash
mysql -h 127.0.0.1 -P 3306 -u root
```

### 3. Execute SQL

```sql
-- Create a table
CREATE TABLE users (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(100),
    age INT
);

-- Insert data
INSERT INTO users (name, age) VALUES ('Alice', 30), ('Bob', 25);

-- Query
SELECT * FROM users WHERE age > 20 ORDER BY name;

-- Aggregation
SELECT AVG(age) as avg_age, COUNT(*) as total FROM users;
```

## Scenario 2: Embedded Library

### 1. Create a Database Instance

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
    // Create a database instance
    db, err := api.NewDB(nil)
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // Register an in-memory data source
    memDS := memory.NewMVCCDataSource(&domain.DataSourceConfig{
        Type:     domain.DataSourceTypeMemory,
        Name:     "default",
        Writable: true,
    })
    memDS.Connect(context.Background())
    db.RegisterDataSource("default", memDS)

    // Create a session
    session := db.Session()
    defer session.Close()

    // Create a table
    session.Execute("CREATE TABLE users (id INT, name VARCHAR(100), age INT)")

    // Insert data
    session.Execute("INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 25)")

    // Query
    rows, err := session.QueryAll("SELECT * FROM users WHERE age > ?", 20)
    if err != nil {
        log.Fatal(err)
    }

    for _, row := range rows {
        fmt.Printf("ID: %v, Name: %v, Age: %v\n", row["id"], row["name"], row["age"])
    }
}
```

### 2. Using File Data Sources

```go
// Query a CSV file
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

### 3. Using GORM

```go
import (
    sqlexecgorm "github.com/kasuganosora/sqlexec/pkg/api/gorm"
    "gorm.io/gorm"
)

// Create a GORM connection
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

## Next Steps

- [Configuration Guide](configuration.md) -- Learn about all configuration options
- [Standalone Server](../standalone-server/overview.md) -- Deploy a production server
- [Embedded Usage](../embedded/overview.md) -- Deep dive into API usage
- [Data Sources](../datasources/overview.md) -- Connect to various data sources
