package main

import (
	"fmt"
	"log"

	"github.com/kasuganosora/sqlexec/pkg/api"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

func main() {
	// 创建 DB
	db, err := api.NewDB(&api.DBConfig{
		CacheEnabled:  false,
		DefaultLogger: api.NewDefaultLogger(api.LogInfo),
		DebugMode:     false,
	})
	if err != nil {
		log.Fatalf("Failed to create DB: %v", err)
	}

	// 创建测试数据源
	ds1 := createTestDataSource("testdb1", []domain.ColumnInfo{
		{Name: "id", Type: "int", Primary: true},
		{Name: "name", Type: "varchar(100)", Nullable: false},
		{Name: "email", Type: "varchar(255)", Nullable: true, Unique: true},
	})

	ds2 := createTestDataSource("testdb2", []domain.ColumnInfo{
		{Name: "id", Type: "int", Primary: true},
		{Name: "description", Type: "text", Nullable: true},
	})

	// 注册数据源
	err = db.RegisterDataSource("testdb1", ds1)
	if err != nil {
		log.Fatalf("Failed to register testdb1: %v", err)
	}

	err = db.RegisterDataSource("testdb2", ds2)
	if err != nil {
		log.Fatalf("Failed to register testdb2: %v", err)
	}

	// 创建会话
	session := db.Session()
	defer session.Close()

	fmt.Println("=== Testing information_schema.schemata ===")
	// 测试 schemata 表
	query := "SELECT * FROM information_schema.schemata"
	rows, err := session.QueryAll(query)
	if err != nil {
		log.Fatalf("Failed to query schemata: %v", err)
	}
	fmt.Printf("Found %d databases:\n", len(rows))
	for _, row := range rows {
		fmt.Printf("  - %s\n", row["schema_name"])
	}

	fmt.Println("\n=== Testing information_schema.tables ===")
	// 测试 tables 表
	query = "SELECT * FROM information_schema.tables"
	rows, err = session.QueryAll(query)
	if err != nil {
		log.Fatalf("Failed to query tables: %v", err)
	}
	fmt.Printf("Found %d tables:\n", len(rows))
	for _, row := range rows {
		fmt.Printf("  - %s.%s\n", row["table_schema"], row["table_name"])
	}

	fmt.Println("\n=== Testing information_schema.columns ===")
	// 测试 columns 表
	query = "SELECT * FROM information_schema.columns WHERE table_schema = 'testdb1'"
	rows, err = session.QueryAll(query)
	if err != nil {
		log.Fatalf("Failed to query columns: %v", err)
	}
	fmt.Printf("Found %d columns in testdb1:\n", len(rows))
	for _, row := range rows {
		fmt.Printf("  - %s (%s)\n", row["column_name"], row["data_type"])
	}

	fmt.Println("\n=== Testing information_schema.table_constraints ===")
	// 测试 table_constraints 表
	query = "SELECT * FROM information_schema.table_constraints WHERE table_schema = 'testdb1'"
	rows, err = session.QueryAll(query)
	if err != nil {
		log.Fatalf("Failed to query table_constraints: %v", err)
	}
	fmt.Printf("Found %d constraints in testdb1:\n", len(rows))
	for _, row := range rows {
		fmt.Printf("  - %s: %s\n", row["constraint_name"], row["constraint_type"])
	}

	fmt.Println("\n=== Testing information_schema.key_column_usage ===")
	// 测试 key_column_usage 表
	query = "SELECT * FROM information_schema.key_column_usage WHERE table_schema = 'testdb1'"
	rows, err = session.QueryAll(query)
	if err != nil {
		log.Fatalf("Failed to query key_column_usage: %v", err)
	}
	fmt.Printf("Found %d key columns in testdb1:\n", len(rows))
	for _, row := range rows {
		fmt.Printf("  - %s: %s\n", row["column_name"], row["constraint_name"])
	}

	fmt.Println("\n=== Testing WHERE clause ===")
	// 测试 WHERE 子句
	query = "SELECT * FROM information_schema.schemata WHERE schema_name = 'testdb1'"
	rows, err = session.QueryAll(query)
	if err != nil {
		log.Fatalf("Failed to query schemata with WHERE: %v", err)
	}
	fmt.Printf("Found %d databases matching 'testdb1':\n", len(rows))
	for _, row := range rows {
		fmt.Printf("  - %s\n", row["schema_name"])
	}

	fmt.Println("\n=== Testing USE information_schema ===")
	// 测试 USE 语句
	_, err = session.Execute("USE information_schema")
	if err != nil {
		log.Fatalf("Failed to USE information_schema: %v", err)
	}
	fmt.Println("Successfully switched to information_schema database")

	// 使用不带前缀的表名查询
	query = "SELECT * FROM schemata"
	rows, err = session.QueryAll(query)
	if err != nil {
		log.Fatalf("Failed to query schemata without prefix: %v", err)
	}
	fmt.Printf("Found %d databases using unqualified table name:\n", len(rows))
	for _, row := range rows {
		fmt.Printf("  - %s\n", row["schema_name"])
	}

	// 测试其他表
	query = "SELECT * FROM tables WHERE table_schema = 'testdb1'"
	rows, err = session.QueryAll(query)
	if err != nil {
		log.Fatalf("Failed to query tables without prefix: %v", err)
	}
	fmt.Printf("Found %d tables in testdb1 using unqualified table name:\n", len(rows))
	for _, row := range rows {
		fmt.Printf("  - %s\n", row["table_name"])
	}

	fmt.Println("\n=== Testing USE regular database ===")
	// 切换回普通数据库
	_, err = session.Execute("USE testdb1")
	if err != nil {
		log.Fatalf("Failed to USE testdb1: %v", err)
	}
	fmt.Println("Successfully switched to testdb1 database")

	// 验证当前数据库是 testdb1
	query = "SELECT * FROM test_table"
	rows, err = session.QueryAll(query)
	if err != nil {
		log.Fatalf("Failed to query test_table in testdb1: %v", err)
	}
	fmt.Printf("Query test_table in testdb1 succeeded (found %d rows)\n", len(rows))

	// 切换回 information_schema
	_, err = session.Execute("USE information_schema")
	if err != nil {
		log.Fatalf("Failed to switch back to information_schema: %v", err)
	}

	// 再次使用限定名测试
	query = "SELECT * FROM information_schema.schemata"
	rows, err = session.QueryAll(query)
	if err != nil {
		log.Fatalf("Failed to query with qualified name after USE: %v", err)
	}
	fmt.Printf("Query with qualified name still works (found %d databases)\n", len(rows))

	fmt.Println("\n=== All tests passed! ===")
}

// createTestDataSource 创建一个简单的测试数据源
func createTestDataSource(name string, columns []domain.ColumnInfo) domain.DataSource {
	mds := api.NewMockDataSourceWithTableInfo(name, columns)

	// Connect the datasource
	err := mds.Connect(nil)
	if err != nil {
		log.Fatalf("Failed to connect datasource: %v", err)
	}

	return mds
}
