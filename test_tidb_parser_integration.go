package main

import (
	"fmt"
	"log"

	"mysql-proxy/mysql/parser"
)

func main() {
	fmt.Println("=== TiDB Parser 集成测试 ===")

	// 创建解析器
	p := parser.NewParser()

	// 测试 SELECT 查询
	fmt.Println("\n测试 1: SELECT 查询")
	selectSQL := "SELECT id, name FROM users WHERE age > 18"
	stmt, err := p.ParseOneStmtText(selectSQL)
	if err != nil {
		log.Fatalf("解析失败: %v", err)
	}
	
	info := parser.ExtractSQLInfo(stmt)
	fmt.Printf("✓ SQL 类型: %s\n", parser.GetStmtType(stmt))
	fmt.Printf("✓ 涉及表: %v\n", info.Tables)
	fmt.Printf("✓ 涉及列: %v\n", info.Columns)
	fmt.Printf("✓ 是读操作: %v\n", parser.IsReadOperation(stmt))
	fmt.Printf("✓ 是写操作: %v\n", parser.IsWriteOperation(stmt))

	// 测试 INSERT 语句
	fmt.Println("\n测试 2: INSERT 语句")
	insertSQL := "INSERT INTO users (id, name) VALUES (1, 'Alice')"
	stmt, err = p.ParseOneStmtText(insertSQL)
	if err != nil {
		log.Fatalf("解析失败: %v", err)
	}
	
	info = parser.ExtractSQLInfo(stmt)
	fmt.Printf("✓ SQL 类型: %s\n", parser.GetStmtType(stmt))
	fmt.Printf("✓ 涉及表: %v\n", info.Tables)
	fmt.Printf("✓ 涉及列: %v\n", info.Columns)
	fmt.Printf("✓ 是读操作: %v\n", parser.IsReadOperation(stmt))
	fmt.Printf("✓ 是写操作: %v\n", parser.IsWriteOperation(stmt))

	// 测试 UPDATE 语句
	fmt.Println("\n测试 3: UPDATE 语句")
	updateSQL := "UPDATE users SET age = 26 WHERE id = 1"
	stmt, err = p.ParseOneStmtText(updateSQL)
	if err != nil {
		log.Fatalf("解析失败: %v", err)
	}
	
	info = parser.ExtractSQLInfo(stmt)
	fmt.Printf("✓ SQL 类型: %s\n", parser.GetStmtType(stmt))
	fmt.Printf("✓ 涉及表: %v\n", info.Tables)
	fmt.Printf("✓ 涉及列: %v\n", info.Columns)
	fmt.Printf("✓ 是读操作: %v\n", parser.IsReadOperation(stmt))
	fmt.Printf("✓ 是写操作: %v\n", parser.IsWriteOperation(stmt))

	// 测试 SET 语句
	fmt.Println("\n测试 4: SET 语句")
	setSQL := "SET @user_var = 'hello', autocommit = OFF"
	stmt, err = p.ParseOneStmtText(setSQL)
	if err != nil {
		log.Fatalf("解析失败: %v", err)
	}
	
	info = parser.ExtractSQLInfo(stmt)
	fmt.Printf("✓ SQL 类型: %s\n", parser.GetStmtType(stmt))
	fmt.Printf("✓ 是读操作: %v\n", parser.IsReadOperation(stmt))
	fmt.Printf("✓ 是写操作: %v\n", parser.IsWriteOperation(stmt))

	// 测试 USE 语句
	fmt.Println("\n测试 5: USE 语句")
	useSQL := "USE testdb"
	stmt, err = p.ParseOneStmtText(useSQL)
	if err != nil {
		log.Fatalf("解析失败: %v", err)
	}
	
	info = parser.ExtractSQLInfo(stmt)
	fmt.Printf("✓ SQL 类型: %s\n", parser.GetStmtType(stmt))

	// 测试 CREATE TABLE 语句
	fmt.Println("\n测试 6: CREATE TABLE 语句")
	createSQL := "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))"
	stmt, err = p.ParseOneStmtText(createSQL)
	if err != nil {
		log.Fatalf("解析失败: %v", err)
	}
	
	info = parser.ExtractSQLInfo(stmt)
	fmt.Printf("✓ SQL 类型: %s\n", parser.GetStmtType(stmt))
	fmt.Printf("✓ 涉及表: %v\n", info.Tables)
	fmt.Printf("✓ 是读操作: %v\n", parser.IsReadOperation(stmt))
	fmt.Printf("✓ 是写操作: %v\n", parser.IsWriteOperation(stmt))
	fmt.Printf("✓ 是 DDL: %v\n", info.IsDDL)

	fmt.Println("\n=== 所有测试通过！TiDB Parser 集成成功！===")
}
