package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

func main() {
	fmt.Println("开始连接 MySQL 服务器...")

	// 连接数据库
	dsn := "root:password@tcp(localhost:3306)/test"
	fmt.Printf("连接字符串: %s\n", dsn)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatal("连接数据库失败:", err)
	}
	defer db.Close()

	fmt.Println("数据库连接已创建，开始 Ping...")

	// 设置连接超时
	db.SetConnMaxLifetime(time.Second * 10)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	// 测试连接
	fmt.Println("执行 Ping...")
	if err := db.Ping(); err != nil {
		log.Fatal("Ping 失败:", err)
	}
	fmt.Println("Ping 成功！连接已建立")

	// 等待一下，让服务器处理完登录
	time.Sleep(1 * time.Second)

	// 测试简单查询
	fmt.Println("\n开始测试简单查询:")
	_, err = db.Exec("SELECT 1")
	if err != nil {
		log.Printf("简单查询失败: %v\n", err)
	} else {
		fmt.Println("简单查询成功")
	}

	// 测试查询
	fmt.Println("\n开始测试 select * from test:")
	rows, err := db.Query("select * from test")
	if err != nil {
		log.Printf("查询失败: %v\n", err)
		return
	}
	defer rows.Close()
	fmt.Println("查询执行成功，开始获取列信息...")

	// 获取列信息
	columns, err := rows.Columns()
	if err != nil {
		log.Fatal("获取列信息失败:", err)
	}
	fmt.Printf("列信息: %v\n", columns)

	// 读取数据
	fmt.Println("开始读取数据行...")
	rowCount := 0
	for rows.Next() {
		var id, name string
		if err := rows.Scan(&id, &name); err != nil {
			log.Fatal("读取行数据失败:", err)
		}
		fmt.Printf("第 %d 行 - ID: %s, Name: %s\n", rowCount+1, id, name)
		rowCount++
	}
	fmt.Printf("共读取了 %d 行数据\n", rowCount)

	// 测试版本查询
	fmt.Println("\n开始测试 select @@version_comment limit 1:")
	row := db.QueryRow("select @@version_comment limit 1")
	var version string
	if err := row.Scan(&version); err != nil {
		log.Printf("查询版本失败: %v\n", err)
	} else {
		fmt.Printf("版本信息: %s\n", version)
	}

	fmt.Println("\n所有测试完成！")
}
