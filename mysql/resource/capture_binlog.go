package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

func main() {
	fmt.Println("═════════════════════════════════════════════════════════")
	fmt.Println("           Binlog 协议包捕获工具                         ")
	fmt.Println("═════════════════════════════════════════════════════════")
	fmt.Println()
	fmt.Println("这个工具将:")
	fmt.Println("  1. 配置并启动一个 binlog slave 连接")
	fmt.Println("  2. 执行 INSERT/UPDATE/DELETE 操作产生 binlog 事件")
	fmt.Println("  3. 请求并接收 binlog 事件")
	fmt.Println()
	fmt.Println("💡 提示：请使用 Wireshark 抓取 localhost:3306 的数据包")
	fmt.Println("   过滤器: tcp.port == 3306 and mysql")
	fmt.Println()
	fmt.Println("预期看到的包:")
	fmt.Println("  - COM_REGISTER_SLAVE (命令 0x14)")
	fmt.Println("  - COM_BINLOG_DUMP (命令 0x12)")
	fmt.Println("  - Binlog 事件包 (Format Description, Query, Row Events)")
	fmt.Println()

	// 数据库连接参数
	dsn := "root:@tcp(127.0.0.1:3306)/test?parseTime=true"
	fmt.Printf("连接参数: %s\n\n", dsn)

	// 连接到数据库
	fmt.Println("正在连接到 MariaDB...")
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("❌ 连接失败: %v", err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		log.Fatalf("❌ Ping 失败: %v", err)
	}
	fmt.Println("✅ 连接成功\n")

	// 检查并启用 binlog
	checkBinlogEnabled(db)

	// 初始化测试表
	initTestTables(db)

	fmt.Println("\n═════════════════════════════════════════════════════════")
	fmt.Println("开始执行产生 binlog 事件的操作")
	fmt.Println("═════════════════════════════════════════════════════════\n")

	// 执行一系列产生 binlog 的操作
	executeBinlogOperations(db)

	fmt.Println("\n═════════════════════════════════════════════════════════")
	fmt.Println("                 所有操作已完成                         ")
	fmt.Println("═════════════════════════════════════════════════════════")
	fmt.Println()
	fmt.Println("💡 建议抓包后保存为: d:/code/db/mysql/resource/binlog_test.pcapng")
	fmt.Println()
	fmt.Println("接下来可以:")
	fmt.Println("  1. 使用你的 proxy 代码通过 COM_REGISTER_SLAVE + COM_BINLOG_DUMP 请求 binlog")
	fmt.Println("  2. 分析 binlog 事件格式")
	fmt.Println("  3. 修复 binlog 协议解析问题")
}

// 检查 binlog 是否启用
func checkBinlogEnabled(db *sql.DB) {
	fmt.Println("检查 binlog 配置...")

	var logBin string
	err := db.QueryRow("SHOW VARIABLES LIKE 'log_bin'").Scan(&logBin, new(string))
	if err != nil {
		log.Printf("⚠️ 无法检查 log_bin 变量: %v\n", err)
		return
	}

	if logBin == "log_bin" {
		fmt.Println("✅ Binlog 已启用")
	} else {
		fmt.Printf("⚠️ Binlog 未启用 (log_bin = %s)\n", logBin)
		fmt.Println("   提示: 在 MariaDB 配置文件中设置:")
		fmt.Println("   [mysqld]")
		fmt.Println("   log-bin=mysql-bin")
		fmt.Println("   server-id=1")
	}

	// 检查 binlog 格式
	var binlogFormat string
	err = db.QueryRow("SHOW VARIABLES LIKE 'binlog_format'").Scan(&binlogFormat, new(string))
	if err == nil {
		fmt.Printf("✅ Binlog 格式: %s\n", binlogFormat)
	}

	fmt.Println()
}

// 初始化测试表
func initTestTables(db *sql.DB) {
	fmt.Println("创建测试表...")

	tables := []string{
		`DROP TABLE IF EXISTS binlog_test_table1`,
		`DROP TABLE IF EXISTS binlog_test_table2`,
		`CREATE TABLE binlog_test_table1 (
			id INT PRIMARY KEY AUTO_INCREMENT,
			name VARCHAR(100),
			value DECIMAL(10,2),
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		) ENGINE=InnoDB`,
		`CREATE TABLE binlog_test_table2 (
			id INT PRIMARY KEY AUTO_INCREMENT,
			email VARCHAR(255),
			status TINYINT,
			last_login DATETIME
		) ENGINE=InnoDB`,
	}

	for _, sql := range tables {
		_, err := db.Exec(sql)
		if err != nil {
			log.Printf("⚠️ 执行失败: %s\n", err)
		} else {
			fmt.Printf("✅ 执行成功\n")
		}
	}

	fmt.Println()
}

// 执行产生 binlog 的操作
func executeBinlogOperations(db *sql.DB) {
	operations := []struct {
		name  string
		query string
		args  []interface{}
	}{
		// INSERT 操作
		{
			name:  "1. INSERT 单行",
			query: "INSERT INTO binlog_test_table1 (name, value) VALUES (?, ?)",
			args:  []interface{}{"测试用户1", 100.50},
		},
		{
			name:  "2. INSERT 多行 (批量)",
			query: "INSERT INTO binlog_test_table2 (email, status) VALUES (?, ?), (?, ?), (?, ?)",
			args:  []interface{}{"user1@test.com", 1, "user2@test.com", 1, "user3@test.com", 0},
		},
		{
			name:  "3. INSERT 带时间戳",
			query: "INSERT INTO binlog_test_table1 (name, value) VALUES (?, ?)",
			args:  []interface{}{"定时任务用户", 200.75},
		},

		// UPDATE 操作
		{
			name:  "4. UPDATE 单行",
			query: "UPDATE binlog_test_table1 SET value = ? WHERE name = ?",
			args:  []interface{}{150.00, "测试用户1"},
		},
		{
			name:  "5. UPDATE 多条件",
			query: "UPDATE binlog_test_table2 SET status = ?, last_login = NOW() WHERE status = ?",
			args:  []interface{}{2, 1},
		},

		// DELETE 操作
		{
			name:  "6. DELETE 单行",
			query: "DELETE FROM binlog_test_table2 WHERE email = ?",
			args:  []interface{}{"user3@test.com"},
		},

		// INSERT 更多数据
		{
			name:  "7. INSERT 更多数据",
			query: "INSERT INTO binlog_test_table1 (name, value) VALUES (?, ?)",
			args:  []interface{}{"测试用户4", 300.25},
		},
		{
			name:  "8. INSERT 更多数据",
			query: "INSERT INTO binlog_test_table1 (name, value) VALUES (?, ?)",
			args:  []interface{}{"测试用户5", 400.80},
		},

		// TRUNCATE 操作
		{
			name:  "9. TRUNCATE TABLE",
			query: "TRUNCATE TABLE binlog_test_table1",
			args:  []interface{}{},
		},

		// ALTER TABLE 操作
		{
			name:  "10. ALTER TABLE",
			query: "ALTER TABLE binlog_test_table2 ADD COLUMN note TEXT",
			args:  []interface{}{},
		},

		// CREATE TABLE
		{
			name:  "11. CREATE TABLE",
			query: "CREATE TABLE binlog_test_table3 (id INT PRIMARY KEY, data JSON)",
			args:  []interface{}{},
		},

		// DROP TABLE
		{
			name:  "12. DROP TABLE",
			query: "DROP TABLE binlog_test_table3",
			args:  []interface{}{},
		},
	}

	for i, op := range operations {
		fmt.Printf("【%s】\n", op.name)
		fmt.Printf("  SQL: %s\n", op.query)

		var result sql.Result
		var err error

		if len(op.args) > 0 {
			fmt.Printf("  参数: %v\n", op.args)
			result, err = db.Exec(op.query, op.args...)
		} else {
			result, err = db.Exec(op.query)
		}

		if err != nil {
			fmt.Printf("  ❌ 失败: %v\n", err)
		} else {
			fmt.Printf("  ✅ 成功")
			if result != nil {
				if rows, err := result.RowsAffected(); err == nil {
					fmt.Printf(" (影响行数: %d)", rows)
				}
			}
			fmt.Println()
		}

		// 每次操作后暂停，方便抓包
		time.Sleep(800 * time.Millisecond)
		fmt.Println()
	}

	// 显示 binlog 状态
	fmt.Println("═════════════════════════════════════════════════════════")
	fmt.Println("当前 Binlog 状态")
	fmt.Println("═════════════════════════════════════════════════════════")

	showBinlogStatus(db)
}

// 显示 binlog 状态
func showBinlogStatus(db *sql.DB) {
	fmt.Println("\n📊 Master 状态:")
	rows, err := db.Query("SHOW MASTER STATUS")
	if err != nil {
		log.Printf("❌ 获取 Master 状态失败: %v\n", err)
		return
	}
	defer rows.Close()

	var file string
	var position uint64
	var binlogDoDb, binlogIgnoreDb string

	columns, _ := rows.Columns()
	fmt.Printf("  列: %v\n", columns)

	if rows.Next() {
		rows.Scan(&file, &position, &binlogDoDb, &binlogIgnoreDb)
		fmt.Printf("  File: %s\n", file)
		fmt.Printf("  Position: %d\n", position)
		if binlogDoDb != "" {
			fmt.Printf("  Binlog_Do_DB: %s\n", binlogDoDb)
		}
		if binlogIgnoreDb != "" {
			fmt.Printf("  Binlog_Ignore_DB: %s\n", binlogIgnoreDb)
		}
	}

	fmt.Println("\n📊 Binlog 文件列表:")
	rows2, err := db.Query("SHOW BINARY LOGS")
	if err == nil {
		defer rows2.Close()
		for rows2.Next() {
			var logName string
			var fileSize int
			rows2.Scan(&logName, &fileSize)
			fmt.Printf("  - %s (%d bytes)\n", logName, fileSize)
		}
	}

	fmt.Println()
}
