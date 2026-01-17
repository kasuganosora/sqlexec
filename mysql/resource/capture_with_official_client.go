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
	fmt.Println("    使用官方 MySQL 客户端库捕获协议包                  ")
	fmt.Println("═════════════════════════════════════════════════════════")
	fmt.Println()

	// 数据库连接参数
	dsn := "root:@tcp(127.0.0.1:3306)/test?parseTime=true"
	fmt.Printf("连接参数:\n")
	fmt.Printf("  DSN: %s\n", dsn)
	fmt.Println()
	fmt.Println("💡 提示：请使用 Wireshark 抓取 localhost:3306 的数据包")
	fmt.Println("   过滤器: tcp.port == 3306 and mysql")
	fmt.Println()

	// 连接到数据库
	fmt.Println("正在连接到 MariaDB...")
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("❌ 连接失败: %v", err)
	}
	defer db.Close()

	// 测试连接
	err = db.Ping()
	if err != nil {
		log.Fatalf("❌ Ping 失败: %v", err)
	}
	fmt.Println("✅ 连接成功")
	fmt.Println()

	// 初始化测试数据
	initTestData(db)

	// 测试场景
	testScenarios := []struct {
		name       string
		query      string
		args       []any
		comment    string
		usePrepare bool // 是否使用预处理语句
	}{
		// ===== 数据库操作 =====
		{
			name:       "场景1: SHOW DATABASES",
			query:      "SHOW DATABASES",
			args:       []any{},
			comment:    "显示所有数据库",
			usePrepare: false,
		},
		{
			name:       "场景2: SHOW TABLES",
			query:      "SHOW TABLES",
			args:       []any{},
			comment:    "显示所有表",
			usePrepare: false,
		},
		{
			name:       "场景3: SHOW CREATE TABLE",
			query:      "SHOW CREATE TABLE mysql_data_types_demo",
			args:       []any{},
			comment:    "显示建表语句",
			usePrepare: false,
		},
		{
			name:       "场景4: DESC/DESCRIBE TABLE",
			query:      "DESCRIBE mysql_data_types_demo",
			args:       []any{},
			comment:    "描述表结构",
			usePrepare: false,
		},

		// ===== 预处理语句 - SELECT 操作 =====
		{
			name:       "场景5: PREPARE SELECT - 单个 INT 参数",
			query:      "SELECT * FROM mysql_data_types_demo WHERE type_int = ?",
			args:       []any{500},
			comment:    "使用 INT 参数查询",
			usePrepare: true,
		},
		{
			name:       "场景6: PREPARE SELECT - 单个 VARCHAR 参数",
			query:      "SELECT * FROM mysql_data_types_demo WHERE type_varchar = ?",
			args:       []any{"variable length"},
			comment:    "使用 VARCHAR 参数查询",
			usePrepare: true,
		},
		{
			name:       "场景7: PREPARE SELECT - 多个参数",
			query:      "SELECT * FROM mysql_data_types_demo WHERE type_int = ? AND type_varchar = ?",
			args:       []any{500, "variable length"},
			comment:    "使用多个参数查询",
			usePrepare: true,
		},
		{
			name:       "场景8: PREPARE SELECT - NULL 参数",
			query:      "SELECT * FROM mysql_data_types_demo WHERE type_bool = ?",
			args:       []any{nil},
			comment:    "使用 NULL 参数查询（关键测试！）",
			usePrepare: true,
		},
		{
			name:       "场景9: PREPARE SELECT - TINYINT",
			query:      "SELECT * FROM mysql_data_types_demo WHERE type_tinyint = ?",
			args:       []any{int8(100)},
			comment:    "使用 TINYINT 参数",
			usePrepare: true,
		},
		{
			name:       "场景10: PREPARE SELECT - BIGINT",
			query:      "SELECT * FROM mysql_data_types_demo WHERE type_bigint = ?",
			args:       []any{int64(9000000000000000000)},
			comment:    "使用 BIGINT 参数",
			usePrepare: true,
		},
		{
			name:       "场景11: PREPARE SELECT - FLOAT",
			query:      "SELECT * FROM mysql_data_types_demo WHERE type_float = ?",
			args:       []any{float32(3.14159)},
			comment:    "使用 FLOAT 参数",
			usePrepare: true,
		},
		{
			name:       "场景12: PREPARE SELECT - DOUBLE",
			query:      "SELECT * FROM mysql_data_types_demo WHERE type_double = ?",
			args:       []any{float64(2.718281828459045)},
			comment:    "使用 DOUBLE 参数",
			usePrepare: true,
		},
		{
			name:       "场景13: PREPARE SELECT - DATE",
			query:      "SELECT * FROM mysql_data_types_demo WHERE type_date = ?",
			args:       []any{"2024-01-15"},
			comment:    "使用 DATE 参数",
			usePrepare: true,
		},
		{
			name:       "场景14: PREPARE SELECT - DATETIME",
			query:      "SELECT * FROM mysql_data_types_demo WHERE type_datetime = ?",
			args:       []any{"2024-01-15 14:30:45"},
			comment:    "使用 DATETIME 参数",
			usePrepare: true,
		},
		{
			name:       "场景15: PREPARE SELECT - 9个参数（NULL bitmap多字节）",
			query:      "SELECT * FROM mysql_data_types_demo WHERE type_int = ? AND type_smallint = ? AND type_mediumint = ? AND type_bigint = ? AND type_float = ? AND type_double = ? AND type_varchar = ? AND type_char = ? AND type_tinyint = ?",
			args:       []any{500, 32000, 8000000, int64(9000000000000000000), 3.14159, 2.718281828459045, "variable length", "fixed", 100},
			comment:    "使用 9 个参数测试 NULL bitmap 多字节",
			usePrepare: true,
		},

		// ===== INSERT 操作 =====
		{
			name:       "场景16: PREPARE INSERT - 插入单行",
			query:      "INSERT INTO test_capture_table (id, name, value) VALUES (?, ?, ?)",
			args:       []any{1, "测试名称1", 100.5},
			comment:    "预处理插入单行数据",
			usePrepare: true,
		},
		{
			name:       "场景17: PREPARE INSERT - 插入带NULL",
			query:      "INSERT INTO test_capture_table (id, name, value) VALUES (?, ?, ?)",
			args:       []any{2, "测试名称2", nil},
			comment:    "预处理插入带NULL值的数据",
			usePrepare: true,
		},
		{
			name:       "场景18: PREPARE INSERT - 批量插入",
			query:      "INSERT INTO test_capture_table (id, name, value) VALUES (?, ?, ?)",
			args:       []any{3, "测试名称3", 300.5},
			comment:    "预处理插入第三行数据",
			usePrepare: true,
		},

		// ===== UPDATE 操作 =====
		{
			name:       "场景19: PREPARE UPDATE - 更新单行",
			query:      "UPDATE test_capture_table SET value = ? WHERE id = ?",
			args:       []any{999.9, 1},
			comment:    "预处理更新单行数据",
			usePrepare: true,
		},
		{
			name:       "场景20: PREPARE UPDATE - 更新为NULL",
			query:      "UPDATE test_capture_table SET value = ? WHERE id = ?",
			args:       []any{nil, 2},
			comment:    "预处理更新为NULL值",
			usePrepare: true,
		},
		{
			name:       "场景21: PREPARE UPDATE - 多条件更新",
			query:      "UPDATE test_capture_table SET value = ? WHERE id = ? AND name = ?",
			args:       []any{888.8, 3, "测试名称3"},
			comment:    "预处理使用多条件更新",
			usePrepare: true,
		},

		// ===== DELETE 操作 =====
		{
			name:       "场景22: PREPARE DELETE - 删除单行",
			query:      "DELETE FROM test_capture_table WHERE id = ?",
			args:       []any{1},
			comment:    "预处理删除单行数据",
			usePrepare: true,
		},
		{
			name:       "场景23: PREPARE DELETE - 多条件删除",
			query:      "DELETE FROM test_capture_table WHERE id = ? AND name = ?",
			args:       []any{2, "测试名称2"},
			comment:    "预处理使用多条件删除",
			usePrepare: true,
		},

		// ===== SET 变量操作 =====
		{
			name:       "场景24: SET SESSION 变量",
			query:      "SET SESSION sql_mode = ?",
			args:       []any{"STRICT_TRANS_TABLES"},
			comment:    "设置会话变量",
			usePrepare: false,
		},
		{
			name:       "场景25: SET 用户变量",
			query:      "SET @test_var = ?",
			args:       []any{"test_value"},
			comment:    "设置用户变量",
			usePrepare: false,
		},

		// ===== 复杂查询 =====
		{
			name:       "场景26: PREPARE SELECT - LIKE 查询",
			query:      "SELECT * FROM mysql_data_types_demo WHERE type_varchar LIKE ?",
			args:       []any{"%variable%"},
			comment:    "使用 LIKE 参数查询",
			usePrepare: true,
		},
		{
			name:       "场景27: PREPARE SELECT - IN 查询",
			query:      "SELECT * FROM mysql_data_types_demo WHERE type_int IN (?, ?, ?)",
			args:       []any{500, 501, 502},
			comment:    "使用 IN 参数查询",
			usePrepare: true,
		},
		{
			name:       "场景28: PREPARE SELECT - BETWEEN 查询",
			query:      "SELECT * FROM mysql_data_types_demo WHERE type_int BETWEEN ? AND ?",
			args:       []any{100, 1000},
			comment:    "使用 BETWEEN 参数查询",
			usePrepare: true,
		},
		{
			name:       "场景29: PREPARE SELECT - ORDER BY 参数",
			query:      "SELECT * FROM mysql_data_types_demo WHERE type_int > ? ORDER BY ? DESC LIMIT ?",
			args:       []any{0, "type_int", 10},
			comment:    "使用 ORDER BY 和 LIMIT 参数",
			usePrepare: true,
		},

		// ===== 统计函数 =====
		{
			name:       "场景30: PREPARE SELECT - COUNT",
			query:      "SELECT COUNT(*) FROM mysql_data_types_demo WHERE type_int > ?",
			args:       []any{0},
			comment:    "使用 COUNT 统计函数",
			usePrepare: true,
		},
		{
			name:       "场景31: PREPARE SELECT - SUM",
			query:      "SELECT SUM(type_int) FROM mysql_data_types_demo WHERE type_int > ?",
			args:       []any{0},
			comment:    "使用 SUM 聚合函数",
			usePrepare: true,
		},
		{
			name:       "场景32: PREPARE SELECT - AVG",
			query:      "SELECT AVG(type_int) FROM mysql_data_types_demo WHERE type_int > ?",
			args:       []any{0},
			comment:    "使用 AVG 聚合函数",
			usePrepare: true,
		},

		// ===== DROP 操作 =====
		{
			name:       "场景33: DROP TABLE",
			query:      "DROP TABLE IF EXISTS test_capture_table",
			args:       []any{},
			comment:    "删除测试表",
			usePrepare: false,
		},
	}

	// 运行测试场景
	for i, scenario := range testScenarios {
		fmt.Printf("【测试场景 %d: %s】\n", i+1, scenario.name)
		fmt.Printf("  说明: %s\n", scenario.comment)
		fmt.Printf("  查询: %s\n", scenario.query)

		if len(scenario.args) > 0 {
			fmt.Printf("  参数数量: %d\n", len(scenario.args))
			for j, arg := range scenario.args {
				fmt.Printf("    参数 %d: %v (%T)\n", j+1, arg, arg)
			}
		}

		// 执行查询
		fmt.Println("\n  → 执行查询...")

		var err error
		var result sql.Result
		var rows *sql.Rows

		// 判断是否应该使用预处理
		if scenario.usePrepare {
			// 使用预处理语句
			rows, err = db.Query(scenario.query, scenario.args...)
		} else {
			// 使用普通查询
			rows, err = db.Query(scenario.query, scenario.args...)
		}

		if err != nil {
			// 如果查询失败，尝试执行（针对 INSERT/UPDATE/DELETE）
			if scenario.usePrepare {
				result, err = db.Exec(scenario.query, scenario.args...)
			} else {
				result, err = db.Exec(scenario.query, scenario.args...)
			}

			if err != nil {
				log.Printf("  ❌ 执行失败: %v\n", err)
			} else {
				fmt.Println("  ✅ 执行成功")
				if result != nil {
					affected, _ := result.RowsAffected()
					fmt.Printf("  影响行数: %d\n", affected)
				}
			}
		} else {
			fmt.Println("  ✅ 查询成功")

			// 读取结果
			columns, _ := rows.Columns()
			fmt.Printf("  返回 %d 列: %v\n", len(columns), columns)

			// 读取最多 2 行
			rowCount := 0
			for rows.Next() {
				values := make([]any, len(columns))
				valuePtrs := make([]any, len(columns))
				for j := range values {
					valuePtrs[j] = &values[j]
				}
				rows.Scan(valuePtrs...)

				if rowCount < 2 {
					fmt.Printf("  行 %d: ", rowCount+1)
					for j, val := range values {
						if j > 0 {
							fmt.Printf(", ")
						}
						if j >= 5 && len(columns) > 5 {
							fmt.Printf("... (共 %d 列)", len(columns))
							break
						}
						fmt.Printf("%v", val)
					}
					fmt.Println()
				}
				rowCount++
			}
			if rowCount > 2 {
				fmt.Printf("  ... (共 %d 行)\n", rowCount)
			}
			rows.Close()
		}

		fmt.Println()
		time.Sleep(500 * time.Millisecond)
	}

	fmt.Println("═════════════════════════════════════════════════════════")
	fmt.Println("                 所有测试场景执行完成                      ")
	fmt.Println("═════════════════════════════════════════════════════════")
	fmt.Println()
	fmt.Println("💡 现在请检查 Wireshark 抓取的数据包")
	fmt.Println("   应该能看到:")
	fmt.Println("   - COM_QUIT (命令 0x01)")
	fmt.Println("   - COM_QUERY (命令 0x03)")
	fmt.Println("   - COM_STMT_PREPARE (命令 0x16)")
	fmt.Println("   - COM_STMT_EXECUTE (命令 0x17) ⭐")
	fmt.Println("   - COM_STMT_CLOSE (命令 0x19)")
	fmt.Println("   - 各种参数类型的包")
	fmt.Println()
	fmt.Println("   建议保存为: d:/code/db/mysql/resource/test_maria_db.pcapng")
}

// 初始化测试数据
func initTestData(db *sql.DB) {
	fmt.Println("正在初始化测试数据...")

	// 创建测试表
	createTableSQL := `
		CREATE TABLE IF NOT EXISTS test_capture_table (
			id INT PRIMARY KEY,
			name VARCHAR(100),
			value DECIMAL(10,2),
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
	`

	_, err := db.Exec(createTableSQL)
	if err != nil {
		log.Printf("警告: 创建测试表失败: %v\n", err)
	} else {
		fmt.Println("✅ 测试表已创建")
	}

	// 清空测试表
	db.Exec("DELETE FROM test_capture_table WHERE 1=1")

	fmt.Println()
}
