package main

import (
	"context"
	"fmt"
	"log"
	"mysql-proxy/mysql"
	"mysql-proxy/mysql/parser"
	"mysql-proxy/mysql/resource"
	"net"
	"os"
	"time"
)

func main() {
	log.Println("=== QueryBuilder集成测试 ===")

	// 创建内存数据源工厂
	factory := resource.NewMemoryFactory()
	config := &resource.DataSourceConfig{
		Type: resource.DataSourceTypeMemory,
		Name: "test",
	}

	// 创建内存数据源
	memoryDS, err := factory.Create(config)
	if err != nil {
		log.Fatalf("创建内存数据源失败: %v", err)
	}

	if err := memoryDS.Connect(context.Background()); err != nil {
		log.Fatalf("连接内存数据源失败: %v", err)
	}

	// 创建测试表
	testTable := &resource.TableInfo{
		Name: "users",
		Columns: []resource.ColumnInfo{
			{Name: "id", Type: "int", Primary: true, Nullable: false},
			{Name: "name", Type: "string", Nullable: false},
			{Name: "age", Type: "int", Nullable: false},
		},
	}
	if err := memoryDS.CreateTable(context.Background(), testTable); err != nil {
		log.Fatalf("创建表失败: %v", err)
	}

	// 插入测试数据
	testRows := []resource.Row{
		{"id": 1, "name": "Alice", "age": 25},
		{"id": 2, "name": "Bob", "age": 30},
		{"id": 3, "name": "Charlie", "age": 35},
	}
	if _, err := memoryDS.Insert(context.Background(), "users", testRows, &resource.InsertOptions{}); err != nil {
		log.Fatalf("插入数据失败: %v", err)
	}

	// 创建MySQL服务器
	server := mysql.NewServer()
	defer func() {
		if ds := server.GetDataSource(); ds != nil {
			ds.Close(context.Background())
		}
	}()

	// 设置数据源
	if err := server.SetDataSource(memoryDS); err != nil {
		log.Fatalf("设置数据源失败: %v", err)
	}

	// 启动服务器
	listener, err := net.Listen("tcp", "127.0.0.1:13306")
	if err != nil {
		log.Fatalf("监听失败: %v", err)
	}
	defer listener.Close()

	log.Println("MySQL服务器启动在 127.0.0.1:13306")
	log.Println("可以使用以下命令测试:")
	log.Println("  mysql -h 127.0.0.1 -P 13306 -u test -ptest")
	log.Println("\n测试SQL示例:")
	log.Println("  SELECT * FROM users;")
	log.Println("  SELECT * FROM users WHERE age > 25;")
	log.Println("  INSERT INTO users (id, name, age) VALUES (4, 'Dave', 40);")
	log.Println("  UPDATE users SET age = age + 1 WHERE id = 1;")
	log.Println("  DELETE FROM users WHERE age < 30;")

	// 等待连接
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				log.Printf("接受连接失败: %v", err)
				continue
			}
			log.Printf("接受新连接: %s", conn.RemoteAddr())

			go func(c net.Conn) {
				defer c.Close()
				if err := server.HandleConn(context.Background(), c); err != nil {
					log.Printf("处理连接失败: %v", err)
				}
			}(conn)
		}
	}()

	// 运行测试客户端
	log.Println("\n=== 运行自动测试 ===")
	time.Sleep(1 * time.Second)

	if err := runTests(server); err != nil {
		log.Fatalf("测试失败: %v", err)
	}

	log.Println("\n=== 所有测试通过! ===")

	// 等待用户输入以保持服务器运行
	log.Println("\n按 Ctrl+C 停止服务器...")
	select {}
}

func runTests(server *mysql.Server) error {
	ctx := context.Background()

	// 测试1: 查询所有数据
	log.Println("\n测试1: SELECT * FROM users")
	result, err := queryDataSource(server, ctx, "SELECT * FROM users")
	if err != nil {
		return fmt.Errorf("测试1失败: %w", err)
	}
	if result.Total != 3 {
		return fmt.Errorf("测试1失败: 期望3行，实际%d行", result.Total)
	}
	log.Printf("测试1通过: 查询到%d行数据", result.Total)

	// 测试2: 条件查询
	log.Println("\n测试2: SELECT * FROM users WHERE age > 25")
	result, err = queryDataSource(server, ctx, "SELECT * FROM users WHERE age > 25")
	if err != nil {
		return fmt.Errorf("测试2失败: %w", err)
	}
	if result.Total != 2 {
		return fmt.Errorf("测试2失败: 期望2行，实际%d行", result.Total)
	}
	log.Printf("测试2通过: 查询到%d行数据", result.Total)

	// 测试3: 排序
	log.Println("\n测试3: SELECT * FROM users ORDER BY age")
	result, err = queryDataSource(server, ctx, "SELECT * FROM users ORDER BY age")
	if err != nil {
		return fmt.Errorf("测试3失败: %w", err)
	}
	if result.Total != 3 {
		return fmt.Errorf("测试3失败: 期望3行，实际%d行", result.Total)
	}
	log.Printf("测试3通过: 查询到%d行数据", result.Total)

	// 测试4: LIMIT
	log.Println("\n测试4: SELECT * FROM users LIMIT 2")
	result, err = queryDataSource(server, ctx, "SELECT * FROM users LIMIT 2")
	if err != nil {
		return fmt.Errorf("测试4失败: %w", err)
	}
	if result.Total != 2 {
		return fmt.Errorf("测试4失败: 期望2行，实际%d行", result.Total)
	}
	log.Printf("测试4通过: 查询到%d行数据", result.Total)

	// 测试5: INSERT
	log.Println("\n测试5: INSERT INTO users (id, name, age) VALUES (4, 'Dave', 40)")
	_, err = queryDataSource(server, ctx, "INSERT INTO users (id, name, age) VALUES (4, 'Dave', 40)")
	if err != nil {
		return fmt.Errorf("测试5失败: %w", err)
	}

	// 验证插入
	result, err = queryDataSource(server, ctx, "SELECT * FROM users")
	if err != nil {
		return fmt.Errorf("测试5验证失败: %w", err)
	}
	if result.Total != 4 {
		return fmt.Errorf("测试5验证失败: 期望4行，实际%d行", result.Total)
	}
	log.Printf("测试5通过: 插入成功，当前共%d行", result.Total)

	// 测试6: UPDATE
	log.Println("\n测试6: UPDATE users SET age = 41 WHERE id = 4")
	_, err = queryDataSource(server, ctx, "UPDATE users SET age = 41 WHERE id = 4")
	if err != nil {
		return fmt.Errorf("测试6失败: %w", err)
	}

	// 验证更新
	result, err = queryDataSource(server, ctx, "SELECT * FROM users WHERE id = 4")
	if err != nil {
		return fmt.Errorf("测试6验证失败: %w", err)
	}
	if len(result.Rows) != 1 {
		return fmt.Errorf("测试6验证失败: 期望1行，实际%d行", len(result.Rows))
	}
	age, ok := result.Rows[0]["age"].(int64)
	if !ok || age != 41 {
		return fmt.Errorf("测试6验证失败: 期望age=41，实际%v", result.Rows[0]["age"])
	}
	log.Printf("测试6通过: 更新成功")

	// 测试7: DELETE
	log.Println("\n测试7: DELETE FROM users WHERE id = 4")
	_, err = queryDataSource(server, ctx, "DELETE FROM users WHERE id = 4")
	if err != nil {
		return fmt.Errorf("测试7失败: %w", err)
	}

	// 验证删除
	result, err = queryDataSource(server, ctx, "SELECT * FROM users")
	if err != nil {
		return fmt.Errorf("测试7验证失败: %w", err)
	}
	if result.Total != 3 {
		return fmt.Errorf("测试7验证失败: 期望3行，实际%d行", result.Total)
	}
	log.Printf("测试7通过: 删除成功，当前共%d行", result.Total)

	return nil
}

func queryDataSource(server *mysql.Server, ctx context.Context, sql string) (*resource.QueryResult, error) {
	ds := server.GetDataSource()
	if ds == nil {
		return nil, fmt.Errorf("未设置数据源")
	}

	// 使用QueryBuilder执行查询
	builder := parser.NewQueryBuilder(ds)
	return builder.BuildAndExecute(ctx, sql)
}

func init() {
	// 设置日志输出
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	// 如果测试失败，退出
	if testing := os.Getenv("TESTING"); testing == "1" {
		log.SetOutput(os.Stdout)
	}
}
