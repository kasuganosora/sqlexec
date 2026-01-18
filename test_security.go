package main

import (
	"fmt"
	"log"
	"mysql-proxy/mysql/security"
	"time"
)

func main() {
	fmt.Println("=== 阶段7安全性测试 ===\n")

	testSQLInjection()
	testAuthorization()
	testEncryption()
	testAuditLog()

	fmt.Println("\n=== 所有安全性测试完成 ===")
}

func testSQLInjection() {
	fmt.Println("1. SQL注入防护测试")
	fmt.Println("-------------------------------")

	detector := security.NewSQLInjectionDetector()

	// 测试安全的SQL
	safeSQL := "SELECT * FROM users WHERE id = 1"
	result := detector.Detect(safeSQL)
	fmt.Printf("安全SQL检测: %v\n", !result.IsDetected)

	// 测试SQL注入
	injectSQL := "SELECT * FROM users WHERE id = 1 OR 1=1"
	result = detector.Detect(injectSQL)
	fmt.Printf("注入SQL检测: %v (严重程度: %s)\n", result.IsDetected, result.GetSeverity())

	// 测试UNION注入
	unionSQL := "SELECT * FROM users WHERE id = 1 UNION SELECT * FROM passwords"
	result = detector.Detect(unionSQL)
	fmt.Printf("UNION注入检测: %v\n", result.IsDetected)

	// 测试输入清理
	input := "admin'; DROP TABLE users;--"
	cleaned := detector.SanitizeInput(input)
	fmt.Printf("输入清理: '%s' -> '%s'\n", input, cleaned)

	// 测试检测并清理
	dirtySQL := "SELECT * FROM users WHERE name = 'admin' OR 1=1 --"
	result, sanitized := detector.DetectAndSanitize(dirtySQL)
	fmt.Printf("检测并清理: 检测=%v, 清理后='%s'\n", result.IsDetected, sanitized)

	fmt.Println()
}

func testAuthorization() {
	fmt.Println("2. 授权管理测试")
	fmt.Println("-------------------------------")

	auth := security.NewAuthorizationManager()

	// 创建用户
	err := auth.CreateUser("admin", security.HashPassword("admin123"), []security.Role{security.RoleAdmin})
	if err != nil {
		log.Fatal(err)
	}

	err = auth.CreateUser("user1", security.HashPassword("user123"), []security.Role{security.RoleUser})
	if err != nil {
		log.Fatal(err)
	}

	err = auth.CreateUser("guest", security.HashPassword("guest123"), []security.Role{security.RoleGuest})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("用户创建成功")

	// 测试权限检查
	hasPerm := auth.HasPermission("admin", security.PermissionDelete, "users")
	fmt.Printf("管理员删除权限: %v\n", hasPerm)

	hasPerm = auth.HasPermission("user1", security.PermissionDelete, "users")
	fmt.Printf("普通用户删除权限: %v\n", hasPerm)

	hasPerm = auth.HasPermission("user1", security.PermissionRead, "users")
	fmt.Printf("普通用户读取权限: %v\n", hasPerm)

	hasPerm = auth.HasPermission("guest", security.PermissionRead, "users")
	fmt.Printf("访客读取权限: %v\n", hasPerm)

	// 授予权限
	err = auth.GrantPermission("user1", security.PermissionDelete, "logs")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("已授予user1删除logs表权限")

	hasPerm = auth.HasPermission("user1", security.PermissionDelete, "logs")
	fmt.Printf("user1删除logs表权限: %v\n", hasPerm)

	// 撤销权限
	err = auth.RevokePermission("user1", security.PermissionDelete, "logs")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("已撤销user1删除logs表权限")

	// 列出用户
	users := auth.ListUsers()
	fmt.Printf("用户列表: %v\n", users)

	// 停用用户
	err = auth.DeactivateUser("guest")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("已停用guest用户")

	hasPerm = auth.HasPermission("guest", security.PermissionRead, "users")
	fmt.Printf("停用后访客读取权限: %v\n", hasPerm)

	fmt.Println()
}

func testEncryption() {
	fmt.Println("3. 数据加密测试")
	fmt.Println("-------------------------------")

	// 创建加密器
	encryptor, err := security.NewEncryptor("my-secret-key-12345")
	if err != nil {
		log.Fatal(err)
	}

	// 测试加密解密
	plaintext := "This is a secret message"
	encrypted, err := encryptor.Encrypt(plaintext)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("原文: %s\n", plaintext)
	fmt.Printf("密文: %s\n", encrypted)

	decrypted, err := encryptor.Decrypt(encrypted)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("解密: %s\n", decrypted)
	fmt.Printf("解密正确: %v\n", plaintext == decrypted)

	// 测试敏感字段管理
	sensitiveFields := []string{
		"users.password",
		"users.email",
		"users.phone",
	}

	manager, err := security.NewSensitiveFieldsManager("encryption-key-12345", sensitiveFields)
	if err != nil {
		log.Fatal(err)
	}

	// 测试字段加密
	record := map[string]interface{}{
		"username": "admin",
		"password": "secret123",
		"email":    "admin@example.com",
		"phone":    "1234567890",
	}

	encryptedRecord, err := manager.EncryptRecord("users", record)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("加密后记录: password=%s, email=%s, phone=%s\n",
		encryptedRecord["password"], encryptedRecord["email"], encryptedRecord["phone"])

	// 测试字段解密
	decryptedRecord, err := manager.DecryptRecord("users", encryptedRecord)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("解密后记录: password=%s, email=%s, phone=%s\n",
		decryptedRecord["password"], decryptedRecord["email"], decryptedRecord["phone"])

	// 测试密码哈希
	password := "mypassword123"
	hashed := security.HashPassword(password)
	fmt.Printf("密码哈希: %s\n", hashed)
	fmt.Printf("密码验证: %v\n", security.VerifyPassword(password, hashed))
	fmt.Printf("错误密码验证: %v\n", security.VerifyPassword("wrongpassword", hashed))

	fmt.Println()
}

func testAuditLog() {
	fmt.Println("4. 审计日志测试")
	fmt.Println("-------------------------------")

	// 创建审计日志记录器
	audit := security.NewAuditLogger(100)

	// 记录登录
	audit.LogLogin("admin", "192.168.1.1", true)
	audit.LogLogin("user1", "192.168.1.2", false)

	// 记录查询
	start := time.Now()
	time.Sleep(10 * time.Millisecond)
	duration := time.Since(start).Milliseconds()
	audit.LogQuery("admin", "mydb", "SELECT * FROM users", duration, true)

	// 记录插入操作
	start = time.Now()
	time.Sleep(5 * time.Millisecond)
	duration = time.Since(start).Milliseconds()
	audit.LogInsert("admin", "mydb", "logs", "INSERT INTO logs (message) VALUES ('test')", duration, true)

	// 记录更新操作
	start = time.Now()
	time.Sleep(3 * time.Millisecond)
	duration = time.Since(start).Milliseconds()
	audit.LogUpdate("admin", "mydb", "users", "UPDATE users SET status = 1", duration, true)

	// 记录删除操作
	start = time.Now()
	time.Sleep(2 * time.Millisecond)
	duration = time.Since(start).Milliseconds()
	audit.LogDelete("admin", "mydb", "logs", "DELETE FROM logs WHERE id = 1", duration, true)

	// 记录DDL操作
	start = time.Now()
	time.Sleep(15 * time.Millisecond)
	duration = time.Since(start).Milliseconds()
	audit.LogDDL("admin", "mydb", "CREATE TABLE test (id INT)", duration, true)

	// 记录权限变更
	audit.LogPermission("admin", "grant", map[string]interface{}{
		"user":  "user1",
		"table": "logs",
		"perm":  "read",
	})

	// 记录SQL注入尝试
	audit.LogInjection("unknown", "192.168.1.100", "SELECT * FROM users WHERE id = 1 OR 1=1")

	// 记录错误
	audit.LogError("user1", "mydb", "Query execution failed", fmt.Errorf("connection timeout"))

	// 获取事件
	events := audit.GetEvents(0, 10)
	fmt.Printf("最近事件数量: %d\n", len(events))

	// 获取用户事件
	userEvents := audit.GetEventsByUser("admin")
	fmt.Printf("admin事件数量: %d\n", len(userEvents))

	// 获取注入事件
	injectionEvents := audit.GetEventsByType(security.EventTypeInjection)
	fmt.Printf("SQL注入事件数量: %d\n", len(injectionEvents))

	// 获取错误事件
	errorEvents := audit.GetEventsByLevel(security.AuditLevelError)
	fmt.Printf("错误事件数量: %d\n", len(errorEvents))

	// 获取时间范围内的事件
	now := time.Now()
	past := now.Add(-1 * time.Hour)
	timeEvents := audit.GetEventsByTimeRange(past, now)
	fmt.Printf("最近1小时事件数量: %d\n", len(timeEvents))

	fmt.Println()
}
