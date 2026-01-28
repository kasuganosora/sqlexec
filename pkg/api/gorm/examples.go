package gorm

import (
	"fmt"
	"log"

	"github.com/kasuganosora/sqlexec/pkg/api"
	"github.com/kasuganosora/sqlexec/pkg/resource/csv"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/resource/json"
	"github.com/kasuganosora/sqlexec/pkg/resource/memory"
	"github.com/kasuganosora/sqlexec/pkg/resource/slice"
	"gorm.io/gorm"
)

// ExampleBasicUsage 基本 GORM 使用示例
func ExampleBasicUsage() {
	// 1. 创建 sqlexec 数据库
	db, err := api.NewDB(&api.DBConfig{
		DebugMode: true,
	})
	if err != nil {
		log.Fatal(err)
	}

	// 2. 注册内存数据源
	config := &domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Name:     "default",
		Writable: true,
	}
	memoryDS := memory.NewMVCCDataSource(config)
	err = db.RegisterDataSource("default", memoryDS)
	if err != nil {
		log.Fatal(err)
	}
	
	// 3. 创建 sqlexec 会话
	session := db.Session()
	
	// 4. 创建 GORM 驱动
	dialector := NewDialector(session)
	
	// 5. 创建 GORM DB
	gormDB, err := gorm.Open(dialector, &gorm.Config{})
	if err != nil {
		log.Fatal(err)
	}
	
	fmt.Println("GORM DB with sqlexec backend created successfully!")
	
	// 6. 使用 GORM
	type User struct {
		ID   uint
		Name  string
		Email string
	}
	
	// 创建记录
	user := User{Name: "John", Email: "john@example.com"}
	result := gormDB.Create(&user)
	if result.Error != nil {
		log.Fatal(result.Error)
	}
	
	fmt.Printf("User created with ID: %d\n", user.ID)
	
	// 查询记录
	var users []User
	gormDB.Find(&users)
	fmt.Printf("Found %d users\n", len(users))
	
	// 更新记录
	gormDB.Model(&User{}).Where("id = ?", user.ID).Update("name", "Jane")
	
	// 删除记录
	gormDB.Delete(&User{}, user.ID)
	
	// 关闭 GORM DB
	sqlDB, _ := gormDB.DB()
	if sqlDB != nil {
		sqlDB.Close()
	}
}

// ExampleQueryWithConditions 条件查询示例
func ExampleQueryWithConditions() {
	gormDB, _ := createGormDB()
	
	type User struct {
		ID   uint
		Name  string
		Age   int
	}
	
	// WHERE 条件
	var users []User
	gormDB.Where("age > ?", 18).Find(&users)
	fmt.Printf("Users with age > 18: %d\n", len(users))
	
	// 多个条件
	gormDB.Where("name = ? AND age > ?", "John", 25).Find(&users)
	
	// OR 条件
	gormDB.Where("name = ? OR age > ?", "John", 25).Find(&users)
	
	// IN 条件
	gormDB.Where("id IN ?", []int{1, 2, 3}).Find(&users)
	
	// LIKE 条件
	gormDB.Where("name LIKE ?", "%John%").Find(&users)
	
	// NULL 检查
	gormDB.Where("email IS NULL").Find(&users)
}

// ExamplePagination 分页查询示例
func ExamplePagination() {
	gormDB, _ := createGormDB()
	
	type User struct {
		ID   uint
		Name  string
	}
	
	// 分页查询
	page := 1
	pageSize := 10
	offset := (page - 1) * pageSize
	
	var users []User
	gormDB.Offset(offset).Limit(pageSize).Find(&users)
	
	fmt.Printf("Page %d: %d users\n", page, len(users))
}

// ExampleSorting 排序查询示例
func ExampleSorting() {
	gormDB, _ := createGormDB()
	
	type User struct {
		ID    uint
		Name   string
		Age    int
		Score  float64
	}
	
	var users []User
	
	// 单列排序
	gormDB.Order("age DESC").Find(&users)
	
	// 多列排序
	gormDB.Order("age DESC, name ASC").Find(&users)
	
	// 先按分数降序，再按年龄升序
	gormDB.Order("score DESC").Order("age ASC").Find(&users)
}

// ExampleTransaction 事务示例
func ExampleTransaction() {
	gormDB, _ := createGormDB()
	
	type Account struct {
		ID      uint
		Balance float64
	}
	
	// 开始事务
	tx := gormDB.Begin()
	
	// 创建账户1
	account1 := Account{Balance: 1000.00}
	if err := tx.Create(&account1).Error; err != nil {
		tx.Rollback()
		log.Fatal(err)
	}
	
	// 创建账户2
	account2 := Account{Balance: 500.00}
	if err := tx.Create(&account2).Error; err != nil {
		tx.Rollback()
		log.Fatal(err)
	}
	
	// 转账：账户1减少100，账户2增加100
	if err := tx.Model(&Account{}).Where("id = ?", account1.ID).Update("balance", account1.Balance-100).Error; err != nil {
		tx.Rollback()
		log.Fatal(err)
	}
	
	if err := tx.Model(&Account{}).Where("id = ?", account2.ID).Update("balance", account2.Balance+100).Error; err != nil {
		tx.Rollback()
		log.Fatal(err)
	}
	
	// 提交事务
	if err := tx.Commit().Error; err != nil {
		log.Fatal(err)
	}
	
	fmt.Println("Transaction completed successfully")
}

// ExampleRawSQL 原生 SQL 查询示例
func ExampleRawSQL() {
	gormDB, _ := createGormDB()
	
	// 使用 Exec 执行原生 SQL
	result := gormDB.Exec("CREATE TABLE IF NOT EXISTS users (id INT PRIMARY KEY, name VARCHAR(255))")
	if result.Error != nil {
		log.Fatal(result.Error)
	}
	
	// 使用 Raw 执行查询
	type User struct {
		ID   uint
		Name string
	}
	
	var users []User
	gormDB.Raw("SELECT * FROM users WHERE age > ?", 18).Scan(&users)
	fmt.Printf("Found %d users with raw SQL\n", len(users))
	
	// 使用 Scan 将结果扫描到 map
	var results []map[string]interface{}
	gormDB.Raw("SELECT * FROM users WHERE age > ?", 18).Scan(&results)
	
	for _, row := range results {
		fmt.Printf("Row: %v\n", row)
	}
}

// ExampleMixedUsage 混合使用 GORM 和 sqlexec API 示例
func ExampleMixedUsage() {
	// 创建 sqlexec 数据库
	db, err := api.NewDB(nil)
	if err != nil {
		log.Fatal(err)
	}

	// 注册内存数据源
	config := &domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Name:     "default",
		Writable: true,
	}
	memoryDS := memory.NewMVCCDataSource(config)
	err = db.RegisterDataSource("default", memoryDS)
	if err != nil {
		log.Fatal(err)
	}

	session := db.Session()
	
	// 创建 GORM DB
	gormDB, _ := gorm.Open(NewDialector(session), &gorm.Config{})
	
	// 1. 使用 GORM 处理简单的 CRUD
	type User struct {
		ID   uint
		Name  string
		Age   int
	}
	
	var users []User
	gormDB.Find(&users)
	fmt.Printf("Found %d users via GORM\n", len(users))
	
	// 2. 使用 sqlexec Session 处理复杂查询
	query, err := session.Query("SELECT u.*, o.order_date FROM users u JOIN orders o ON u.id = o.user_id WHERE u.age > ? ORDER BY u.created_at DESC LIMIT 10", 18)
	if err != nil {
		log.Fatal(err)
	}
	
	fmt.Println("Complex query via sqlexec:")
	for query.Next() {
		row := query.Row()
		fmt.Printf("Row: %v\n", row)
	}
	query.Close()
	
	// 3. 在同一个应用中，可以同时使用两种方式
	// - 简单的 CRUD 操作使用 GORM
	// - 复杂的查询和报表使用 sqlexec
}

// ExampleErrorHandling 错误处理示例
func ExampleErrorHandling() {
	gormDB, _ := createGormDB()
	
	type User struct {
		ID   uint
		Name  string
	}
	
	// 处理创建错误
	user := User{Name: "John"}
	result := gormDB.Create(&user)
	if result.Error != nil {
		fmt.Printf("Create failed: %v\n", result.Error)
		// result.Error 包含了 sqlexec 的错误信息
	} else {
		fmt.Printf("User created with ID: %d\n", user.ID)
	}
	
	// 处理查询错误
	var users []User
	result = gormDB.Where("id = ?", 999).Find(&users)
	if result.Error != nil {
		fmt.Printf("Query failed: %v\n", result.Error)
	} else {
		fmt.Printf("Query successful, found %d users\n", len(users))
	}
}

// ExampleBatchOperations 批量操作示例
func ExampleBatchOperations() {
	gormDB, _ := createGormDB()
	
	type User struct {
		ID   uint
		Name  string
	}
	
	// 批量创建
	users := []User{
		{Name: "User1"},
		{Name: "User2"},
		{Name: "User3"},
		{Name: "User4"},
		{Name: "User5"},
	}
	
	if err := gormDB.Create(&users).Error; err != nil {
		log.Fatal(err)
	}
	
	fmt.Printf("Created %d users in batch\n", len(users))
	
	// 批量更新
	if err := gormDB.Model(&User{}).Where("id IN ?", []int{1, 2, 3}).Updates(map[string]interface{}{"age": 30}).Error; err != nil {
		log.Fatal(err)
	}
	
	// 批量删除
	if err := gormDB.Where("id IN ?", []int{4, 5}).Delete(&User{}).Error; err != nil {
		log.Fatal(err)
	}
}

// ExampleAssociations 关联查询示例（简单版）
func ExampleAssociations() {
	gormDB, _ := createGormDB()

	type Order struct {
		ID      uint
		UserID  uint
		Amount  float64
	}

	type User struct {
		ID     uint
		Name   string
		Orders []Order
	}

	// 查询用户及其订单
	var users []User
	gormDB.Preload("Orders").Find(&users)

	for _, user := range users {
		fmt.Printf("User %s has %d orders\n", user.Name, len(user.Orders))
	}

	// 注意：完整的关联需要手动实现 JOIN 查询
	// 这里只是演示 Preload 的基本概念
}

// ExampleAggregation 聚合查询示例
func ExampleAggregation() {
	gormDB, _ := createGormDB()
	
	type User struct {
		ID   uint
		Name  string
		Age   int
	}
	
	var count int64
	gormDB.Model(&User{}).Count(&count)
	fmt.Printf("Total users: %d\n", count)
	
	var avgAge float64
	gormDB.Model(&User{}).Select("AVG(age)").Scan(&avgAge)
	fmt.Printf("Average age: %.2f\n", avgAge)
	
	var totalAge int64
	gormDB.Model(&User{}).Select("SUM(age)").Scan(&totalAge)
	fmt.Printf("Total age: %d\n", totalAge)
	
	// GROUP BY
	type AgeGroup struct {
		Age     int
		UserCount int
	}
	
	var ageGroups []AgeGroup
	gormDB.Model(&User{}).Select("age, COUNT(*) as user_count").Group("age").Scan(&ageGroups)
	
	for _, group := range ageGroups {
		fmt.Printf("Age %d: %d users\n", group.Age, group.UserCount)
	}
}

// createGormDB 辅助函数：创建 GORM DB
func createGormDB() (*gorm.DB, error) {
	// 创建 sqlexec 数据库
	db, err := api.NewDB(&api.DBConfig{
		DebugMode: false,
	})
	if err != nil {
		return nil, err
	}

	// 注册内存数据源
	config := &domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Name:     "default",
		Writable: true,
	}
	memoryDS := memory.NewMVCCDataSource(config)
	err1 := db.RegisterDataSource("default", memoryDS)
	if err1 != nil {
		log.Fatal(err1)
	}

	// 创建 sqlexec 会话
	session := db.Session()

	// 创建 GORM 驱动
	dialector := NewDialector(session)

	// 创建 GORM DB
	gormDB, err2 := gorm.Open(dialector, &gorm.Config{})
	if err2 != nil {
		log.Fatal(err2)
	}

	return gormDB, nil
}

// ExampleAdvancedFunctions 高级功能示例
func ExampleAdvancedFunctions() {
	gormDB, _ := createGormDB()
	
	type User struct {
		ID   uint
		Name  string
	}
	
	// Select 特定字段
	var users []User
	gormDB.Select("id", "name").Find(&users)
	
	// Distinct
	var names []string
	gormDB.Model(&User{}).Distinct("name").Pluck("name", &names)
	fmt.Printf("Unique names: %v\n", names)
	
	// HAVING 子句
	var ageCounts []struct {
		Age     int
		UserCount int
	}
	gormDB.Model(&User{}).Select("age, COUNT(*) as user_count").Group("age").Having("COUNT(*) > ?", 1).Scan(&ageCounts)
	
	for _, ac := range ageCounts {
		fmt.Printf("Age %d: %d users (count > 1)\n", ac.Age, ac.UserCount)
	}
}

// ExampleCountAndExistence 计数和存在性检查示例
func ExampleCountAndExistence() {
	gormDB, _ := createGormDB()

	type User struct {
		ID   uint
		Name  string
	}

	// 计数
	var count int64
	gormDB.Model(&User{}).Count(&count)
	fmt.Printf("Total users: %d\n", count)

	// 条件计数
	var ageCount int64
	gormDB.Model(&User{}).Where("age > ?", 18).Count(&ageCount)
	fmt.Printf("Users older than 18: %d\n", ageCount)

	// 检查记录是否存在
	var exists int64
	gormDB.Model(&User{}).Where("name = ?", "John").Count(&exists)
	if exists > 0 {
		fmt.Println("User 'John' exists")
	} else {
		fmt.Println("User 'John' does not exist")
	}

	// First 查询
	var user User
	result := gormDB.First(&user)
	if result.Error != nil {
		fmt.Printf("First query failed: %v\n", result.Error)
	} else {
		fmt.Printf("First user: %s\n", user.Name)
	}

	// Take 查询
	result = gormDB.Take(&user)
	if result.Error != nil {
		fmt.Printf("Take query failed: %v\n", result.Error)
	}

	// Last 查询
	result = gormDB.Last(&user)
	if result.Error != nil {
		fmt.Printf("Last query failed: %v\n", result.Error)
	}

	// Find 查询
	var users []User
	result = gormDB.Find(&users)
	if result.Error != nil {
		fmt.Printf("Find query failed: %v\n", result.Error)
	}
}

// ExampleAutoMigrate 自动迁移示例
func ExampleAutoMigrate() {
	// 创建 sqlexec 数据库
	db, err := api.NewDB(&api.DBConfig{
		DebugMode: true,
	})
	if err != nil {
		log.Fatal(err)
	}

	// 注册内存数据源
	config := &domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Name:     "default",
		Writable: true,
	}
	memoryDS := memory.NewMVCCDataSource(config)
	err = db.RegisterDataSource("default", memoryDS)
	if err != nil {
		log.Fatal(err)
	}

	// 创建会话
	session := db.Session()

	// 创建 GORM DB
	gormDB, err := gorm.Open(NewDialector(session), &gorm.Config{})
	if err != nil {
		log.Fatal(err)
	}

	// 定义模型
	type User struct {
		ID        uint
		Name      string
		Email     string
		Age       int
		CreatedAt string
	}

	type Product struct {
		ID          uint
		Name        string
		Price       float64
		Description string
	}

	// 自动迁移 - 创建表（如果不存在）
	err = gormDB.AutoMigrate(&User{}, &Product{})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("AutoMigrate completed successfully!")

	// 再次迁移 - 添加新列
	type UserWithAddress struct {
		ID      uint
		Name    string
		Address string // 新字段
	}

	err = gormDB.AutoMigrate(&UserWithAddress{})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("AutoMigrate with new column completed successfully!")
}

// ExampleMemoryDataSource 使用内存数据源的示例
func ExampleMemoryDataSource() {
	// 创建 sqlexec 数据库
	db, err := api.NewDB(&api.DBConfig{
		DebugMode: true,
	})
	if err != nil {
		log.Fatal(err)
	}

	// 创建内存数据源配置
	config := &domain.DataSourceConfig{
		Type:    domain.DataSourceTypeMemory,
		Name:    "memory",
		Writable: true,
	}

	// 创建并注册内存数据源
	memoryDS := memory.NewMVCCDataSource(config)
	err1 := db.RegisterDataSource("memory", memoryDS)
	if err1 != nil {
		log.Fatal(err1)
	}

	// 创建会话
	session := db.Session()

	// 创建 GORM DB
	gormDB, err2 := gorm.Open(NewDialector(session), &gorm.Config{})
	if err2 != nil {
		log.Fatal(err2)
	}

	// 使用 GORM 操作内存数据
	type Product struct {
		ID    uint
		Name  string
		Price float64
	}

	product := Product{Name: "Laptop", Price: 999.99}
	gormDB.Create(&product)

	var products []Product
	gormDB.Find(&products)
	fmt.Printf("Found %d products in memory\n", len(products))
}

// ExampleCSVDataSource 使用CSV数据源的示例
func ExampleCSVDataSource() {
	// 创建 sqlexec 数据库
	db, err1 := api.NewDB(&api.DBConfig{
		DebugMode: true,
	})
	if err1 != nil {
		log.Fatal(err1)
	}

	// 创建CSV数据源配置
	config := &domain.DataSourceConfig{
		Type:    domain.DataSourceTypeCSV,
		Name:    "csv",
		Writable: false, // CSV默认只读
		Options: map[string]interface{}{
			"delimiter": ",",
			"header":    true,
		},
	}

	// 创建并注册CSV数据源（指向CSV文件路径）
	csvDS := csv.NewCSVAdapter(config, "data/products.csv")
	err2 := db.RegisterDataSource("csv", csvDS)
	if err2 != nil {
		log.Fatal(err2)
	}

	// 创建会话
	session := db.Session()

	// 创建 GORM DB
	gormDB, err3 := gorm.Open(NewDialector(session), &gorm.Config{})
	if err3 != nil {
		log.Fatal(err3)
	}

	// 使用 GORM 查询 CSV 数据
	type Product struct {
		ID    uint
		Name  string
		Price float64
	}

	var products []Product
	gormDB.Find(&products)
	fmt.Printf("Found %d products from CSV file\n", len(products))
}

// ExampleJSONDataSource 使用JSON数据源的示例
func ExampleJSONDataSource() {
	// 创建 sqlexec 数据库
	db, err := api.NewDB(&api.DBConfig{
		DebugMode: true,
	})
	if err != nil {
		log.Fatal(err)
	}

	// 创建JSON数据源配置
	config := &domain.DataSourceConfig{
		Type:    domain.DataSourceTypeJSON,
		Name:    "json",
		Writable: false, // JSON默认只读
		Options: map[string]interface{}{
			"array_root": "", // 空字符串表示根是数组
		},
	}

	// 创建并注册JSON数据源（指向JSON文件路径）
	jsonDS := json.NewJSONAdapter(config, "data/users.json")
	err2 := db.RegisterDataSource("json", jsonDS)
	if err2 != nil {
		log.Fatal(err2)
	}

	// 创建会话
	session := db.Session()

	// 创建 GORM DB
	gormDB, err3 := gorm.Open(NewDialector(session), &gorm.Config{})
	if err3 != nil {
		log.Fatal(err3)
	}

	// 使用 GORM 查询 JSON 数据
	type User struct {
		ID    uint
		Name  string
		Email string
	}

	var users []User
	gormDB.Find(&users)
	fmt.Printf("Found %d users from JSON file\n", len(users))
}

// ExampleSliceDataSource 使用Slice数据源的示例
func ExampleSliceDataSource() {
	// 创建 sqlexec 数据库
	db, err3 := api.NewDB(&api.DBConfig{
		DebugMode: true,
	})
	if err3 != nil {
		log.Fatal(err3)
	}

	// 准备数据
	data := []map[string]interface{}{
		{"id": 1, "name": "Alice", "age": 25},
		{"id": 2, "name": "Bob", "age": 30},
		{"id": 3, "name": "Charlie", "age": 28},
	}

	// 创建Slice数据源配置
	config := &domain.DataSourceConfig{
		Type:    "slice",
		Name:    "slice",
		Writable: false,
		Options: map[string]interface{}{
			"data":           data,
			"table_name":     "people",
			"database_name":  "default",
			"mvcc_supported": false,
		},
	}

	// 使用工厂创建Slice数据源
	sliceFactory := slice.NewFactory()
	sliceDS, err4 := sliceFactory.Create(config)
	if err4 != nil {
		log.Fatal(err4)
	}

	// 注册数据源
	err5 := db.RegisterDataSource("slice", sliceDS)
	if err5 != nil {
		log.Fatal(err5)
	}

	// 创建会话
	session := db.Session()

	// 创建 GORM DB
	gormDB, err6 := gorm.Open(NewDialector(session), &gorm.Config{})
	if err6 != nil {
		log.Fatal(err6)
	}

	// 使用 GORM 查询 Slice 数据
	type Person struct {
		ID   uint
		Name string
		Age  int
	}

	var people []Person
	gormDB.Find(&people)
	fmt.Printf("Found %d people from slice data\n", len(people))
}
