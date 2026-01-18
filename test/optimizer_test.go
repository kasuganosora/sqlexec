package test

import (
	"context"
	"testing"

	"mysql-proxy/mysql/optimizer"
	"mysql-proxy/mysql/parser"
	"mysql-proxy/mysql/resource"
)

// TestOptimizerSimple 测试简单优化器
func TestOptimizerSimple(t *testing.T) {
	dataSource, err := resource.NewMemoryFactory().Create(nil)
	if err != nil {
		t.Fatalf("创建数据源失败: %v", err)
	}

	if !dataSource.IsConnected() {
		if err := dataSource.Connect(context.Background()); err != nil {
			t.Fatalf("连接数据源失败: %v", err)
		}
	}
	defer dataSource.Close(context.Background())

	tableInfo := &resource.TableInfo{
		Name: "users",
		Columns: []resource.ColumnInfo{
			{Name: "id", Type: "int64", Primary: true, Nullable: false},
			{Name: "name", Type: "string", Nullable: false},
			{Name: "age", Type: "int64", Nullable: true},
			{Name: "city", Type: "string", Nullable: true},
		},
	}

	err = dataSource.CreateTable(context.Background(), tableInfo)
	if err != nil {
		t.Fatalf("创建表失败: %v", err)
	}

	rows := []resource.Row{
		{"id": int64(1), "name": "Alice", "age": int64(25), "city": "Beijing"},
		{"id": int64(2), "name": "Bob", "age": int64(30), "city": "Shanghai"},
		{"id": int64(3), "name": "Charlie", "age": int64(35), "city": "Guangzhou"},
		{"id": int64(4), "name": "David", "age": int64(28), "city": "Shenzhen"},
		{"id": int64(5), "name": "Eve", "age": int64(32), "city": "Beijing"},
	}

	_, err = dataSource.Insert(context.Background(), "users", rows, &resource.InsertOptions{Replace: false})
	if err != nil {
		t.Fatalf("插入数据失败: %v", err)
	}

	executor := optimizer.NewOptimizedExecutor(dataSource, true)

	// 测试基本查询
	result, err := executeSQL(t, executor, "SELECT * FROM users")
	if err != nil {
		t.Fatalf("查询失败: %v", err)
	}

	if len(result.Rows) != 5 {
		t.Errorf("期望5行, 实际 %d", len(result.Rows))
	}

	// 测试WHERE条件
	result, err = executeSQL(t, executor, "SELECT * FROM users WHERE age > 30")
	if err != nil {
		t.Fatalf("查询失败: %v", err)
	}

	if len(result.Rows) != 2 {
		t.Errorf("期望2行, 实际 %d", len(result.Rows))
	}

	// 测试LIMIT
	result, err = executeSQL(t, executor, "SELECT * FROM users LIMIT 2")
	if err != nil {
		t.Fatalf("查询失败: %v", err)
	}

	if len(result.Rows) != 2 {
		t.Errorf("期望2行, 实际 %d", len(result.Rows))
	}
}

// TestOptimizerIntegration 测试优化器集成
func TestOptimizerIntegration(t *testing.T) {
	dataSource, err := resource.NewMemoryFactory().Create(nil)
	if err != nil {
		t.Fatalf("创建数据源失败: %v", err)
	}

	if !dataSource.IsConnected() {
		if err := dataSource.Connect(context.Background()); err != nil {
			t.Fatalf("连接数据源失败: %v", err)
		}
	}
	defer dataSource.Close(context.Background())

	createTable(t, dataSource)
	insertTestData(t, dataSource)

	executor := optimizer.NewOptimizedExecutor(dataSource, true)

	t.Run("启用优化器", func(t *testing.T) {
		result, err := executeSQL(t, executor, "SELECT * FROM users WHERE age > 30")
		if err != nil {
			t.Fatalf("查询失败: %v", err)
		}

		if len(result.Rows) != 2 {
			t.Errorf("期望2行, 实际 %d", len(result.Rows))
		}
	})

	t.Run("禁用优化器", func(t *testing.T) {
		executor.SetUseOptimizer(false)
		result, err := executeSQL(t, executor, "SELECT name, city FROM users WHERE city = 'Beijing'")
		if err != nil {
			t.Fatalf("查询失败: %v", err)
		}

		if len(result.Rows) != 2 {
			t.Errorf("期望2行, 实际 %d", len(result.Rows))
		}
	})
}

// TestPredicatePushdown 测试谓词下推
func TestPredicatePushdown(t *testing.T) {
	dataSource, err := resource.NewMemoryFactory().Create(nil)
	if err != nil {
		t.Fatalf("创建数据源失败: %v", err)
	}

	if !dataSource.IsConnected() {
		if err := dataSource.Connect(context.Background()); err != nil {
			t.Fatalf("连接数据源失败: %v", err)
		}
	}
	defer dataSource.Close(context.Background())

	createTable(t, dataSource)
	insertTestData(t, dataSource)

	optCtx := &optimizer.OptimizationContext{
		DataSource: dataSource,
		TableInfo: map[string]*resource.TableInfo{
			"users": {
				Name: "users",
				Columns: []resource.ColumnInfo{
					{Name: "id", Type: "int64"},
					{Name: "name", Type: "string"},
					{Name: "age", Type: "int64"},
				},
			},
		},
		Stats: make(map[string]*optimizer.Statistics),
	}

	opt := optimizer.NewQueryOptimizer()

	filters := []resource.Filter{
		{Field: "age", Operator: ">", Value: int64(30)},
	}

	plan, err := opt.OptimizeSelect("users", filters, nil, nil, nil, optCtx)
	if err != nil {
		t.Fatalf("优化失败: %v", err)
	}

	if plan == nil {
		t.Error("优化计划不应该为空")
	}
}

// TestLimitPushdown 测试LIMIT下推
func TestLimitPushdown(t *testing.T) {
	dataSource, err := resource.NewMemoryFactory().Create(nil)
	if err != nil {
		t.Fatalf("创建数据源失败: %v", err)
	}

	if !dataSource.IsConnected() {
		if err := dataSource.Connect(context.Background()); err != nil {
			t.Fatalf("连接数据源失败: %v", err)
		}
	}
	defer dataSource.Close(context.Background())

	createTable(t, dataSource)
	insertTestData(t, dataSource)

	optCtx := &optimizer.OptimizationContext{
		DataSource: dataSource,
		TableInfo: map[string]*resource.TableInfo{
			"users": {
				Name: "users",
				Columns: []resource.ColumnInfo{
					{Name: "id", Type: "int64"},
					{Name: "name", Type: "string"},
				},
			},
		},
		Stats: make(map[string]*optimizer.Statistics),
	}

	opt := optimizer.NewQueryOptimizer()

	limit := int64(3)
	offset := int64(0)

	plan, err := opt.OptimizeSelect("users", nil, &limit, &offset, nil, optCtx)
	if err != nil {
		t.Fatalf("优化失败: %v", err)
	}

	if plan == nil {
		t.Error("优化计划不应该为空")
	}
}

// TestColumnPruning 测试列裁剪
func TestColumnPruning(t *testing.T) {
	dataSource, err := resource.NewMemoryFactory().Create(nil)
	if err != nil {
		t.Fatalf("创建数据源失败: %v", err)
	}

	if !dataSource.IsConnected() {
		if err := dataSource.Connect(context.Background()); err != nil {
			t.Fatalf("连接数据源失败: %v", err)
		}
	}
	defer dataSource.Close(context.Background())

	createTable(t, dataSource)
	insertTestData(t, dataSource)

	optCtx := &optimizer.OptimizationContext{
		DataSource: dataSource,
		TableInfo: map[string]*resource.TableInfo{
			"users": {
				Name: "users",
				Columns: []resource.ColumnInfo{
					{Name: "id", Type: "int64"},
					{Name: "name", Type: "string"},
					{Name: "age", Type: "int64"},
				},
			},
		},
		Stats: make(map[string]*optimizer.Statistics),
	}

	opt := optimizer.NewQueryOptimizer()

	selectedColumns := []string{"name"}

	plan, err := opt.OptimizeSelect("users", nil, nil, nil, selectedColumns, optCtx)
	if err != nil {
		t.Fatalf("优化失败: %v", err)
	}

	if plan == nil {
		t.Error("优化计划不应该为空")
	}
}

// TestIndexManager 测试索引管理器
func TestIndexManager(t *testing.T) {
	indexManager := optimizer.NewIndexManager()

	index := &optimizer.Index{
		Name:       "idx_age",
		TableName:  "users",
		Columns:    []string{"age"},
		Unique:      false,
		Cardinality: 50,
	}

	err := indexManager.AddIndex(index)
	if err != nil {
		t.Fatalf("添加索引失败: %v", err)
	}

	indices := indexManager.GetIndices("users")
	if len(indices) != 1 {
		t.Errorf("期望1个索引, 实际 %d", len(indices))
	}

	hasIndex := indexManager.HasIndex("users", []string{"age"})
	if !hasIndex {
		t.Error("应该存在age索引")
	}
}

// TestPerformanceOptimizer 测试性能优化器
func TestPerformanceOptimizer(t *testing.T) {
	perfOptimizer := optimizer.NewPerformanceOptimizer()

	if perfOptimizer == nil {
		t.Fatal("性能优化器不应该为空")
	}

	// 测试优化建议
	ctx := &optimizer.OptimizationContext{
		DataSource: nil,
		TableInfo: make(map[string]*resource.TableInfo),
		Stats: make(map[string]*optimizer.Statistics),
	}

	optimization := perfOptimizer.OptimizeScan("users", nil, ctx)
	if optimization == nil {
		t.Error("优化结果不应该为空")
	}
}

// Helper functions

func executeSQL(t *testing.T, executor *optimizer.OptimizedExecutor, sql string) (*resource.QueryResult, error) {
	t.Helper()

	adapter := parser.NewSQLAdapter()
	parseResult, err := adapter.Parse(sql)
	if err != nil {
		return nil, err
	}

	if !parseResult.Success {
		return nil, nil
	}

	if parseResult.Statement.Type != parser.SQLTypeSelect {
		return nil, nil
	}

	return executor.ExecuteSelect(context.Background(), parseResult.Statement.Select)
}

func createTable(t *testing.T, ds resource.DataSource) {
	t.Helper()

	tableInfo := &resource.TableInfo{
		Name: "users",
		Columns: []resource.ColumnInfo{
			{Name: "id", Type: "int64", Primary: true, Nullable: false},
			{Name: "name", Type: "string", Nullable: false},
			{Name: "age", Type: "int64", Nullable: true},
			{Name: "city", Type: "string", Nullable: true},
		},
	}

	err := ds.CreateTable(context.Background(), tableInfo)
	if err != nil {
		t.Fatalf("创建表失败: %v", err)
	}
}

func insertTestData(t *testing.T, ds resource.DataSource) {
	t.Helper()

	rows := []resource.Row{
		{"id": int64(1), "name": "Alice", "age": int64(25), "city": "Beijing"},
		{"id": int64(2), "name": "Bob", "age": int64(30), "city": "Shanghai"},
		{"id": int64(3), "name": "Charlie", "age": int64(35), "city": "Guangzhou"},
		{"id": int64(4), "name": "David", "age": int64(28), "city": "Shenzhen"},
		{"id": int64(5), "name": "Eve", "age": int64(32), "city": "Beijing"},
	}

	_, err := ds.Insert(context.Background(), "users", rows, &resource.InsertOptions{Replace: false})
	if err != nil {
		t.Fatalf("插入数据失败: %v", err)
	}
}
