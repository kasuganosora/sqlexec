package application

import (
	"context"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// TestNewDataSourceManager 测试创建数据源管理器
func TestNewDataSourceManager(t *testing.T) {
	manager := NewDataSourceManager()

	if manager == nil {
		t.Errorf("NewDataSourceManager() returned nil")
	}

	if manager.sources == nil {
		t.Errorf("Expected sources map to be initialized")
	}

	if manager.registry == nil {
		t.Errorf("Expected registry to be initialized")
	}
}

// TestNewDataSourceManagerWithRegistry 测试使用注册表创建管理器
func TestNewDataSourceManagerWithRegistry(t *testing.T) {
	registry := NewRegistry()
	manager := NewDataSourceManagerWithRegistry(registry)

	if manager == nil {
		t.Errorf("NewDataSourceManagerWithRegistry() returned nil")
	}

	if manager.registry != registry {
		t.Errorf("Expected manager to use provided registry")
	}
}

// TestDataSourceManager_SetEnabledTypes 测试设置启用类型
func TestDataSourceManager_SetEnabledTypes(t *testing.T) {
	manager := NewDataSourceManager()

	types := []domain.DataSourceType{"memory", "memory"}
	manager.SetEnabledTypes(types)

	// 验证类型已设置
	if !manager.IsTypeEnabled("memory") {
		t.Errorf("Expected 'memory' type to be enabled")
	}

	if !manager.IsTypeEnabled("memory") {
		t.Errorf("Expected 'memory' type to be enabled")
	}

	if manager.IsTypeEnabled("parquet") {
		t.Errorf("Expected 'parquet' type to be disabled")
	}
}

// TestDataSourceManager_IsTypeEnabled 测试检查类型是否启用
func TestDataSourceManager_IsTypeEnabled(t *testing.T) {
	manager := NewDataSourceManager()

	// 默认情况下，所有类型应该启用
	if !manager.IsTypeEnabled("memory") {
		t.Errorf("Expected all types to be enabled by default")
	}

	// 设置启用类型
	manager.SetEnabledTypes([]domain.DataSourceType{"memory"})

	// 验证只启用的类型
	if !manager.IsTypeEnabled("memory") {
		t.Errorf("Expected 'memory' type to be enabled")
	}

	// 验证未启用的类型
	if manager.IsTypeEnabled("csv") {
		t.Errorf("Expected 'csv' type to be disabled")
	}
}

// TestDataSourceManager_Register 测试注册数据源
func TestDataSourceManager_Register(t *testing.T) {
	manager := NewDataSourceManager()

	ds := &MockDataSource{connected: false}

	// 注册数据源
	err := manager.Register("test-ds", ds)
	if err != nil {
		t.Errorf("Register() error = %v", err)
	}

	// 验证默认数据源已设置
	if manager.defaultDS != "test-ds" {
		t.Errorf("Expected default data source to be set")
	}

	// 尝试重复注册
	err = manager.Register("test-ds", ds)
	if err == nil {
		t.Errorf("Expected error when registering duplicate data source")
	}
}

// TestDataSourceManager_Unregister 测试注销数据源
func TestDataSourceManager_Unregister(t *testing.T) {
	manager := NewDataSourceManager()

	ds := &MockDataSource{connected: false}

	// 注册数据源
	if err := manager.Register("test-ds", ds); err != nil {
		t.Errorf("Register() error = %v", err)
	}

	// 注销数据源
	err := manager.Unregister("test-ds")
	if err != nil {
		t.Errorf("Unregister() error = %v", err)
	}

	// 尝试注销不存在的数据源
	err = manager.Unregister("nonexistent")
	if err == nil {
		t.Errorf("Expected error when unregistering nonexistent data source")
	}
}

// TestDataSourceManager_Get 测试获取数据源
func TestDataSourceManager_Get(t *testing.T) {
	manager := NewDataSourceManager()

	ds := &MockDataSource{connected: false}

	// 注册数据源
	if err := manager.Register("test-ds", ds); err != nil {
		t.Errorf("Register() error = %v", err)
	}

	// 获取数据源
	got, err := manager.Get("test-ds")
	if err != nil {
		t.Errorf("Get() error = %v", err)
	}

	if got == nil {
		t.Errorf("Expected datasource to be returned")
	}

	// 获取不存在的数据源
	_, err = manager.Get("nonexistent")
	if err == nil {
		t.Errorf("Expected error when getting nonexistent data source")
	}
}

// TestDataSourceManager_GetDefault 测试获取默认数据源
func TestDataSourceManager_GetDefault(t *testing.T) {
	manager := NewDataSourceManager()

	// 没有设置默认数据源
	_, err := manager.GetDefault()
	if err == nil {
		t.Errorf("Expected error when no default data source set")
	}

	// 注册数据源（会自动设为默认）
	ds := &MockDataSource{connected: false}
	if err := manager.Register("default-ds", ds); err != nil {
		t.Errorf("Register() error = %v", err)
	}

	// 获取默认数据源
	defaultDS, err := manager.GetDefault()
	if err != nil {
		t.Errorf("GetDefault() error = %v", err)
	}

	if defaultDS == nil {
		t.Errorf("Expected default datasource to be returned")
	}
}

// TestDataSourceManager_SetDefault 测试设置默认数据源
func TestDataSourceManager_SetDefault(t *testing.T) {
	manager := NewDataSourceManager()

	ds1 := &MockDataSource{connected: false}
	ds2 := &MockDataSource{connected: false}

	// 注册两个数据源
	if err := manager.Register("ds1", ds1); err != nil {
		t.Errorf("Register() error = %v", err)
	}
	if err := manager.Register("ds2", ds2); err != nil {
		t.Errorf("Register() error = %v", err)
	}

	// 设置默认数据源
	err := manager.SetDefault("ds2")
	if err != nil {
		t.Errorf("SetDefault() error = %v", err)
	}

	// 验证默认数据源已更新
	if manager.defaultDS != "ds2" {
		t.Errorf("Expected default to be 'ds2'")
	}

	// 尝试设置不存在的数据源为默认
	err = manager.SetDefault("nonexistent")
	if err == nil {
		t.Errorf("Expected error when setting nonexistent data source as default")
	}
}

// TestDataSourceManager_List 测试列出所有数据源
func TestDataSourceManager_List(t *testing.T) {
	manager := NewDataSourceManager()

	// 注册多个数据源
	for i := 0; i < 3; i++ {
		ds := &MockDataSource{connected: false}
		if err := manager.Register("ds"+string(rune('1'+i)), ds); err != nil {
			t.Errorf("Register() error = %v", err)
		}
	}

	// 列出数据源
	list := manager.List()

	if len(list) != 3 {
		t.Errorf("Expected 3 data sources, got %d", len(list))
	}
}

// TestDataSourceManager_ConnectAll 测试连接所有数据源
func TestDataSourceManager_ConnectAll(t *testing.T) {
	manager := NewDataSourceManager()
	ctx := context.Background()

	// 注册多个数据源
	for i := 0; i < 3; i++ {
		ds := &MockDataSource{connected: false}
		if err := manager.Register("ds"+string(rune('1'+i)), ds); err != nil {
			t.Errorf("Register() error = %v", err)
		}
	}

	// 连接所有数据源
	err := manager.ConnectAll(ctx)
	if err != nil {
		t.Errorf("ConnectAll() error = %v", err)
	}

	// 验证所有数据源已连接
	for _, ds := range manager.sources {
		if !ds.IsConnected() {
			t.Errorf("Expected data source to be connected")
		}
	}
}

// TestDataSourceManager_CloseAll 测试关闭所有数据源
func TestDataSourceManager_CloseAll(t *testing.T) {
	manager := NewDataSourceManager()
	ctx := context.Background()

	// 注册并连接数据源
	for i := 0; i < 3; i++ {
		ds := &MockDataSource{connected: false}
		if err := manager.Register("ds"+string(rune('1'+i)), ds); err != nil {
			t.Errorf("Register() error = %v", err)
		}
	}

	// 连接所有数据源
	if err := manager.ConnectAll(ctx); err != nil {
		t.Errorf("ConnectAll() error = %v", err)
	}

	// 关闭所有数据源
	err := manager.CloseAll(ctx)
	if err != nil {
		t.Errorf("CloseAll() error = %v", err)
	}

	// 验证所有数据源已关闭
	for _, ds := range manager.sources {
		if ds.IsConnected() {
			t.Errorf("Expected data source to be disconnected")
		}
	}
}

// TestDataSourceManager_CreateFromConfig 测试从配置创建数据源
func TestDataSourceManager_CreateFromConfig(t *testing.T) {
	manager := NewDataSourceManager()

	// 注册工厂
	factory := &MockFactory{dsType: "memory", shouldErr: false}
	if err := manager.registry.Register(factory); err != nil {
		t.Errorf("Register() error = %v", err)
	}

	// 从配置创建
	config := &domain.DataSourceConfig{
		Type: "memory",
		Name: "test-ds",
	}

	ds, err := manager.CreateFromConfig(config)
	if err != nil {
		t.Errorf("CreateFromConfig() error = %v", err)
	}

	if ds == nil {
		t.Errorf("Expected datasource to be created")
	}
}

// TestDataSourceManager_CreateFromConfig_TypeDisabled 测试创建禁用类型
func TestDataSourceManager_CreateFromConfig_TypeDisabled(t *testing.T) {
	manager := NewDataSourceManager()

	// 只启用特定类型（不启用memory）
	manager.SetEnabledTypes([]domain.DataSourceType{"csv"})

	// 注册工厂
	factory := &MockFactory{dsType: "memory", shouldErr: false}
	if err := manager.registry.Register(factory); err != nil {
		t.Errorf("Register() error = %v", err)
	}

	// 尝试创建未启用类型的数据源
	config := &domain.DataSourceConfig{
		Type: "memory",
		Name: "test-ds",
	}

	_, err := manager.CreateFromConfig(config)
	if err == nil {
		t.Errorf("Expected error when creating disabled type data source")
	}
}

// TestDataSourceManager_CreateAndRegister 测试创建并注册数据源
func TestDataSourceManager_CreateAndRegister(t *testing.T) {
	manager := NewDataSourceManager()
	ctx := context.Background()

	// 注册工厂
	factory := &MockFactory{dsType: "memory", shouldErr: false}
	if err := manager.registry.Register(factory); err != nil {
		t.Errorf("Register() error = %v", err)
	}

	// 创建并注册
	config := &domain.DataSourceConfig{
		Type: "memory",
		Name: "test-ds",
	}

	err := manager.CreateAndRegister(ctx, "test-ds", config)
	if err != nil {
		t.Errorf("CreateAndRegister() error = %v", err)
	}

	// 验证数据源已注册
	_, err = manager.Get("test-ds")
	if err != nil {
		t.Errorf("Expected data source to be registered")
	}

	// 验证数据源已连接
	ds, _ := manager.Get("test-ds")
	if !ds.IsConnected() {
		t.Errorf("Expected data source to be connected")
	}
}

// TestDataSourceManager_GetStatus 测试获取数据源状态
func TestDataSourceManager_GetStatus(t *testing.T) {
	manager := NewDataSourceManager()

	// 注册多个数据源
	ds1 := &MockDataSource{connected: true}
	ds2 := &MockDataSource{connected: false}

	if err := manager.Register("ds1", ds1); err != nil {
		t.Errorf("Register() error = %v", err)
	}
	if err := manager.Register("ds2", ds2); err != nil {
		t.Errorf("Register() error = %v", err)
	}

	// 获取状态
	status := manager.GetStatus()

	if len(status) != 2 {
		t.Errorf("Expected 2 status entries, got %d", len(status))
	}

	if status["ds1"] != true {
		t.Errorf("Expected ds1 to be connected")
	}

	if status["ds2"] != false {
		t.Errorf("Expected ds2 to be disconnected")
	}
}

// TestDataSourceManager_GetDefaultName 测试获取默认数据源名称
func TestDataSourceManager_GetDefaultName(t *testing.T) {
	manager := NewDataSourceManager()

	// 初始状态
	if manager.GetDefaultName() != "" {
		t.Errorf("Expected default name to be empty initially")
	}

	// 注册数据源
	ds := &MockDataSource{connected: false}
	if err := manager.Register("test-ds", ds); err != nil {
		t.Errorf("Register() error = %v", err)
	}

	// 验证默认名称已设置
	if manager.GetDefaultName() != "test-ds" {
		t.Errorf("Expected default name 'test-ds', got '%s'", manager.GetDefaultName())
	}
}

// TestDataSourceManager_GetTables 测试获取表列表
func TestDataSourceManager_GetTables(t *testing.T) {
	manager := NewDataSourceManager()
	ctx := context.Background()

	ds := &MockDataSource{connected: true}
	if err := manager.Register("test-ds", ds); err != nil {
		t.Errorf("Register() error = %v", err)
	}

	// 获取表列表
	tables, err := manager.GetTables(ctx, "test-ds")
	if err != nil {
		t.Errorf("GetTables() error = %v", err)
	}

	if tables == nil {
		t.Errorf("Expected tables to be returned")
	}
}

// TestDataSourceManager_Query 测试查询数据
func TestDataSourceManager_Query(t *testing.T) {
	manager := NewDataSourceManager()
	ctx := context.Background()

	ds := &MockDataSource{connected: true}
	if err := manager.Register("test-ds", ds); err != nil {
		t.Errorf("Register() error = %v", err)
	}

	// 查询数据
	result, err := manager.Query(ctx, "test-ds", "test-table", &domain.QueryOptions{})
	if err != nil {
		t.Errorf("Query() error = %v", err)
	}

	if result == nil {
		t.Errorf("Expected result to be returned")
	}
}

// TestGetDefaultManager 测试获取默认管理器
func TestGetDefaultManager(t *testing.T) {
	manager1 := GetDefaultManager()
	manager2 := GetDefaultManager()

	if manager1 == nil {
		t.Errorf("GetDefaultManager() returned nil")
	}

	if manager1 != manager2 {
		t.Errorf("Expected same manager instance")
	}
}

// TestDataSourceManager_CreateTable 测试创建表
func TestDataSourceManager_CreateTable(t *testing.T) {
	manager := NewDataSourceManager()
	ctx := context.Background()

	ds := &MockDataSource{connected: true}
	if err := manager.Register("test-ds", ds); err != nil {
		t.Errorf("Register() error = %v", err)
	}

	tableInfo := &domain.TableInfo{
		Name: "test-table",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int64", Primary: true},
		},
	}

	err := manager.CreateTable(ctx, "test-ds", tableInfo)
	if err != nil {
		t.Errorf("CreateTable() error = %v", err)
	}
}

// TestDataSourceManager_DropTable 测试删除表
func TestDataSourceManager_DropTable(t *testing.T) {
	manager := NewDataSourceManager()
	ctx := context.Background()

	ds := &MockDataSource{connected: true}
	if err := manager.Register("test-ds", ds); err != nil {
		t.Errorf("Register() error = %v", err)
	}

	err := manager.DropTable(ctx, "test-ds", "test-table")
	if err != nil {
		t.Errorf("DropTable() error = %v", err)
	}
}

// TestDataSourceManager_TruncateTable 测试清空表
func TestDataSourceManager_TruncateTable(t *testing.T) {
	manager := NewDataSourceManager()
	ctx := context.Background()

	ds := &MockDataSource{connected: true}
	if err := manager.Register("test-ds", ds); err != nil {
		t.Errorf("Register() error = %v", err)
	}

	err := manager.TruncateTable(ctx, "test-ds", "test-table")
	if err != nil {
		t.Errorf("TruncateTable() error = %v", err)
	}
}

// TestDataSourceManager_Execute 测试执行SQL
func TestDataSourceManager_Execute(t *testing.T) {
	manager := NewDataSourceManager()
	ctx := context.Background()

	ds := &MockDataSource{connected: true}
	if err := manager.Register("test-ds", ds); err != nil {
		t.Errorf("Register() error = %v", err)
	}

	result, err := manager.Execute(ctx, "test-ds", "SELECT * FROM test")
	if err != nil {
		t.Errorf("Execute() error = %v", err)
	}

	if result == nil {
		t.Errorf("Expected result to be returned")
	}
}

// TestDataSourceManager_Insert 测试插入数据
func TestDataSourceManager_Insert(t *testing.T) {
	manager := NewDataSourceManager()
	ctx := context.Background()

	ds := &MockDataSource{connected: true}
	if err := manager.Register("test-ds", ds); err != nil {
		t.Errorf("Register() error = %v", err)
	}

	rows := []domain.Row{{"id": 1, "name": "test"}}
	count, err := manager.Insert(ctx, "test-ds", "test-table", rows, nil)
	if err != nil {
		t.Errorf("Insert() error = %v", err)
	}

	if count != 0 {
		t.Errorf("Expected 0 rows inserted, got %d", count)
	}
}

// TestDataSourceManager_Update 测试更新数据
func TestDataSourceManager_Update(t *testing.T) {
	manager := NewDataSourceManager()
	ctx := context.Background()

	ds := &MockDataSource{connected: true}
	if err := manager.Register("test-ds", ds); err != nil {
		t.Errorf("Register() error = %v", err)
	}

	filters := []domain.Filter{{Field: "id", Operator: "=", Value: 1}}
	updates := domain.Row{"name": "updated"}
	count, err := manager.Update(ctx, "test-ds", "test-table", filters, updates, nil)
	if err != nil {
		t.Errorf("Update() error = %v", err)
	}

	if count != 0 {
		t.Errorf("Expected 0 rows updated, got %d", count)
	}
}

// TestDataSourceManager_Delete 测试删除数据
func TestDataSourceManager_Delete(t *testing.T) {
	manager := NewDataSourceManager()
	ctx := context.Background()

	ds := &MockDataSource{connected: true}
	if err := manager.Register("test-ds", ds); err != nil {
		t.Errorf("Register() error = %v", err)
	}

	filters := []domain.Filter{{Field: "id", Operator: "=", Value: 1}}
	count, err := manager.Delete(ctx, "test-ds", "test-table", filters, nil)
	if err != nil {
		t.Errorf("Delete() error = %v", err)
	}

	if count != 0 {
		t.Errorf("Expected 0 rows deleted, got %d", count)
	}
}
