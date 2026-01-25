package application

import (
	"context"
	"errors"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// MockFactory 模拟工厂实现
type MockFactory struct {
	dsType    domain.DataSourceType
	shouldErr bool
}

func (m *MockFactory) Create(config *domain.DataSourceConfig) (domain.DataSource, error) {
	if m.shouldErr {
		return nil, errors.New("factory error")
	}
	return &MockDataSource{connected: false}, nil
}

func (m *MockFactory) GetType() domain.DataSourceType {
	return m.dsType
}

// MockDataSource 模拟数据源
type MockDataSource struct {
	connected bool
}

func (m *MockDataSource) Connect(ctx context.Context) error {
	m.connected = true
	return nil
}

func (m *MockDataSource) Close(ctx context.Context) error {
	m.connected = false
	return nil
}

func (m *MockDataSource) IsConnected() bool {
	return m.connected
}

func (m *MockDataSource) IsWritable() bool {
	return true
}

func (m *MockDataSource) GetConfig() *domain.DataSourceConfig {
	return &domain.DataSourceConfig{Type: "mock"}
}

func (m *MockDataSource) GetTables(ctx context.Context) ([]string, error) {
	return []string{}, nil
}

func (m *MockDataSource) GetTableInfo(ctx context.Context, tableName string) (*domain.TableInfo, error) {
	return nil, nil
}

func (m *MockDataSource) Query(ctx context.Context, tableName string, options *domain.QueryOptions) (*domain.QueryResult, error) {
	return &domain.QueryResult{Columns: []domain.ColumnInfo{}, Rows: []domain.Row{}, Total: 0}, nil
}

func (m *MockDataSource) Insert(ctx context.Context, tableName string, rows []domain.Row, options *domain.InsertOptions) (int64, error) {
	return 0, nil
}

func (m *MockDataSource) Update(ctx context.Context, tableName string, filters []domain.Filter, updates domain.Row, options *domain.UpdateOptions) (int64, error) {
	return 0, nil
}

func (m *MockDataSource) Delete(ctx context.Context, tableName string, filters []domain.Filter, options *domain.DeleteOptions) (int64, error) {
	return 0, nil
}

func (m *MockDataSource) CreateTable(ctx context.Context, tableInfo *domain.TableInfo) error {
	return nil
}

func (m *MockDataSource) DropTable(ctx context.Context, tableName string) error {
	return nil
}

func (m *MockDataSource) TruncateTable(ctx context.Context, tableName string) error {
	return nil
}

func (m *MockDataSource) Execute(ctx context.Context, sql string) (*domain.QueryResult, error) {
	return &domain.QueryResult{Columns: []domain.ColumnInfo{}, Rows: []domain.Row{}, Total: 0}, nil
}

// TestNewRegistry 测试创建注册表
func TestNewRegistry(t *testing.T) {
	registry := NewRegistry()

	if registry == nil {
		t.Errorf("NewRegistry() returned nil")
	}

	if registry.factories == nil {
		t.Errorf("Expected factories map to be initialized")
	}
}

// TestRegistry_Register 测试注册工厂
func TestRegistry_Register(t *testing.T) {
	registry := NewRegistry()

	factory := &MockFactory{dsType: "test", shouldErr: false}

	// 注册工厂
	err := registry.Register(factory)
	if err != nil {
		t.Errorf("Register() error = %v", err)
	}

	// 验证工厂已注册
	exists := registry.Exists("test")
	if !exists {
		t.Errorf("Expected factory to be registered")
	}

	// 尝试重复注册
	err = registry.Register(factory)
	if err == nil {
		t.Errorf("Expected error when registering duplicate factory")
	}
}

// TestRegistry_Unregister 测试注销工厂
func TestRegistry_Unregister(t *testing.T) {
	registry := NewRegistry()

	factory := &MockFactory{dsType: "test", shouldErr: false}

	// 注册工厂
	if err := registry.Register(factory); err != nil {
		t.Errorf("Register() error = %v", err)
	}

	// 注销工厂
	err := registry.Unregister("test")
	if err != nil {
		t.Errorf("Unregister() error = %v", err)
	}

	// 验证工厂已注销
	exists := registry.Exists("test")
	if exists {
		t.Errorf("Expected factory to be unregistered")
	}

	// 尝试注销不存在的工厂
	err = registry.Unregister("test")
	if err == nil {
		t.Errorf("Expected error when unregistering nonexistent factory")
	}
}

// TestRegistry_Get 测试获取工厂
func TestRegistry_Get(t *testing.T) {
	registry := NewRegistry()

	factory := &MockFactory{dsType: "test", shouldErr: false}

	// 注册工厂
	if err := registry.Register(factory); err != nil {
		t.Errorf("Register() error = %v", err)
	}

	// 获取工厂
	got, err := registry.Get("test")
	if err != nil {
		t.Errorf("Get() error = %v", err)
	}

	if got == nil {
		t.Errorf("Expected factory to be returned")
	}

	if got.GetType() != "test" {
		t.Errorf("Expected factory type 'test', got %v", got.GetType())
	}

	// 获取不存在的工厂
	_, err = registry.Get("nonexistent")
	if err == nil {
		t.Errorf("Expected error when getting nonexistent factory")
	}
}

// TestRegistry_Create 测试创建数据源
func TestRegistry_Create(t *testing.T) {
	registry := NewRegistry()

	factory := &MockFactory{dsType: "test", shouldErr: false}

	// 注册工厂
	if err := registry.Register(factory); err != nil {
		t.Errorf("Register() error = %v", err)
	}

	// 创建数据源
	config := &domain.DataSourceConfig{
		Type: "test",
		Name: "test-ds",
	}

	ds, err := registry.Create(config)
	if err != nil {
		t.Errorf("Create() error = %v", err)
	}

	if ds == nil {
		t.Errorf("Expected datasource to be returned")
	}
}

// TestRegistry_Create_FactoryError 测试工厂创建失败
func TestRegistry_Create_FactoryError(t *testing.T) {
	registry := NewRegistry()

	factory := &MockFactory{dsType: "test", shouldErr: true}

	// 注册会返回错误的工厂
	if err := registry.Register(factory); err != nil {
		t.Errorf("Register() error = %v", err)
	}

	// 创建数据源
	config := &domain.DataSourceConfig{
		Type: "test",
		Name: "test-ds",
	}

	_, err := registry.Create(config)
	if err == nil {
		t.Errorf("Expected error when factory returns error")
	}
}

// TestRegistry_Create_FactoryNotFound 测试工厂未找到
func TestRegistry_Create_FactoryNotFound(t *testing.T) {
	registry := NewRegistry()

	// 创建数据源使用未注册的工厂
	config := &domain.DataSourceConfig{
		Type: "nonexistent",
		Name: "test-ds",
	}

	_, err := registry.Create(config)
	if err == nil {
		t.Errorf("Expected error when factory not found")
	}
}

// TestRegistry_List 测试列出所有工厂
func TestRegistry_List(t *testing.T) {
	registry := NewRegistry()

	// 注册多个工厂
	types := []domain.DataSourceType{"type1", "type2", "type3"}
	for _, dsType := range types {
		factory := &MockFactory{dsType: dsType, shouldErr: false}
		if err := registry.Register(factory); err != nil {
			t.Errorf("Register() error = %v", err)
		}
	}

	// 列出工厂
	list := registry.List()

	if len(list) != 3 {
		t.Errorf("Expected 3 factories, got %d", len(list))
	}
}

// TestRegistry_Exists 测试检查工厂是否存在
func TestRegistry_Exists(t *testing.T) {
	registry := NewRegistry()

	factory := &MockFactory{dsType: "test", shouldErr: false}

	// 注册前检查
	exists := registry.Exists("test")
	if exists {
		t.Errorf("Expected factory to not exist before registration")
	}

	// 注册工厂
	if err := registry.Register(factory); err != nil {
		t.Errorf("Register() error = %v", err)
	}

	// 注册后检查
	exists = registry.Exists("test")
	if !exists {
		t.Errorf("Expected factory to exist after registration")
	}
}

// TestRegistry_Clear 测试清空工厂
func TestRegistry_Clear(t *testing.T) {
	registry := NewRegistry()

	// 注册多个工厂
	for i := 0; i < 3; i++ {
		factory := &MockFactory{dsType: domain.DataSourceType("type" + string(rune('0'+i))), shouldErr: false}
		if err := registry.Register(factory); err != nil {
			t.Errorf("Register() error = %v", err)
		}
	}

	// 验证有工厂
	list := registry.List()
	if len(list) != 3 {
		t.Errorf("Expected 3 factories before clear, got %d", len(list))
	}

	// 清空
	registry.Clear()

	// 验证已清空
	list = registry.List()
	if len(list) != 0 {
		t.Errorf("Expected 0 factories after clear, got %d", len(list))
	}
}

// TestGetRegistry 测试获取全局注册表
func TestGetRegistry(t *testing.T) {
	registry := GetRegistry()

	if registry == nil {
		t.Errorf("GetRegistry() returned nil")
	}

	// 多次调用应该返回同一个实例
	registry2 := GetRegistry()
	if registry != registry2 {
		t.Errorf("Expected same registry instance")
	}
}

// TestRegisterFactory 测试全局注册工厂
func TestRegisterFactory(t *testing.T) {
	factory := &MockFactory{dsType: "test-global", shouldErr: false}

	// 全局注册
	err := RegisterFactory(factory)
	if err != nil {
		t.Errorf("RegisterFactory() error = %v", err)
	}

	// 验证已注册
	exists := GetRegistry().Exists("test-global")
	if !exists {
		t.Errorf("Expected factory to be registered globally")
	}
}

// TestUnregisterFactory 测试全局注销工厂
func TestUnregisterFactory(t *testing.T) {
	factory := &MockFactory{dsType: "test-unregister", shouldErr: false}

	// 先注册
	if err := RegisterFactory(factory); err != nil {
		t.Errorf("RegisterFactory() error = %v", err)
	}

	// 全局注销
	err := UnregisterFactory("test-unregister")
	if err != nil {
		t.Errorf("UnregisterFactory() error = %v", err)
	}

	// 验证已注销
	exists := GetRegistry().Exists("test-unregister")
	if exists {
		t.Errorf("Expected factory to be unregistered globally")
	}
}

// TestGetFactory 测试全局获取工厂
func TestGetFactory(t *testing.T) {
	factory := &MockFactory{dsType: "test-get", shouldErr: false}

	// 注册
	if err := RegisterFactory(factory); err != nil {
		t.Errorf("RegisterFactory() error = %v", err)
	}

	// 获取
	got, err := GetFactory("test-get")
	if err != nil {
		t.Errorf("GetFactory() error = %v", err)
	}

	if got == nil {
		t.Errorf("Expected factory to be returned")
	}
}

// TestCreateDataSource 测试全局创建数据源
func TestCreateDataSource(t *testing.T) {
	factory := &MockFactory{dsType: "test-create-ds", shouldErr: false}

	// 注册
	if err := RegisterFactory(factory); err != nil {
		t.Errorf("RegisterFactory() error = %v", err)
	}

	// 创建
	config := &domain.DataSourceConfig{
		Type: "test-create-ds",
		Name: "test",
	}

	ds, err := CreateDataSource(config)
	if err != nil {
		t.Errorf("CreateDataSource() error = %v", err)
	}

	if ds == nil {
		t.Errorf("Expected datasource to be returned")
	}
}

// TestGetSupportedTypes 测试获取支持的类型
func TestGetSupportedTypes(t *testing.T) {
	// 清空全局注册表
	GetRegistry().Clear()

	// 注册多个工厂
	types := []domain.DataSourceType{"type1", "type2", "type3"}
	for _, dsType := range types {
		factory := &MockFactory{dsType: dsType, shouldErr: false}
		if err := RegisterFactory(factory); err != nil {
			t.Errorf("RegisterFactory() error = %v", err)
		}
	}

	// 获取支持的类型
	supported := GetSupportedTypes()

	if len(supported) != 3 {
		t.Errorf("Expected 3 supported types, got %d", len(supported))
	}
}
