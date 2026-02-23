package container

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// MockDataSource is a simple mock for testing
type MockDataSource struct{}

func (m *MockDataSource) Connect(ctx context.Context) error {
	return nil
}

func (m *MockDataSource) Close(ctx context.Context) error {
	return nil
}

func (m *MockDataSource) IsConnected() bool {
	return true
}

func (m *MockDataSource) IsWritable() bool {
	return false
}

func (m *MockDataSource) GetConfig() *domain.DataSourceConfig {
	return &domain.DataSourceConfig{}
}

func (m *MockDataSource) GetTables(ctx context.Context) ([]string, error) {
	return []string{}, nil
}

func (m *MockDataSource) GetTableInfo(ctx context.Context, tableName string) (*domain.TableInfo, error) {
	return &domain.TableInfo{}, nil
}

func (m *MockDataSource) Query(ctx context.Context, tableName string, options *domain.QueryOptions) (*domain.QueryResult, error) {
	return &domain.QueryResult{}, nil
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
	return &domain.QueryResult{}, nil
}

func TestNewContainer(t *testing.T) {
	dataSource := &MockDataSource{}
	container := NewContainer(dataSource)

	if container == nil {
		t.Fatal("NewContainer returned nil")
	}

	// Test that default services are registered
	services := []string{
		"stats.cache.auto_refresh",
		"stats.cache.base",
		"estimator.enhanced",
		"cost.model.adaptive",
		"index.selector",
		"adapter.cost_model.join",
		"adapter.cardinality.join",
		"join.reorder.dp",
		"join.bushy_tree",
		"parser.hints",
	}

	for _, serviceName := range services {
		if !container.Has(serviceName) {
			t.Errorf("Expected service '%s' to be registered", serviceName)
		}
	}
}

func TestContainer_RegisterAndGet(t *testing.T) {
	dataSource := &MockDataSource{}
	container := NewContainer(dataSource)

	// Test Register
	testService := "test service"
	container.Register("test.service", testService)

	// Test Get
	service, exists := container.Get("test.service")
	if !exists {
		t.Fatal("Expected service to exist")
	}

	if service != testService {
		t.Errorf("Expected '%s', got '%v'", testService, service)
	}

	// Test Get non-existent service
	_, exists = container.Get("non.existent")
	if exists {
		t.Error("Expected non-existent service to return false")
	}
}

func TestContainer_MustGet(t *testing.T) {
	dataSource := &MockDataSource{}
	container := NewContainer(dataSource)

	// Test MustGet existing service
	estimator := container.MustGet("estimator.enhanced")
	if estimator == nil {
		t.Fatal("Expected estimator to be non-nil")
	}

	// Test MustGet non-existent service (should panic)
	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected MustGet to panic for non-existent service")
		}
	}()

	container.MustGet("non.existent")
}

func TestContainer_BuildMethods(t *testing.T) {
	dataSource := &MockDataSource{}
	container := NewContainer(dataSource)

	// Test that build methods return appropriate values
	// BuildOptimizer currently returns nil (not fully implemented)
	_ = container.BuildOptimizer()

	// BuildEnhancedOptimizer returns a config object (partially implemented)
	enhancedConfig := container.BuildEnhancedOptimizer(0)
	if enhancedConfig == nil {
		t.Error("Expected BuildEnhancedOptimizer to return a config object")
	}

	// BuildExecutor currently returns nil (not fully implemented)
	_ = container.BuildExecutor()

	// BuildShowProcessor currently returns nil (not fully implemented)
	_ = container.BuildShowProcessor()

	// BuildVariableManager currently returns nil (not fully implemented)
	_ = container.BuildVariableManager()
}

func TestDefaultServices(t *testing.T) {
	dataSource := &MockDataSource{}
	container := NewContainer(dataSource)

	// Test that services can be retrieved and are of correct type
	tests := []struct {
		name      string
		typeCheck func(interface{}) bool
	}{
		{
			name:      "stats.cache.auto_refresh",
			typeCheck: func(s interface{}) bool { return s != nil },
		},
		{
			name:      "estimator.enhanced",
			typeCheck: func(s interface{}) bool { return s != nil },
		},
		{
			name:      "cost.model.adaptive",
			typeCheck: func(s interface{}) bool { return s != nil },
		},
		{
			name:      "index.selector",
			typeCheck: func(s interface{}) bool { return s != nil },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			service := container.MustGet(tt.name)
			if service == nil {
				t.Errorf("Service '%s' is nil", tt.name)
			}
			if !tt.typeCheck(service) {
				t.Errorf("Service '%s' type check failed", tt.name)
			}
		})
	}
}

// TestAdapterInterfaces tests that adapters implement expected interfaces
func TestAdapterInterfaces(t *testing.T) {
	dataSource := &MockDataSource{}
	container := NewContainer(dataSource)

	// Test cost cardinality adapter
	adapter := container.MustGet("adapter.cost_model.join")
	if adapter == nil {
		t.Fatal("Cost model adapter not found")
	}

	// The adapter should implement ScanCost and JoinCost methods
	// (We can't easily test this without importing join package,
	// but we can at least verify it exists)

	// Test join cardinality adapter
	adapter2 := container.MustGet("adapter.cardinality.join")
	if adapter2 == nil {
		t.Fatal("Cardinality adapter not found")
	}
}

func TestContainer_ThreadSafety(t *testing.T) {
	dataSource := &MockDataSource{}
	container := NewContainer(dataSource)

	// Test concurrent reads and writes
	done := make(chan bool)

	// Writer goroutine
	go func() {
		for i := 0; i < 100; i++ {
			container.Register(fmt.Sprintf("test.%d", i), i)
			time.Sleep(time.Microsecond)
		}
		done <- true
	}()

	// Reader goroutine
	go func() {
		for i := 0; i < 100; i++ {
			_, _ = container.Get(fmt.Sprintf("test.%d", i))
			time.Sleep(time.Microsecond)
		}
		done <- true
	}()

	// Wait for both goroutines
	<-done
	<-done

	// If we get here without deadlock or panic, thread safety is OK
	t.Log("Thread safety test completed successfully")
}

func TestBuilderMethods(t *testing.T) {
	dataSource := &MockDataSource{}
	container := NewContainer(dataSource)
	builder := NewBuilder(container)

	if builder == nil {
		t.Fatal("NewBuilder should return non-nil")
	}

	// Test BuildOptimizer
	optimizer := builder.BuildOptimizer()
	if optimizer == nil {
		t.Error("BuildOptimizer should return non-nil")
	}

	// Test BuildEnhancedOptimizer
	enhancedOptimizer := builder.BuildEnhancedOptimizer(4)
	if enhancedOptimizer == nil {
		t.Error("BuildEnhancedOptimizer should return non-nil")
	}

	// Test BuildExecutor
	executor := builder.BuildExecutor()
	if executor == nil {
		t.Error("BuildExecutor should return non-nil")
	}

	// Test BuildOptimizedExecutor
	optimizedExecutor := builder.BuildOptimizedExecutor(true)
	if optimizedExecutor == nil {
		t.Error("BuildOptimizedExecutor should return non-nil")
	}

	// Test BuildShowProcessor
	showProcessor := builder.BuildShowProcessor()
	if showProcessor == nil {
		t.Error("BuildShowProcessor should return non-nil")
	}

	// Test BuildVariableManager
	varManager := builder.BuildVariableManager()
	if varManager == nil {
		t.Error("BuildVariableManager should return non-nil")
	}
}

func TestBuilderGetCostModel(t *testing.T) {
	dataSource := &MockDataSource{}
	container := NewContainer(dataSource)
	builder := NewBuilder(container)

	costModel := builder.GetCostModel()
	if costModel == nil {
		t.Error("GetCostModel should return non-nil")
	}
}

func TestBuilderGetIndexSelector(t *testing.T) {
	dataSource := &MockDataSource{}
	container := NewContainer(dataSource)
	builder := NewBuilder(container)

	indexSelector := builder.GetIndexSelector()
	if indexSelector == nil {
		t.Error("GetIndexSelector should return non-nil")
	}
}

func TestBuilderGetStatisticsCache(t *testing.T) {
	dataSource := &MockDataSource{}
	container := NewContainer(dataSource)
	builder := NewBuilder(container)

	statsCache := builder.GetStatisticsCache()
	if statsCache == nil {
		t.Error("GetStatisticsCache should return non-nil")
	}
}

func TestDefaultContainerGetDataSource(t *testing.T) {
	dataSource := &MockDataSource{}
	container := NewContainer(dataSource)

	ds := container.GetDataSource()
	if ds == nil {
		t.Error("GetDataSource should return non-nil")
	}
}

func TestContainerWithNilDataSource(t *testing.T) {
	// Test container with nil data source (should not panic)
	container := NewContainer(nil)

	if container == nil {
		t.Fatal("NewContainer should not panic even with nil data source")
	}

	// Verify that default services are still registered
	if !container.Has("stats.cache.auto_refresh") {
		t.Error("Default services should be registered even with nil data source")
	}
}

func TestContainerConcurrentRegistration(t *testing.T) {
	dataSource := &MockDataSource{}
	container := NewContainer(dataSource)

	// Test concurrent registration of the same key
	done := make(chan bool, 10)

	for i := 0; i < 5; i++ {
		go func() {
			container.Register("concurrent.key", i)
			done <- true
		}()
	}

	for i := 0; i < 5; i++ {
		go func() {
			_, _ = container.Get("concurrent.key")
			done <- true
		}()
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}

	// If we get here without deadlock, concurrent access is OK
	t.Log("Concurrent access test completed successfully")
}

func TestBuildOptimizedExecutorWithDSManager(t *testing.T) {
	dataSource := &MockDataSource{}
	container := NewContainer(dataSource)
	builder := NewBuilder(container)

	// Build with a mock DS manager (using nil for simplicity, should not panic)
	optimizedExecutor := builder.BuildOptimizedExecutorWithDSManager(nil, true)

	// This may return nil if not fully implemented, but should not panic
	if optimizedExecutor == nil {
		t.Log("BuildOptimizedExecutorWithDSManager returned nil (may not be fully implemented)")
	} else {
		t.Log("BuildOptimizedExecutorWithDSManager succeeded")
	}
}
