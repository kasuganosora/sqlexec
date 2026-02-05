package optimizer

import (
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/resource/memory"
)

// TestOptimizedExecutorWithEnhancedOptimizer 测试使用增强优化器
func TestOptimizedExecutorWithEnhancedOptimizer(t *testing.T) {
	// Create test data source
	dataSource := createTestDataSource()

	// Create executor with enhanced optimizer enabled
	executor := NewOptimizedExecutorWithEnhanced(dataSource, true, true)

	if !executor.useEnhanced {
		t.Error("Expected useEnhanced to be true")
	}

	// Check optimizer type
	if _, ok := executor.optimizer.(*EnhancedOptimizer); !ok {
		t.Error("Expected optimizer to be *EnhancedOptimizer")
	}
}

// TestOptimizedExecutorWithBaseOptimizer 测试使用基础优化器
func TestOptimizedExecutorWithBaseOptimizer(t *testing.T) {
	// Create test data source
	dataSource := createTestDataSource()

	// Create executor with base optimizer
	executor := NewOptimizedExecutorWithEnhanced(dataSource, true, false)

	if executor.useEnhanced {
		t.Error("Expected useEnhanced to be false")
	}

	// Check optimizer type
	if _, ok := executor.optimizer.(*Optimizer); !ok {
		t.Error("Expected optimizer to be *Optimizer")
	}
}

// TestOptimizedExecutorDefaultUsesEnhanced 测试默认构造函数使用增强优化器
func TestOptimizedExecutorDefaultUsesEnhanced(t *testing.T) {
	dataSource := createTestDataSource()

	// Create executor using default constructor
	executor := NewOptimizedExecutor(dataSource, true)

	if !executor.useEnhanced {
		t.Error("Expected default useEnhanced to be true")
	}

	// Check optimizer type
	if _, ok := executor.optimizer.(*EnhancedOptimizer); !ok {
		t.Error("Expected default optimizer to be *EnhancedOptimizer")
	}
}

// TestOptimizedExecutorSetUseEnhanced 测试动态切换优化器
func TestOptimizedExecutorSetUseEnhanced(t *testing.T) {
	dataSource := createTestDataSource()

	// Create executor with base optimizer
	executor := NewOptimizedExecutorWithEnhanced(dataSource, true, false)

	// Verify initial state
	if executor.useEnhanced {
		t.Error("Expected initial useEnhanced to be false")
	}
	if _, ok := executor.optimizer.(*Optimizer); !ok {
		t.Error("Expected initial optimizer to be *Optimizer")
	}

	// Switch to enhanced optimizer
	executor.SetUseEnhanced(true)

	// Verify new state
	if !executor.useEnhanced {
		t.Error("Expected useEnhanced to be true after SetUseEnhanced(true)")
	}
	if _, ok := executor.optimizer.(*EnhancedOptimizer); !ok {
		t.Error("Expected optimizer to be *EnhancedOptimizer after SetUseEnhanced(true)")
	}

	// Switch back to base optimizer
	executor.SetUseEnhanced(false)

	// Verify final state
	if executor.useEnhanced {
		t.Error("Expected useEnhanced to be false after SetUseEnhanced(false)")
	}
	if _, ok := executor.optimizer.(*Optimizer); !ok {
		t.Error("Expected optimizer to be *Optimizer after SetUseEnhanced(false)")
	}
}

// TestGetUseEnhanced 测试 GetUseEnhanced 方法
func TestGetUseEnhanced(t *testing.T) {
	dataSource := createTestDataSource()

	// Test with enhanced optimizer enabled
	executor1 := NewOptimizedExecutorWithEnhanced(dataSource, true, true)
	if !executor1.GetUseEnhanced() {
		t.Error("Expected GetUseEnhanced() to return true")
	}

	// Test with enhanced optimizer disabled
	executor2 := NewOptimizedExecutorWithEnhanced(dataSource, true, false)
	if executor2.GetUseEnhanced() {
		t.Error("Expected GetUseEnhanced() to return false")
	}

	// Test after dynamic change
	executor2.SetUseEnhanced(true)
	if !executor2.GetUseEnhanced() {
		t.Error("Expected GetUseEnhanced() to return true after SetUseEnhanced(true)")
	}
}

// TestEnhancedOptimizerParallelism 测试增强优化器的并行度
func TestEnhancedOptimizerParallelism(t *testing.T) {
	dataSource := createTestDataSource()

	// Create with parallelism=0 (auto)
	executorAuto := NewOptimizedExecutorWithEnhanced(dataSource, true, true)
	enhancedOpt, ok := executorAuto.optimizer.(*EnhancedOptimizer)
	if !ok {
		t.Fatal("Expected optimizer to be *EnhancedOptimizer")
	}
	if enhancedOpt.GetParallelism() != 0 {
		t.Errorf("Expected parallelism to be 0 (auto), got %d", enhancedOpt.GetParallelism())
	}

	// Create with parallelism=4
	executorFixed := NewOptimizedExecutorWithDSManagerAndEnhanced(dataSource, nil, true, true)
	enhancedOpt2, ok := executorFixed.optimizer.(*EnhancedOptimizer)
	if !ok {
		t.Fatal("Expected optimizer to be *EnhancedOptimizer")
	}
	if enhancedOpt2.GetParallelism() != 0 {
		t.Errorf("Expected parallelism to be 0 (auto), got %d", enhancedOpt2.GetParallelism())
	}
}

// Helper function to create test data source
func createTestDataSource() domain.DataSource {
	config := &domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Name:     "test_db",
		Writable: true,
	}
	dataSource := memory.NewMVCCDataSource(config)
	if err := dataSource.Connect(nil); err != nil {
		panic(err)
	}
	return dataSource
}
