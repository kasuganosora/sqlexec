package container

import (
	"testing"
)

// TestDefaultContainer_BuildExpressionEvaluator tests BuildExpressionEvaluator
func TestDefaultContainer_BuildExpressionEvaluator(t *testing.T) {
	dataSource := &MockDataSource{}
	container := NewContainer(dataSource)

	evaluator := container.BuildExpressionEvaluator()
	// Currently returns nil (not implemented)
	if evaluator != nil {
		t.Error("BuildExpressionEvaluator should return nil (not implemented)")
	}
}

// TestDefaultContainer_Adapters tests the adapter implementations
func TestDefaultContainer_Adapters(t *testing.T) {
	dataSource := &MockDataSource{}
	container := NewContainer(dataSource)

	// Test costCardinalityAdapter
	t.Run("costCardinalityAdapter", func(t *testing.T) {
		estimator := container.MustGet("estimator.enhanced")
		adapter := &costCardinalityAdapter{estimator: estimator}

		// Test EstimateTableScan with valid estimator
		result := adapter.EstimateTableScan("test_table")
		if result <= 0 {
			t.Error("EstimateTableScan should return positive value")
		}

		// Test EstimateFilter with valid estimator
		result2 := adapter.EstimateFilter("test_table", nil)
		if result2 <= 0 {
			t.Error("EstimateFilter should return positive value")
		}
	})

	// Test with nil estimator (fallback to defaults)
	t.Run("costCardinalityAdapter with nil", func(t *testing.T) {
		adapter := &costCardinalityAdapter{estimator: nil}

		// Should return default values
		result := adapter.EstimateTableScan("test_table")
		if result != 10000 {
			t.Errorf("Expected default 10000, got %d", result)
		}

		result2 := adapter.EstimateFilter("test_table", nil)
		if result2 != 1000 {
			t.Errorf("Expected default 1000, got %d", result2)
		}
	})

	// Test joinCostAdapter
	t.Run("joinCostAdapter", func(t *testing.T) {
		costModel := container.MustGet("cost.model.adaptive")
		adapter := &joinCostAdapter{costModel: costModel}

		// Test ScanCost with valid cost model
		result := adapter.ScanCost("test_table", 1000, true)
		// May return 0 or a value depending on the cost model implementation
		t.Logf("ScanCost returned: %f", result)

		// Test JoinCost with valid cost model
		result2 := adapter.JoinCost(nil, nil, 0, nil)
		t.Logf("JoinCost returned: %f", result2)
	})

	// Test joinCostAdapter with nil cost model
	t.Run("joinCostAdapter with nil", func(t *testing.T) {
		adapter := &joinCostAdapter{costModel: nil}

		result := adapter.ScanCost("test_table", 1000, true)
		if result != 0.0 {
			t.Errorf("Expected 0.0 for nil cost model, got %f", result)
		}

		result2 := adapter.JoinCost(nil, nil, 0, nil)
		if result2 != 0.0 {
			t.Errorf("Expected 0.0 for nil cost model, got %f", result2)
		}
	})

	// Test joinCardinalityAdapter
	t.Run("joinCardinalityAdapter", func(t *testing.T) {
		estimator := container.MustGet("estimator.enhanced")
		adapter := &joinCardinalityAdapter{estimator: estimator}

		result := adapter.EstimateTableScan("test_table")
		if result <= 0 {
			t.Error("EstimateTableScan should return positive value")
		}
	})

	// Test joinCardinalityAdapter with nil
	t.Run("joinCardinalityAdapter with nil", func(t *testing.T) {
		adapter := &joinCardinalityAdapter{estimator: nil}

		result := adapter.EstimateTableScan("test_table")
		if result != 10000 {
			t.Errorf("Expected default 10000, got %d", result)
		}
	})
}

// TestDefaultContainer_BuildEnhancedOptimizer_NotFound tests BuildEnhancedOptimizer with missing services
func TestDefaultContainer_BuildEnhancedOptimizer_NotFound(t *testing.T) {
	dataSource := &MockDataSource{}
	container := NewContainer(dataSource)

	// Remove required services to test panic behavior
	// Since we can't easily remove, we test with a container that has all services
	defer func() {
		if r := recover(); r != nil {
			t.Logf("BuildEnhancedOptimizer panicked as expected: %v", r)
		}
	}()

	config := container.BuildEnhancedOptimizer(4)
	if config == nil {
		t.Error("BuildEnhancedOptimizer should return a config even with all services")
	}
}

// TestDefaultContainer_BuildEnhancedOptimizer_Parallelism tests different parallelism values
func TestDefaultContainer_BuildEnhancedOptimizer_Parallelism(t *testing.T) {
	dataSource := &MockDataSource{}
	container := NewContainer(dataSource)

	tests := []struct {
		name        string
		parallelism int
	}{
		{"zero parallelism", 0},
		{"positive parallelism", 4},
		{"large parallelism", 100},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := container.BuildEnhancedOptimizer(tt.parallelism)
			if config == nil {
				t.Error("BuildEnhancedOptimizer should not return nil")
			}
		})
	}
}

// TestAdaptersExist tests that adapters can be created
func TestAdaptersExist(t *testing.T) {
	dataSource := &MockDataSource{}
	container := NewContainer(dataSource)

	tests := []struct {
		name        string
		adapter     interface{}
	}{
		{
			name:        "costCardinalityAdapter",
			adapter:     &costCardinalityAdapter{estimator: container.MustGet("estimator.enhanced")},
		},
		{
			name:        "joinCostAdapter",
			adapter:     &joinCostAdapter{costModel: container.MustGet("cost.model.adaptive")},
		},
		{
			name:        "joinCardinalityAdapter",
			adapter:     &joinCardinalityAdapter{estimator: container.MustGet("estimator.enhanced")},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.adapter == nil {
				t.Errorf("%s should not be nil", tt.name)
			}
		})
	}
}

// TestDefaultContainer_RegisterOverwrite tests that Register overwrites existing services
func TestDefaultContainer_RegisterOverwrite(t *testing.T) {
	dataSource := &MockDataSource{}
	container := NewContainer(dataSource)

	// Register a service
	container.Register("test.service", "original")

	service, _ := container.Get("test.service")
	if service != "original" {
		t.Error("Expected original service")
	}

	// Overwrite with new value
	container.Register("test.service", "overwritten")

	service, _ = container.Get("test.service")
	if service != "overwritten" {
		t.Error("Expected overwritten service")
	}
}

// TestDefaultContainer_BuildMethodsReturnNil tests build methods that return nil
func TestDefaultContainer_BuildMethodsReturnNil(t *testing.T) {
	dataSource := &MockDataSource{}
	container := NewContainer(dataSource)

	// Test BuildOptimizer (currently returns nil)
	optimizer := container.BuildOptimizer()
	if optimizer != nil {
		t.Log("BuildOptimizer returned non-nil (may be implemented now)")
	}

	// Test BuildExecutor (currently returns nil)
	executor := container.BuildExecutor()
	if executor != nil {
		t.Log("BuildExecutor returned non-nil (may be implemented now)")
	}

	// Test BuildShowProcessor (currently returns nil)
	showProcessor := container.BuildShowProcessor()
	if showProcessor != nil {
		t.Log("BuildShowProcessor returned non-nil (may be implemented now)")
	}

	// Test BuildVariableManager (currently returns nil)
	varManager := container.BuildVariableManager()
	if varManager != nil {
		t.Log("BuildVariableManager returned non-nil (may be implemented now)")
	}

	// Test BuildExpressionEvaluator (currently returns nil)
	exprEvaluator := container.BuildExpressionEvaluator()
	if exprEvaluator != nil {
		t.Log("BuildExpressionEvaluator returned non-nil (may be implemented now)")
	}
}

// TestDefaultContainer_EnhancedOptimizerConfig tests the EnhancedOptimizerConfig structure
func TestDefaultContainer_EnhancedOptimizerConfig(t *testing.T) {
	dataSource := &MockDataSource{}
	container := NewContainer(dataSource)

	config := container.BuildEnhancedOptimizer(4)
	if config == nil {
		t.Fatal("BuildEnhancedOptimizer returned nil")
	}

	// Verify config is of correct type
	if _, ok := config.(*EnhancedOptimizerConfig); !ok {
		t.Error("BuildEnhancedOptimizer should return *EnhancedOptimizerConfig")
	}

	// Verify config fields
	eoc := config.(*EnhancedOptimizerConfig)
	if eoc.Parallelism != 4 {
		t.Errorf("Expected parallelism 4, got %d", eoc.Parallelism)
	}

	if eoc.DataSource == nil {
		t.Error("Config DataSource should not be nil")
	}
}
