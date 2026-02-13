package builtin

import (
	"sync"
	"testing"
)

func TestResetGlobalRegistry(t *testing.T) {
	// Save original state
	original := globalRegistry
	defer func() {
		globalRegistry = original
	}()

	// Register a test function
	testFunc := &FunctionInfo{
		Name:        "test_func_reset",
		Type:        FunctionTypeScalar,
		Handler:     func(args []interface{}) (interface{}, error) { return nil, nil },
		Description: "Test function",
	}
	RegisterGlobal(testFunc)

	// Verify it exists
	_, exists := GetGlobal("test_func_reset")
	if !exists {
		t.Fatal("function should exist after registration")
	}

	// Reset
	ResetGlobalRegistry()

	// Verify it no longer exists
	_, exists = GetGlobal("test_func_reset")
	if exists {
		t.Error("function should not exist after reset")
	}
}

func TestResetGlobalRegistryWith(t *testing.T) {
	// Save original state
	original := globalRegistry
	defer func() {
		globalRegistry = original
	}()

	// Create custom registry
	customRegistry := NewFunctionRegistry()
	testFunc := &FunctionInfo{
		Name:        "custom_func",
		Type:        FunctionTypeScalar,
		Handler:     func(args []interface{}) (interface{}, error) { return nil, nil },
		Description: "Custom function",
	}
	customRegistry.Register(testFunc)

	// Set as global
	ResetGlobalRegistryWith(customRegistry)

	// Verify custom function exists
	_, exists := GetGlobal("custom_func")
	if !exists {
		t.Error("custom function should exist after ResetGlobalRegistryWith")
	}

	// Test with nil (should not change)
	ResetGlobalRegistryWith(nil)
	_, exists = GetGlobal("custom_func")
	if !exists {
		t.Error("function should still exist after ResetGlobalRegistryWith(nil)")
	}
}

func TestFunctionRegistry_Concurrency(t *testing.T) {
	registry := NewFunctionRegistry()

	const numGoroutines = 100
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			name := string(rune('a' + id%26))
			info := &FunctionInfo{
				Name:    name,
				Type:    FunctionTypeScalar,
				Handler: func(args []interface{}) (interface{}, error) { return id, nil },
			}
			_ = registry.Register(info)
			_, _ = registry.Get(name)
		}(i)
	}

	wg.Wait()
}

func TestFunctionRegistry_Unregister(t *testing.T) {
	registry := NewFunctionRegistry()

	testFunc := &FunctionInfo{
		Name:        "test_unregister",
		Type:        FunctionTypeScalar,
		Handler:     func(args []interface{}) (interface{}, error) { return nil, nil },
		Description: "Test function",
	}

	// Register and verify
	registry.Register(testFunc)
	_, exists := registry.Get("test_unregister")
	if !exists {
		t.Fatal("function should exist after registration")
	}

	// Unregister and verify
	result := registry.Unregister("test_unregister")
	if !result {
		t.Error("Unregister should return true for existing function")
	}

	_, exists = registry.Get("test_unregister")
	if exists {
		t.Error("function should not exist after unregistration")
	}

	// Unregister non-existent should return false
	result = registry.Unregister("non_existent")
	if result {
		t.Error("Unregister should return false for non-existent function")
	}
}

func TestFunctionRegistry_List(t *testing.T) {
	registry := NewFunctionRegistry()

	// Register multiple functions
	for i := 0; i < 5; i++ {
		info := &FunctionInfo{
			Name:     string(rune('a' + i)),
			Type:     FunctionTypeScalar,
			Handler:  func(args []interface{}) (interface{}, error) { return nil, nil },
			Category: "test",
		}
		registry.Register(info)
	}

	list := registry.List()
	if len(list) != 5 {
		t.Errorf("expected 5 functions, got %d", len(list))
	}
}

func TestFunctionRegistry_ListByCategory(t *testing.T) {
	registry := NewFunctionRegistry()

	// Register functions with different categories
	for i := 0; i < 3; i++ {
		info := &FunctionInfo{
			Name:     "math_" + string(rune('a'+i)),
			Type:     FunctionTypeScalar,
			Handler:  func(args []interface{}) (interface{}, error) { return nil, nil },
			Category: "math",
		}
		registry.Register(info)
	}

	for i := 0; i < 2; i++ {
		info := &FunctionInfo{
			Name:     "string_" + string(rune('a'+i)),
			Type:     FunctionTypeScalar,
			Handler:  func(args []interface{}) (interface{}, error) { return nil, nil },
			Category: "string",
		}
		registry.Register(info)
	}

	mathFuncs := registry.ListByCategory("math")
	if len(mathFuncs) != 3 {
		t.Errorf("expected 3 math functions, got %d", len(mathFuncs))
	}

	stringFuncs := registry.ListByCategory("string")
	if len(stringFuncs) != 2 {
		t.Errorf("expected 2 string functions, got %d", len(stringFuncs))
	}
}

func TestFunctionRegistry_Exists(t *testing.T) {
	registry := NewFunctionRegistry()

	testFunc := &FunctionInfo{
		Name:    "exists_test",
		Type:    FunctionTypeScalar,
		Handler: func(args []interface{}) (interface{}, error) { return nil, nil },
	}

	if registry.Exists("exists_test") {
		t.Error("function should not exist before registration")
	}

	registry.Register(testFunc)

	if !registry.Exists("exists_test") {
		t.Error("function should exist after registration")
	}
}

func TestFunctionRegistry_RegisterNil(t *testing.T) {
	registry := NewFunctionRegistry()

	err := registry.Register(nil)
	if err == nil {
		t.Error("should return error for nil function info")
	}
}

func TestFunctionRegistry_RegisterEmptyName(t *testing.T) {
	registry := NewFunctionRegistry()

	err := registry.Register(&FunctionInfo{
		Name:    "",
		Type:    FunctionTypeScalar,
		Handler: func(args []interface{}) (interface{}, error) { return nil, nil },
	})
	if err == nil {
		t.Error("should return error for empty function name")
	}
}

func TestFunctionRegistry_RegisterNilHandler(t *testing.T) {
	registry := NewFunctionRegistry()

	err := registry.Register(&FunctionInfo{
		Name:    "test",
		Type:    FunctionTypeScalar,
		Handler: nil,
	})
	if err == nil {
		t.Error("should return error for nil handler")
	}
}
