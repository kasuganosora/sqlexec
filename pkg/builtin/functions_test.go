package builtin

import (
	"testing"
)

func TestNewFunctionRegistry(t *testing.T) {
	registry := NewFunctionRegistry()
	if registry == nil {
		t.Fatal("NewFunctionRegistry returned nil")
	}
	if registry.functions == nil {
		t.Error("functions map should be initialized")
	}
}

func TestRegister(t *testing.T) {
	registry := NewFunctionRegistry()

	info := &FunctionInfo{
		Name:        "test_func",
		Type:        FunctionTypeScalar,
		Description: "Test function",
		Handler: func(args []interface{}) (interface{}, error) {
			return "test", nil
		},
	}

	err := registry.Register(info)
	if err != nil {
		t.Errorf("Register() error = %v", err)
	}

	// 验证函数已注册
	fn, exists := registry.Get("test_func")
	if !exists {
		t.Error("Function should be registered")
	}
	if fn.Name != "test_func" {
		t.Error("Function name mismatch")
	}
}

func TestRegisterErrors(t *testing.T) {
	registry := NewFunctionRegistry()

	tests := []struct {
		name        string
		info        *FunctionInfo
		expectError bool
	}{
		{"Nil info", nil, true},
		{"Empty name", &FunctionInfo{Name: "", Handler: func(args []interface{}) (interface{}, error) { return nil, nil }}, true},
		{"Nil handler", &FunctionInfo{Name: "test", Handler: nil}, true},
		{"Valid info", &FunctionInfo{
			Name: "test_func",
			Type: FunctionTypeScalar,
			Handler: func(args []interface{}) (interface{}, error) {
				return "test", nil
			},
		}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := registry.Register(tt.info)
			if (err != nil) != tt.expectError {
				t.Errorf("Register() error = %v, expectError %v", err, tt.expectError)
			}
		})
	}
}

func TestGet(t *testing.T) {
	registry := NewFunctionRegistry()

	info := &FunctionInfo{
		Name: "test_func",
		Type: FunctionTypeScalar,
		Handler: func(args []interface{}) (interface{}, error) {
			return "test", nil
		},
	}
	registry.Register(info)

	// 测试获取存在的函数
	fn, exists := registry.Get("test_func")
	if !exists {
		t.Error("Should find registered function")
	}
	if fn.Name != "test_func" {
		t.Error("Function name mismatch")
	}

	// 测试获取不存在的函数
	_, exists = registry.Get("nonexistent")
	if exists {
		t.Error("Should not find nonexistent function")
	}
}

func TestList(t *testing.T) {
	registry := NewFunctionRegistry()

	// 注册几个函数
	names := []string{"func1", "func2", "func3"}
	for _, name := range names {
		registry.Register(&FunctionInfo{
			Name: name,
			Type: FunctionTypeScalar,
			Handler: func(args []interface{}) (interface{}, error) {
				return nil, nil
			},
		})
	}

	list := registry.List()
	if len(list) != len(names) {
		t.Errorf("List() returned %d functions, want %d", len(list), len(names))
	}
}

func TestListByCategory(t *testing.T) {
	registry := NewFunctionRegistry()

	// 注册不同类别的函数
	registry.Register(&FunctionInfo{
		Name:     "math_func",
		Category: "math",
		Type:     FunctionTypeScalar,
		Handler:  func(args []interface{}) (interface{}, error) { return nil, nil },
	})

	registry.Register(&FunctionInfo{
		Name:     "string_func",
		Category: "string",
		Type:     FunctionTypeScalar,
		Handler:  func(args []interface{}) (interface{}, error) { return nil, nil },
	})

	registry.Register(&FunctionInfo{
		Name:     "another_math_func",
		Category: "math",
		Type:     FunctionTypeScalar,
		Handler:  func(args []interface{}) (interface{}, error) { return nil, nil },
	})

	// 测试列出 math 类别的函数
	mathFuncs := registry.ListByCategory("math")
	if len(mathFuncs) != 2 {
		t.Errorf("ListByCategory('math') returned %d functions, want 2", len(mathFuncs))
	}

	// 测试列出 string 类别的函数
	stringFuncs := registry.ListByCategory("string")
	if len(stringFuncs) != 1 {
		t.Errorf("ListByCategory('string') returned %d functions, want 1", len(stringFuncs))
	}

	// 测试不存在的类别
	empty := registry.ListByCategory("nonexistent")
	if len(empty) != 0 {
		t.Error("ListByCategory('nonexistent') should return empty list")
	}
}

func TestExists(t *testing.T) {
	registry := NewFunctionRegistry()

	registry.Register(&FunctionInfo{
		Name: "test_func",
		Type: FunctionTypeScalar,
		Handler: func(args []interface{}) (interface{}, error) {
			return nil, nil
		},
	})

	if !registry.Exists("test_func") {
		t.Error("Exists() should return true for registered function")
	}

	if registry.Exists("nonexistent") {
		t.Error("Exists() should return false for nonexistent function")
	}
}

func TestUnregister(t *testing.T) {
	registry := NewFunctionRegistry()

	registry.Register(&FunctionInfo{
		Name: "test_func",
		Type: FunctionTypeScalar,
		Handler: func(args []interface{}) (interface{}, error) {
			return nil, nil
		},
	})

	// 注销存在的函数
	removed := registry.Unregister("test_func")
	if !removed {
		t.Error("Unregister() should return true for existing function")
	}

	if registry.Exists("test_func") {
		t.Error("Function should be unregistered")
	}

	// 注销不存在的函数
	removed = registry.Unregister("nonexistent")
	if removed {
		t.Error("Unregister() should return false for nonexistent function")
	}
}

func TestGlobalRegistry(t *testing.T) {
	// 测试全局注册表
	info := &FunctionInfo{
		Name: "global_test_func",
		Type: FunctionTypeScalar,
		Handler: func(args []interface{}) (interface{}, error) {
			return "test", nil
		},
	}

	err := RegisterGlobal(info)
	if err != nil {
		t.Errorf("RegisterGlobal() error = %v", err)
	}

	// 从全局注册表获取
	fn, exists := GetGlobal("global_test_func")
	if !exists {
		t.Error("Should find function in global registry")
	}
	if fn.Name != "global_test_func" {
		t.Error("Function name mismatch")
	}
}

func TestGetGlobalRegistry(t *testing.T) {
	registry := GetGlobalRegistry()
	if registry == nil {
		t.Fatal("GetGlobalRegistry() returned nil")
	}

	// 验证是同一个注册表
	if registry != GetGlobalRegistry() {
		t.Error("GetGlobalRegistry() should return the same instance")
	}
}

func TestFunctionTypeConstants(t *testing.T) {
	types := []struct {
		name  string
		value FunctionType
	}{
		{"Scalar", FunctionTypeScalar},
		{"Aggregate", FunctionTypeAggregate},
		{"Window", FunctionTypeWindow},
	}

	for _, tt := range types {
		if tt.value == 0 && tt.name != "Scalar" {
			t.Errorf("FunctionType %s = %d", tt.name, tt.value)
		}
	}
}

func TestFunctionInfo(t *testing.T) {
	info := &FunctionInfo{
		Name:        "test",
		Type:        FunctionTypeScalar,
		Description: "Test function",
		Example:     "TEST() -> result",
		Category:    "test",
		Signatures: []FunctionSignature{
			{
				Name:       "test",
				ReturnType: "string",
				ParamTypes: []string{"string"},
				Variadic:   false,
			},
		},
		Handler: func(args []interface{}) (interface{}, error) {
			return nil, nil
		},
	}

	if info.Name != "test" {
		t.Error("Name mismatch")
	}
	if info.Type != FunctionTypeScalar {
		t.Error("Type mismatch")
	}
	if len(info.Signatures) != 1 {
		t.Error("Should have one signature")
	}
	if info.Signatures[0].Variadic {
		t.Error("Signature should not be variadic")
	}
}

func TestConcurrentRegistryAccess(t *testing.T) {
	registry := NewFunctionRegistry()
	done := make(chan bool)

	// 并发注册函数
	for i := 0; i < 10; i++ {
		go func(n int) {
			name := "func_" + string(rune('0'+n))
			registry.Register(&FunctionInfo{
				Name: name,
				Type: FunctionTypeScalar,
				Handler: func(args []interface{}) (interface{}, error) {
					return nil, nil
				},
			})
			done <- true
		}(i)
	}

	// 等待所有goroutine完成
	for i := 0; i < 10; i++ {
		<-done
	}

	// 验证函数已注册
	list := registry.List()
	if len(list) != 10 {
		t.Errorf("Expected 10 functions, got %d", len(list))
	}
}

func TestFunctionSignature(t *testing.T) {
	sig := FunctionSignature{
		Name:       "test_func",
		ReturnType: "string",
		ParamTypes: []string{"string", "integer"},
		Variadic:   false,
	}

	if sig.Name != "test_func" {
		t.Error("Name mismatch")
	}
	if sig.ReturnType != "string" {
		t.Error("ReturnType mismatch")
	}
	if len(sig.ParamTypes) != 2 {
		t.Error("Should have 2 parameter types")
	}
	if sig.Variadic {
		t.Error("Should not be variadic")
	}
}

func TestOverwriteFunction(t *testing.T) {
	registry := NewFunctionRegistry()

	// 注册第一个版本
	info1 := &FunctionInfo{
		Name:        "test_func",
		Type:        FunctionTypeScalar,
		Description: "First version",
		Handler: func(args []interface{}) (interface{}, error) {
			return "v1", nil
		},
	}
	registry.Register(info1)

	// 注册第二个版本（覆盖）
	info2 := &FunctionInfo{
		Name:        "test_func",
		Type:        FunctionTypeScalar,
		Description: "Second version",
		Handler: func(args []interface{}) (interface{}, error) {
			return "v2", nil
		},
	}
	registry.Register(info2)

	// 验证已被覆盖
	fn, _ := registry.Get("test_func")
	if fn.Description != "Second version" {
		t.Error("Function should be overwritten")
	}

	result, _ := fn.Handler(nil)
	if result != "v2" {
		t.Error("Handler should be updated")
	}
}
