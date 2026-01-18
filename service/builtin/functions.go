package builtin

import (
	"fmt"
	"sync"
)

// FunctionType 函数类型
type FunctionType int

const (
	FunctionTypeScalar FunctionType = iota // 标量函数
	FunctionTypeAggregate                  // 聚合函数
	FunctionTypeWindow                    // 窗口函数
)

// FunctionSignature 函数签名
type FunctionSignature struct {
	Name       string
	ReturnType string
	ParamTypes []string
	Variadic   bool // 是否可变参数
}

// FunctionHandle 函数处理函数
type FunctionHandle func(args []interface{}) (interface{}, error)

// FunctionInfo 函数信息
type FunctionInfo struct {
	Name        string
	Type        FunctionType
	Signatures  []FunctionSignature
	Handler     FunctionHandle
	Description string
	Example     string
	Category    string // math, string, date, aggregate�?
}

// FunctionRegistry 函数注册�?
type FunctionRegistry struct {
	mu        sync.RWMutex
	functions map[string]*FunctionInfo
}

// NewFunctionRegistry 创建函数注册�?
func NewFunctionRegistry() *FunctionRegistry {
	return &FunctionRegistry{
		functions: make(map[string]*FunctionInfo),
	}
}

// Register 注册函数
func (r *FunctionRegistry) Register(info *FunctionInfo) error {
	if info == nil {
		return fmt.Errorf("function info cannot be nil")
	}
	if info.Name == "" {
		return fmt.Errorf("function name cannot be empty")
	}
	if info.Handler == nil {
		return fmt.Errorf("function handler cannot be nil")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.functions[info.Name] = info
	return nil
}

// Get 获取函数
func (r *FunctionRegistry) Get(name string) (*FunctionInfo, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	info, exists := r.functions[name]
	return info, exists
}

// List 列出所有函�?
func (r *FunctionRegistry) List() []*FunctionInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()

	list := make([]*FunctionInfo, 0, len(r.functions))
	for _, info := range r.functions {
		list = append(list, info)
	}
	return list
}

// ListByCategory 按类别列出函�?
func (r *FunctionRegistry) ListByCategory(category string) []*FunctionInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()

	list := make([]*FunctionInfo, 0)
	for _, info := range r.functions {
		if info.Category == category {
			list = append(list, info)
		}
	}
	return list
}

// Exists 检查函数是否存�?
func (r *FunctionRegistry) Exists(name string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	_, exists := r.functions[name]
	return exists
}

// Unregister 注销函数
func (r *FunctionRegistry) Unregister(name string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.functions[name]; exists {
		delete(r.functions, name)
		return true
	}
	return false
}

// 全局函数注册�?
var globalRegistry = NewFunctionRegistry()

// GetGlobalRegistry 获取全局函数注册�?
func GetGlobalRegistry() *FunctionRegistry {
	return globalRegistry
}

// RegisterGlobal 注册全局函数
func RegisterGlobal(info *FunctionInfo) error {
	return globalRegistry.Register(info)
}

// GetGlobal 获取全局函数
func GetGlobal(name string) (*FunctionInfo, bool) {
	return globalRegistry.Get(name)
}
