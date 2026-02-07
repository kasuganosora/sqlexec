package optimizer

import (
	"fmt"
	"strings"
	"sync"
)

// DefaultVariableManager 是 VariableManager 接口的默认实现
type DefaultVariableManager struct {
	mu        sync.RWMutex
	variables map[string]interface{}
}

// NewDefaultVariableManager 创建默认的变量管理器
func NewDefaultVariableManager() *DefaultVariableManager {
	vm := &DefaultVariableManager{
		variables: make(map[string]interface{}),
	}
	vm.initDefaultVariables()
	return vm
}

// initDefaultVariables 初始化默认的系统变量
func (vm *DefaultVariableManager) initDefaultVariables() {
	// 系统变量
	vm.variables["VERSION"] = "8.0.0-sqlexec"
	vm.variables["VERSION_COMMENT"] = "sqlexec MySQL-compatible database"
	vm.variables["PORT"] = 3307
	vm.variables["HOSTNAME"] = "localhost"
	vm.variables["DATADIR"] = "/var/lib/mysql"
	vm.variables["SERVER_ID"] = 1

	// 常用变量
	vm.variables["AUTOCOMMIT"] = "ON"
	vm.variables["SQL_MODE"] = "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES"
	vm.variables["CHARACTER_SET_CLIENT"] = "utf8mb4"
	vm.variables["CHARACTER_SET_CONNECTION"] = "utf8mb4"
	vm.variables["CHARACTER_SET_DATABASE"] = "utf8mb4"
	vm.variables["CHARACTER_SET_RESULTS"] = "utf8mb4"
	vm.variables["CHARACTER_SET_SERVER"] = "utf8mb4"
	vm.variables["COLLATION_CONNECTION"] = "utf8mb4_unicode_ci"
	vm.variables["COLLATION_DATABASE"] = "utf8mb4_unicode_ci"
	vm.variables["COLLATION_SERVER"] = "utf8mb4_unicode_ci"

	// 时区
	vm.variables["TIME_ZONE"] = "SYSTEM"
	vm.variables["SYSTEM_TIME_ZONE"] = "UTC"
}

// GetVariable 获取变量的值
func (vm *DefaultVariableManager) GetVariable(name string) (interface{}, bool) {
	vm.mu.RLock()
	defer vm.mu.RUnlock()

	// 标准化变量名（大写，去除空格）
	varName := normalizeVariableName(name)

	// 处理 @@ 前缀
	varName = strings.TrimPrefix(varName, "@@")
	varName = strings.TrimPrefix(varName, "GLOBAL.")
	varName = strings.TrimPrefix(varName, "SESSION.")
	varName = strings.TrimPrefix(varName, "LOCAL.")

	value, exists := vm.variables[varName]
	return value, exists
}

// SetVariable 设置变量的值
func (vm *DefaultVariableManager) SetVariable(name string, value interface{}) error {
	vm.mu.Lock()
	defer vm.mu.Unlock()

	// 标准化变量名
	varName := normalizeVariableName(name)
	varName = strings.TrimPrefix(varName, "@@")
	varName = strings.TrimPrefix(varName, "GLOBAL.")
	varName = strings.TrimPrefix(varName, "SESSION.")
	varName = strings.TrimPrefix(varName, "LOCAL.")

	// 验证变量是否可以设置
	if !vm.isVariableModifiable(varName) {
		return fmt.Errorf("variable %s cannot be modified", varName)
	}

	vm.variables[varName] = value
	return nil
}

// ListVariables 返回所有变量
func (vm *DefaultVariableManager) ListVariables() map[string]interface{} {
	vm.mu.RLock()
	defer vm.mu.RUnlock()

	// 创建副本
	result := make(map[string]interface{}, len(vm.variables))
	for k, v := range vm.variables {
		result[k] = v
	}
	return result
}

// GetVariableNames 返回所有变量名
func (vm *DefaultVariableManager) GetVariableNames() []string {
	vm.mu.RLock()
	defer vm.mu.RUnlock()

	names := make([]string, 0, len(vm.variables))
	for name := range vm.variables {
		names = append(names, name)
	}
	return names
}

// isVariableModifiable 检查变量是否可以修改
func (vm *DefaultVariableManager) isVariableModifiable(name string) bool {
	// 只读变量列表
	readOnlyVars := []string{
		"VERSION",
		"VERSION_COMMENT",
		"HOSTNAME",
		"SERVER_ID",
	}

	for _, ro := range readOnlyVars {
		if strings.EqualFold(name, ro) {
			return false
		}
	}
	return true
}

// normalizeVariableName 标准化变量名
func normalizeVariableName(name string) string {
	// 去除前后空格
	name = strings.TrimSpace(name)
	// 转换为大写
	name = strings.ToUpper(name)
	return name
}

// EvaluateSystemVariable 评估系统变量（用于表达式求值）
func (vm *DefaultVariableManager) EvaluateSystemVariable(varName string) (interface{}, error) {
	// 移除 @@ 前缀
	name := strings.TrimPrefix(varName, "@@")
	name = strings.TrimPrefix(name, "GLOBAL.")
	name = strings.TrimPrefix(name, "SESSION.")
	name = strings.TrimPrefix(name, "LOCAL.")

	value, exists := vm.GetVariable(name)
	if !exists {
		return nil, fmt.Errorf("unknown system variable: %s", name)
	}
	return value, nil
}

// SessionVariableManager 是会话变量管理器
type SessionVariableManager struct {
	global   *DefaultVariableManager
	session  map[string]interface{}
	mu       sync.RWMutex
}

// NewSessionVariableManager 创建会话变量管理器
func NewSessionVariableManager(global *DefaultVariableManager) *SessionVariableManager {
	return &SessionVariableManager{
		global:  global,
		session: make(map[string]interface{}),
	}
}

// GetVariable 获取变量值（优先从会话变量获取）
func (svm *SessionVariableManager) GetVariable(name string) (interface{}, bool) {
	// 标准化变量名
	varName := normalizeVariableName(name)
	varName = strings.TrimPrefix(varName, "@@")
	
	// 检查是否是 GLOBAL. 前缀
	if strings.HasPrefix(name, "@@GLOBAL.") {
		return svm.global.GetVariable(varName)
	}
	
	varName = strings.TrimPrefix(varName, "SESSION.")
	varName = strings.TrimPrefix(varName, "LOCAL.")

	svm.mu.RLock()
	defer svm.mu.RUnlock()

	// 先查会话变量
	if value, exists := svm.session[varName]; exists {
		return value, true
	}

	// 再查全局变量
	return svm.global.GetVariable(varName)
}

// SetVariable 设置变量值
func (svm *SessionVariableManager) SetVariable(name string, value interface{}) error {
	// 标准化变量名
	varName := normalizeVariableName(name)
	
	// 检查是否是 GLOBAL. 前缀
	if strings.HasPrefix(varName, "@@GLOBAL.") || strings.HasPrefix(name, "GLOBAL.") {
		return svm.global.SetVariable(varName, value)
	}
	
	varName = strings.TrimPrefix(varName, "@@")
	varName = strings.TrimPrefix(varName, "SESSION.")
	varName = strings.TrimPrefix(varName, "LOCAL.")

	svm.mu.Lock()
	defer svm.mu.Unlock()

	svm.session[varName] = value
	return nil
}

// ListVariables 返回所有变量（合并会话和全局）
func (svm *SessionVariableManager) ListVariables() map[string]interface{} {
	svm.mu.RLock()
	defer svm.mu.RUnlock()

	// 从全局变量开始
	result := svm.global.ListVariables()

	// 用会话变量覆盖
	for k, v := range svm.session {
		result[k] = v
	}

	return result
}

// GetVariableNames 返回所有变量名
func (svm *SessionVariableManager) GetVariableNames() []string {
	// 合并全局和会话变量名
	globalNames := svm.global.GetVariableNames()
	
	svm.mu.RLock()
	defer svm.mu.RUnlock()

	nameSet := make(map[string]bool)
	for _, name := range globalNames {
		nameSet[name] = true
	}
	for name := range svm.session {
		nameSet[name] = true
	}

	names := make([]string, 0, len(nameSet))
	for name := range nameSet {
		names = append(names, name)
	}
	return names
}

// Ensure DefaultVariableManager implements the interface
// This will be checked when we create the interface in executor package
