package builtin

import (
	"fmt"
	"sync"
)

// FunctionCategory 函数类别
type FunctionCategory string

const (
	CategoryMath      FunctionCategory = "math"
	CategoryString    FunctionCategory = "string"
	CategoryDate      FunctionCategory = "date"
	CategoryAggregate FunctionCategory = "aggregate"
	CategoryControl   FunctionCategory = "control"
	CategoryJSON      FunctionCategory = "json"
	CategorySystem    FunctionCategory = "system"
	CategoryUser      FunctionCategory = "user"
)

// FunctionScope 函数作用域
type FunctionScope string

const (
	ScopeGlobal FunctionScope = "global" // 全局函数
	ScopeUser   FunctionScope = "user"   // 用户自定义函数
	ScopeSession FunctionScope = "session" // 会话函数
)

// FunctionMetadata 函数元数据
type FunctionMetadata struct {
	Name        string            // 函数名（小写）
	DisplayName string            // 显示名称
	Type        FunctionType      // 函数类型：标量/聚合/窗口
	Scope       FunctionScope     // 作用域
	Category    FunctionCategory  // 类别
	Variadic    bool              // 是否可变参数
	MinArgs     int               // 最小参数数
	MaxArgs     int               // 最大参数数（-1表示无限制）
	Handler     FunctionHandle    // 处理函数
	AggregateHandler AggregateHandle  // 聚合函数处理
	AggregateResult   AggregateResult    // 聚合函数结果
	Description string            // 描述
	Examples    []string          // 示例
	Parameters  []FunctionParam   // 参数定义
	ReturnType  string            // 返回类型
	Tags        []string          // 标签
}

// FunctionParam 函数参数定义
type FunctionParam struct {
	Name        string // 参数名
	Type        string // 参数类型
	Description string // 描述
	Required    bool   // 是否必需
	Default     interface{} // 默认值
}

// FunctionFilter 函数过滤器
type FunctionFilter struct {
	Category   *FunctionCategory // 类别过滤
	Type       *FunctionType   // 类型过滤
	Scope      *FunctionScope  // 作用域过滤
	MinArgs    *int           // 最小参数数过滤
	MaxArgs    *int           // 最大参数数过滤
	NamePrefix string         // 名称前缀过滤
}

// FunctionRegistry 函数注册表（可扩展版本）
type FunctionRegistryExt struct {
	mu              sync.RWMutex
	scalars         map[string]*FunctionMetadata // 标量函数
	aggregates      map[string]*FunctionMetadata // 聚合函数
	userFunctions   map[string]*FunctionMetadata // 用户自定义函数
	sessionFunctions map[string]*FunctionMetadata // 会话函数
	aliases         map[string]string           // 函数别名
}

// NewFunctionRegistryExt 创建扩展函数注册表
func NewFunctionRegistryExt() *FunctionRegistryExt {
	return &FunctionRegistryExt{
		scalars:         make(map[string]*FunctionMetadata),
		aggregates:      make(map[string]*FunctionMetadata),
		userFunctions:   make(map[string]*FunctionMetadata),
		sessionFunctions: make(map[string]*FunctionMetadata),
		aliases:         make(map[string]string),
	}
}

// RegisterScalar 注册标量函数
func (r *FunctionRegistryExt) RegisterScalar(meta *FunctionMetadata) error {
	return r.registerFunction(meta, false)
}

// RegisterAggregate 注册聚合函数
func (r *FunctionRegistryExt) RegisterAggregate(meta *FunctionMetadata) error {
	if meta.AggregateHandler == nil || meta.AggregateResult == nil {
		return fmt.Errorf("aggregate function must have handler and result function")
	}
	return r.registerFunction(meta, true)
}

// RegisterUserFunction 注册用户自定义函数
func (r *FunctionRegistryExt) RegisterUserFunction(meta *FunctionMetadata) error {
	meta.Scope = ScopeUser
	return r.registerFunction(meta, false)
}

// RegisterSessionFunction 注册会话函数
func (r *FunctionRegistryExt) RegisterSessionFunction(meta *FunctionMetadata) error {
	meta.Scope = ScopeSession
	return r.registerFunction(meta, false)
}

// registerFunction 内部注册函数
func (r *FunctionRegistryExt) registerFunction(meta *FunctionMetadata, isAggregate bool) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// 验证元数据
	if meta.Name == "" {
		return fmt.Errorf("function name cannot be empty")
	}
	if meta.Handler == nil && meta.AggregateHandler == nil {
		return fmt.Errorf("function handler cannot be nil")
	}

	// 标准化函数名（小写）
	normalizedName := normalizeName(meta.Name)
	meta.Name = normalizedName

	// 检查参数范围
	if meta.MinArgs < 0 {
		meta.MinArgs = 0
	}
	if meta.MaxArgs < -1 {
		meta.MaxArgs = -1
	}

	// 注册到相应的存储
	if meta.Scope == ScopeUser {
		if _, exists := r.userFunctions[normalizedName]; exists {
			return fmt.Errorf("user function %s already exists", normalizedName)
		}
		r.userFunctions[normalizedName] = meta
	} else if meta.Scope == ScopeSession {
		if _, exists := r.sessionFunctions[normalizedName]; exists {
			return fmt.Errorf("session function %s already exists", normalizedName)
		}
		r.sessionFunctions[normalizedName] = meta
	} else {
		if isAggregate {
			if _, exists := r.aggregates[normalizedName]; exists {
				return fmt.Errorf("aggregate function %s already exists", normalizedName)
			}
			r.aggregates[normalizedName] = meta
		} else {
			if _, exists := r.scalars[normalizedName]; exists {
				return fmt.Errorf("scalar function %s already exists", normalizedName)
			}
			r.scalars[normalizedName] = meta
		}
	}

	// 注册别名
	if meta.DisplayName != "" && meta.DisplayName != normalizedName {
		r.aliases[meta.DisplayName] = normalizedName
	}

	return nil
}

// Get 获取函数
func (r *FunctionRegistryExt) Get(name string) (*FunctionMetadata, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	normalizedName := normalizeName(name)

	// 按优先级查找：会话 > 用户 > 全局
	if meta, ok := r.sessionFunctions[normalizedName]; ok {
		return meta, true
	}
	if meta, ok := r.userFunctions[normalizedName]; ok {
		return meta, true
	}
	if meta, ok := r.scalars[normalizedName]; ok {
		return meta, true
	}
	if meta, ok := r.aggregates[normalizedName]; ok {
		return meta, true
	}

	// 查找别名
	if aliasName, ok := r.aliases[normalizedName]; ok {
		return r.Get(aliasName)
	}

	return nil, false
}

// GetScalar 获取标量函数
func (r *FunctionRegistryExt) GetScalar(name string) (*FunctionMetadata, bool) {
	meta, ok := r.Get(name)
	if !ok {
		return nil, false
	}
	if meta.Type != FunctionTypeScalar {
		return nil, false
	}
	return meta, true
}

// GetAggregate 获取聚合函数
func (r *FunctionRegistryExt) GetAggregate(name string) (*FunctionMetadata, bool) {
	meta, ok := r.Get(name)
	if !ok {
		return nil, false
	}
	if meta.Type != FunctionTypeAggregate {
		return nil, false
	}
	return meta, true
}

// Exists 检查函数是否存在
func (r *FunctionRegistryExt) Exists(name string) bool {
	_, ok := r.Get(name)
	return ok
}

// Unregister 注销函数
func (r *FunctionRegistryExt) Unregister(name string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	normalizedName := normalizeName(name)

	// 尝试从各个存储中删除
	if _, ok := r.scalars[normalizedName]; ok {
		delete(r.scalars, normalizedName)
		return true
	}
	if _, ok := r.aggregates[normalizedName]; ok {
		delete(r.aggregates, normalizedName)
		return true
	}
	if _, ok := r.userFunctions[normalizedName]; ok {
		delete(r.userFunctions, normalizedName)
		return true
	}
	if _, ok := r.sessionFunctions[normalizedName]; ok {
		delete(r.sessionFunctions, normalizedName)
		return true
	}

	return false
}

// ClearUserFunctions 清除所有用户函数
func (r *FunctionRegistryExt) ClearUserFunctions() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.userFunctions = make(map[string]*FunctionMetadata)
}

// ClearSessionFunctions 清除所有会话函数
func (r *FunctionRegistryExt) ClearSessionFunctions() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.sessionFunctions = make(map[string]*FunctionMetadata)
}

// List 列出所有函数
func (r *FunctionRegistryExt) List() []*FunctionMetadata {
	r.mu.RLock()
	defer r.mu.RUnlock()

	list := make([]*FunctionMetadata, 0)
	
	// 按优先级添加：会话 > 用户 > 全局
	for _, meta := range r.sessionFunctions {
		list = append(list, meta)
	}
	for _, meta := range r.userFunctions {
		list = append(list, meta)
	}
	for _, meta := range r.scalars {
		list = append(list, meta)
	}
	for _, meta := range r.aggregates {
		list = append(list, meta)
	}
	
	return list
}

// ListByCategory 按类别列出函数
func (r *FunctionRegistryExt) ListByCategory(category FunctionCategory) []*FunctionMetadata {
	filter := &FunctionFilter{
		Category: &category,
	}
	return r.ListWithFilter(filter)
}

// ListByType 按类型列出函数
func (r *FunctionRegistryExt) ListByType(fnType FunctionType) []*FunctionMetadata {
	filter := &FunctionFilter{
		Type: &fnType,
	}
	return r.ListWithFilter(filter)
}

// ListUserFunctions 列出用户函数
func (r *FunctionRegistryExt) ListUserFunctions() []*FunctionMetadata {
	r.mu.RLock()
	defer r.mu.RUnlock()

	list := make([]*FunctionMetadata, 0, len(r.userFunctions))
	for _, meta := range r.userFunctions {
		list = append(list, meta)
	}
	return list
}

// ListWithFilter 使用过滤器列出函数
func (r *FunctionRegistryExt) ListWithFilter(filter *FunctionFilter) []*FunctionMetadata {
	r.mu.RLock()
	defer r.mu.RUnlock()

	list := make([]*FunctionMetadata, 0)
	
	// 检查所有存储
	stores := []map[string]*FunctionMetadata{
		r.sessionFunctions,
		r.userFunctions,
		r.scalars,
		r.aggregates,
	}
	
	for _, store := range stores {
		for _, meta := range store {
			if r.matchFilter(meta, filter) {
				list = append(list, meta)
			}
		}
	}
	
	return list
}

// matchFilter 检查函数是否匹配过滤器
func (r *FunctionRegistryExt) matchFilter(meta *FunctionMetadata, filter *FunctionFilter) bool {
	if filter == nil {
		return true
	}

	// 类别过滤
	if filter.Category != nil && meta.Category != *filter.Category {
		return false
	}

	// 类型过滤
	if filter.Type != nil && meta.Type != *filter.Type {
		return false
	}

	// 作用域过滤
	if filter.Scope != nil && meta.Scope != *filter.Scope {
		return false
	}

	// 最小参数数过滤
	if filter.MinArgs != nil && meta.MaxArgs >= 0 && meta.MaxArgs < *filter.MinArgs {
		return false
	}

	// 最大参数数过滤
	if filter.MaxArgs != nil && meta.MinArgs > *filter.MaxArgs {
		return false
	}

	// 名称前缀过滤
	if filter.NamePrefix != "" && len(filter.NamePrefix) > 0 {
		if len(meta.Name) < len(filter.NamePrefix) {
			return false
		}
		if meta.Name[:len(filter.NamePrefix)] != filter.NamePrefix {
			return false
		}
	}

	return true
}

// Count 统计函数总数
func (r *FunctionRegistryExt) Count() int {
	return len(r.List())
}

// CountByCategory 按类别统计函数数
func (r *FunctionRegistryExt) CountByCategory(category FunctionCategory) int {
	return len(r.ListByCategory(category))
}

// Search 搜索函数
func (r *FunctionRegistryExt) Search(keyword string) []*FunctionMetadata {
	keyword = normalizeName(keyword)
	
	r.mu.RLock()
	defer r.mu.RUnlock()

	list := make([]*FunctionMetadata, 0)
	
	// 检查所有存储
	stores := []map[string]*FunctionMetadata{
		r.sessionFunctions,
		r.userFunctions,
		r.scalars,
		r.aggregates,
	}
	
	for _, store := range stores {
		for _, meta := range store {
			// 搜索名称
			if contains(meta.Name, keyword) {
				list = append(list, meta)
				continue
			}
			// 搜索显示名称
			if contains(meta.DisplayName, keyword) {
				list = append(list, meta)
				continue
			}
			// 搜索描述
			if contains(meta.Description, keyword) {
				list = append(list, meta)
				continue
			}
		}
	}
	
	return list
}

// normalizeName 标准化函数名
func normalizeName(name string) string {
	// 转换为小写
	return toLowerCase(name)
}

// toLowerCase 转小写
func toLowerCase(s string) string {
	result := ""
	for _, c := range s {
		if c >= 'A' && c <= 'Z' {
			result += string(c + 32)
		} else {
			result += string(c)
		}
	}
	return result
}

// contains 字符串包含
func contains(s, substr string) bool {
	return len(s) >= len(substr) && findSubstring(s, substr) >= 0
}

// findSubstring 查找子字符串
func findSubstring(s, substr string) int {
	if len(substr) == 0 {
		return 0
	}
	if len(s) < len(substr) {
		return -1
	}
	
	for i := 0; i <= len(s)-len(substr); i++ {
		match := true
		for j := 0; j < len(substr); j++ {
			if s[i+j] != substr[j] {
				match = false
				break
			}
		}
		if match {
			return i
		}
	}
	return -1
}

// AddAlias 添加函数别名
func (r *FunctionRegistryExt) AddAlias(alias, name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	normalizedName := normalizeName(name)
	normalizedAlias := normalizeName(alias)

	// 检查目标函数是否存在
	if !r.existsInternal(normalizedName) {
		return fmt.Errorf("function %s not found", normalizedName)
	}

	r.aliases[normalizedAlias] = normalizedName
	return nil
}

// existsInternal 内部检查函数是否存在
func (r *FunctionRegistryExt) existsInternal(name string) bool {
	if _, ok := r.scalars[name]; ok {
		return true
	}
	if _, ok := r.aggregates[name]; ok {
		return true
	}
	if _, ok := r.userFunctions[name]; ok {
		return true
	}
	if _, ok := r.sessionFunctions[name]; ok {
		return true
	}
	return false
}

// RemoveAlias 删除别名
func (r *FunctionRegistryExt) RemoveAlias(alias string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	normalizedAlias := normalizeName(alias)
	delete(r.aliases, normalizedAlias)
}

// GetAliases 获取所有别名
func (r *FunctionRegistryExt) GetAliases() map[string]string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	aliases := make(map[string]string)
	for k, v := range r.aliases {
		aliases[k] = v
	}
	return aliases
}
