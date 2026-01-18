package builtin

import (
	"encoding/json"
	"fmt"
)

// FunctionAPI 函数注册表API
type FunctionAPI struct {
	registry *FunctionRegistryExt
}

// NewFunctionAPI 创建函数API
func NewFunctionAPI() *FunctionAPI {
	return &FunctionAPI{
		registry: NewFunctionRegistryExt(),
	}
}

// GetRegistry 获取底层注册表
func (api *FunctionAPI) GetRegistry() *FunctionRegistryExt {
	return api.registry
}

// ============ 函数注册 API ============

// RegisterFunction 注册函数（通用接口）
func (api *FunctionAPI) RegisterFunction(fn FunctionRegisterFunc) error {
	meta, err := fn()
	if err != nil {
		return fmt.Errorf("function registration failed: %w", err)
	}
	
	if meta.Type == FunctionTypeAggregate {
		return api.registry.RegisterAggregate(meta)
	}
	return api.registry.RegisterScalar(meta)
}

// RegisterScalarFunction 注册标量函数
func (api *FunctionAPI) RegisterScalarFunction(name, displayName, description string,
	handler FunctionHandle, options ...FunctionOption) error {
	
	meta := &FunctionMetadata{
		Name:        name,
		DisplayName:  displayName,
		Type:        FunctionTypeScalar,
		Scope:       ScopeGlobal,
		Handler:     handler,
		Description:  description,
		Parameters:  []FunctionParam{},
		Examples:     []string{},
		Tags:         []string{},
	}
	
	// 应用选项
	for _, opt := range options {
		opt(meta)
	}
	
	return api.registry.RegisterScalar(meta)
}

// RegisterAggregateFunction 注册聚合函数
func (api *FunctionAPI) RegisterAggregateFunction(name, displayName, description string,
	handler AggregateHandle, result AggregateResult, options ...FunctionOption) error {
	
	meta := &FunctionMetadata{
		Name:             name,
		DisplayName:       displayName,
		Type:             FunctionTypeAggregate,
		Scope:            ScopeGlobal,
		AggregateHandler:  handler,
		AggregateResult:    result,
		Description:       description,
		Parameters:       []FunctionParam{},
		Examples:          []string{},
		Tags:             []string{},
	}
	
	// 应用选项
	for _, opt := range options {
		opt(meta)
	}
	
	return api.registry.RegisterAggregate(meta)
}

// RegisterUserFunction 注册用户自定义函数
func (api *FunctionAPI) RegisterUserFunction(name, displayName, description string,
	handler FunctionHandle, options ...FunctionOption) error {
	
	meta := &FunctionMetadata{
		Name:        name,
		DisplayName:  displayName,
		Type:        FunctionTypeScalar,
		Scope:       ScopeUser,
		Handler:     handler,
		Description:  description,
		Parameters:  []FunctionParam{},
		Examples:     []string{},
		Tags:         []string{},
	}
	
	// 应用选项
	for _, opt := range options {
		opt(meta)
	}
	
	return api.registry.RegisterUserFunction(meta)
}

// ============ 函数查询 API ============

// GetFunction 获取函数
func (api *FunctionAPI) GetFunction(name string) (*FunctionMetadata, error) {
	meta, ok := api.registry.Get(name)
	if !ok {
		return nil, fmt.Errorf("function %s not found", name)
	}
	return meta, nil
}

// ListFunctions 列出所有函数
func (api *FunctionAPI) ListFunctions() []*FunctionMetadata {
	return api.registry.List()
}

// ListFunctionsByCategory 按类别列出函数
func (api *FunctionAPI) ListFunctionsByCategory(category FunctionCategory) []*FunctionMetadata {
	return api.registry.ListByCategory(category)
}

// ListFunctionsByType 按类型列出函数
func (api *FunctionAPI) ListFunctionsByType(fnType FunctionType) []*FunctionMetadata {
	return api.registry.ListByType(fnType)
}

// SearchFunctions 搜索函数
func (api *FunctionAPI) SearchFunctions(keyword string) []*FunctionMetadata {
	return api.registry.Search(keyword)
}

// ============ 函数统计 API ============

// CountFunctions 统计函数总数
func (api *FunctionAPI) CountFunctions() int {
	return api.registry.Count()
}

// CountFunctionsByCategory 按类别统计
func (api *FunctionAPI) CountFunctionsByCategory(category FunctionCategory) int {
	return api.registry.CountByCategory(category)
}

// ============ 函数管理 API ============

// UnregisterFunction 注销函数
func (api *FunctionAPI) UnregisterFunction(name string) error {
	if !api.registry.Unregister(name) {
		return fmt.Errorf("function %s not found", name)
	}
	return nil
}

// ClearUserFunctions 清除用户函数
func (api *FunctionAPI) ClearUserFunctions() {
	api.registry.ClearUserFunctions()
}

// ClearSessionFunctions 清除会话函数
func (api *FunctionAPI) ClearSessionFunctions() {
	api.registry.ClearSessionFunctions()
}

// ============ 别名管理 API ============

// AddFunctionAlias 添加函数别名
func (api *FunctionAPI) AddFunctionAlias(alias, name string) error {
	return api.registry.AddAlias(alias, name)
}

// RemoveFunctionAlias 删除别名
func (api *FunctionAPI) RemoveFunctionAlias(alias string) {
	api.registry.RemoveAlias(alias)
}

// GetFunctionAliases 获取所有别名
func (api *FunctionAPI) GetFunctionAliases() map[string]string {
	return api.registry.GetAliases()
}

// ============ 文档 API ============

// GenerateDocumentation 生成函数文档
func (api *FunctionAPI) GenerateDocumentation() string {
	docs := "# 内置函数文档\n\n"
	
	// 按类别分组
	categories := []FunctionCategory{
		CategoryMath,
		CategoryString,
		CategoryDate,
		CategoryAggregate,
		CategoryControl,
		CategoryJSON,
		CategorySystem,
	}
	
	for _, category := range categories {
		functions := api.registry.ListByCategory(category)
		if len(functions) == 0 {
			continue
		}
		
		docs += fmt.Sprintf("## %s\n\n", string(category))
		
		for _, fn := range functions {
			docs += api.generateFunctionDoc(fn)
		}
	}
	
	return docs
}

// GenerateJSON 生成JSON格式文档
func (api *FunctionAPI) GenerateJSON() (string, error) {
	functions := api.registry.List()
	
	data := map[string]interface{}{
		"functions": functions,
		"count":     len(functions),
		"categories": api.getCategoryStats(),
	}
	
	jsonBytes, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return "", err
	}
	
	return string(jsonBytes), nil
}

// generateFunctionDoc 生成单个函数文档
func (api *FunctionAPI) generateFunctionDoc(meta *FunctionMetadata) string {
	docs := fmt.Sprintf("### %s\n\n", meta.DisplayName)
	
	if meta.Description != "" {
		docs += fmt.Sprintf("**描述**: %s\n\n", meta.Description)
	}
	
	docs += fmt.Sprintf("**类型**: %s\n\n", api.getTypeName(meta.Type))
	docs += fmt.Sprintf("**作用域**: %s\n\n", string(meta.Scope))
	docs += fmt.Sprintf("**类别**: %s\n\n", string(meta.Category))
	
	// 参数
	if len(meta.Parameters) > 0 {
		docs += "**参数**:\n\n"
		for i, param := range meta.Parameters {
			required := "可选"
			if param.Required {
				required = "必需"
			}
			docs += fmt.Sprintf("%d. `%s` (%s) - %s [%s]\n", 
				i+1, param.Name, param.Type, param.Description, required)
		}
		docs += "\n"
	}
	
	// 示例
	if len(meta.Examples) > 0 {
		docs += "**示例**:\n\n"
		for _, example := range meta.Examples {
			docs += fmt.Sprintf("```sql\n%s\n```\n\n", example)
		}
	}
	
	// 返回类型
	if meta.ReturnType != "" {
		docs += fmt.Sprintf("**返回类型**: %s\n\n", meta.ReturnType)
	}
	
	docs += "---\n\n"
	return docs
}

// getTypeName 获取类型名称
func (api *FunctionAPI) getTypeName(fnType FunctionType) string {
	switch fnType {
	case FunctionTypeScalar:
		return "标量函数"
	case FunctionTypeAggregate:
		return "聚合函数"
	case FunctionTypeWindow:
		return "窗口函数"
	default:
		return "未知"
	}
}

// getCategoryStats 获取类别统计
func (api *FunctionAPI) getCategoryStats() map[string]int {
	stats := make(map[string]int)
	
	categories := []FunctionCategory{
		CategoryMath,
		CategoryString,
		CategoryDate,
		CategoryAggregate,
		CategoryControl,
		CategoryJSON,
		CategorySystem,
	}
	
	for _, category := range categories {
		stats[string(category)] = api.registry.CountByCategory(category)
	}
	
	return stats
}

// ============ 函数选项 ============

// FunctionOption 函数选项类型
type FunctionOption func(*FunctionMetadata)

// WithCategory 设置类别
func WithCategory(category FunctionCategory) FunctionOption {
	return func(meta *FunctionMetadata) {
		meta.Category = category
	}
}

// WithVariadic 设置可变参数
func WithVariadic() FunctionOption {
	return func(meta *FunctionMetadata) {
		meta.Variadic = true
	}
}

// WithMinArgs 设置最小参数数
func WithMinArgs(min int) FunctionOption {
	return func(meta *FunctionMetadata) {
		meta.MinArgs = min
	}
}

// WithMaxArgs 设置最大参数数
func WithMaxArgs(max int) FunctionOption {
	return func(meta *FunctionMetadata) {
		meta.MaxArgs = max
	}
}

// WithArgRange 设置参数范围
func WithArgRange(min, max int) FunctionOption {
	return func(meta *FunctionMetadata) {
		meta.MinArgs = min
		meta.MaxArgs = max
	}
}

// WithReturnType 设置返回类型
func WithReturnType(returnType string) FunctionOption {
	return func(meta *FunctionMetadata) {
		meta.ReturnType = returnType
	}
}

// WithParameter 添加参数定义
func WithParameter(name, typeName, description string, required bool) FunctionOption {
	return func(meta *FunctionMetadata) {
		meta.Parameters = append(meta.Parameters, FunctionParam{
			Name:        name,
			Type:        typeName,
			Description: description,
			Required:    required,
		})
	}
}

// WithExample 添加示例
func WithExample(example string) FunctionOption {
	return func(meta *FunctionMetadata) {
		meta.Examples = append(meta.Examples, example)
	}
}

// WithTag 添加标签
func WithTag(tag string) FunctionOption {
	return func(meta *FunctionMetadata) {
		meta.Tags = append(meta.Tags, tag)
	}
}

// WithTags 添加多个标签
func WithTags(tags []string) FunctionOption {
	return func(meta *FunctionMetadata) {
		meta.Tags = append(meta.Tags, tags...)
	}
}

// FunctionRegisterFunc 函数注册函数类型
type FunctionRegisterFunc func() (*FunctionMetadata, error)

// ============ UDF 管理 API ============

// RegisterUDF 注册用户自定义函数
func (api *FunctionAPI) RegisterUDF(udf *UDFFunction) error {
	manager := GetGlobalUDFManager()
	if err := manager.Register(udf); err != nil {
		return err
	}
	
	// 同时注册到函数注册表，以便在SQL中使用
	// 类型转换：UDFHandler -> FunctionHandle
	wrappedHandler := func(args []interface{}) (interface{}, error) {
		return udf.Handler(args)
	}
	
	return api.RegisterScalarFunction(
		udf.Metadata.Name,
		udf.Metadata.Name,
		udf.Metadata.Description,
		wrappedHandler,
		WithCategory(CategoryUser),
		WithReturnType(udf.Metadata.ReturnType),
		WithTags([]string{"udf"}),
	)
}

// RegisterUDFFromBuilder 通过构建器注册UDF
func (api *FunctionAPI) RegisterUDFFromBuilder(builder *UDFBuilder) error {
	udf := builder.Build()
	return api.RegisterUDF(udf)
}

// UnregisterUDF 注销用户自定义函数
func (api *FunctionAPI) UnregisterUDF(name string) error {
	// 从UDF管理器中移除
	manager := GetGlobalUDFManager()
	if err := manager.Unregister(name); err != nil {
		return err
	}
	
	// 从函数注册表中移除
	return api.UnregisterFunction(name)
}

// GetUDF 获取用户自定义函数
func (api *FunctionAPI) GetUDF(name string) (*UDFFunction, error) {
	manager := GetGlobalUDFManager()
	udf, exists := manager.Get(name)
	if !exists {
		return nil, fmt.Errorf("UDF %s not found", name)
	}
	return udf, nil
}

// ListUDFs 列出所有用户自定义函数
func (api *FunctionAPI) ListUDFs() []*UDFFunction {
	manager := GetGlobalUDFManager()
	return manager.List()
}

// CountUDFs 统计UDF数量
func (api *FunctionAPI) CountUDFs() int {
	manager := GetGlobalUDFManager()
	return manager.Count()
}

// UDFExists 检查UDF是否存在
func (api *FunctionAPI) UDFExists(name string) bool {
	manager := GetGlobalUDFManager()
	return manager.Exists(name)
}

// ClearUDFs 清除所有UDF
func (api *FunctionAPI) ClearUDFs() {
	// 从函数注册表中清除
	api.ClearUserFunctions()
	
	// 从UDF管理器中清除
	manager := GetGlobalUDFManager()
	manager.Clear()
}
