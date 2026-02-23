package builtin

import (
	"fmt"
	"regexp"
	"strings"
	"sync"
	"text/template"
	"time"
)

// ==================== UDF 定义 ====================

// UDFMetadata 用户自定义函数元数据
type UDFMetadata struct {
	Name        string         // 函数名称
	Parameters  []UDFParameter // 参数列表
	ReturnType  string         // 返回类型
	Body        string         // 函数体（SQL表达式或计算逻辑）
	Determinism bool           // 是否确定性的（相同输入总是产生相同输出）
	Description string         // 函数描述
	CreatedAt   time.Time      // 创建时间
	ModifiedAt  time.Time      // 修改时间
	Author      string         // 创建者
	Language    string         // 语言（SQL, GO等）
}

// UDFParameter UDF参数定义
type UDFParameter struct {
	Name     string      // 参数名称
	Type     string      // 参数类型
	Optional bool        // 是否可选
	Required bool        // 是否必需（默认false）
	Default  interface{} // 默认值
}

// UDFFunction 用户自定义函数
type UDFFunction struct {
	Metadata *UDFMetadata
	Handler  UDFHandler
}

// UDFHandler UDF处理函数类型
type UDFHandler func(args []interface{}) (interface{}, error)

// UDFManager UDF管理器
type UDFManager struct {
	functions map[string]*UDFFunction
	mu        sync.RWMutex
}

// ==================== UDF 管理器 ====================

// NewUDFManager 创建UDF管理器
func NewUDFManager() *UDFManager {
	return &UDFManager{
		functions: make(map[string]*UDFFunction),
	}
}

// Register 注册UDF
func (m *UDFManager) Register(udf *UDFFunction) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if udf.Metadata.Name == "" {
		return fmt.Errorf("function name cannot be empty")
	}

	// 检查是否已存在
	if _, exists := m.functions[strings.ToLower(udf.Metadata.Name)]; exists {
		return fmt.Errorf("function '%s' already exists", udf.Metadata.Name)
	}

	// 编译函数体
	handler, err := m.compileUDF(udf.Metadata)
	if err != nil {
		return fmt.Errorf("failed to compile function body: %w", err)
	}

	udf.Handler = handler
	udf.Metadata.CreatedAt = time.Now()
	udf.Metadata.ModifiedAt = time.Now()

	m.functions[strings.ToLower(udf.Metadata.Name)] = udf
	return nil
}

// Unregister 注销UDF
func (m *UDFManager) Unregister(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := strings.ToLower(name)
	if _, exists := m.functions[key]; !exists {
		return fmt.Errorf("function '%s' not found", name)
	}

	delete(m.functions, key)
	return nil
}

// Get 获取UDF
func (m *UDFManager) Get(name string) (*UDFFunction, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	udf, exists := m.functions[strings.ToLower(name)]
	return udf, exists
}

// List 列出所有UDF
func (m *UDFManager) List() []*UDFFunction {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]*UDFFunction, 0, len(m.functions))
	for _, udf := range m.functions {
		result = append(result, udf)
	}
	return result
}

// Exists 检查函数是否存在
func (m *UDFManager) Exists(name string) bool {
	_, exists := m.Get(name)
	return exists
}

// Count 返回UDF数量
func (m *UDFManager) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.functions)
}

// Clear 清除所有UDF
func (m *UDFManager) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.functions = make(map[string]*UDFFunction)
}

// ==================== UDF 编译和执行 ====================

// compileUDF 编译UDF函数体
func (m *UDFManager) compileUDF(meta *UDFMetadata) (UDFHandler, error) {
	if meta.Language == "" {
		meta.Language = "SQL"
	}

	switch strings.ToUpper(meta.Language) {
	case "SQL":
		return m.compileSQLExpression(meta)
	case "GO":
		return m.compileGoExpression(meta)
	default:
		return nil, fmt.Errorf("unsupported language: %s", meta.Language)
	}
}

// compileSQLExpression 编译SQL表达式
func (m *UDFManager) compileSQLExpression(meta *UDFMetadata) (UDFHandler, error) {
	// 解析SQL表达式，检查语法
	expr := strings.TrimSpace(meta.Body)

	// 简单表达式：直接返回值
	if m.isSimpleExpression(expr) {
		return func(args []interface{}) (interface{}, error) {
			return m.evaluateSimpleExpression(expr, args, meta.Parameters)
		}, nil
	}

	// 函数调用：调用内置函数
	if m.isFunctionCall(expr) {
		return m.compileFunctionCall(expr, meta)
	}

	// 算术表达式
	if m.isArithmeticExpression(expr) {
		return func(args []interface{}) (interface{}, error) {
			return m.evaluateArithmeticExpression(expr, args, meta.Parameters)
		}, nil
	}

	// 复杂表达式：使用模板引擎
	return m.compileTemplateExpression(meta)
}

// compileGoExpression 编译Go表达式（简单实现）
func (m *UDFManager) compileGoExpression(meta *UDFMetadata) (UDFHandler, error) {
	// 这里可以实现更复杂的Go代码编译
	// 目前仅支持简单的算术表达式
	expr := strings.TrimSpace(meta.Body)

	return func(args []interface{}) (interface{}, error) {
		return m.evaluateArithmeticExpression(expr, args, meta.Parameters)
	}, nil
}

// compileTemplateExpression 编译模板表达式
func (m *UDFManager) compileTemplateExpression(meta *UDFMetadata) (UDFHandler, error) {
	tmpl, err := template.New("udf").Parse(meta.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to parse template: %w", err)
	}

	return func(args []interface{}) (interface{}, error) {
		// 构建模板数据
		data := make(map[string]interface{})
		for i, param := range meta.Parameters {
			if i < len(args) {
				data[param.Name] = args[i]
			} else if param.Default != nil {
				data[param.Name] = param.Default
			} else {
				data[param.Name] = nil
			}
		}

		var buf strings.Builder
		if err := tmpl.Execute(&buf, data); err != nil {
			return nil, fmt.Errorf("template execution failed: %w", err)
		}

		return buf.String(), nil
	}, nil
}

// ==================== 表达式求值 ====================

// isSimpleExpression 检查是否为简单表达式
func (m *UDFManager) isSimpleExpression(expr string) bool {
	// 参数引用：@param 或 :param
	re := regexp.MustCompile(`^@?\w+$`)
	return re.MatchString(expr)
}

// isFunctionCall 检查是否为函数调用
func (m *UDFManager) isFunctionCall(expr string) bool {
	re := regexp.MustCompile(`^\w+\(.*\)$`)
	return re.MatchString(expr)
}

// isArithmeticExpression 检查是否为算术表达式
func (m *UDFManager) isArithmeticExpression(expr string) bool {
	// 包含运算符且不是函数调用
	if m.isFunctionCall(expr) {
		return false
	}

	operators := []string{"+", "-", "*", "/", "%", "^"}
	for _, op := range operators {
		if strings.Contains(expr, op) {
			return true
		}
	}
	return false
}

// evaluateSimpleExpression 求值简单表达式
func (m *UDFManager) evaluateSimpleExpression(expr string, args []interface{}, params []UDFParameter) (interface{}, error) {
	// 移除前缀
	expr = strings.TrimPrefix(expr, "@")
	expr = strings.TrimPrefix(expr, ":")

	// 查找参数
	for i, param := range params {
		if strings.EqualFold(param.Name, expr) {
			if i < len(args) {
				return args[i], nil
			}
			if param.Default != nil {
				return param.Default, nil
			}
			return nil, fmt.Errorf("parameter '%s' not provided and no default value", expr)
		}
	}

	return nil, fmt.Errorf("parameter '%s' not found", expr)
}

// compileFunctionCall 编译函数调用
func (m *UDFManager) compileFunctionCall(expr string, meta *UDFMetadata) (UDFHandler, error) {
	// 提取函数名和参数
	re := regexp.MustCompile(`^(\w+)\((.*)\)$`)
	matches := re.FindStringSubmatch(expr)
	if len(matches) != 3 {
		return nil, fmt.Errorf("invalid function call: %s", expr)
	}

	funcName := matches[1]
	argsExpr := matches[2]

	return func(args []interface{}) (interface{}, error) {
		// 检查是否是内置函数
		if info, exists := GetGlobal(funcName); exists {
			// 解析参数
			argValues, err := m.parseFunctionArgs(argsExpr, args, meta.Parameters)
			if err != nil {
				return nil, err
			}
			return info.Handler(argValues)
		}

		// 检查是否是其他UDF
		// 这里暂不实现UDF递归调用

		return nil, fmt.Errorf("function '%s' not found", funcName)
	}, nil
}

// parseFunctionArgs 解析函数参数
func (m *UDFManager) parseFunctionArgs(argsExpr string, udfArgs []interface{}, params []UDFParameter) ([]interface{}, error) {
	if argsExpr == "" {
		return []interface{}{}, nil
	}

	// 简单分割，实际应该更复杂
	args := strings.Split(argsExpr, ",")
	result := make([]interface{}, 0, len(args))

	for _, arg := range args {
		arg = strings.TrimSpace(arg)

		// 检查是否是参数引用
		if m.isSimpleExpression(arg) {
			val, err := m.evaluateSimpleExpression(arg, udfArgs, params)
			if err != nil {
				return nil, err
			}
			result = append(result, val)
		} else {
			// 字面量
			result = append(result, arg)
		}
	}

	return result, nil
}

// evaluateArithmeticExpression 求值算术表达式
func (m *UDFManager) evaluateArithmeticExpression(expr string, args []interface{}, params []UDFParameter) (interface{}, error) {
	// 简单实现：替换参数，然后求值
	// 实际应该使用表达式解析器

	result := expr

	// 替换参数
	for i, param := range params {
		if i >= len(args) {
			break
		}
		// 替换 @param 或 :param
		result = strings.ReplaceAll(result, "@"+param.Name, fmt.Sprintf("%v", args[i]))
		result = strings.ReplaceAll(result, ":"+param.Name, fmt.Sprintf("%v", args[i]))
	}

	// 简单的算术求值（仅支持基础运算）
	// 实际应该使用表达式求值器
	if strings.Contains(result, "+") {
		parts := strings.Split(result, "+")
		if len(parts) == 2 {
			val1, err1 := ToFloat64(strings.TrimSpace(parts[0]))
			val2, err2 := ToFloat64(strings.TrimSpace(parts[1]))
			if err1 == nil && err2 == nil {
				return val1 + val2, nil
			}
		}
	}

	if strings.Contains(result, "-") {
		parts := strings.Split(result, "-")
		if len(parts) == 2 {
			val1, err1 := ToFloat64(strings.TrimSpace(parts[0]))
			val2, err2 := ToFloat64(strings.TrimSpace(parts[1]))
			if err1 == nil && err2 == nil {
				return val1 - val2, nil
			}
		}
	}

	if strings.Contains(result, "*") {
		parts := strings.Split(result, "*")
		if len(parts) == 2 {
			val1, err1 := ToFloat64(strings.TrimSpace(parts[0]))
			val2, err2 := ToFloat64(strings.TrimSpace(parts[1]))
			if err1 == nil && err2 == nil {
				return val1 * val2, nil
			}
		}
	}

	if strings.Contains(result, "/") {
		parts := strings.Split(result, "/")
		if len(parts) == 2 {
			val1, err1 := ToFloat64(strings.TrimSpace(parts[0]))
			val2, err2 := ToFloat64(strings.TrimSpace(parts[1]))
			if err1 == nil && err2 == nil && val2 != 0 {
				return val1 / val2, nil
			}
		}
	}

	// 如果无法求值，返回原始字符串
	return result, nil
}

// ==================== 全局 UDF 管理器 ====================

var (
	globalUDFManager *UDFManager
	udfOnce          sync.Once
)

// GetGlobalUDFManager 获取全局UDF管理器
func GetGlobalUDFManager() *UDFManager {
	udfOnce.Do(func() {
		globalUDFManager = NewUDFManager()
	})
	return globalUDFManager
}

// ==================== UDF 构建器 ====================

// UDFBuilder UDF构建器
type UDFBuilder struct {
	metadata *UDFMetadata
}

// NewUDFBuilder 创建UDF构建器
func NewUDFBuilder(name string) *UDFBuilder {
	return &UDFBuilder{
		metadata: &UDFMetadata{
			Name:        name,
			Language:    "SQL",
			Determinism: true,
			CreatedAt:   time.Now(),
			ModifiedAt:  time.Now(),
		},
	}
}

// WithParameter 添加参数
func (b *UDFBuilder) WithParameter(name, typ string, optional bool) *UDFBuilder {
	b.metadata.Parameters = append(b.metadata.Parameters, UDFParameter{
		Name:     name,
		Type:     typ,
		Optional: optional,
	})
	return b
}

// WithReturnType 设置返回类型
func (b *UDFBuilder) WithReturnType(typ string) *UDFBuilder {
	b.metadata.ReturnType = typ
	return b
}

// WithBody 设置函数体
func (b *UDFBuilder) WithBody(body string) *UDFBuilder {
	b.metadata.Body = body
	return b
}

// WithLanguage 设置语言
func (b *UDFBuilder) WithLanguage(lang string) *UDFBuilder {
	b.metadata.Language = lang
	return b
}

// WithDeterminism 设置确定性
func (b *UDFBuilder) WithDeterminism(determinism bool) *UDFBuilder {
	b.metadata.Determinism = determinism
	return b
}

// WithDescription 设置描述
func (b *UDFBuilder) WithDescription(desc string) *UDFBuilder {
	b.metadata.Description = desc
	return b
}

// WithAuthor 设置作者
func (b *UDFBuilder) WithAuthor(author string) *UDFBuilder {
	b.metadata.Author = author
	return b
}

// Build 构建UDF
func (b *UDFBuilder) Build() *UDFFunction {
	return &UDFFunction{
		Metadata: b.metadata,
	}
}
