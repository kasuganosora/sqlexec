package builtin

// FunctionBuilder 函数构建器，用于方便地构建和注册函数
type FunctionBuilder struct {
	meta *FunctionMetadata
}

// NewFunctionBuilder 创建函数构建器
func NewFunctionBuilder(name, displayName string) *FunctionBuilder {
	return &FunctionBuilder{
		meta: &FunctionMetadata{
			Name:            name,
			DisplayName:      displayName,
			Type:            FunctionTypeScalar,
			Scope:           ScopeGlobal,
			Parameters:      []FunctionParam{},
			Examples:        []string{},
			Tags:           []string{},
		},
	}
}

// AsScalar 设置为标量函数
func (b *FunctionBuilder) AsScalar() *FunctionBuilder {
	b.meta.Type = FunctionTypeScalar
	return b
}

// AsAggregate 设置为聚合函数
func (b *FunctionBuilder) AsAggregate(handler AggregateHandle, result AggregateResult) *FunctionBuilder {
	b.meta.Type = FunctionTypeAggregate
	b.meta.AggregateHandler = handler
	b.meta.AggregateResult = result
	return b
}

// AsUser 设置为用户函数
func (b *FunctionBuilder) AsUser() *FunctionBuilder {
	b.meta.Scope = ScopeUser
	return b
}

// AsSession 设置为会话函数
func (b *FunctionBuilder) AsSession() *FunctionBuilder {
	b.meta.Scope = ScopeSession
	return b
}

// WithDescription 设置描述
func (b *FunctionBuilder) WithDescription(description string) *FunctionBuilder {
	b.meta.Description = description
	return b
}

// WithCategory 设置类别
func (b *FunctionBuilder) WithCategory(category FunctionCategory) *FunctionBuilder {
	b.meta.Category = category
	return b
}

// WithHandler 设置处理函数（标量函数）
func (b *FunctionBuilder) WithHandler(handler FunctionHandle) *FunctionBuilder {
	b.meta.Handler = handler
	return b
}

// WithReturnType 设置返回类型
func (b *FunctionBuilder) WithReturnType(returnType string) *FunctionBuilder {
	b.meta.ReturnType = returnType
	return b
}

// WithVariadic 设置为可变参数
func (b *FunctionBuilder) WithVariadic() *FunctionBuilder {
	b.meta.Variadic = true
	return b
}

// WithArgRange 设置参数范围
func (b *FunctionBuilder) WithArgRange(min, max int) *FunctionBuilder {
	b.meta.MinArgs = min
	b.meta.MaxArgs = max
	return b
}

// WithMinArgs 设置最小参数数
func (b *FunctionBuilder) WithMinArgs(min int) *FunctionBuilder {
	b.meta.MinArgs = min
	return b
}

// WithMaxArgs 设置最大参数数
func (b *FunctionBuilder) WithMaxArgs(max int) *FunctionBuilder {
	b.meta.MaxArgs = max
	return b
}

// WithParameter 添加参数
func (b *FunctionBuilder) WithParameter(name, typeName, description string, required bool) *FunctionBuilder {
	b.meta.Parameters = append(b.meta.Parameters, FunctionParam{
		Name:        name,
		Type:        typeName,
		Description: description,
		Required:    required,
	})
	return b
}

// WithParameters 批量添加参数
func (b *FunctionBuilder) WithParameters(params []FunctionParam) *FunctionBuilder {
	b.meta.Parameters = append(b.meta.Parameters, params...)
	return b
}

// WithExample 添加示例
func (b *FunctionBuilder) WithExample(example string) *FunctionBuilder {
	b.meta.Examples = append(b.meta.Examples, example)
	return b
}

// WithExamples 批量添加示例
func (b *FunctionBuilder) WithExamples(examples []string) *FunctionBuilder {
	b.meta.Examples = append(b.meta.Examples, examples...)
	return b
}

// WithTag 添加标签
func (b *FunctionBuilder) WithTag(tag string) *FunctionBuilder {
	b.meta.Tags = append(b.meta.Tags, tag)
	return b
}

// WithTags 批量添加标签
func (b *FunctionBuilder) WithTags(tags []string) *FunctionBuilder {
	b.meta.Tags = append(b.meta.Tags, tags...)
	return b
}

// Build 构建元数据
func (b *FunctionBuilder) Build() *FunctionMetadata {
	return b.meta
}

// Register 注册函数到API
func (b *FunctionBuilder) Register(api *FunctionAPI) error {
	meta := b.Build()
	
	if meta.Type == FunctionTypeAggregate {
		return api.GetRegistry().RegisterAggregate(meta)
	}
	return api.GetRegistry().RegisterScalar(meta)
}

// ============ 预定义的函数构建器 ============

// MathFunctionBuilder 数学函数构建器
func MathFunctionBuilder(name, displayName, description string) *FunctionBuilder {
	return NewFunctionBuilder(name, displayName).
		AsScalar().
		WithCategory(CategoryMath).
		WithDescription(description).
		WithReturnType("number")
}

// StringFunctionBuilder 字符串函数构建器
func StringFunctionBuilder(name, displayName, description string) *FunctionBuilder {
	return NewFunctionBuilder(name, displayName).
		AsScalar().
		WithCategory(CategoryString).
		WithDescription(description).
		WithReturnType("string")
}

// DateFunctionBuilder 日期函数构建器
func DateFunctionBuilder(name, displayName, description string, returnType string) *FunctionBuilder {
	return NewFunctionBuilder(name, displayName).
		AsScalar().
		WithCategory(CategoryDate).
		WithDescription(description).
		WithReturnType(returnType)
}

// AggregateFunctionBuilder 聚合函数构建器
func AggregateFunctionBuilder(name, displayName, description string, returnType string) *FunctionBuilder {
	return NewFunctionBuilder(name, displayName).
		AsAggregate(nil, nil).
		WithCategory(CategoryAggregate).
		WithDescription(description).
		WithReturnType(returnType)
}

// ============ 便捷函数注册方法 ============

// RegisterSimpleScalar 注册简单标量函数
func RegisterSimpleScalar(api *FunctionAPI, category FunctionCategory, 
	name, displayName, description, returnType string, 
	handler FunctionHandle, argCount int) error {
	
	return api.RegisterScalarFunction(name, displayName, description, handler,
		WithCategory(category),
		WithReturnType(returnType),
		WithArgRange(argCount, argCount),
	)
}

// RegisterVariadicScalar 注册可变参数标量函数
func RegisterVariadicScalar(api *FunctionAPI, category FunctionCategory,
	name, displayName, description, returnType string, 
	handler FunctionHandle, minArgs int) error {
	
	return api.RegisterScalarFunction(name, displayName, description, handler,
		WithCategory(category),
		WithReturnType(returnType),
		WithMinArgs(minArgs),
		WithMaxArgs(-1),
		WithVariadic(),
	)
}

// RegisterSimpleAggregate 注册简单聚合函数
func RegisterSimpleAggregate(api *FunctionAPI,
	name, displayName, description, returnType string,
	handler AggregateHandle, result AggregateResult) error {
	
	return api.RegisterAggregateFunction(name, displayName, description, handler, result,
		WithCategory(CategoryAggregate),
		WithReturnType(returnType),
	)
}

// ============ 示例使用 ============

// 示例1: 使用构建器注册简单函数
/*
api := builtin.NewFunctionAPI()

builtin.RegisterSimpleScalar(api, builtin.CategoryMath, 
	"myfunc", "MyFunc", "我的自定义函数", "number",
	func(args []interface{}) (interface{}, error) {
		return args[0], nil
	}, 
	1,
)
*/

// 示例2: 使用构建器创建复杂函数
/*
api := builtin.NewFunctionAPI()

err := builtin.MathFunctionBuilder("complex", "Complex", "复杂计算函数").
	WithDescription("执行复杂的数学计算").
	WithParameter("x", "number", "X坐标", true).
	WithParameter("y", "number", "Y坐标", true).
	WithParameter("operation", "string", "操作类型", true).
	WithExample("SELECT complex(1, 2, 'add') FROM table").
	WithExample("SELECT complex(10, 5, 'multiply') FROM table").
	WithTags([]string{"math", "custom", "experimental"}).
	WithHandler(func(args []interface{}) (interface{}, error) {
		x, _ := toFloat64(args[0])
		y, _ := toFloat64(args[1])
		op := args[2].(string)
		
		switch op {
		case "add":
			return x + y, nil
		case "multiply":
			return x * y, nil
		default:
			return nil, fmt.Errorf("unknown operation: %s", op)
		}
	}).
	Register(api)

if err != nil {
	log.Fatal(err)
}
*/

// 示例3: 批量注册函数
/*
api := builtin.NewFunctionAPI()

// 批量注册数学函数
mathFunctions := []struct{
	name, displayName, description string
	handler FunctionHandle
}{
	{"sin", "Sin", "正弦函数", mathSin},
	{"cos", "Cos", "余弦函数", mathCos},
	{"tan", "Tan", "正切函数", mathTan},
}

for _, fn := range mathFunctions {
	err := builtin.RegisterSimpleScalar(api, builtin.CategoryMath,
		fn.name, fn.displayName, fn.description, "number",
		fn.handler, 1,
	)
	if err != nil {
		log.Printf("Failed to register %s: %v", fn.name, err)
	}
}
*/
