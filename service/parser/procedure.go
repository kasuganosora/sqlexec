package parser

import (
	"fmt"
)

// ProcedureInfo 存储过程信息
type ProcedureInfo struct {
	Name     string
	Params   []ProcedureParam
	Body     *BlockStmt
	Returns  []ColumnInfo // 返回�?用于函数)
}

// FunctionInfo 函数信息
type FunctionInfo struct {
	Name       string
	Params     []ProcedureParam
	ReturnType string
	Body       *BlockStmt
}

// ProcedureParam 参数定义
type ProcedureParam struct {
	Name      string
	ParamType ParamType
	DataType  string
}

// ParamType 参数类型
type ParamType int

const (
	ParamTypeIn     ParamType = iota // IN参数
	ParamTypeOut                     // OUT参数
	ParamTypeInOut                   // INOUT参数
)

// BlockStmt 语句�?
type BlockStmt struct {
	Declarations []Declaration
	Statements  []Statement
}

// Declaration 变量声明
type Declaration struct {
	Name     string
	DataType string
	Initial  interface{} // 初始�?可�?
}

// Statement 语句
type Statement interface{}

// ProcedureStmt 存储过程语句
type ProcedureStmt struct {
	Name   string
	Params []ProcedureParam
	Body   *BlockStmt
}

// FunctionStmt 函数语句
type FunctionStmt struct {
	Name       string
	Params     []ProcedureParam
	ReturnType string
	Body       *BlockStmt
}

// IfStmt IF语句
type IfStmt struct {
	Condition  Expression
	Then       *BlockStmt
	ElseIfs    []*ElseIfStmt
	Else       *BlockStmt
}

// ElseIfStmt ELSE IF语句
type ElseIfStmt struct {
	Condition Expression
	Then      *BlockStmt
}

// WhileStmt WHILE语句
type WhileStmt struct {
	Condition Expression
	Body      *BlockStmt
}

// CaseStmt CASE语句
type CaseStmt struct {
	Expression Expression // 可选的表达�?
	Cases      []CaseWhen
	Else       *BlockStmt
}

// CaseWhen CASE WHEN
type CaseWhen struct {
	Condition Expression
	Then      *BlockStmt
}

// SetStmt SET语句
type SetStmt struct {
	Variable string
	Value    Expression
}

// DeclareStmt DECLARE语句
type DeclareStmt struct {
	Variables []Declaration
}

// ReturnStmt RETURN语句
type ReturnStmt struct {
	Expression Expression // 返回�?可�?
}

// CallStmt CALL语句
type CallStmt struct {
	ProcedureName string
	Args         []Expression // 参数
}

// 创建辅助函数

// NewProcedure 创建存储过程
func NewProcedure(name string, body *BlockStmt, params ...ProcedureParam) *ProcedureStmt {
	return &ProcedureStmt{
		Name:   name,
		Params: params,
		Body:   body,
	}
}

// NewFunction 创建函数
func NewFunction(name string, returnType string, body *BlockStmt, params ...ProcedureParam) *FunctionStmt {
	return &FunctionStmt{
		Name:       name,
		ReturnType:  returnType,
		Params:     params,
		Body:       body,
	}
}

// NewBlock 创建语句�?
func NewBlock(decls []Declaration, stmts []Statement) *BlockStmt {
	return &BlockStmt{
		Declarations: decls,
		Statements:  stmts,
	}
}

// NewIf 创建IF语句
func NewIf(condition Expression, thenBlock *BlockStmt) *IfStmt {
	return &IfStmt{
		Condition: condition,
		Then:      thenBlock,
		ElseIfs:   []*ElseIfStmt{},
		Else:      nil,
	}
}

// AddElseIf 添加ELSE IF
func (is *IfStmt) AddElseIf(condition Expression, thenBlock *BlockStmt) {
	is.ElseIfs = append(is.ElseIfs, &ElseIfStmt{
		Condition: condition,
		Then:      thenBlock,
	})
}

// AddElse 添加ELSE
func (is *IfStmt) AddElse(elseBlock *BlockStmt) {
	is.Else = elseBlock
}

// NewWhile 创建WHILE语句
func NewWhile(condition Expression, body *BlockStmt) *WhileStmt {
	return &WhileStmt{
		Condition: condition,
		Body:      body,
	}
}

// NewCase 创建CASE语句
func NewCase(expr Expression) *CaseStmt {
	return &CaseStmt{
		Expression: expr,
		Cases:      []CaseWhen{},
		Else:       nil,
	}
}

// AddWhen 添加WHEN
func (cs *CaseStmt) AddWhen(condition Expression, thenBlock *BlockStmt) {
	cs.Cases = append(cs.Cases, CaseWhen{
		Condition: condition,
		Then:      thenBlock,
	})
}

// AddElse 添加ELSE
func (cs *CaseStmt) AddElse(elseBlock *BlockStmt) {
	cs.Else = elseBlock
}

// NewSet 创建SET语句
func NewSet(variable string, value Expression) *SetStmt {
	return &SetStmt{
		Variable: variable,
		Value:    value,
	}
}

// NewDeclare 创建DECLARE语句
func NewDeclare(decls ...Declaration) *DeclareStmt {
	return &DeclareStmt{
		Variables: decls,
	}
}

// NewParam 创建参数
func NewParam(name string, paramType ParamType, dataType string) ProcedureParam {
	return ProcedureParam{
		Name:      name,
		ParamType: paramType,
		DataType:  dataType,
	}
}

// NewReturn 创建RETURN语句
func NewReturn(expr Expression) *ReturnStmt {
	return &ReturnStmt{
		Expression: expr,
	}
}

// NewCall 创建CALL语句
func NewCall(procedureName string, args ...Expression) *CallStmt {
	return &CallStmt{
		ProcedureName: procedureName,
		Args:         args,
	}
}

// 变量帮助函数

// ParseVariableName 解析变量�?
// 支持格式: @var_name, @@var_name(系统变量), :var_name(绑定变量)
func ParseVariableName(name string) (string, error) {
	if len(name) == 0 {
		return "", fmt.Errorf("empty variable name")
	}
	
	// 移除前缀
	switch {
	case name[0] == '@':
		if len(name) > 1 && name[1] == '@' {
			// 系统变量: @@var_name
			return name[2:], nil
		}
		// 用户变量: @var_name
		return name[1:], nil
	case name[0] == ':':
		// 绑定变量: :var_name
		return name[1:], nil
	default:
		// 无前缀
		return name, nil
	}
}

// 验证函数

// ValidateProcedure 验证存储过程
func ValidateProcedure(proc *ProcedureStmt) error {
	if proc == nil {
		return fmt.Errorf("procedure is nil")
	}
	
	if proc.Name == "" {
		return fmt.Errorf("procedure name is required")
	}
	
	if proc.Body == nil {
		return fmt.Errorf("procedure body is required")
	}
	
	// 检查参数名重复
	paramNames := make(map[string]bool)
	for _, param := range proc.Params {
		if param.Name == "" {
			return fmt.Errorf("parameter name is required")
		}
		
		if paramNames[param.Name] {
			return fmt.Errorf("duplicate parameter name: %s", param.Name)
		}
		paramNames[param.Name] = true
	}
	
	return nil
}

// ValidateFunction 验证函数
func ValidateFunction(fn *FunctionStmt) error {
	if fn == nil {
		return fmt.Errorf("function is nil")
	}
	
	if fn.Name == "" {
		return fmt.Errorf("function name is required")
	}
	
	if fn.ReturnType == "" {
		return fmt.Errorf("function return type is required")
	}
	
	if fn.Body == nil {
		return fmt.Errorf("function body is required")
	}
	
	// 检查参数名重复
	paramNames := make(map[string]bool)
	for _, param := range fn.Params {
		if param.Name == "" {
			return fmt.Errorf("parameter name is required")
		}
		
		if paramNames[param.Name] {
			return fmt.Errorf("duplicate parameter name: %s", param.Name)
		}
		paramNames[param.Name] = true
	}
	
	// 检查函数体是否有RETURN语句
	hasReturn := false
	for _, stmt := range fn.Body.Statements {
		if _, ok := stmt.(*ReturnStmt); ok {
			hasReturn = true
			break
		}
	}
	
	if !hasReturn {
		return fmt.Errorf("function must have a RETURN statement")
	}
	
	return nil
}

// 类型转换

// ToProcedureParamType 转换参数类型字符串为ParamType
func ToProcedureParamType(mode string) (ParamType, error) {
	switch mode {
	case "IN":
		return ParamTypeIn, nil
	case "OUT":
		return ParamTypeOut, nil
	case "INOUT":
		return ParamTypeInOut, nil
	default:
		return ParamTypeIn, fmt.Errorf("unknown parameter type: %s", mode)
	}
}

// ToString 返回参数类型的字符串表示
func (pt ParamType) ToString() string {
	switch pt {
	case ParamTypeIn:
		return "IN"
	case ParamTypeOut:
		return "OUT"
	case ParamTypeInOut:
		return "INOUT"
	default:
		return "IN"
	}
}
