package optimizer

import (
	"context"
	"fmt"
	"github.com/kasuganosora/sqlexec/pkg/parser"
)

// ProcedureExecutor 存储过程执行器
type ProcedureExecutor struct {
	// 变量作用域栈
	scopeStack []*Scope
	
	// 存储过程缓存
	procedures map[string]*parser.ProcedureInfo
	
	// 函数缓存
	functions map[string]*parser.FunctionInfo
	
	// 执行上下文
	ctx context.Context
}

// Scope 变量作用域
type Scope struct {
	Variables map[string]interface{}
	Parent    *Scope
}

// NewScope 创建新作用域
func NewScope(parent *Scope) *Scope {
	return &Scope{
		Variables: make(map[string]interface{}),
		Parent:    parent,
	}
}

// GetVariable 获取变量值
func (s *Scope) GetVariable(name string) (interface{}, bool) {
	if val, exists := s.Variables[name]; exists {
		return val, true
	}
	
	// 查找父作用域
	if s.Parent != nil {
		return s.Parent.GetVariable(name)
	}
	
	return nil, false
}

// SetVariable 设置变量值
func (s *Scope) SetVariable(name string, value interface{}) {
	s.Variables[name] = value
}

// NewProcedureExecutor 创建存储过程执行器
func NewProcedureExecutor() *ProcedureExecutor {
	return &ProcedureExecutor{
		scopeStack: make([]*Scope, 0),
		procedures: make(map[string]*parser.ProcedureInfo),
		functions:   make(map[string]*parser.FunctionInfo),
	}
}

// RegisterProcedure 注册存储过程
func (pe *ProcedureExecutor) RegisterProcedure(proc *parser.ProcedureInfo) error {
	if err := parser.ValidateProcedure(&parser.ProcedureStmt{
		Name:   proc.Name,
		Params: proc.Params,
		Body:   proc.Body,
	}); err != nil {
		return fmt.Errorf("procedure validation failed: %w", err)
	}
	
	pe.procedures[proc.Name] = proc
	return nil
}

// RegisterFunction 注册函数
func (pe *ProcedureExecutor) RegisterFunction(fn *parser.FunctionInfo) error {
	if err := parser.ValidateFunction(&parser.FunctionStmt{
		Name:       fn.Name,
		ReturnType:  fn.ReturnType,
		Params:     fn.Params,
		Body:       fn.Body,
	}); err != nil {
		return fmt.Errorf("function validation failed: %w", err)
	}
	
	pe.functions[fn.Name] = fn
	return nil
}

// ExecuteProcedure 执行存储过程
func (pe *ProcedureExecutor) ExecuteProcedure(ctx context.Context, name string, args ...interface{}) ([]map[string]interface{}, error) {
	pe.ctx = ctx
	
	// 查找存储过程
	proc, exists := pe.procedures[name]
	if !exists {
		return nil, fmt.Errorf("procedure not found: %s", name)
	}
	
	// 创建新作用域
	newScope := NewScope(pe.currentScope())
	pe.pushScope(newScope)
	defer pe.popScope()
	
	// 绑定参数
	if len(args) != len(proc.Params) {
		return nil, fmt.Errorf("parameter count mismatch: expected %d, got %d", len(proc.Params), len(args))
	}
	
	for i, param := range proc.Params {
		newScope.SetVariable(param.Name, args[i])
	}
	
	// 执行存储过程体
	return pe.executeBlock(proc.Body)
}

// ExecuteFunction 执行函数
func (pe *ProcedureExecutor) ExecuteFunction(ctx context.Context, name string, args ...interface{}) (interface{}, error) {
	pe.ctx = ctx
	
	// 查找函数
	fn, exists := pe.functions[name]
	if !exists {
		return nil, fmt.Errorf("function not found: %s", name)
	}
	
	// 创建新作用域
	newScope := NewScope(pe.currentScope())
	pe.pushScope(newScope)
	defer pe.popScope()
	
	// 绑定参数
	if len(args) != len(fn.Params) {
		return nil, fmt.Errorf("parameter count mismatch: expected %d, got %d", len(fn.Params), len(args))
	}
	
	for i, param := range fn.Params {
		newScope.SetVariable(param.Name, args[i])
	}
	
	// 执行函数体
	result, err := pe.executeBlockWithReturn(fn.Body)
	return result, err
}

// executeBlock 执行语句块
func (pe *ProcedureExecutor) executeBlock(block *parser.BlockStmt) ([]map[string]interface{}, error) {
	if block == nil {
		return nil, nil
	}
	
	// 执行声明
	for _, decl := range block.Declarations {
		if err := pe.executeDeclaration(decl); err != nil {
			return nil, err
		}
	}
	
	// 执行语句
	for _, stmt := range block.Statements {
		switch s := stmt.(type) {
		case *parser.IfStmt:
			if err := pe.executeIf(s); err != nil {
				return nil, err
			}
		case *parser.WhileStmt:
			if err := pe.executeWhile(s); err != nil {
				return nil, err
			}
		case *parser.SetStmt:
			if err := pe.executeSet(s); err != nil {
				return nil, err
			}
		case *parser.CaseStmt:
			if err := pe.executeCase(s); err != nil {
				return nil, err
			}
		case *parser.ReturnStmt:
			// RETURN语句在函数中处理
			return nil, fmt.Errorf("RETURN statement not allowed in procedure")
		}
	}
	
	// 返回空结果集
	return []map[string]interface{}{}, nil
}

// executeBlockWithReturn 执行语句块(支持RETURN)
func (pe *ProcedureExecutor) executeBlockWithReturn(block *parser.BlockStmt) (interface{}, error) {
	if block == nil {
		return nil, nil
	}
	
	// 执行声明
	for _, decl := range block.Declarations {
		if err := pe.executeDeclaration(decl); err != nil {
			return nil, err
		}
	}
	
	// 执行语句
	for _, stmt := range block.Statements {
		switch s := stmt.(type) {
		case *parser.IfStmt:
			if err := pe.executeIf(s); err != nil {
				return nil, err
			}
		case *parser.WhileStmt:
			if err := pe.executeWhile(s); err != nil {
				return nil, err
			}
		case *parser.SetStmt:
			if err := pe.executeSet(s); err != nil {
				return nil, err
			}
		case *parser.CaseStmt:
			if err := pe.executeCase(s); err != nil {
				return nil, err
			}
	case *parser.ReturnStmt:
		// RETURN语句,返回值
		result, err := pe.evaluateExpression(&s.Expression)
		if err != nil {
			return nil, err
		}
		return result, nil
	}
	}
	
	return nil, nil
}

// executeDeclaration 执行变量声明
func (pe *ProcedureExecutor) executeDeclaration(decl parser.Declaration) error {
	scope := pe.currentScope()
	
	// 设置初始值
	value := interface{}(nil)
	if decl.Initial != nil {
		// 初始值直接使用，无需表达式求值
		value = decl.Initial
	}
	
	scope.SetVariable(decl.Name, value)
	return nil
}

// executeIf 执行IF语句
func (pe *ProcedureExecutor) executeIf(ifStmt *parser.IfStmt) error {
	// 计算条件
	cond, err := pe.evaluateExpression(&ifStmt.Condition)
	if err != nil {
		return err
	}
	
	// 判断条件
	if isTrue(cond) {
		// 执行THEN块
		_, err := pe.executeBlock(ifStmt.Then)
		return err
	}
	
	// 执行ELSE IF块
	for _, elif := range ifStmt.ElseIfs {
		cond, err := pe.evaluateExpression(&elif.Condition)
		if err != nil {
			return err
		}
		
		if isTrue(cond) {
			_, err := pe.executeBlock(elif.Then)
			return err
		}
	}
	
	// 执行ELSE块
	if ifStmt.Else != nil {
		_, err := pe.executeBlock(ifStmt.Else)
		return err
	}
	
	return nil
}

// executeWhile 执行WHILE语句
func (pe *ProcedureExecutor) executeWhile(whileStmt *parser.WhileStmt) error {
	// 循环执行
	for {
		// 计算条件
		cond, err := pe.evaluateExpression(&whileStmt.Condition)
		if err != nil {
			return err
		}
		
		// 如果条件为假,退出循环
		if !isTrue(cond) {
			break
		}
		
		// 执行循环体
		_, err = pe.executeBlock(whileStmt.Body)
		if err != nil {
			return err
		}
	}
	
	return nil
}

// executeSet 执行SET语句
func (pe *ProcedureExecutor) executeSet(setStmt *parser.SetStmt) error {
	// 计算值
	value, err := pe.evaluateExpression(&setStmt.Value)
	if err != nil {
		return err
	}
	
	// 设置变量
	scope := pe.currentScope()
	scope.SetVariable(setStmt.Variable, value)
	
	return nil
}

// executeCase 执行CASE语句
func (pe *ProcedureExecutor) executeCase(caseStmt *parser.CaseStmt) error {
	// 如果有表达式,先计算
	var caseExpr interface{}
	if caseStmt.Expression.Type != "" {
		val, err := pe.evaluateExpression(&caseStmt.Expression)
		if err != nil {
			return err
		}
		caseExpr = val
	}
	
	// 检查每个WHEN
	for _, when := range caseStmt.Cases {
		cond, err := pe.evaluateExpression(&when.Condition)
		if err != nil {
			return err
		}
		
		// 如果没有表达式,直接判断WHEN条件
		if caseStmt.Expression.Type == "" {
			if isTrue(cond) {
				_, err := pe.executeBlock(when.Then)
				return err
			}
		} else {
			// 有表达式,比较值
			if compareValuesEqual(cond, caseExpr) {
				_, err := pe.executeBlock(when.Then)
				return err
			}
		}
	}
	
	// 执行ELSE块
	if caseStmt.Else != nil {
		_, err := pe.executeBlock(caseStmt.Else)
		return err
	}
	
	return nil
}

// evaluateExpression 计算表达式
func (pe *ProcedureExecutor) evaluateExpression(expr *parser.Expression) (interface{}, error) {
	// 简化实现:只支持简单的常量和变量
	if expr.Value != nil {
		return expr.Value, nil
	}
	
	if expr.Column != "" {
		scope := pe.currentScope()
		val, exists := scope.GetVariable(expr.Column)
		if !exists {
			return nil, fmt.Errorf("variable not found: %s", expr.Column)
		}
		return val, nil
	}
	
	if expr.Function != "" {
		// 简化实现:不支持复杂函数
		return nil, fmt.Errorf("function evaluation not yet implemented: %s", expr.Function)
	}
	
	return nil, fmt.Errorf("unsupported expression")
}

// 作用域管理

// pushScope 推入新作用域
func (pe *ProcedureExecutor) pushScope(scope *Scope) {
	pe.scopeStack = append(pe.scopeStack, scope)
}

// popScope 弹出作用域
func (pe *ProcedureExecutor) popScope() {
	if len(pe.scopeStack) > 0 {
		pe.scopeStack = pe.scopeStack[:len(pe.scopeStack)-1]
	}
}

// currentScope 获取当前作用域
func (pe *ProcedureExecutor) currentScope() *Scope {
	if len(pe.scopeStack) == 0 {
		return NewScope(nil)
	}
	return pe.scopeStack[len(pe.scopeStack)-1]
}

// 辅助函数

// isTrue 判断值是否为真
func isTrue(value interface{}) bool {
	if value == nil {
		return false
	}
	
	switch v := value.(type) {
	case bool:
		return v
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return v != 0
	case float32, float64:
		return v != 0.0
	case string:
		return v != "" && v != "0"
	default:
		return true
	}
}

// compareValuesEqual 比较两个值是否相等


