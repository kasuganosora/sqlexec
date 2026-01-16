package datasource

import (
	"fmt"
	"strings"
)

// Validator SQL验证器
type Validator struct {
	configManager *ConfigManager
}

// NewValidator 创建验证器
func NewValidator(configManager *ConfigManager) *Validator {
	return &Validator{
		configManager: configManager,
	}
}

// Validate 验证查询
func (v *Validator) Validate(query *Query) error {
	// 验证表是否存在
	if query.Table != "tables" && query.Table != "databases" && query.Table != "status" && query.Table != "variables" {
		if _, ok := v.configManager.GetTable("test", query.Table); !ok {
			return fmt.Errorf("表不存在: %s", query.Table)
		}
	}

	// 验证字段
	if err := v.validateFields(query); err != nil {
		return err
	}

	// 验证JOIN
	if err := v.validateJoins(query); err != nil {
		return err
	}

	// 验证WHERE条件
	if err := v.validateConditions(query.Where); err != nil {
		return err
	}

	// 验证HAVING条件
	if err := v.validateConditions(query.Having); err != nil {
		return err
	}

	// 验证GROUP BY
	if err := v.validateGroupBy(query); err != nil {
		return err
	}

	// 验证ORDER BY
	if err := v.validateOrderBy(query); err != nil {
		return err
	}

	return nil
}

// validateFields 验证字段
func (v *Validator) validateFields(query *Query) error {
	if len(query.Fields) == 0 {
		return nil
	}

	// 获取主表配置
	tableConfig, ok := v.configManager.GetTable("test", query.Table)
	if !ok {
		return nil // 对于SHOW和DESCRIBE语句，不需要验证字段
	}

	// 合并主表和所有 JOIN 表的字段名
	fieldSet := make(map[string]struct{})
	for _, f := range tableConfig.Fields {
		fieldSet[f.Name] = struct{}{}
	}
	// 加入 JOIN 表字段
	for _, join := range query.Joins {
		if joinTable, ok := v.configManager.GetTable("test", join.Table); ok {
			for _, f := range joinTable.Fields {
				fieldSet[f.Name] = struct{}{}
			}
		}
	}

	// 验证每个字段
	for _, field := range query.Fields {
		if field == "*" {
			continue
		}

		// 检查是否是函数调用
		if strings.Contains(field, "(") && strings.Contains(field, ")") {
			// 验证函数调用
			if err := v.validateFunctionCall(field); err != nil {
				return err
			}
			continue
		}

		// 验证普通字段
		if _, found := fieldSet[field]; !found {
			return fmt.Errorf("字段不存在: %s", field)
		}
	}

	return nil
}

// validateFunctionCall 验证函数调用
func (v *Validator) validateFunctionCall(field string) error {
	// 解析函数名和参数
	funcName := strings.Split(field, "(")[0]
	argsStr := strings.TrimSuffix(strings.Split(field, "(")[1], ")")
	args := strings.Split(argsStr, ",")

	// 验证函数名
	if !v.isValidFunction(funcName) {
		return fmt.Errorf("不支持的函数: %s", funcName)
	}

	// 验证参数数量
	if err := v.validateFunctionArgs(funcName, len(args)); err != nil {
		return err
	}

	return nil
}

// isValidFunction 检查是否是有效的函数
func (v *Validator) isValidFunction(name string) bool {
	// 内置函数列表
	builtinFunctions := map[string]bool{
		"CONCAT":      true,
		"UPPER":       true,
		"LOWER":       true,
		"ABS":         true,
		"ROUND":       true,
		"NOW":         true,
		"DATE_FORMAT": true,
		"COUNT":       true,
		"SUM":         true,
		"AVG":         true,
		"MAX":         true,
		"MIN":         true,
	}

	return builtinFunctions[name]
}

// validateFunctionArgs 验证函数参数数量
func (v *Validator) validateFunctionArgs(funcName string, argCount int) error {
	// 函数参数数量要求
	argRequirements := map[string]int{
		"CONCAT":      -1, // 可变参数
		"UPPER":       1,
		"LOWER":       1,
		"ABS":         1,
		"ROUND":       2,
		"NOW":         0,
		"DATE_FORMAT": 2,
		"COUNT":       -1, // 可变参数
		"SUM":         -1, // 可变参数
		"AVG":         -1, // 可变参数
		"MAX":         -1, // 可变参数
		"MIN":         -1, // 可变参数
	}

	required := argRequirements[funcName]
	if required == -1 {
		// 可变参数函数
		return nil
	}

	if argCount != required {
		return fmt.Errorf("函数 %s 需要 %d 个参数，但提供了 %d 个", funcName, required, argCount)
	}

	return nil
}

// validateJoins 验证JOIN
func (v *Validator) validateJoins(query *Query) error {
	for _, join := range query.Joins {
		// 验证连接表是否存在
		if _, ok := v.configManager.GetTable("test", join.Table); !ok {
			return fmt.Errorf("连接表不存在: %s", join.Table)
		}

		// 验证连接条件
		if join.Condition == "" {
			return fmt.Errorf("JOIN 缺少条件")
		}

		// 验证连接条件格式
		parts := strings.Split(join.Condition, "=")
		if len(parts) != 2 {
			return fmt.Errorf("无效的JOIN条件: %s", join.Condition)
		}
	}

	return nil
}

// validateConditions 验证条件
func (v *Validator) validateConditions(conditions []Condition) error {
	for _, cond := range conditions {
		// 验证操作符
		if !v.isValidOperator(cond.Operator) {
			return fmt.Errorf("不支持的操作符: %s", cond.Operator)
		}

		// 验证字段
		if strings.Contains(cond.Field, "(") && strings.Contains(cond.Field, ")") {
			// 验证函数调用
			if err := v.validateFunctionCall(cond.Field); err != nil {
				return err
			}
		}
	}

	return nil
}

// isValidOperator 检查是否是有效的操作符
func (v *Validator) isValidOperator(op string) bool {
	validOperators := map[string]bool{
		"=":    true,
		"!=":   true,
		">":    true,
		">=":   true,
		"<":    true,
		"<=":   true,
		"LIKE": true,
		"IN":   true,
	}

	return validOperators[op]
}

// validateGroupBy 验证GROUP BY
func (v *Validator) validateGroupBy(query *Query) error {
	if len(query.GroupBy) == 0 {
		return nil
	}

	// 获取表配置
	tableConfig, ok := v.configManager.GetTable("test", query.Table)
	if !ok {
		return nil // 对于SHOW和DESCRIBE语句，不需要验证GROUP BY
	}

	// 验证每个分组字段
	for _, field := range query.GroupBy {
		found := false
		for _, f := range tableConfig.Fields {
			if f.Name == field {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("GROUP BY 字段不存在: %s", field)
		}
	}

	return nil
}

// validateOrderBy 验证ORDER BY
func (v *Validator) validateOrderBy(query *Query) error {
	if len(query.OrderBy) == 0 {
		return nil
	}

	// 获取表配置
	tableConfig, ok := v.configManager.GetTable("test", query.Table)
	if !ok {
		return nil // 对于SHOW和DESCRIBE语句，不需要验证ORDER BY
	}

	// 验证每个排序字段
	for _, order := range query.OrderBy {
		found := false
		for _, f := range tableConfig.Fields {
			if f.Name == order.Field {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("ORDER BY 字段不存在: %s", order.Field)
		}
	}

	return nil
}

// Optimizer SQL优化器
type Optimizer struct {
	configManager *ConfigManager
}

// NewOptimizer 创建优化器
func NewOptimizer(configManager *ConfigManager) *Optimizer {
	return &Optimizer{
		configManager: configManager,
	}
}

// Optimize 优化查询
func (o *Optimizer) Optimize(query *Query) *Query {
	// 创建查询副本
	optimized := *query

	// 优化WHERE条件
	optimized.Where = o.optimizeConditions(optimized.Where)

	// 优化JOIN顺序
	optimized.Joins = o.optimizeJoins(optimized.Joins)

	// 优化ORDER BY
	optimized.OrderBy = o.optimizeOrderBy(optimized.OrderBy)

	return &optimized
}

// optimizeConditions 优化条件
func (o *Optimizer) optimizeConditions(conditions []Condition) []Condition {
	if len(conditions) == 0 {
		return conditions
	}

	// 合并相同的条件
	uniqueConditions := make(map[string]Condition)
	for _, cond := range conditions {
		key := fmt.Sprintf("%s%s%v", cond.Field, cond.Operator, cond.Value)
		uniqueConditions[key] = cond
	}

	// 转换回切片
	result := make([]Condition, 0, len(uniqueConditions))
	for _, cond := range uniqueConditions {
		result = append(result, cond)
	}

	return result
}

// optimizeJoins 优化JOIN顺序
func (o *Optimizer) optimizeJoins(joins []Join) []Join {
	if len(joins) <= 1 {
		return joins
	}

	// 将INNER JOIN放在前面
	result := make([]Join, 0, len(joins))
	for _, join := range joins {
		if join.Type == JoinTypeInner {
			result = append(result, join)
		}
	}
	for _, join := range joins {
		if join.Type != JoinTypeInner {
			result = append(result, join)
		}
	}

	return result
}

// optimizeOrderBy 优化ORDER BY
func (o *Optimizer) optimizeOrderBy(orderBy []OrderBy) []OrderBy {
	if len(orderBy) == 0 {
		return nil
	}

	// 复制并优化 ORDER BY
	optimized := make([]OrderBy, len(orderBy))
	for i, order := range orderBy {
		// 移除字段名中的表名前缀
		field := order.Field
		if strings.HasSuffix(field, ".*") {
			field = field[:len(field)-2]
		}
		optimized[i] = OrderBy{
			Field:     field,
			Direction: order.Direction,
		}
	}
	return optimized
}
