package generated

import (
	"fmt"

	"github.com/kasuganosora/sqlexec/pkg/builtin"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// VirtualCalculator VIRTUAL 列计算引擎
// 负责 VIRTUAL 生成列的动态计算，不持久化存储
type VirtualCalculator struct {
	exprCache   *ExpressionCache
	functionAPI *builtin.FunctionAPI
}

// NewVirtualCalculator 创建 VIRTUAL 列计算器
func NewVirtualCalculator() *VirtualCalculator {
	return &VirtualCalculator{
		exprCache:   NewExpressionCache(),
		functionAPI: builtin.NewFunctionAPI(),
	}
}

// NewVirtualCalculatorWithCache 使用指定缓存创建计算器
func NewVirtualCalculatorWithCache(cache *ExpressionCache) *VirtualCalculator {
	return &VirtualCalculator{
		exprCache:   cache,
		functionAPI: builtin.NewFunctionAPI(),
	}
}

// CalculateColumn 计算单个 VIRTUAL 列的值
// VIRTUAL 列在查询时动态计算，不存储到表数据中
func (v *VirtualCalculator) CalculateColumn(
	col *domain.ColumnInfo,
	row domain.Row,
	schema *domain.TableInfo,
) (interface{}, error) {
	if col == nil {
		return nil, fmt.Errorf("column info is nil")
	}

	// 检查是否为 VIRTUAL 列
	if !col.IsGenerated || col.GeneratedType != "VIRTUAL" {
		return nil, fmt.Errorf("column is not a VIRTUAL generated column")
	}

	if col.GeneratedExpr == "" {
		return nil, fmt.Errorf("generated expression is empty")
	}

	// 使用现有的求值器计算表达式
	evaluator := NewGeneratedColumnEvaluator()
	result, err := evaluator.Evaluate(col.GeneratedExpr, row, schema)
	if err != nil {
		// VIRTUAL 列计算失败时返回 NULL 和错误
		return nil, err
	}

	// 类型转换到列的目标类型
	castedResult, castErr := CastToType(result, col.Type)
	if castErr != nil {
		// 类型转换失败时返回 NULL 和错误
		return nil, castErr
	}

	return castedResult, nil
}

// CalculateRowVirtuals 计算行中所有 VIRTUAL 列的值
// 返回包含 VIRTUAL 列值的新行
func (v *VirtualCalculator) CalculateRowVirtuals(
	row domain.Row,
	schema *domain.TableInfo,
) (domain.Row, error) {
	// 创建结果行，复制原始数据
	result := make(domain.Row)
	for k, val := range row {
		result[k] = val
	}

	// 获取所有 VIRTUAL 列的计算顺序（只包括 VIRTUAL）
	order, err := v.getVirtualColumnOrder(schema)
	if err != nil {
		// 如果获取顺序失败，返回原始行
		return result, nil
	}

	// 逐个计算所有 VIRTUAL 列
	for _, colName := range order {
		colInfo := v.getColumnInfo(colName, schema)
		if colInfo == nil || !colInfo.IsGenerated {
			continue
		}

		// 只计算 VIRTUAL 列
		if colInfo.GeneratedType != "VIRTUAL" {
			continue
		}

		// 计算列值
		value, err := v.CalculateColumn(colInfo, result, schema)
		if err != nil {
			// 计算失败时设为 NULL
			result[colName] = nil
		} else {
			// 无论 value 是否为 nil，都赋值（因为 nil 是有效的 NULL 值）
			result[colName] = value
		}
	}

	return result, nil
}

// CalculateBatchVirtuals 批量计算多行 VIRTUAL 列
// 性能优化：一次性计算多行，减少重复编译开销
func (v *VirtualCalculator) CalculateBatchVirtuals(
	rows []domain.Row,
	schema *domain.TableInfo,
) ([]domain.Row, error) {
	if len(rows) == 0 {
		return rows, nil
	}

	results := make([]domain.Row, len(rows))

	for i, row := range rows {
		calculated, err := v.CalculateRowVirtuals(row, schema)
		if err != nil {
			return nil, fmt.Errorf("failed to calculate virtual columns for row %d: %w", i, err)
		}
		results[i] = calculated
	}

	return results, nil
}

// GetVirtualColumnNames 获取所有 VIRTUAL 列的名称
func (v *VirtualCalculator) GetVirtualColumnNames(schema *domain.TableInfo) []string {
	names := make([]string, 0)
	for _, col := range schema.Columns {
		if col.IsGenerated && col.GeneratedType == "VIRTUAL" {
			names = append(names, col.Name)
		}
	}
	return names
}

// HasVirtualColumns 检查表是否包含 VIRTUAL 列
func (v *VirtualCalculator) HasVirtualColumns(schema *domain.TableInfo) bool {
	for _, col := range schema.Columns {
		if col.IsGenerated && col.GeneratedType == "VIRTUAL" {
			return true
		}
	}
	return false
}

// getEvaluationOrder 获取生成列的计算顺序（拓扑排序）
func (v *VirtualCalculator) getEvaluationOrder(schema *domain.TableInfo) ([]string, error) {
	// 复用 evaluator 的拓扑排序逻辑
	evaluator := NewGeneratedColumnEvaluator()
	order, err := evaluator.GetEvaluationOrder(schema)
	return order, err
}

// getVirtualColumnOrder 获取 VIRTUAL 列的计算顺序
func (v *VirtualCalculator) getVirtualColumnOrder(schema *domain.TableInfo) ([]string, error) {
	// 收集所有 VIRTUAL 列
	virtualCols := make([]string, 0)
	for _, col := range schema.Columns {
		if col.IsGenerated && col.GeneratedType == "VIRTUAL" {
			virtualCols = append(virtualCols, col.Name)
		}
	}

	// 对 VIRTUAL 列进行拓扑排序（基于依赖关系）
	validator := &GeneratedColumnValidator{}
	graph := validator.BuildDependencyGraph(schema)

	order := make([]string, 0)
	inDegree := make(map[string]int)
	visited := make(map[string]bool)

	// 初始化入度
	for _, colName := range virtualCols {
		inDegree[colName] = 0
	}

	// 计算入度（仅考虑 VIRTUAL 列之间的依赖）
	for _, colName := range virtualCols {
		if deps, ok := graph[colName]; ok {
			// 只计算依赖也是 VIRTUAL 列的依赖
			virtualDeps := make([]string, 0)
			for _, dep := range deps {
				// 检查依赖是否是 VIRTUAL 列
				for _, col := range schema.Columns {
					if col.Name == dep && col.IsGenerated && col.GeneratedType == "VIRTUAL" {
						virtualDeps = append(virtualDeps, dep)
						break
					}
				}
			}
			inDegree[colName] = len(virtualDeps)
		}
	}

	// 拓扑排序
	queue := make([]string, 0)
	for _, colName := range virtualCols {
		if inDegree[colName] == 0 {
			queue = append(queue, colName)
			visited[colName] = true
		}
	}

	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]
		order = append(order, node)

		// 减少入度
		for _, colName := range virtualCols {
			if !visited[colName] {
				if deps, ok := graph[colName]; ok {
					for _, dep := range deps {
						// 只处理 VIRTUAL 依赖
						isVirtualDep := false
						for _, col := range schema.Columns {
							if col.Name == dep && col.IsGenerated && col.GeneratedType == "VIRTUAL" {
								isVirtualDep = true
								break
							}
						}
						if isVirtualDep && dep == node {
							inDegree[colName]--
							break
						}
					}
				}
			}
		}

		// 将入度为0的节点加入队列
		for _, colName := range virtualCols {
			if !visited[colName] && inDegree[colName] == 0 {
				queue = append(queue, colName)
				visited[colName] = true
			}
		}
	}

	return order, nil
}

// IsVirtualColumn 检查列是否为 VIRTUAL 类型
func IsVirtualColumn(colName string, schema *domain.TableInfo) bool {
	for _, col := range schema.Columns {
		if col.Name == colName && col.IsGenerated && col.GeneratedType == "VIRTUAL" {
			return true
		}
	}
	return false
}

// getColumnInfo 获取列信息
func (v *VirtualCalculator) getColumnInfo(name string, schema *domain.TableInfo) *domain.ColumnInfo {
	for i, col := range schema.Columns {
		if col.Name == name {
			return &schema.Columns[i]
		}
	}
	return nil
}
