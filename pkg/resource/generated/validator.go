package generated

import (
	"fmt"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// GeneratedColumnValidator 生成列验证器
type GeneratedColumnValidator struct{}

// ValidateSchema 验证生成列定义
func (v *GeneratedColumnValidator) ValidateSchema(tableInfo *domain.TableInfo) error {
	// 1. 检查依赖的列是否存在
	if err := v.CheckDependenciesExist(tableInfo); err != nil {
		return err
	}

	// 2. 检查是否引用了生成列自身
	if err := v.CheckSelfReference(tableInfo); err != nil {
		return err
	}

	// 3. 检查循环依赖（拓扑排序）
	if err := v.CheckCyclicDependency(tableInfo); err != nil {
		return err
	}

	return nil
}

// BuildDependencyGraph 构建依赖图
func (v *GeneratedColumnValidator) BuildDependencyGraph(tableInfo *domain.TableInfo) map[string][]string {
	graph := make(map[string][]string)

	// 构建列名到列信息的映射
	colMap := make(map[string]domain.ColumnInfo)
	for _, col := range tableInfo.Columns {
		colMap[col.Name] = col
	}

	// 为每个生成列构建依赖边
	for _, col := range tableInfo.Columns {
		if col.IsGenerated {
			if len(col.GeneratedDepends) > 0 {
				graph[col.Name] = col.GeneratedDepends
			} else {
				// 即使没有依赖，也要添加空边，确保所有生成列都在图中
				graph[col.Name] = []string{}
			}
		}
	}

	return graph
}

// CheckCyclicDependency 检查循环依赖（拓扑排序）
func (v *GeneratedColumnValidator) CheckCyclicDependency(tableInfo *domain.TableInfo) error {
	graph := v.BuildDependencyGraph(tableInfo)

	// 如果没有生成列，直接返回
	if len(graph) == 0 {
		return nil
	}

	// 拓扑排序
	inDegree := make(map[string]int)
	visited := make(map[string]bool)
	result := make([]string, 0)

	// 收集所有生成列名
	generatedColSet := make(map[string]bool)
	for _, col := range tableInfo.Columns {
		if col.IsGenerated {
			generatedColSet[col.Name] = true
		}
	}

	// 计算入度：只计算依赖其他生成列的边
	for colName, deps := range graph {
		inDegree[colName] = 0
		// 只计算依赖中也是生成列的
		for _, dep := range deps {
			if generatedColSet[dep] {
				inDegree[colName]++
			}
		}
	}

	// Kahn 算法进行拓扑排序
	queue := make([]string, 0)
	for colName, degree := range inDegree {
		if degree == 0 {
			queue = append(queue, colName)
			visited[colName] = true
		}
	}

	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]
		result = append(result, node)

		// 找到所有依赖 node 的生成列，减少它们的入度
		for otherColName, deps := range graph {
			if !visited[otherColName] && contains(deps, node) {
				inDegree[otherColName]--
				if inDegree[otherColName] == 0 {
					queue = append(queue, otherColName)
					visited[otherColName] = true
				}
			}
		}
	}

	// 检查是否存在循环依赖
	generatedColCount := countGeneratedColumns(tableInfo)
	if len(result) != generatedColCount {
		// 找出哪些节点在循环中
		var cycleNodes []string
		for _, col := range tableInfo.Columns {
			if col.IsGenerated && !contains(result, col.Name) {
				cycleNodes = append(cycleNodes, col.Name)
			}
		}
		if len(cycleNodes) > 0 {
			return fmt.Errorf("cyclic dependency detected in generated columns: %v", cycleNodes)
		}
	}

	return nil
}

// CheckDependenciesExist 检查依赖的列是否存在
func (v *GeneratedColumnValidator) CheckDependenciesExist(tableInfo *domain.TableInfo) error {
	// 构建列名集合
	colNames := make(map[string]bool)
	for _, col := range tableInfo.Columns {
		colNames[col.Name] = true
	}

	// 检查每个生成列的依赖
	for _, col := range tableInfo.Columns {
		if col.IsGenerated {
			for _, dep := range col.GeneratedDepends {
				if !colNames[dep] {
					return fmt.Errorf("generated column '%s' references non-existent column '%s'", col.Name, dep)
				}
			}
		}
	}

	return nil
}

// CheckSelfReference 检查生成列是否引用自身
func (v *GeneratedColumnValidator) CheckSelfReference(tableInfo *domain.TableInfo) error {
	for _, col := range tableInfo.Columns {
		if col.IsGenerated {
			for _, dep := range col.GeneratedDepends {
				if dep == col.Name {
					return fmt.Errorf("generated column '%s' references itself", col.Name)
				}
			}
		}
	}
	return nil
}

// CheckAutoIncrementReference 检查生成列是否引用了 AUTO_INCREMENT 列
func (v *GeneratedColumnValidator) CheckAutoIncrementReference(tableInfo *domain.TableInfo) error {
	// 收集所有 AUTO_INCREMENT 列名
	autoIncCols := make(map[string]bool)
	for _, col := range tableInfo.Columns {
		if col.AutoIncrement {
			autoIncCols[col.Name] = true
		}
	}

	// 检查生成列是否引用了 AUTO_INCREMENT 列
	for _, col := range tableInfo.Columns {
		if col.IsGenerated {
			for _, dep := range col.GeneratedDepends {
				if autoIncCols[dep] {
					return fmt.Errorf("generated column '%s' references AUTO_INCREMENT column '%s'", col.Name, dep)
				}
			}
		}
	}

	return nil
}

// contains 检查字符串切片是否包含某个元素
func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

// countGeneratedColumns 统计生成列数量
func countGeneratedColumns(tableInfo *domain.TableInfo) int {
	count := 0
	for _, col := range tableInfo.Columns {
		if col.IsGenerated {
			count++
		}
	}
	return count
}
