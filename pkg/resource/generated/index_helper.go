package generated

import (
	"fmt"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// IndexHelper 生成列索引辅助器
// 负责处理 STORED 和 VIRTUAL 生成列的索引创建和维护
type IndexHelper struct {
	evaluator *GeneratedColumnEvaluator
}

// NewIndexHelper 创建索引辅助器
func NewIndexHelper() *IndexHelper {
	return &IndexHelper{
		evaluator: NewGeneratedColumnEvaluator(),
	}
}

// NewIndexHelperWithEvaluator 使用指定求值器创建索引辅助器
func NewIndexHelperWithEvaluator(evaluator *GeneratedColumnEvaluator) *IndexHelper {
	return &IndexHelper{
		evaluator: evaluator,
	}
}

// CanIndexGeneratedColumn 检查生成列是否可以创建索引
// STORED 列：可以创建普通索引
// VIRTUAL 列：可以创建函数索引（但有限制）
func (h *IndexHelper) CanIndexGeneratedColumn(col *domain.ColumnInfo) (bool, string) {
	if col == nil || !col.IsGenerated {
		return false, "column is not a generated column"
	}

	switch col.GeneratedType {
	case "STORED":
		// STORED 列可以创建普通索引
		return true, ""
	case "VIRTUAL":
		// VIRTUAL 列可以创建函数索引，但有条件限制
		// 第二阶段：基础支持，检查表达式复杂度
		if h.isIndexableExpression(col.GeneratedExpr) {
			return true, ""
		}
		return false, "VIRTUAL column expression is too complex for indexing"
	default:
		return false, fmt.Sprintf("unsupported generated column type: %s", col.GeneratedType)
	}
}

// GetIndexValueForGenerated 获取生成列的索引值
// STORED 列：直接使用存储的值
// VIRTUAL 列：实时计算值
func (h *IndexHelper) GetIndexValueForGenerated(
	colName string,
	row domain.Row,
	schema *domain.TableInfo,
) (interface{}, error) {
	colInfo := h.getColumnInfo(colName, schema)
	if colInfo == nil {
		return nil, fmt.Errorf("column not found: %s", colName)
	}

	if !colInfo.IsGenerated {
		// 非生成列，直接返回值
		return row[colName], nil
	}

	switch colInfo.GeneratedType {
	case "STORED":
		// STORED 列，使用存储的值
		return row[colName], nil
	case "VIRTUAL":
		// VIRTUAL 列，实时计算
		virtualCalc := NewVirtualCalculator()
		return virtualCalc.CalculateColumn(colInfo, row, schema)
	default:
		return nil, fmt.Errorf("unsupported generated column type: %s", colInfo.GeneratedType)
	}
}

// ValidateIndexDefinition 验证生成列索引定义
func (h *IndexHelper) ValidateIndexDefinition(
	colName string,
	schema *domain.TableInfo,
) error {
	colInfo := h.getColumnInfo(colName, schema)
	if colInfo == nil {
		return fmt.Errorf("column not found: %s", colName)
	}

	if !colInfo.IsGenerated {
		// 非生成列，通过现有验证逻辑
		return nil
	}

	// 验证生成列类型
	if colInfo.GeneratedType == "VIRTUAL" {
		// VIRTUAL 列索引需要额外检查
		if !h.isIndexableExpression(colInfo.GeneratedExpr) {
			return fmt.Errorf("VIRTUAL column %s has a non-indexable expression", colName)
		}
	}

	return nil
}

// GetIndexableGeneratedColumns 获取可以创建索引的生成列列表
func (h *IndexHelper) GetIndexableGeneratedColumns(
	schema *domain.TableInfo,
) []string {
	indexable := make([]string, 0)
	
	for _, col := range schema.Columns {
		if !col.IsGenerated {
			continue
		}
		
		canIndex, _ := h.CanIndexGeneratedColumn(&col)
		if canIndex {
			indexable = append(indexable, col.Name)
		}
	}
	
	return indexable
}

// isIndexableExpression 检查表达式是否可索引
// 简化实现：检查表达式复杂度和依赖
func (h *IndexHelper) isIndexableExpression(expr string) bool {
	if expr == "" {
		return false
	}

	// 简单判断：表达式长度不超过一定限制
	// 实际应该解析 AST 并分析节点类型
	if len(expr) > 1000 {
		return false
	}

	// TODO: 更复杂的表达式分析
	// 1. 检查是否包含不确定性函数（如 RAND(), NOW()）
	// 2. 检查是否包含子查询
	// 3. 检查是否包含用户自定义函数（非确定性）
	// 4. 检查依赖列是否可索引（如 TEXT, BLOB 类型）

	return true
}

// getColumnInfo 获取列信息
func (h *IndexHelper) getColumnInfo(name string, schema *domain.TableInfo) *domain.ColumnInfo {
	for i, col := range schema.Columns {
		if col.Name == name {
			return &schema.Columns[i]
		}
	}
	return nil
}
