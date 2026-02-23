package generated

import (
	"fmt"
	"strings"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// nonDeterministicFunctions lists SQL functions that produce non-deterministic
// results and therefore cannot be used in indexable generated column expressions.
var nonDeterministicFunctions = []string{
	"RAND(",
	"NOW(",
	"UUID(",
	"CURRENT_TIMESTAMP(",
	"CURRENT_DATE(",
	"CURRENT_TIME(",
	"SYSDATE(",
	"UNIX_TIMESTAMP(",
	"CONNECTION_ID(",
	"LAST_INSERT_ID(",
	"UUID_SHORT(",
	"SLEEP(",
	"BENCHMARK(",
	"GET_LOCK(",
	"RELEASE_LOCK(",
	"ROW_COUNT(",
	"FOUND_ROWS(",
}

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

// isIndexableExpression checks whether a generated column expression can be
// indexed. It performs string-based detection of patterns that would make an
// expression non-indexable:
//  1. Non-deterministic functions (RAND(), NOW(), UUID(), etc.)
//  2. Subqueries (presence of SELECT keyword)
//  3. Expression length exceeding the complexity threshold
func (h *IndexHelper) isIndexableExpression(expr string) bool {
	if expr == "" {
		return false
	}

	// Reject expressions that are too long (complexity threshold)
	if len(expr) > 1000 {
		return false
	}

	upper := strings.ToUpper(expr)

	// 1. Check for non-deterministic functions
	for _, fn := range nonDeterministicFunctions {
		if strings.Contains(upper, fn) {
			return false
		}
	}

	// 2. Check for subqueries (SELECT keyword presence indicates a subquery)
	// Use word-boundary-aware check: look for SELECT preceded by start-of-string
	// or a non-alphanumeric character to avoid false positives on column names
	// like "user_select".
	if h.containsKeyword(upper, "SELECT") {
		return false
	}

	return true
}

// containsKeyword checks if the expression contains a SQL keyword as a whole
// word (not part of an identifier). It looks for the keyword preceded by a
// non-alphanumeric character (or start of string) and followed by a
// non-alphanumeric character (or end of string).
func (h *IndexHelper) containsKeyword(upperExpr, keyword string) bool {
	idx := 0
	for {
		pos := strings.Index(upperExpr[idx:], keyword)
		if pos < 0 {
			return false
		}
		absPos := idx + pos
		// Check character before the keyword
		if absPos > 0 {
			before := upperExpr[absPos-1]
			if isAlphaNumeric(before) {
				idx = absPos + len(keyword)
				continue
			}
		}
		// Check character after the keyword
		afterPos := absPos + len(keyword)
		if afterPos < len(upperExpr) {
			after := upperExpr[afterPos]
			if isAlphaNumeric(after) {
				idx = absPos + len(keyword)
				continue
			}
		}
		return true
	}
}

// isAlphaNumeric returns true if the byte is a letter, digit, or underscore.
func isAlphaNumeric(b byte) bool {
	return (b >= 'A' && b <= 'Z') || (b >= 'a' && b <= 'z') || (b >= '0' && b <= '9') || b == '_'
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
