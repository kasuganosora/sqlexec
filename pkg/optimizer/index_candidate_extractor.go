package optimizer

import (
	"fmt"
	"strings"

	"github.com/kasuganosora/sqlexec/pkg/parser"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// IndexCandidateExtractor 索引候选提取器
// 从 SQL 语句中提取可索引的列，作为索引推荐的候选
type IndexCandidateExtractor struct {
	excludeTypes    map[string]bool // 排除的列类型（对于 BTree 索引）
	fulltextSupport *FullTextIndexSupport
	spatialSupport  *SpatialIndexSupport
}

// NewIndexCandidateExtractor 创建索引候选提取器
func NewIndexCandidateExtractor() *IndexCandidateExtractor {
	return &IndexCandidateExtractor{
		excludeTypes: map[string]bool{
			"BLOB":       true,
			"TEXT":       true,
			"JSON":       true,
			"LONGBLOB":   true,
			"LONGTEXT":   true,
			"MEDIUMTEXT": true,
		},
		fulltextSupport: NewFullTextIndexSupport(),
		spatialSupport:  NewSpatialIndexSupport(),
	}
}

// ExtractFromSQL 从 SQL 语句提取索引候选
func (e *IndexCandidateExtractor) ExtractFromSQL(stmt *parser.SQLStatement, tableInfo map[string]*domain.TableInfo) ([]*IndexCandidate, error) {
	if stmt.Select == nil {
		return nil, fmt.Errorf("only SELECT statements are supported")
	}

	var candidates []*IndexCandidate

	// 从 WHERE 子句提取 BTree 索引候选
	whereCandidates := e.extractFromWhere(stmt.Select.Where)
	candidates = append(candidates, whereCandidates...)

	// 从 WHERE 子句提取全文索引候选
	fulltextCandidates := e.extractFulltextFromWhere(stmt.Select.Where, tableInfo)
	candidates = append(candidates, fulltextCandidates...)

	// 从 WHERE 子句提取空间索引候选
	spatialCandidates := e.extractSpatialFromWhere(stmt.Select.Where, tableInfo)
	candidates = append(candidates, spatialCandidates...)

	// 从 JOIN 条件提取
	joinCandidates := e.extractFromJoins(stmt.Select.Joins, tableInfo)
	candidates = append(candidates, joinCandidates...)

	// 从 ORDER BY 子句提取
	orderCandidates := e.extractFromOrderBy(stmt.Select.OrderBy)
	candidates = append(candidates, orderCandidates...)

	// 从 GROUP BY 子句提取
	groupCandidates := e.extractFromGroupBy(stmt.Select.GroupBy)
	candidates = append(candidates, groupCandidates...)

	// 去重和合并
	return e.deduplicateCandidates(candidates), nil
}

// extractFulltextFromWhere 从 WHERE 子句提取全文索引候选
func (e *IndexCandidateExtractor) extractFulltextFromWhere(where *parser.Expression, tableInfo map[string]*domain.TableInfo) []*IndexCandidate {
	if where == nil {
		return nil
	}

	var candidates []*IndexCandidate

	// 遍历所有表
	for tableName, tbl := range tableInfo {
		// 构建列类型映射
		columnTypes := make(map[string]string)
		for _, col := range tbl.Columns {
			columnTypes[col.Name] = col.Type
		}

		// 提取全文索引候选
		exprStr := fmt.Sprintf("%v", where)
		ftCandidates := e.fulltextSupport.ExtractFullTextIndexCandidates(tableName, exprStr, columnTypes)
		candidates = append(candidates, ftCandidates...)
	}

	return candidates
}

// extractSpatialFromWhere 从 WHERE 子句提取空间索引候选
func (e *IndexCandidateExtractor) extractSpatialFromWhere(where *parser.Expression, tableInfo map[string]*domain.TableInfo) []*IndexCandidate {
	if where == nil {
		return nil
	}

	var candidates []*IndexCandidate

	// 遍历所有表
	for tableName, tbl := range tableInfo {
		// 构建列类型映射
		columnTypes := make(map[string]string)
		for _, col := range tbl.Columns {
			columnTypes[col.Name] = col.Type
		}

		// 提取空间索引候选
		exprStr := fmt.Sprintf("%v", where)
		spCandidates := e.spatialSupport.ExtractSpatialIndexCandidates(tableName, exprStr, columnTypes)
		candidates = append(candidates, spCandidates...)
	}

	return candidates
}

// extractFromWhere 从 WHERE 子句提取索引候选
func (e *IndexCandidateExtractor) extractFromWhere(where *parser.Expression) []*IndexCandidate {
	if where == nil {
		return nil
	}

	var candidates []*IndexCandidate

	// 递归遍历表达式树
	e.traverseExpression(where, &candidates, "WHERE", 4)

	return candidates
}

// extractFromJoins 从 JOIN 条件提取索引候选
func (e *IndexCandidateExtractor) extractFromJoins(joins []parser.JoinInfo, tableInfo map[string]*domain.TableInfo) []*IndexCandidate {
	var candidates []*IndexCandidate

	for _, join := range joins {
		if join.Condition != nil {
			e.traverseExpression(join.Condition, &candidates, "JOIN", 3)
		}
	}

	return candidates
}

// extractFromOrderBy 从 ORDER BY 子句提取索引候选
func (e *IndexCandidateExtractor) extractFromOrderBy(orderBy []parser.OrderByItem) []*IndexCandidate {
	if len(orderBy) == 0 {
		return nil
	}

	var candidates []*IndexCandidate
	var columns []string

	// 收集所有 ORDER BY 列
	for _, item := range orderBy {
		if item.Column != "" {
			columns = append(columns, item.Column)
		}
	}

	if len(columns) > 0 {
		// 单列或多列索引
		candidates = append(candidates, &IndexCandidate{
			Columns:  columns,
			Priority: 1,
			Source:   "ORDER",
			Unique:   false,
		})
	}

	return candidates
}

// extractFromGroupBy 从 GROUP BY 子句提取索引候选
func (e *IndexCandidateExtractor) extractFromGroupBy(groupBy []string) []*IndexCandidate {
	if len(groupBy) == 0 {
		return nil
	}

	var candidates []*IndexCandidate
	var columns []string

	// 收集所有 GROUP BY 列
	for _, col := range groupBy {
		if col != "" {
			columns = append(columns, col)
		}
	}

	if len(columns) > 0 {
		// GROUP BY 通常可以考虑创建索引
		candidates = append(candidates, &IndexCandidate{
			Columns:  columns,
			Priority: 2,
			Source:   "GROUP",
			Unique:   false,
		})
	}

	return candidates
}

// traverseExpression 递归遍历表达式树，提取可索引列
func (e *IndexCandidateExtractor) traverseExpression(expr *parser.Expression, candidates *[]*IndexCandidate, source string, priority int) {
	if expr == nil {
		return
	}

	// 如果是列表达式，提取列名并返回（避免重复）
	if (expr.Type == parser.ExprTypeColumn || expr.Type == "") && expr.Column != "" {
		*candidates = append(*candidates, &IndexCandidate{
			Columns:  []string{expr.Column},
			Priority: priority,
			Source:   source,
			Unique:   false,
		})
		return
	}

	// 处理逻辑运算符（AND, OR）- 需要递归处理子表达式
	if expr.Operator == "AND" || expr.Operator == "OR" {
		if expr.Left != nil {
			e.traverseExpression(expr.Left, candidates, source, priority)
		}
		if expr.Right != nil {
			e.traverseExpression(expr.Right, candidates, source, priority)
		}
		return
	}

	// 处理比较运算符（=, >, <, LIKE, IN, BETWEEN 等）
	if e.isIndexableComparison(expr) {
		// 提取左边的列
		if expr.Left != nil {
			e.traverseExpression(expr.Left, candidates, source, priority)
		}
		// 提取右边的列（如果有的话）
		if expr.Right != nil {
			e.traverseExpression(expr.Right, candidates, source, priority)
		}
		return
	}

	// 处理 IN 表达式
	if expr.Operator == "IN" {
		if expr.Left != nil && expr.Left.Column != "" {
			*candidates = append(*candidates, &IndexCandidate{
				Columns:  []string{expr.Left.Column},
				Priority: priority,
				Source:   source,
				Unique:   false,
			})
		}
	}

	// 处理 BETWEEN 表达式
	if expr.Operator == "BETWEEN" {
		if expr.Left != nil && expr.Left.Column != "" {
			*candidates = append(*candidates, &IndexCandidate{
				Columns:  []string{expr.Left.Column},
				Priority: priority,
				Source:   source,
				Unique:   false,
			})
		}
	}
}

// isIndexableComparison 判断是否为可索引的比较运算
func (e *IndexCandidateExtractor) isIndexableComparison(expr *parser.Expression) bool {
	if expr == nil || expr.Operator == "" {
		return false
	}

	// 逻辑运算符不是比较运算
	if expr.Operator == "AND" || expr.Operator == "OR" {
		return false
	}

	// 可索引的运算符
	indexableOps := map[string]bool{
		"=":       true,
		"!=":      true,
		">":       true,
		">=":      true,
		"<":       true,
		"<=":      true,
		"LIKE":    true,
		"IN":      true,
		"BETWEEN": true,
	}

	return indexableOps[expr.Operator]
}

// isColumnLeft 判断列是否在运算符左边
func (e *IndexCandidateExtractor) isColumnLeft(expr *parser.Expression) bool {
	return expr.Left != nil && expr.Left.Column != ""
}

// isColumnTypeSupported 检查列类型是否支持索引
func (e *IndexCandidateExtractor) isColumnTypeSupported(columnType string) bool {
	if columnType == "" {
		return true // 未知类型，假设支持
	}
	return !e.excludeTypes[strings.ToUpper(columnType)]
}

// deduplicateCandidates 去重并合并候选
func (e *IndexCandidateExtractor) deduplicateCandidates(candidates []*IndexCandidate) []*IndexCandidate {
	// 使用 map 去重
	seen := make(map[string]bool)
	var result []*IndexCandidate

	for _, candidate := range candidates {
		key := e.buildCandidateKey(candidate)
		if !seen[key] {
			seen[key] = true
			result = append(result, candidate)
		}
	}

	// 按优先级排序（WHERE > JOIN > GROUP > ORDER）
	for i := 0; i < len(result); i++ {
		for j := i + 1; j < len(result); j++ {
			if result[i].Priority < result[j].Priority {
				result[i], result[j] = result[j], result[i]
			}
		}
	}

	return result
}

// buildCandidateKey 构建候选的唯一键
func (e *IndexCandidateExtractor) buildCandidateKey(candidate *IndexCandidate) string {
	key := strings.Join(candidate.Columns, ",")
	return key
}

// ExtractForTable 提取指定表的索引候选
func (e *IndexCandidateExtractor) ExtractForTable(candidates []*IndexCandidate, tableName string) []*IndexCandidate {
	var result []*IndexCandidate
	for _, candidate := range candidates {
		// 检查列是否属于指定表
		for _, col := range candidate.Columns {
			if strings.HasPrefix(col, tableName+".") || !strings.Contains(col, ".") {
				result = append(result, candidate)
				break
			}
		}
	}
	return result
}

// GetColumnInfo 获取列信息
func (e *IndexCandidateExtractor) GetColumnInfo(tableInfo map[string]*domain.TableInfo, tableName, columnName string) (string, bool) {
	if tableInfo == nil {
		return "", false
	}

	if table, exists := tableInfo[tableName]; exists {
		for _, col := range table.Columns {
			if col.Name == columnName {
				return col.Type, true
			}
		}
	}

	return "", false
}

// FilterByColumnType 根据列类型过滤候选
func (e *IndexCandidateExtractor) FilterByColumnType(candidates []*IndexCandidate, tableInfo map[string]*domain.TableInfo) []*IndexCandidate {
	var result []*IndexCandidate

	for _, candidate := range candidates {
		supported := true
		for _, col := range candidate.Columns {
			// 解析表名和列名
			tableName, colName := e.parseColumnReference(col)

			// 获取列类型
			if tableName != "" && colName != "" {
				if table, exists := tableInfo[tableName]; exists {
					for _, colInfo := range table.Columns {
						if colInfo.Name == colName {
							if !e.isColumnTypeSupported(colInfo.Type) {
								supported = false
							}
							break
						}
					}
				}
			}
		}

		if supported {
			result = append(result, candidate)
		}
	}

	return result
}

// parseColumnReference 解析列引用
func (e *IndexCandidateExtractor) parseColumnReference(colRef string) (tableName, colName string) {
	if colRef == "" {
		return "", ""
	}

	parts := strings.Split(colRef, ".")
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return "", colRef
}

// MergeCandidates 合并相似的候选
func (e *IndexCandidateExtractor) MergeCandidates(candidates []*IndexCandidate) []*IndexCandidate {
	if len(candidates) == 0 {
		return candidates
	}

	// 合并多列索引
	// 例如：如果已有 [id, name] 候选，可以合并单列 [id] 和 [name]
	merged := make(map[string]*IndexCandidate)

	for _, candidate := range candidates {
		key := strings.Join(candidate.Columns, ",")

		if existing, exists := merged[key]; exists {
			// 保留优先级更高的
			if candidate.Priority > existing.Priority {
				merged[key] = candidate
			}
		} else {
			merged[key] = candidate
		}
	}

	// 转换回切片
	var result []*IndexCandidate
	for _, candidate := range merged {
		result = append(result, candidate)
	}

	// 按优先级排序
	for i := 0; i < len(result); i++ {
		for j := i + 1; j < len(result); j++ {
			if result[i].Priority < result[j].Priority {
				result[i], result[j] = result[j], result[i]
			}
		}
	}

	return result
}
