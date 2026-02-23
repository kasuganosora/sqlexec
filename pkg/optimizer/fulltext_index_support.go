package optimizer

import (
	"fmt"
	"regexp"
	"strings"
)

// FullTextIndexSupport 全文索引支持
type FullTextIndexSupport struct {
	// 可配置的最小词长度
	MinWordLength int
	// 停用词列表
	StopWords []string
}

// NewFullTextIndexSupport 创建全文索引支持实例
func NewFullTextIndexSupport() *FullTextIndexSupport {
	return &FullTextIndexSupport{
		MinWordLength: 4,
		StopWords: []string{
			"the", "a", "an", "and", "or", "but", "is", "are", "was", "were",
			"be", "been", "being", "have", "has", "had", "do", "does", "did",
			"will", "would", "shall", "should", "can", "could", "may", "might",
			"must", "i", "you", "he", "she", "it", "we", "they", "this", "that",
			"these", "those", "am", "isn't", "aren't", "wasn't", "weren't",
			"haven't", "hasn't", "hadn't", "don't", "doesn't", "didn't", "won't",
			"wouldn't", "shan't", "shouldn't", "can't", "couldn't", "mayn't",
			"mightn't", "mustn't", "in", "on", "at", "by", "for", "with", "about",
			"against", "between", "into", "through", "during", "before", "after",
			"above", "below", "to", "from", "up", "down", "of", "off", "over",
			"under", "again", "further", "then", "once", "here", "there", "when",
			"where", "why", "how", "all", "any", "both", "each", "few", "more",
			"most", "other", "some", "such", "no", "nor", "not", "only", "own",
			"same", "so", "than", "too", "very", "s", "t", "can", "will", "just",
			"don", "should", "now",
		},
	}
}

// IsFullTextExpression 检查表达式是否为全文索引相关表达式
func (fts *FullTextIndexSupport) IsFullTextExpression(expr string) bool {
	// 检查 MATCH AGAINST 表达式
	if fts.isMatchAgainstExpression(expr) {
		return true
	}

	// 检查 FULLTEXT 函数调用
	if fts.isFulltextFunction(expr) {
		return true
	}

	// 检查 LIKE 表达式（带通配符）
	if fts.isLikeWithWildcard(expr) {
		return true
	}

	return false
}

// isMatchAgainstExpression 检查是否为 MATCH AGAINST 表达式
func (fts *FullTextIndexSupport) isMatchAgainstExpression(expr string) bool {
	// 匹配模式：MATCH (col1, col2, ...) AGAINST ('search_term')
	pattern := `(?i)MATCH\s*\(([^)]+)\)\s+AGAINST\s*\(`
	re := regexp.MustCompile(pattern)
	return re.MatchString(expr)
}

// isFulltextFunction 检查是否为 FULLTEXT 函数调用
func (fts *FullTextIndexSupport) isFulltextFunction(expr string) bool {
	// 匹配模式：FULLTEXT(col1, col2, ...) 或是全文相关函数
	pattern := `(?i)FULLTEXT\s*\(`
	re := regexp.MustCompile(pattern)
	return re.MatchString(expr)
}

// isLikeWithWildcard 检查是否为带通配符的 LIKE 表达式
func (fts *FullTextIndexSupport) isLikeWithWildcard(expr string) bool {
	// 匹配模式：col LIKE '%term%' 或 col LIKE '%term'
	// 注意：前缀 LIKE 'term%' 可以使用普通索引，不需要全文索引
	pattern := `(?i)\w+\s+LIKE\s+'([^']*)'`
	re := regexp.MustCompile(pattern)
	matches := re.FindStringSubmatch(expr)

	if len(matches) == 2 {
		// 检查 LIKE 值是否有前导通配符（需要全文索引）
		likeValue := matches[1]
		return strings.HasPrefix(likeValue, "%")
	}

	return false
}

// IsColumnTypeCompatible 检查列类型是否支持全文索引
func (fts *FullTextIndexSupport) IsColumnTypeCompatible(columnType string) bool {
	// 全文索引支持的列类型：CHAR, VARCHAR, TEXT, MEDIUMTEXT, LONGTEXT
	compatibleTypes := []string{
		"CHAR", "VARCHAR", "TEXT", "MEDIUMTEXT", "LONGTEXT", "TINYTEXT",
		"CHARACTER", "NCHAR", "NVARCHAR", "NTEXT",
	}

	upperType := strings.ToUpper(columnType)
	for _, compatible := range compatibleTypes {
		if strings.HasPrefix(upperType, compatible) {
			return true
		}
	}

	return false
}

// ExtractFullTextIndexCandidates 从表达式中提取全文索引候选
func (fts *FullTextIndexSupport) ExtractFullTextIndexCandidates(
	tableName string,
	expression string,
	columnTypes map[string]string,
) []*IndexCandidate {
	var candidates []*IndexCandidate

	// 检查是否为全文表达式
	if !fts.IsFullTextExpression(expression) {
		return candidates
	}

	// 提取 MATCH AGAINST 中的列
	cols := fts.extractColumnsFromMatchAgainst(expression)
	if len(cols) == 0 {
		// 尝试从 LIKE 表达式中提取列
		cols = fts.extractColumnsFromLike(expression)
	}

	// 为每个兼容的列创建索引候选
	for _, col := range cols {
		colType, exists := columnTypes[col]
		if !exists {
			continue
		}

		if fts.IsColumnTypeCompatible(colType) {
			candidate := &IndexCandidate{
				TableName: tableName,
				Columns:   []string{col},
				Priority:  4, // 全文查询通常优先级较高
				Source:    "FULLTEXT",
				Unique:    false,
				IndexType: IndexTypeFullText,
			}
			candidates = append(candidates, candidate)
		}
	}

	// 尝试创建复合全文索引（多个列）
	if len(cols) > 1 {
		allCompatible := true
		for _, col := range cols {
			colType, exists := columnTypes[col]
			if !exists || !fts.IsColumnTypeCompatible(colType) {
				allCompatible = false
				break
			}
		}

		if allCompatible {
			candidate := &IndexCandidate{
				TableName: tableName,
				Columns:   cols,
				Priority:  3, // 复合索引优先级稍低
				Source:    "FULLTEXT",
				Unique:    false,
				IndexType: IndexTypeFullText,
			}
			candidates = append(candidates, candidate)
		}
	}

	return candidates
}

// extractColumnsFromMatchAgainst 从 MATCH AGAINST 表达式中提取列
func (fts *FullTextIndexSupport) extractColumnsFromMatchAgainst(expr string) []string {
	pattern := `(?i)MATCH\s*\(([^)]+)\)`
	re := regexp.MustCompile(pattern)
	matches := re.FindStringSubmatch(expr)

	if len(matches) < 2 {
		return []string{}
	}

	// 解析列列表
	columnsStr := strings.TrimSpace(matches[1])
	columns := strings.Split(columnsStr, ",")

	var result []string
	for _, col := range columns {
		col = strings.TrimSpace(col)
		if col != "" {
			result = append(result, col)
		}
	}

	return result
}

// extractColumnsFromLike 从 LIKE 表达式中提取列
func (fts *FullTextIndexSupport) extractColumnsFromLike(expr string) []string {
	pattern := `(?i)(\w+)\s+LIKE\s+'`
	re := regexp.MustCompile(pattern)
	matches := re.FindStringSubmatch(expr)

	if len(matches) < 2 {
		return []string{}
	}

	return []string{strings.TrimSpace(matches[1])}
}

// EstimateFullTextIndexStats 估算全文索引统计信息
func (fts *FullTextIndexSupport) EstimateFullTextIndexStats(
	tableName string,
	columns []string,
	rowCount int64,
) *HypotheticalIndexStats {
	if len(columns) == 0 || rowCount == 0 {
		return nil
	}

	// 估算倒排索引大小
	// 假设：每行平均有 10 个不同的词，每个词平均 5 个字符
	// 倒排索引大小 = 行数 * 词数 * 词长 * 2 (包含指针和文档ID)
	avgWordsPerRow := 10
	avgWordLength := 5
	invertedIndexSize := int64(rowCount) * int64(avgWordsPerRow) * int64(avgWordLength) * 2

	// 估算不同的词汇量（NDV）
	// 假设每 100 行有 1 个新的唯一词
	ndv := rowCount / 100
	if ndv < 100 {
		ndv = 100
	}

	// 全文索引的选择性通常较高，假设为 0.01-0.05
	selectivity := 0.02

	// NULL 值比例
	nullFraction := 0.05

	// 相关性因子
	correlation := 0.1

	stats := &HypotheticalIndexStats{
		NDV:           ndv,
		Selectivity:   selectivity,
		EstimatedSize: invertedIndexSize,
		NullFraction:  nullFraction,
		Correlation:   correlation,
	}

	return stats
}

// CalculateFullTextSearchBenefit 计算全文索引的搜索收益
func (fts *FullTextIndexSupport) CalculateFullTextSearchBenefit(
	rowCount int64,
	searchTermLength int,
	exactMatch bool,
) float64 {
	// 基础收益：行数越多，收益越大
	baseBenefit := float64(rowCount) / 10000.0

	// 搜索词长度影响：词越长，选择性越高，收益越大
	lengthFactor := float64(searchTermLength) / 10.0
	if lengthFactor > 1.0 {
		lengthFactor = 1.0
	}

	// 精确匹配收益更高
	matchTypeFactor := 1.0
	if exactMatch {
		matchTypeFactor = 1.5
	}

	// 计算总收益，不封顶以保持差异
	totalBenefit := baseBenefit * lengthFactor * matchTypeFactor

	return totalBenefit
}

// GetFullTextIndexDDL 生成创建全文索引的 DDL
func (fts *FullTextIndexSupport) GetFullTextIndexDDL(
	tableName string,
	columns []string,
	indexName string,
) string {
	if indexName == "" {
		// 生成默认索引名
		indexName = fmt.Sprintf("ft_%s_%s", tableName, strings.Join(columns, "_"))
	}

	columnsStr := strings.Join(columns, ", ")
	ddl := fmt.Sprintf("CREATE FULLTEXT INDEX %s ON %s(%s)",
		indexName, tableName, columnsStr)

	return ddl
}

// OptimizeFullTextQuery 优化全文查询建议
func (fts *FullTextIndexSupport) OptimizeFullTextQuery(
	query string,
	tables map[string]bool,
) []string {
	var suggestions []string

	// 检查是否有全文搜索但没有全文索引
	if fts.IsFullTextExpression(query) {
		suggestions = append(suggestions,
			"Consider adding FULLTEXT indexes for columns used in MATCH AGAINST queries",
		)
	}

	// 检查是否使用了带前导通配符的 LIKE
	if fts.isLikeWithWildcard(query) {
		suggestions = append(suggestions,
			"Leading wildcard in LIKE prevents regular index usage, consider FULLTEXT index",
		)
	}

	return suggestions
}

// IsStopWord 检查是否为停用词
func (fts *FullTextIndexSupport) IsStopWord(word string) bool {
	lowerWord := strings.ToLower(word)
	for _, stopWord := range fts.StopWords {
		if lowerWord == strings.ToLower(stopWord) {
			return true
		}
	}
	return false
}

// TokenizeText 文本分词
func (fts *FullTextIndexSupport) TokenizeText(text string) []string {
	// 简单分词：按空格和标点符号分割
	re := regexp.MustCompile(`[^\w']+`)
	tokens := re.Split(text, -1)

	var result []string
	for _, token := range tokens {
		token = strings.TrimSpace(token)
		// 过滤停用词和过短的词
		if token != "" && !fts.IsStopWord(token) && len(token) >= fts.MinWordLength {
			result = append(result, strings.ToLower(token))
		}
	}

	return result
}
