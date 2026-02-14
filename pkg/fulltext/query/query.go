package query

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/kasuganosora/sqlexec/pkg/fulltext/bm25"
	"github.com/kasuganosora/sqlexec/pkg/fulltext/index"
	"github.com/kasuganosora/sqlexec/pkg/utils"
)

// SearchResult 搜索结果（本地定义避免导入循环）
type SearchResult struct {
	DocID int64
	Score float64
	Doc   *index.Document
}

// Query 查询接口
type Query interface {
	Execute(idx *index.InvertedIndex) []SearchResult
	SetBoost(boost float64)
	GetBoost() float64
}

// BaseQuery 基础查询
type BaseQuery struct {
	boost float64
}

// SetBoost 设置权重
func (q *BaseQuery) SetBoost(boost float64) {
	q.boost = boost
}

// GetBoost 获取权重
func (q *BaseQuery) GetBoost() float64 {
	return q.boost
}

// TermQuery 词项查询
type TermQuery struct {
	BaseQuery
	Field string
	Term  string
}

// NewTermQuery 创建词项查询
func NewTermQuery(field, term string) *TermQuery {
	return &TermQuery{
		Field: field,
		Term:  term,
		BaseQuery: BaseQuery{
			boost: 1.0,
		},
	}
}

// Execute 执行查询
func (q *TermQuery) Execute(idx *index.InvertedIndex) []SearchResult {
	// 创建查询向量
	termID := hashString(q.Term)
	queryVector := bm25.NewSparseVector()
	queryVector.Set(termID, 1.0*q.boost)
	
	idxResults := idx.Search(queryVector)
	
	// 转换为本地类型并应用权重
	results := make([]SearchResult, len(idxResults))
	for i, r := range idxResults {
		results[i] = SearchResult{
			DocID: r.DocID,
			Score: r.Score * q.boost,
			Doc:   r.Doc,
		}
	}
	
	return results
}

// PhraseQuery 短语查询
type PhraseQuery struct {
	BaseQuery
	Field   string
	Phrases []string
	Slop    int
}

// NewPhraseQuery 创建短语查询
func NewPhraseQuery(field string, phrases []string, slop int) *PhraseQuery {
	return &PhraseQuery{
		Field:   field,
		Phrases: phrases,
		Slop:    slop,
		BaseQuery: BaseQuery{
			boost: 1.0,
		},
	}
}

// Execute 执行查询
func (q *PhraseQuery) Execute(idx *index.InvertedIndex) []SearchResult {
	// 将短语转换为tokens
	tokens := make([]index.AnalyzerToken, len(q.Phrases))
	for i, phrase := range q.Phrases {
		tokens[i] = index.AnalyzerToken{
			Text:     phrase,
			Position: i,
		}
	}
	
	idxResults := idx.SearchPhrase(tokens, q.Slop)
	
	// 转换为本地类型并应用权重
	results := make([]SearchResult, len(idxResults))
	for i, r := range idxResults {
		results[i] = SearchResult{
			DocID: r.DocID,
			Score: r.Score * q.boost,
			Doc:   r.Doc,
		}
	}
	
	return results
}

// BooleanQuery 布尔查询
type BooleanQuery struct {
	BaseQuery
	Must           []Query
	Should         []Query
	MustNot        []Query
	MinShouldMatch int
}

// NewBooleanQuery 创建布尔查询
func NewBooleanQuery() *BooleanQuery {
	return &BooleanQuery{
		Must:           make([]Query, 0),
		Should:         make([]Query, 0),
		MustNot:        make([]Query, 0),
		MinShouldMatch: 1,
		BaseQuery: BaseQuery{
			boost: 1.0,
		},
	}
}

// AddMust 添加必须匹配的条件
func (q *BooleanQuery) AddMust(query Query) {
	q.Must = append(q.Must, query)
}

// AddShould 添加应该匹配的条件
func (q *BooleanQuery) AddShould(query Query) {
	q.Should = append(q.Should, query)
}

// AddMustNot 添加必须不匹配的条件
func (q *BooleanQuery) AddMustNot(query Query) {
	q.MustNot = append(q.MustNot, query)
}

// Execute 执行查询
func (q *BooleanQuery) Execute(idx *index.InvertedIndex) []SearchResult {
	// 收集所有结果
	docScores := make(map[int64]*booleanDoc)
	
	// 处理Must查询（AND）
	for _, query := range q.Must {
		results := query.Execute(idx)
		for _, result := range results {
			if doc, exists := docScores[result.DocID]; exists {
				doc.mustCount++
				doc.score += result.Score
			} else {
				docScores[result.DocID] = &booleanDoc{
					docID:     result.DocID,
					mustCount: 1,
					score:     result.Score,
				}
			}
		}
	}
	
	// 处理Should查询（OR）
	for _, query := range q.Should {
		results := query.Execute(idx)
		for _, result := range results {
			if doc, exists := docScores[result.DocID]; exists {
				doc.shouldCount++
				doc.score += result.Score
			} else {
				docScores[result.DocID] = &booleanDoc{
					docID:       result.DocID,
					shouldCount: 1,
					score:       result.Score,
				}
			}
		}
	}
	
	// 处理MustNot查询（NOT）
	mustNotDocIDs := make(map[int64]bool)
	for _, query := range q.MustNot {
		results := query.Execute(idx)
		for _, result := range results {
			mustNotDocIDs[result.DocID] = true
		}
	}
	
	// 过滤和收集结果
	var results []SearchResult
	for docID, doc := range docScores {
		// 排除MustNot的文档
		if mustNotDocIDs[docID] {
			continue
		}
		
		// 检查Must条件是否全部满足
		if len(q.Must) > 0 && doc.mustCount < len(q.Must) {
			continue
		}
		
		// 检查Should条件是否满足最小要求
		if len(q.Should) > 0 && doc.shouldCount < q.MinShouldMatch {
			continue
		}
		
		results = append(results, SearchResult{
			DocID: docID,
			Score: doc.score * q.boost,
			Doc:   idx.GetDocument(docID),
		})
	}
	
	return results
}

type booleanDoc struct {
	docID       int64
	mustCount   int
	shouldCount int
	score       float64
}

// MatchAllQuery 匹配所有文档
type MatchAllQuery struct {
	BaseQuery
}

// NewMatchAllQuery 创建MatchAll查询
func NewMatchAllQuery() *MatchAllQuery {
	return &MatchAllQuery{
		BaseQuery: BaseQuery{
			boost: 1.0,
		},
	}
}

// Execute 执行查询
func (q *MatchAllQuery) Execute(idx *index.InvertedIndex) []SearchResult {
	// 返回所有文档
	stats := idx.GetStats()
	results := make([]SearchResult, 0, stats.TotalDocs)
	
	// 这里简化实现，实际需要遍历所有文档
	return results
}

// MatchNoneQuery 不匹配任何文档
type MatchNoneQuery struct {
	BaseQuery
}

// NewMatchNoneQuery 创建MatchNone查询
func NewMatchNoneQuery() *MatchNoneQuery {
	return &MatchNoneQuery{
		BaseQuery: BaseQuery{
			boost: 1.0,
		},
	}
}

// Execute 执行查询
func (q *MatchNoneQuery) Execute(idx *index.InvertedIndex) []SearchResult {
	return nil
}

// RangeQuery 范围查询
type RangeQuery struct {
	BaseQuery
	Field      string
	Min        interface{}
	Max        interface{}
	IncludeMin bool
	IncludeMax bool
}

// NewRangeQuery 创建范围查询
func NewRangeQuery(field string, min, max interface{}, includeMin, includeMax bool) *RangeQuery {
	return &RangeQuery{
		Field:      field,
		Min:        min,
		Max:        max,
		IncludeMin: includeMin,
		IncludeMax: includeMax,
		BaseQuery: BaseQuery{
			boost: 1.0,
		},
	}
}

// Execute 执行查询
func (q *RangeQuery) Execute(idx *index.InvertedIndex) []SearchResult {
	var results []SearchResult
	
	// 遍历所有文档，检查字段值是否在范围内
	allDocIDs := idx.GetAllDocIDs()
	
	for _, docID := range allDocIDs {
		doc := idx.GetDocument(docID)
		if doc == nil {
			continue
		}

		// 获取字段值
		fieldValue, exists := doc.Fields[q.Field]
		if !exists {
			continue
		}

		// 检查值是否在范围内
		if q.isInRange(fieldValue) {
			// 计算分数
			score := 1.0 * q.boost
			
			results = append(results, SearchResult{
				DocID: docID,
				Score: score,
				Doc:   doc,
			})
		}
	}

	return results
}

// isInRange 检查值是否在范围内
func (q *RangeQuery) isInRange(value interface{}) bool {
	switch v := value.(type) {
	case int:
		return q.compareInt(int64(v))
	case int64:
		return q.compareInt(v)
	case float64:
		return q.compareFloat(v)
	case float32:
		return q.compareFloat(float64(v))
	case string:
		return q.compareString(v)
	default:
		// 尝试转换为字符串比较
		str := toString(value)
		return q.compareString(str)
	}
}

// compareInt 比较整数
func (q *RangeQuery) compareInt(value int64) bool {
	min, minOk := convertToInt64(q.Min)
	max, maxOk := convertToInt64(q.Max)

	// 检查最小值
	if minOk {
		if q.IncludeMin {
			if value < min {
				return false
			}
		} else {
			if value <= min {
				return false
			}
		}
	}

	// 检查最大值
	if maxOk {
		if q.IncludeMax {
			if value > max {
				return false
			}
		} else {
			if value >= max {
				return false
			}
		}
	}

	return true
}

// compareFloat 比较浮点数
func (q *RangeQuery) compareFloat(value float64) bool {
	min, minOk := convertToFloat64(q.Min)
	max, maxOk := convertToFloat64(q.Max)

	// 检查最小值
	if minOk {
		if q.IncludeMin {
			if value < min {
				return false
			}
		} else {
			if value <= min {
				return false
			}
		}
	}

	// 检查最大值
	if maxOk {
		if q.IncludeMax {
			if value > max {
				return false
			}
		} else {
			if value >= max {
				return false
			}
		}
	}

	return true
}

// compareString 比较字符串（字典序）
func (q *RangeQuery) compareString(value string) bool {
	min := toString(q.Min)
	max := toString(q.Max)

	// 检查最小值
	if min != "" {
		if q.IncludeMin {
			if value < min {
				return false
			}
		} else {
			if value <= min {
				return false
			}
		}
	}

	// 检查最大值
	if max != "" {
		if q.IncludeMax {
			if value > max {
				return false
			}
		} else {
			if value >= max {
				return false
			}
		}
	}

	return true
}

// convertToInt64 转换为 int64
func convertToInt64(v interface{}) (int64, bool) {
	switch val := v.(type) {
	case int:
		return int64(val), true
	case int64:
		return val, true
	case float64:
		return int64(val), true
	default:
		return 0, false
	}
}

// convertToFloat64 转换为 float64
func convertToFloat64(v interface{}) (float64, bool) {
	switch val := v.(type) {
	case float64:
		return val, true
	case float32:
		return float64(val), true
	case int:
		return float64(val), true
	case int64:
		return float64(val), true
	default:
		return 0, false
	}
}

// toString 转换为字符串
func toString(v interface{}) string {
	if v == nil {
		return ""
	}
	switch val := v.(type) {
	case string:
		return val
	case int:
		return strconv.Itoa(val)
	case int64:
		return strconv.FormatInt(val, 10)
	case float64:
		return strconv.FormatFloat(val, 'f', -1, 64)
	default:
		return fmt.Sprintf("%v", v)
	}
}

// FuzzyQuery 模糊查询
type FuzzyQuery struct {
	BaseQuery
	Field    string
	Term     string
	Distance int
}

// NewFuzzyQuery 创建模糊查询
func NewFuzzyQuery(field, term string, distance int) *FuzzyQuery {
	return &FuzzyQuery{
		Field:    field,
		Term:     term,
		Distance: distance,
		BaseQuery: BaseQuery{
			boost: 1.0,
		},
	}
}

// Execute 执行查询
func (q *FuzzyQuery) Execute(idx *index.InvertedIndex) []SearchResult {
	if q.Distance <= 0 {
		q.Distance = 2 // 默认编辑距离
	}
	
	// 直接遍历所有文档，找到匹配的文档
	var results []SearchResult
	allDocIDs := idx.GetAllDocIDs()
	
	for _, docID := range allDocIDs {
		doc := idx.GetDocument(docID)
		if doc == nil {
			continue
		}

		// 获取字段值
		fieldValue, exists := doc.Fields[q.Field]
		if !exists {
			continue
		}

		// 提取词并检查是否有匹配的词
		terms := simpleTokenize(fieldValue)
		maxSimilarity := 0.0
		
		for _, term := range terms {
			// 计算编辑距离
			distance := levenshteinDistance(q.Term, term)
			
			if distance <= q.Distance {
				// 计算相似度分数
				maxLen := utils.MaxInt(len(q.Term), len(term))
				if maxLen > 0 {
					similarity := 1.0 - float64(distance)/float64(maxLen)
					if similarity > maxSimilarity {
						maxSimilarity = similarity
					}
				}
			}
		}
		
		// 如果有匹配的词，添加结果
		if maxSimilarity > 0 {
			results = append(results, SearchResult{
				DocID: docID,
				Score: maxSimilarity * q.boost,
				Doc:   doc,
			})
		}
	}

	return results
}

// simpleTokenize 简单分词（用于从字段值中提取词）
func simpleTokenize(value interface{}) []string {
	var text string
	switch v := value.(type) {
	case string:
		text = v
	default:
		text = toString(value)
	}

	// 简单的空格分词
	var terms []string
	var current strings.Builder
	
	for _, r := range text {
		if r == ' ' || r == '\t' || r == '\n' || r == '\r' {
			if current.Len() > 0 {
				terms = append(terms, current.String())
				current.Reset()
			}
		} else {
			current.WriteRune(r)
		}
	}
	
	if current.Len() > 0 {
		terms = append(terms, current.String())
	}
	
	return terms
}

// levenshteinDistance 计算 Levenshtein 编辑距离
func levenshteinDistance(s1, s2 string) int {
	// 处理特殊情况
	if s1 == s2 {
		return 0
	}
	if len(s1) == 0 {
		return len(s2)
	}
	if len(s2) == 0 {
		return len(s1)
	}

	// 创建距离矩阵（优化空间使用一维数组）
	lenS1 := len(s1) + 1
	lenS2 := len(s2) + 1
	
	// 如果字符串很长，限制计算范围
	if lenS1 > 100 || lenS2 > 100 {
		// 对于长字符串，只计算前100个字符的距离
		if len(s1) > 100 {
			s1 = s1[:100]
		}
		if len(s2) > 100 {
			s2 = s2[:100]
		}
		lenS1 = len(s1) + 1
		lenS2 = len(s2) + 1
	}
	
	dist := make([]int, lenS1*lenS2)
	
	// 初始化第一行和第一列
	for i := 0; i < lenS1; i++ {
		dist[i] = i
	}
	for j := 0; j < lenS2; j++ {
		dist[j*lenS1] = j
	}

	// 计算编辑距离
	for j := 1; j < lenS2; j++ {
		for i := 1; i < lenS1; i++ {
			cost := 0
			if s1[i-1] != s2[j-1] {
				cost = 1
			}
			
			deletion := dist[(j-1)*lenS1+i] + 1
			insertion := dist[j*lenS1+i-1] + 1
			substitution := dist[(j-1)*lenS1+i-1] + cost
			
			dist[j*lenS1+i] = utils.MinInt(deletion, utils.MinInt(insertion, substitution))
		}
	}

	return dist[(lenS2-1)*lenS1+(lenS1-1)]
}

// 辅助函数
func hashString(s string) int64 {
	h := int64(0)
	for _, c := range s {
		h = h*31 + int64(c)
	}
	return h
}
