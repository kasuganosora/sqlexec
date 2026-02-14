package query

import (
	"regexp"
	"strings"

	"github.com/kasuganosora/sqlexec/pkg/fulltext/index"
)

// DisjunctionMaxQuery 析取最大查询（取多个查询中的最高分）
type DisjunctionMaxQuery struct {
	BaseQuery
	Queries   []Query
	TieBreaker float64 // 次高分的权重系数 (0-1)
}

// NewDisjunctionMaxQuery 创建析取最大查询
func NewDisjunctionMaxQuery(queries []Query, tieBreaker float64) *DisjunctionMaxQuery {
	if tieBreaker < 0 || tieBreaker > 1 {
		tieBreaker = 0.0
	}
	return &DisjunctionMaxQuery{
		Queries:    queries,
		TieBreaker: tieBreaker,
		BaseQuery: BaseQuery{
			boost: 1.0,
		},
	}
}

// Execute 执行查询
func (q *DisjunctionMaxQuery) Execute(idx *index.InvertedIndex) []SearchResult {
	// 收集所有子查询的结果
	docScores := make(map[int64]*disjunctionDoc)
	
	for _, query := range q.Queries {
		results := query.Execute(idx)
		for _, result := range results {
			if doc, exists := docScores[result.DocID]; exists {
				// 更新最高分和次高分
				if result.Score > doc.MaxScore {
					doc.SecondScore = doc.MaxScore
					doc.MaxScore = result.Score
				} else if result.Score > doc.SecondScore {
					doc.SecondScore = result.Score
				}
			} else {
				docScores[result.DocID] = &disjunctionDoc{
					docID:       result.DocID,
					MaxScore:    result.Score,
					SecondScore: 0,
				}
			}
		}
	}
	
	// 计算最终分数（最高分 + TieBreaker * 次高分）
	results := make([]SearchResult, 0, len(docScores))
	for docID, doc := range docScores {
		score := doc.MaxScore + q.TieBreaker*doc.SecondScore
		results = append(results, SearchResult{
			DocID: docID,
			Score: score * q.boost,
			Doc:   idx.GetDocument(docID),
		})
	}
	
	return results
}

type disjunctionDoc struct {
	docID       int64
	MaxScore    float64
	SecondScore float64
}

// ConstScoreQuery 常数分数查询（包装查询并赋予固定分数）
type ConstScoreQuery struct {
	BaseQuery
	Query Query
	Score float64
}

// NewConstScoreQuery 创建常数分数查询
func NewConstScoreQuery(query Query, score float64) *ConstScoreQuery {
	return &ConstScoreQuery{
		Query: query,
		Score: score,
		BaseQuery: BaseQuery{
			boost: 1.0,
		},
	}
}

// Execute 执行查询
func (q *ConstScoreQuery) Execute(idx *index.InvertedIndex) []SearchResult {
	results := q.Query.Execute(idx)
	
	// 替换分数为常数
	for i := range results {
		results[i].Score = q.Score * q.boost
	}
	
	return results
}

// TermSetQuery 词集查询（匹配任意一个词）
type TermSetQuery struct {
	BaseQuery
	Field string
	Terms []string
}

// NewTermSetQuery 创建词集查询
func NewTermSetQuery(field string, terms []string) *TermSetQuery {
	return &TermSetQuery{
		Field: field,
		Terms: terms,
		BaseQuery: BaseQuery{
			boost: 1.0,
		},
	}
}

// Execute 执行查询
func (q *TermSetQuery) Execute(idx *index.InvertedIndex) []SearchResult {
	// 使用 OR 逻辑合并多个词项查询
	boolQuery := NewBooleanQuery()
	
	for _, term := range q.Terms {
		boolQuery.AddShould(NewTermQuery(q.Field, term))
	}
	
	boolQuery.MinShouldMatch = 1
	
	return boolQuery.Execute(idx)
}

// PhrasePrefixQuery 短语前缀查询（自动补全）
type PhrasePrefixQuery struct {
	BaseQuery
	Field       string
	PrefixTerms []string
}

// NewPhrasePrefixQuery 创建短语前缀查询
func NewPhrasePrefixQuery(field string, prefixTerms []string) *PhrasePrefixQuery {
	return &PhrasePrefixQuery{
		Field:       field,
		PrefixTerms: prefixTerms,
		BaseQuery: BaseQuery{
			boost: 1.0,
		},
	}
}

// Execute 执行查询
func (q *PhrasePrefixQuery) Execute(idx *index.InvertedIndex) []SearchResult {
	// 简化实现：将前缀词作为短语处理
	// 实际实现需要前缀匹配逻辑
	return NewPhraseQuery(q.Field, q.PrefixTerms, 0).Execute(idx)
}

// RegexQuery 正则表达式查询
type RegexQuery struct {
	BaseQuery
	Field   string
	Pattern string
}

// NewRegexQuery 创建正则查询
func NewRegexQuery(field, pattern string) *RegexQuery {
	return &RegexQuery{
		Field:   field,
		Pattern: pattern,
		BaseQuery: BaseQuery{
			boost: 1.0,
		},
	}
}

// Execute 执行查询
func (q *RegexQuery) Execute(idx *index.InvertedIndex) []SearchResult {
	// 编译正则表达式
	re, err := regexp.Compile(q.Pattern)
	if err != nil {
		return nil
	}
	
	var results []SearchResult
	
	// 遍历所有文档，检查字段值是否匹配正则
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
		
		// 转换为字符串
		strValue := toStringRegex(fieldValue)
		
		// 检查是否匹配正则
		if re.MatchString(strValue) {
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

// toStringRegex 转换为字符串
func toStringRegex(value interface{}) string {
	switch v := value.(type) {
	case string:
		return v
	default:
		return toString(v)
	}
}

// EmptyQuery 空查询（不返回任何结果）
type EmptyQuery struct {
	BaseQuery
}

// NewEmptyQuery 创建空查询
func NewEmptyQuery() *EmptyQuery {
	return &EmptyQuery{
		BaseQuery: BaseQuery{
			boost: 1.0,
		},
	}
}

// Execute 执行查询
func (q *EmptyQuery) Execute(idx *index.InvertedIndex) []SearchResult {
	return nil
}

// AllDocsQuery 匹配所有文档
type AllDocsQuery struct {
	BaseQuery
}

// NewAllDocsQuery 创建匹配所有文档的查询
func NewAllDocsQuery() *AllDocsQuery {
	return &AllDocsQuery{
		BaseQuery: BaseQuery{
			boost: 1.0,
		},
	}
}

// Execute 执行查询
func (q *AllDocsQuery) Execute(idx *index.InvertedIndex) []SearchResult {
	allDocIDs := idx.GetAllDocIDs()
	results := make([]SearchResult, 0, len(allDocIDs))

	for _, docID := range allDocIDs {
		results = append(results, SearchResult{
			DocID: docID,
			Score: 1.0 * q.boost,
			Doc:   idx.GetDocument(docID),
		})
	}
	return results
}

// NestedQuery 嵌套字段查询（用于JSON字段）
type NestedQuery struct {
	BaseQuery
	Path  string // 嵌套路径，如 "metadata.color"
	Query Query
}

// NewNestedQuery 创建嵌套查询
func NewNestedQuery(path string, query Query) *NestedQuery {
	return &NestedQuery{
		Path:  path,
		Query: query,
		BaseQuery: BaseQuery{
			boost: 1.0,
		},
	}
}

// Execute 执行查询
func (q *NestedQuery) Execute(idx *index.InvertedIndex) []SearchResult {
	// 简化实现：展开嵌套路径并执行子查询
	// 实际实现需要处理JSON字段的嵌套结构
	return q.Query.Execute(idx)
}

// ExistsQuery 字段存在查询
type ExistsQuery struct {
	BaseQuery
	Field string
}

// NewExistsQuery 创建字段存在查询
func NewExistsQuery(field string) *ExistsQuery {
	return &ExistsQuery{
		Field: field,
		BaseQuery: BaseQuery{
			boost: 1.0,
		},
	}
}

// Execute 执行查询
func (q *ExistsQuery) Execute(idx *index.InvertedIndex) []SearchResult {
	var results []SearchResult

	allDocIDs := idx.GetAllDocIDs()
	for _, docID := range allDocIDs {
		doc := idx.GetDocument(docID)
		if doc == nil {
			continue
		}
		if _, exists := doc.Fields[q.Field]; exists {
			results = append(results, SearchResult{
				DocID: docID,
				Score: 1.0 * q.boost,
				Doc:   doc,
			})
		}
	}
	return results
}

// PrefixQuery 前缀查询（用于自动补全）
type PrefixQuery struct {
	BaseQuery
	Field  string
	Prefix string
}

// NewPrefixQuery 创建前缀查询
func NewPrefixQuery(field, prefix string) *PrefixQuery {
	return &PrefixQuery{
		Field:  field,
		Prefix: prefix,
		BaseQuery: BaseQuery{
			boost: 1.0,
		},
	}
}

// Execute 执行查询
func (q *PrefixQuery) Execute(idx *index.InvertedIndex) []SearchResult {
	var results []SearchResult

	allDocIDs := idx.GetAllDocIDs()
	for _, docID := range allDocIDs {
		doc := idx.GetDocument(docID)
		if doc == nil {
			continue
		}
		fieldValue, exists := doc.Fields[q.Field]
		if !exists {
			continue
		}
		strValue := toStringRegex(fieldValue)
		if strings.HasPrefix(strValue, q.Prefix) {
			results = append(results, SearchResult{
				DocID: docID,
				Score: 1.0 * q.boost,
				Doc:   doc,
			})
		}
	}
	return results
}

// WildcardQuery 通配符查询（* 和 ?）
type WildcardQuery struct {
	BaseQuery
	Field    string
	Wildcard string
}

// NewWildcardQuery 创建通配符查询
func NewWildcardQuery(field, wildcard string) *WildcardQuery {
	return &WildcardQuery{
		Field:    field,
		Wildcard: wildcard,
		BaseQuery: BaseQuery{
			boost: 1.0,
		},
	}
}

// Execute 执行查询
func (q *WildcardQuery) Execute(idx *index.InvertedIndex) []SearchResult {
	// 转换通配符为正则表达式
	pattern := q.Wildcard
	pattern = strings.ReplaceAll(pattern, "*", ".*")
	pattern = strings.ReplaceAll(pattern, "?", ".")
	pattern = "^" + pattern + "$"
	
	return NewRegexQuery(q.Field, pattern).Execute(idx)
}

// FunctionScoreQuery 函数评分查询（根据函数动态计算分数）
type FunctionScoreQuery struct {
	BaseQuery
	Query    Query
	ScoreFunc func(docID int64, baseScore float64) float64
}

// NewFunctionScoreQuery 创建函数评分查询
func NewFunctionScoreQuery(query Query, scoreFunc func(docID int64, baseScore float64) float64) *FunctionScoreQuery {
	return &FunctionScoreQuery{
		Query:     query,
		ScoreFunc: scoreFunc,
		BaseQuery: BaseQuery{
			boost: 1.0,
		},
	}
}

// Execute 执行查询
func (q *FunctionScoreQuery) Execute(idx *index.InvertedIndex) []SearchResult {
	results := q.Query.Execute(idx)
	
	// 应用评分函数
	for i := range results {
		results[i].Score = q.ScoreFunc(results[i].DocID, results[i].Score)
	}
	
	return results
}

// QueryBuilder 查询构建器（流式API）
type QueryBuilder struct {
	query Query
}

// NewQueryBuilder 创建查询构建器
func NewQueryBuilder() *QueryBuilder {
	return &QueryBuilder{}
}

// Term 添加词项查询
func (b *QueryBuilder) Term(field, value string, boost float64) *QueryBuilder {
	q := NewTermQuery(field, value)
	q.SetBoost(boost)
	b.query = q
	return b
}

// Phrase 添加短语查询
func (b *QueryBuilder) Phrase(field string, phrases []string, slop int, boost float64) *QueryBuilder {
	q := NewPhraseQuery(field, phrases, slop)
	q.SetBoost(boost)
	b.query = q
	return b
}

// Bool 添加布尔查询
func (b *QueryBuilder) Bool(fn func(*BooleanQuery)) *QueryBuilder {
	q := NewBooleanQuery()
	fn(q)
	b.query = q
	return b
}

// Range 添加范围查询
func (b *QueryBuilder) Range(field string, min, max interface{}, includeMin, includeMax bool) *QueryBuilder {
	q := NewRangeQuery(field, min, max, includeMin, includeMax)
	b.query = q
	return b
}

// Fuzzy 添加模糊查询
func (b *QueryBuilder) Fuzzy(field, term string, distance int, boost float64) *QueryBuilder {
	q := NewFuzzyQuery(field, term, distance)
	q.SetBoost(boost)
	b.query = q
	return b
}

// Build 构建查询
func (b *QueryBuilder) Build() Query {
	return b.query
}
