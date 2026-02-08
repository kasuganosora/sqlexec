package sql

import (
	"fmt"
	"strings"
)

// FulltextField 字段配置函数（SQL函数式配置）
// 示例: fulltext_field('content', fast=>FALSE, tokenizer=>'jieba')
type FulltextField struct {
	Name       string
	Fast       bool
	Tokenizer  string
	Record     string
	FieldNorms bool
	Boost      float64
}

// NewFulltextField 创建字段配置
func NewFulltextField(name string) *FulltextField {
	return &FulltextField{
		Name:       name,
		Fast:       false,
		Tokenizer:  "standard",
		Record:     "position",
		FieldNorms: true,
		Boost:      1.0,
	}
}

// WithFast 设置Fast选项
func (f *FulltextField) WithFast(fast bool) *FulltextField {
	f.Fast = fast
	return f
}

// WithTokenizer 设置分词器
func (f *FulltextField) WithTokenizer(tokenizer string) *FulltextField {
	f.Tokenizer = tokenizer
	return f
}

// WithRecord 设置记录类型
func (f *FulltextField) WithRecord(record string) *FulltextField {
	f.Record = record
	return f
}

// WithBoost 设置权重
func (f *FulltextField) WithBoost(boost float64) *FulltextField {
	f.Boost = boost
	return f
}

// ToSQL 生成SQL片段
func (f *FulltextField) ToSQL() string {
	return fmt.Sprintf("fulltext_field('%s', fast=>%v, tokenizer=>'%s', record=>'%s', boost=>%.2f)",
		f.Name, f.Fast, f.Tokenizer, f.Record, f.Boost)
}

// FulltextTokenizer 分词器配置函数
// 示例: fulltext_tokenizer('jieba', hmm=>TRUE, search=>TRUE)
type FulltextTokenizer struct {
	Type          string
	HMM           bool
	SearchMode    bool
	DictPath      string
	StopWordsPath string
	MinGram       int
	MaxGram       int
	PrefixOnly    bool
}

// NewFulltextTokenizer 创建分词器配置
func NewFulltextTokenizer(tokenizerType string) *FulltextTokenizer {
	return &FulltextTokenizer{
		Type:       tokenizerType,
		HMM:        true,
		SearchMode: false,
		MinGram:    2,
		MaxGram:    3,
		PrefixOnly: false,
	}
}

// WithHMM 设置HMM
func (t *FulltextTokenizer) WithHMM(hmm bool) *FulltextTokenizer {
	t.HMM = hmm
	return t
}

// WithSearchMode 设置搜索模式
func (t *FulltextTokenizer) WithSearchMode(search bool) *FulltextTokenizer {
	t.SearchMode = search
	return t
}

// WithDict 设置词典路径
func (t *FulltextTokenizer) WithDict(path string) *FulltextTokenizer {
	t.DictPath = path
	return t
}

// WithStopWords 设置停用词路径
func (t *FulltextTokenizer) WithStopWords(path string) *FulltextTokenizer {
	t.StopWordsPath = path
	return t
}

// WithNgram 设置N-gram参数
func (t *FulltextTokenizer) WithNgram(minGram, maxGram int, prefixOnly bool) *FulltextTokenizer {
	t.MinGram = minGram
	t.MaxGram = maxGram
	t.PrefixOnly = prefixOnly
	return t
}

// ToSQL 生成SQL片段
func (t *FulltextTokenizer) ToSQL() string {
	switch t.Type {
	case "jieba":
		return fmt.Sprintf("fulltext_tokenizer('jieba', hmm=>%v, search=>%v)",
			t.HMM, t.SearchMode)
	case "ngram":
		return fmt.Sprintf("fulltext_tokenizer('ngram', min_gram=>%d, max_gram=>%d, prefix_only=>%v)",
			t.MinGram, t.MaxGram, t.PrefixOnly)
	default:
		return fmt.Sprintf("fulltext_tokenizer('%s')", t.Type)
	}
}

// FulltextConfig 查询配置函数
// 示例: fulltext_config('field:term^2 AND field2:term3')
type FulltextConfig struct {
	Query          string
	EnableHybrid   bool
	FusionStrategy string
	FTWeight       float64
	VecWeight      float64
}

// NewFulltextConfig 创建查询配置
func NewFulltextConfig(query string) *FulltextConfig {
	return &FulltextConfig{
		Query:          query,
		EnableHybrid:   false,
		FusionStrategy: "rrf",
		FTWeight:       0.7,
		VecWeight:      0.3,
	}
}

// WithHybrid 启用混合搜索
func (c *FulltextConfig) WithHybrid(strategy string, ftWeight, vecWeight float64) *FulltextConfig {
	c.EnableHybrid = true
	c.FusionStrategy = strategy
	c.FTWeight = ftWeight
	c.VecWeight = vecWeight
	return c
}

// ToSQL 生成SQL片段
func (c *FulltextConfig) ToSQL() string {
	if c.EnableHybrid {
		return fmt.Sprintf("fulltext_config('%s', enable_hybrid=>TRUE, fusion_strategy=>'%s', ft_weight=>%.2f, vec_weight=>%.2f)",
			escapeSQLString(c.Query), c.FusionStrategy, c.FTWeight, c.VecWeight)
	}
	return fmt.Sprintf("fulltext_config('%s')", escapeSQLString(c.Query))
}

// FulltextTerm 词项查询函数
// 示例: fulltext_term('field', 'value', boost=>2.0)
func FulltextTerm(field, value string, boost float64) string {
	return fmt.Sprintf("fulltext_term('%s', '%s', boost=>%.2f)",
		field, escapeSQLString(value), boost)
}

// FulltextPhrase 短语查询函数
// 示例: fulltext_phrase('field', ARRAY['word1', 'word2'], slop=>0)
func FulltextPhrase(field string, terms []string, slop int, boost float64) string {
	termsStr := "ARRAY['" + strings.Join(escapeSQLStrings(terms), "', '") + "']"
	return fmt.Sprintf("fulltext_phrase('%s', %s, slop=>%d, boost=>%.2f)",
		field, termsStr, slop, boost)
}

// FulltextPhrasePrefix 短语前缀查询函数
// 示例: fulltext_phrase_prefix('field', ARRAY['word1', 'word2'])
func FulltextPhrasePrefix(field string, terms []string, boost float64) string {
	termsStr := "ARRAY['" + strings.Join(escapeSQLStrings(terms), "', '") + "']"
	return fmt.Sprintf("fulltext_phrase_prefix('%s', %s, boost=>%.2f)",
		field, termsStr, boost)
}

// FulltextFuzzy 模糊查询函数
// 示例: fulltext_fuzzy('field', 'term', distance=>2)
func FulltextFuzzy(field, term string, distance int, boost float64) string {
	return fmt.Sprintf("fulltext_fuzzy('%s', '%s', distance=>%d, boost=>%.2f)",
		field, escapeSQLString(term), distance, boost)
}

// FulltextRegex 正则查询函数
// 示例: fulltext_regex('field', 'pattern')
func FulltextRegex(field, pattern string, boost float64) string {
	return fmt.Sprintf("fulltext_regex('%s', '%s', boost=>%.2f)",
		field, escapeSQLString(pattern), boost)
}

// FulltextRange 范围查询函数
// 示例: fulltext_range('field', '2023-01-01', '2024-12-31')
func FulltextRange(field, min, max string, includeMin, includeMax bool, boost float64) string {
	return fmt.Sprintf("fulltext_range('%s', '%s', '%s', include_min=>%v, include_max=>%v, boost=>%.2f)",
		field, escapeSQLString(min), escapeSQLString(max), includeMin, includeMax, boost)
}

// BooleanClause 布尔查询子句
type BooleanClause struct {
	Must    []string
	Should  []string
	MustNot []string
}

// FulltextBooleanSQL 生成布尔查询SQL
func FulltextBooleanSQL(clause BooleanClause, minShouldMatch int, boost float64) string {
	var parts []string

	if len(clause.Must) > 0 {
		parts = append(parts, fmt.Sprintf("must=>ARRAY[%s]", strings.Join(clause.Must, ", ")))
	}
	if len(clause.Should) > 0 {
		parts = append(parts, fmt.Sprintf("should=>ARRAY[%s]", strings.Join(clause.Should, ", ")))
		parts = append(parts, fmt.Sprintf("minimum_should_match=>%d", minShouldMatch))
	}
	if len(clause.MustNot) > 0 {
		parts = append(parts, fmt.Sprintf("must_not=>ARRAY[%s]", strings.Join(clause.MustNot, ", ")))
	}

	parts = append(parts, fmt.Sprintf("boost=>%.2f", boost))

	return fmt.Sprintf("fulltext_boolean(%s)", strings.Join(parts, ", "))
}

// FulltextDisjunctionMax 析取最大查询函数
// 示例: fulltext_disjunction_max(ARRAY[q1, q2], tie_breaker=>0.3)
func FulltextDisjunctionMax(queries []string, tieBreaker, boost float64) string {
	return fmt.Sprintf("fulltext_disjunction_max(ARRAY[%s], tie_breaker=>%.2f, boost=>%.2f)",
		strings.Join(queries, ", "), tieBreaker, boost)
}

// FulltextBoost 提升查询函数
// 示例: fulltext_boost(query, 2.0)
func FulltextBoost(query string, boost float64) string {
	return fmt.Sprintf("fulltext_boost(%s, %.2f)", query, boost)
}

// FulltextExists 字段存在查询函数
// 示例: fulltext_exists('field')
func FulltextExists(field string, boost float64) string {
	return fmt.Sprintf("fulltext_exists('%s', boost=>%.2f)", field, boost)
}

// FulltextPrefix 前缀查询函数
// 示例: fulltext_prefix('field', 'prefix')
func FulltextPrefix(field, prefix string, boost float64) string {
	return fmt.Sprintf("fulltext_prefix('%s', '%s', boost=>%.2f)",
		field, escapeSQLString(prefix), boost)
}

// FulltextWildcard 通配符查询函数
// 示例: fulltext_wildcard('field', 'prefix*')
func FulltextWildcard(field, wildcard string, boost float64) string {
	return fmt.Sprintf("fulltext_wildcard('%s', '%s', boost=>%.2f)",
		field, escapeSQLString(wildcard), boost)
}

// FulltextNested 嵌套查询函数（用于JSON字段）
// 示例: fulltext_nested('metadata', fulltext_term('color', 'red'))
func FulltextNested(path, query string, boost float64) string {
	return fmt.Sprintf("fulltext_nested('%s', %s, boost=>%.2f)",
		path, query, boost)
}

// BM25Score BM25分数函数
// 示例: bm25_score(content, '关键词') AS score
func BM25Score(field, query string) string {
	return fmt.Sprintf("bm25_score(%s, '%s')", field, escapeSQLString(query))
}

// BM25Rank BM25排名函数
// 示例: bm25_rank(content, '关键词') AS rank
func BM25Rank(field, query string) string {
	return fmt.Sprintf("bm25_rank(%s, '%s')", field, escapeSQLString(query))
}

// HybridScore 混合分数函数
// 示例: hybrid_score(ft_score, vec_score, ft_weight=>0.7, vec_weight=>0.3)
func HybridScore(ftScore, vecScore string, ftWeight, vecWeight float64) string {
	return fmt.Sprintf("hybrid_score(%s, %s, ft_weight=>%.2f, vec_weight=>%.2f)",
		ftScore, vecScore, ftWeight, vecWeight)
}

// HybridRank 混合排名函数
// 示例: hybrid_rank(content, embedding, '关键词', '[0.1, 0.2, ...]')
func HybridRank(field, vectorField, query, vector string) string {
	return fmt.Sprintf("hybrid_rank(%s, %s, '%s', '%s')",
		field, vectorField, escapeSQLString(query), vector)
}

// RRFRank RRF融合排名函数
// 示例: rrf_rank(bm25_rank(...), vector_rank(...), k=>60)
func RRFRank(ranks []string, k int) string {
	return fmt.Sprintf("rrf_rank(%s, k=>%d)", strings.Join(ranks, ", "), k)
}

// Highlight 高亮函数
// 示例: highlight(content, '关键词', '<mark>', '</mark>')
func Highlight(field, query, preTag, postTag string) string {
	return fmt.Sprintf("highlight(%s, '%s', '%s', '%s')",
		field, escapeSQLString(query), preTag, postTag)
}

// HighlightMulti 多片段高亮函数
// 示例: highlight_multi(content, '关键词', '<mark>', '</mark>', 3, 150)
func HighlightMulti(field, query, preTag, postTag string, numFragments, fragmentLen int) string {
	return fmt.Sprintf("highlight_multi(%s, '%s', '%s', '%s', %d, %d)",
		field, escapeSQLString(query), preTag, postTag, numFragments, fragmentLen)
}

// Tokenize 分词函数
// 示例: fulltext_tokenize('jieba', '永和服装饰品有限公司')
func Tokenize(tokenizer, text string) string {
	return fmt.Sprintf("fulltext_tokenize('%s', '%s')", tokenizer, escapeSQLString(text))
}

// escapeSQLString 转义SQL字符串
func escapeSQLString(s string) string {
	s = strings.ReplaceAll(s, "'", "''")
	s = strings.ReplaceAll(s, "\\", "\\\\")
	return s
}

// escapeSQLStrings 转义多个SQL字符串
func escapeSQLStrings(strs []string) []string {
	result := make([]string, len(strs))
	for i, s := range strs {
		result[i] = escapeSQLString(s)
	}
	return result
}

// SQLGenerator SQL生成器
type SQLGenerator struct {
	SelectColumns []string
	FromTable     string
	WhereField    string
	Query         string
	OrderBy       string
	Limit         int
	Offset        int
}

// NewSQLGenerator 创建SQL生成器
func NewSQLGenerator(table, field, query string) *SQLGenerator {
	return &SQLGenerator{
		SelectColumns: []string{"*"},
		FromTable:     table,
		WhereField:    field,
		Query:         query,
		OrderBy:       "bm25_score DESC",
		Limit:         10,
		Offset:        0,
	}
}

// Select 设置查询列
func (g *SQLGenerator) Select(columns ...string) *SQLGenerator {
	g.SelectColumns = columns
	return g
}

// OrderByScore 设置按分数排序
func (g *SQLGenerator) OrderByScore(desc bool) *SQLGenerator {
	if desc {
		g.OrderBy = "bm25_score DESC"
	} else {
		g.OrderBy = "bm25_score ASC"
	}
	return g
}

// Paginate 设置分页
func (g *SQLGenerator) Paginate(limit, offset int) *SQLGenerator {
	g.Limit = limit
	g.Offset = offset
	return g
}

// Build 生成SQL
func (g *SQLGenerator) Build() string {
	columns := strings.Join(g.SelectColumns, ", ")

	sql := fmt.Sprintf("SELECT %s FROM %s WHERE %s @@ fulltext_config('%s')",
		columns, g.FromTable, g.WhereField, escapeSQLString(g.Query))

	if g.OrderBy != "" {
		sql += fmt.Sprintf(" ORDER BY %s", g.OrderBy)
	}

	if g.Limit > 0 {
		sql += fmt.Sprintf(" LIMIT %d", g.Limit)
	}

	if g.Offset > 0 {
		sql += fmt.Sprintf(" OFFSET %d", g.Offset)
	}

	return sql
}

// BuildWithScore 生成带分数的SQL
func (g *SQLGenerator) BuildWithScore() string {
	columns := strings.Join(g.SelectColumns, ", ")
	if columns == "*" {
		columns = "*, " + BM25Score(g.WhereField, g.Query) + " AS bm25_score"
	} else {
		columns += ", " + BM25Score(g.WhereField, g.Query) + " AS bm25_score"
	}

	sql := fmt.Sprintf("SELECT %s FROM %s WHERE %s @@ fulltext_config('%s')",
		columns, g.FromTable, g.WhereField, escapeSQLString(g.Query))

	if g.OrderBy != "" {
		sql += fmt.Sprintf(" ORDER BY %s", g.OrderBy)
	}

	if g.Limit > 0 {
		sql += fmt.Sprintf(" LIMIT %d", g.Limit)
	}

	if g.Offset > 0 {
		sql += fmt.Sprintf(" OFFSET %d", g.Offset)
	}

	return sql
}

// QueryType 查询类型
const (
	QueryTypeTerm               = "term"
	QueryTypePhrase             = "phrase"
	QueryTypePhrasePrefix       = "phrase_prefix"
	QueryTypeFuzzy              = "fuzzy"
	QueryTypeRegex              = "regex"
	QueryTypeRange              = "range"
	QueryTypeBoolean            = "boolean"
	QueryTypeDisjunctionMax     = "disjunction_max"
	QueryTypeConstScore         = "const_score"
	QueryTypeEmpty              = "empty"
	QueryTypeTermSet            = "term_set"
	QueryTypePrefix             = "prefix"
	QueryTypeWildcard           = "wildcard"
	QueryTypeExists             = "exists"
	QueryTypeNested             = "nested"
)
