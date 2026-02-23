package fulltext

import "github.com/kasuganosora/sqlexec/pkg/fulltext/analyzer"

// Token 分词结果（别名）
type Token = analyzer.Token

// TokenizerType 分词器类型（别名）
type TokenizerType = analyzer.TokenizerType

// 分词器类型常量
const (
	TokenizerTypeJieba    = analyzer.TokenizerTypeJieba
	TokenizerTypeNgram    = analyzer.TokenizerTypeNgram
	TokenizerTypeLindera  = analyzer.TokenizerTypeLindera
	TokenizerTypeEnglish  = analyzer.TokenizerTypeEnglish
	TokenizerTypeStandard = analyzer.TokenizerTypeStandard
	TokenizerTypeRaw      = analyzer.TokenizerTypeRaw
)

// Document 文档
type Document struct {
	ID      int64
	Content string
	Fields  map[string]interface{}
}

// SearchResult 搜索结果
type SearchResult struct {
	DocID int64
	Score float64
	Doc   *Document
}

// SearchResultWithHighlight 带高亮的搜索结果
type SearchResultWithHighlight struct {
	SearchResult
	Highlights []string
}

// FieldType 字段类型
type FieldType string

const (
	FieldTypeText     FieldType = "text"
	FieldTypeNumeric  FieldType = "numeric"
	FieldTypeBoolean  FieldType = "boolean"
	FieldTypeDatetime FieldType = "datetime"
	FieldTypeJSON     FieldType = "json"
)

// RecordType 记录类型
type RecordType string

const (
	RecordTypeBasic    RecordType = "basic"    // 仅记录文档ID
	RecordTypeFreq     RecordType = "freq"     // 记录词频
	RecordTypePosition RecordType = "position" // 记录位置（用于短语查询）
	RecordTypeScore    RecordType = "score"    // 记录BM25分数
)

// BM25Params BM25参数
type BM25Params struct {
	K1 float64 // 词频饱和参数 (1.2-2.0)
	B  float64 // 长度归一化参数 (0-1)
}

// DefaultBM25Params 默认BM25参数
var DefaultBM25Params = BM25Params{
	K1: 1.2,
	B:  0.75,
}

// FieldConfig 字段配置
type FieldConfig struct {
	Name       string
	Type       FieldType
	Tokenizer  TokenizerType
	Record     RecordType
	Fast       bool
	FieldNorms bool
	Boost      float64
}

// Config 全文搜索配置
type Config struct {
	BM25Params  BM25Params
	StopWords   []string
	MinTokenLen int
	MaxTokenLen int
}

// DefaultConfig 默认配置
var DefaultConfig = &Config{
	BM25Params:  DefaultBM25Params,
	StopWords:   analyzer.DefaultEnglishStopWords,
	MinTokenLen: 2,
	MaxTokenLen: 100,
}
