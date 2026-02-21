package analyzer

import (
	"strings"
	"unicode"
)

// Token 分词结果
type Token struct {
	Text     string // 词文本
	Position int    // 位置
	Start    int    // 起始偏移
	End      int    // 结束偏移
	Type     string // 词性
}

// TokenizerType 分词器类型
type TokenizerType string

const (
	TokenizerTypeJieba    TokenizerType = "jieba"
	TokenizerTypeNgram    TokenizerType = "ngram"
	TokenizerTypeLindera  TokenizerType = "lindera"
	TokenizerTypeEnglish  TokenizerType = "english"
	TokenizerTypeStandard TokenizerType = "standard"
	TokenizerTypeRaw      TokenizerType = "raw"
)

// DefaultEnglishStopWords 默认英文停用词
var DefaultEnglishStopWords = []string{
	"the", "a", "an", "and", "or", "but", "is", "are", "was", "were",
	"be", "been", "being", "have", "has", "had", "do", "does", "did",
	"will", "would", "shall", "should", "can", "could", "may", "might",
	"must", "i", "you", "he", "she", "it", "we", "they", "this", "that",
	"these", "those", "in", "on", "at", "by", "for", "with", "about",
	"to", "from", "up", "down", "of", "off", "over", "under", "again",
}

// DefaultChineseStopWords 默认中文停用词（简化版）
var DefaultChineseStopWords = []string{
	"的", "了", "和", "是", "在", "有", "我", "他", "她", "它",
	"们", "这", "那", "之", "与", "或", "及", "等", "对", "为",
}

// MergeStopWords 合并停用词
func MergeStopWords(lists ...[]string) map[string]bool {
	result := make(map[string]bool)
	for _, list := range lists {
		for _, word := range list {
			result[word] = true
		}
	}
	return result
}

// Tokenizer 分词器接口
type Tokenizer interface {
	Tokenize(text string) ([]Token, error)
	TokenizeForSearch(text string) ([]Token, error)
}

// BaseTokenizer 基础分词器
type BaseTokenizer struct {
	StopWords   map[string]bool
	MinTokenLen int
	MaxTokenLen int
	Lowercase   bool
}

// NewBaseTokenizer 创建基础分词器
func NewBaseTokenizer(stopWords []string, minLen, maxLen int) *BaseTokenizer {
	return &BaseTokenizer{
		StopWords:   MergeStopWords(stopWords),
		MinTokenLen: minLen,
		MaxTokenLen: maxLen,
		Lowercase:   true,
	}
}

// IsStopWord 检查是否为停用词
func (t *BaseTokenizer) IsStopWord(word string) bool {
	return t.StopWords[word]
}

// FilterToken 过滤词
func (t *BaseTokenizer) FilterToken(token string) (string, bool) {
	if t.Lowercase {
		token = strings.ToLower(token)
	}
	
	// 过滤停用词
	if t.IsStopWord(token) {
		return "", false
	}
	
	// 过滤长度不符的词
	runes := []rune(token)
	if len(runes) < t.MinTokenLen || len(runes) > t.MaxTokenLen {
		return "", false
	}
	
	return token, true
}

// StandardTokenizer 标准分词器（按空格和标点分词）
type StandardTokenizer struct {
	*BaseTokenizer
}

// NewStandardTokenizer 创建标准分词器
func NewStandardTokenizer(stopWords []string) *StandardTokenizer {
	return &StandardTokenizer{
		BaseTokenizer: NewBaseTokenizer(stopWords, 2, 100),
	}
}

// Tokenize 分词
func (t *StandardTokenizer) Tokenize(text string) ([]Token, error) {
	return t.tokenizeInternal(text, false)
}

// TokenizeForSearch 搜索模式分词
func (t *StandardTokenizer) TokenizeForSearch(text string) ([]Token, error) {
	return t.tokenizeInternal(text, true)
}

func (t *StandardTokenizer) tokenizeInternal(text string, forSearch bool) ([]Token, error) {
	var tokens []Token
	position := 0
	
	// 使用Unicode分词
	start := -1
	runes := []rune(text)
	
	for i, r := range runes {
		if t.isTokenChar(r) {
			if start == -1 {
				start = i
			}
		} else {
			if start != -1 {
				token := string(runes[start:i])
				if filtered, ok := t.FilterToken(token); ok {
					tokens = append(tokens, Token{
						Text:     filtered,
						Position: position,
						Start:    start,
						End:      i,
					})
					position++
				}
				start = -1
			}
		}
	}
	
	// 处理最后一个词
	if start != -1 {
		token := string(runes[start:])
		if filtered, ok := t.FilterToken(token); ok {
			tokens = append(tokens, Token{
				Text:     filtered,
				Position: position,
				Start:    start,
				End:      len(runes),
			})
		}
	}
	
	return tokens, nil
}

func (t *StandardTokenizer) isTokenChar(r rune) bool {
	// 字母、数字、汉字都是有效字符
	return unicode.IsLetter(r) || unicode.IsNumber(r) || (r >= 0x4E00 && r <= 0x9FFF)
}

// NgramTokenizer N-gram分词器
type NgramTokenizer struct {
	*BaseTokenizer
	MinGram    int
	MaxGram    int
	PrefixOnly bool
}

// NewNgramTokenizer 创建N-gram分词器
func NewNgramTokenizer(minGram, maxGram int, prefixOnly bool, stopWords []string) *NgramTokenizer {
	return &NgramTokenizer{
		BaseTokenizer: NewBaseTokenizer(stopWords, minGram, maxGram),
		MinGram:       minGram,
		MaxGram:       maxGram,
		PrefixOnly:    prefixOnly,
	}
}

// Tokenize N-gram分词
func (t *NgramTokenizer) Tokenize(text string) ([]Token, error) {
	runes := []rune(text)
	var tokens []Token
	position := 0
	
	for i := 0; i < len(runes); i++ {
		maxJ := t.MaxGram
		if t.PrefixOnly {
			maxJ = t.MinGram
		}
		
		for j := t.MinGram; j <= maxJ && i+j <= len(runes); j++ {
			token := string(runes[i : i+j])
			if filtered, ok := t.FilterToken(token); ok {
				tokens = append(tokens, Token{
					Text:     filtered,
					Position: position,
					Start:    i,
					End:      i + j,
				})
				position++
			}
		}
	}
	
	return tokens, nil
}

// TokenizeForSearch 搜索模式分词（更细粒度）
func (t *NgramTokenizer) TokenizeForSearch(text string) ([]Token, error) {
	// 搜索模式下使用更小的粒度
	runes := []rune(text)
	var tokens []Token
	position := 0
	
	for i := 0; i < len(runes); i++ {
		for j := 1; j <= t.MaxGram && i+j <= len(runes); j++ {
			if j < t.MinGram && !t.PrefixOnly {
				continue
			}
			
			token := string(runes[i : i+j])
			if filtered, ok := t.FilterToken(token); ok {
				tokens = append(tokens, Token{
					Text:     filtered,
					Position: position,
					Start:    i,
					End:      i + j,
				})
				position++
			}
		}
	}
	
	return tokens, nil
}

// EnglishTokenizer 英文分词器（带词干提取）
type EnglishTokenizer struct {
	*BaseTokenizer
}

// NewEnglishTokenizer 创建英文分词器
func NewEnglishTokenizer(stopWords []string) *EnglishTokenizer {
	if stopWords == nil {
		stopWords = DefaultEnglishStopWords
	}
	return &EnglishTokenizer{
		BaseTokenizer: NewBaseTokenizer(stopWords, 2, 100),
	}
}

// Tokenize 英文分词
func (t *EnglishTokenizer) Tokenize(text string) ([]Token, error) {
	// 使用标准分词
	standard := NewStandardTokenizer(nil)
	tokens, err := standard.Tokenize(text)
	if err != nil {
		return nil, err
	}
	
	// 词干提取
	for i := range tokens {
		tokens[i].Text = t.stem(tokens[i].Text)
	}
	
	return tokens, nil
}

// TokenizeForSearch 搜索模式分词
func (t *EnglishTokenizer) TokenizeForSearch(text string) ([]Token, error) {
	return t.Tokenize(text)
}

// 简单的词干提取（Porter Stemmer简化版）
func (t *EnglishTokenizer) stem(word string) string {
	// 移除常见的后缀
	suffixes := []string{"ing", "edly", "ed", "ly", "s", "es", "ies"}
	for _, suffix := range suffixes {
		if strings.HasSuffix(word, suffix) && len(word) > len(suffix)+2 {
			return word[:len(word)-len(suffix)]
		}
	}
	return word
}

// TokenizerFactory 分词器工厂
func TokenizerFactory(tokenizerType TokenizerType, options map[string]interface{}) (Tokenizer, error) {
	switch tokenizerType {
	case TokenizerTypeStandard:
		return NewStandardTokenizer(nil), nil
	case TokenizerTypeNgram:
		minGram := 2
		maxGram := 3
		prefixOnly := false
		
		if v, ok := options["min_gram"].(int); ok {
			minGram = v
		}
		if v, ok := options["max_gram"].(int); ok {
			maxGram = v
		}
		if v, ok := options["prefix_only"].(bool); ok {
			prefixOnly = v
		}
		
		return NewNgramTokenizer(minGram, maxGram, prefixOnly, nil), nil
	case TokenizerTypeEnglish:
		return NewEnglishTokenizer(nil), nil
	case TokenizerTypeJieba:
		dictPath := ""
		hmmPath := ""
		userDictPath := ""
		
		if v, ok := options["dict_path"].(string); ok {
			dictPath = v
		}
		if v, ok := options["hmm_path"].(string); ok {
			hmmPath = v
		}
		if v, ok := options["user_dict_path"].(string); ok {
			userDictPath = v
		}
		
		return NewJiebaTokenizer(dictPath, hmmPath, userDictPath, nil)
	default:
		return NewStandardTokenizer(nil), nil
	}
}
