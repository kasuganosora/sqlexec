package query

import (
	"regexp"
	"strconv"
	"strings"
)

// Parser 查询解析器
type Parser struct {
	defaultField string
}

// NewParser 创建查询解析器
func NewParser(defaultField string) *Parser {
	return &Parser{
		defaultField: defaultField,
	}
}

// Parse 解析查询字符串
// 支持语法：
// - 简单词: keyword
// - 短语: "exact phrase"
// - 字段限定: title:keyword
// - 布尔: +must -must_not OR AND NOT
// - 模糊: keyword~2
// - 邻近: "word1 word2"~5
// - 提升: keyword^2.0
func (p *Parser) Parse(queryStr string) (Query, error) {
	queryStr = strings.TrimSpace(queryStr)
	if queryStr == "" {
		return NewMatchAllQuery(), nil
	}
	
	// 分词解析
	tokens := p.tokenize(queryStr)
	
	// 构建查询树
	return p.buildQuery(tokens)
}

// token 解析后的token
type token struct {
	typ   tokenType
	value string
}

type tokenType int

const (
	tokenTerm tokenType = iota
	tokenPhrase
	tokenField
	tokenAnd
	tokenOr
	tokenNot
	tokenPlus
	tokenMinus
	tokenLParen
	tokenRParen
	tokenFuzzy
	tokenBoost
	tokenEOF
)

// tokenize 分词
func (p *Parser) tokenize(queryStr string) []token {
	var tokens []token
	
	// 定义正则表达式模式
	patterns := []struct {
		typ     tokenType
		pattern string
	}{
		{tokenPhrase, `"([^"]*)"`},
		{tokenField, `(\w+):`},
		{tokenAnd, `\b(AND|&&)\b`},
		{tokenOr, `\b(OR|\|\|)\b`},
		{tokenNot, `\b(NOT)\b`},
		{tokenPlus, `\+`},
		{tokenMinus, `-`},
		{tokenLParen, `\(`},
		{tokenRParen, `\)`},
		{tokenFuzzy, `~\d*`},
		{tokenBoost, `\^\d+\.?\d*`},
		{tokenTerm, `[^\s\(\)\"\+\-\~\^]+`},
	}
	
	remaining := queryStr
	for len(remaining) > 0 {
		remaining = strings.TrimLeft(remaining, " \t\n\r")
		if len(remaining) == 0 {
			break
		}
		
		matched := false
		for _, pattern := range patterns {
			re := regexp.MustCompile(`^` + pattern.pattern)
			if loc := re.FindStringIndex(remaining); loc != nil {
				value := remaining[loc[0]:loc[1]]
				tokens = append(tokens, token{typ: pattern.typ, value: value})
				remaining = remaining[loc[1]:]
				matched = true
				break
			}
		}
		
		if !matched {
			// 跳过未知字符
			remaining = remaining[1:]
		}
	}
	
	tokens = append(tokens, token{typ: tokenEOF})
	return tokens
}

// buildQuery 构建查询
func (p *Parser) buildQuery(tokens []token) (Query, error) {
	if len(tokens) == 0 {
		return NewMatchAllQuery(), nil
	}
	
	// 简化实现：支持基本语法
	// 实际应该实现完整的查询解析器
	
	var queries []Query
	var mustQueries []Query
	var mustNotQueries []Query
	
	i := 0
	for i < len(tokens)-1 {
		tok := tokens[i]
		
		switch tok.typ {
		case tokenTerm:
			query := p.parseTerm(tokens, &i)
			if query != nil {
				queries = append(queries, query)
			}
			
		case tokenPhrase:
			phrase := strings.Trim(tok.value, `"`)
			words := strings.Fields(phrase)
			query := NewPhraseQuery(p.defaultField, words, 0)
			queries = append(queries, query)
			i++
			
		case tokenPlus:
			i++
			if i < len(tokens) {
				query := p.parseTerm(tokens, &i)
				if query != nil {
					mustQueries = append(mustQueries, query)
				}
			}
			
		case tokenMinus:
			i++
			if i < len(tokens) {
				query := p.parseTerm(tokens, &i)
				if query != nil {
					mustNotQueries = append(mustNotQueries, query)
				}
			}
			
		case tokenField:
			field := strings.TrimSuffix(tok.value, ":")
			i++
			if i < len(tokens) && tokens[i].typ == tokenTerm {
				query := NewTermQuery(field, tokens[i].value)
				queries = append(queries, query)
				i++
			}
			
		default:
			i++
		}
	}
	
	// 构建布尔查询
	boolQuery := NewBooleanQuery()
	
	// 添加must查询
	for _, q := range mustQueries {
		boolQuery.AddMust(q)
	}
	
	// 添加mustNot查询
	for _, q := range mustNotQueries {
		boolQuery.AddMustNot(q)
	}
	
	// 添加should查询
	for _, q := range queries {
		boolQuery.AddShould(q)
	}
	
	// 如果没有查询条件，返回MatchAll
	if len(mustQueries) == 0 && len(mustNotQueries) == 0 && len(queries) == 0 {
		return NewMatchAllQuery(), nil
	}
	
	return boolQuery, nil
}

// parseTerm 解析词项
func (p *Parser) parseTerm(tokens []token, i *int) Query {
	if *i >= len(tokens) {
		return nil
	}
	
	tok := tokens[*i]
	if tok.typ != tokenTerm {
		return nil
	}
	
	term := tok.value
	boost := 1.0
	
	*i++
	
	// 检查是否有提升权重
	if *i < len(tokens) && tokens[*i].typ == tokenBoost {
		boostStr := strings.TrimPrefix(tokens[*i].value, "^")
		if b, err := strconv.ParseFloat(boostStr, 64); err == nil {
			boost = b
		}
		*i++
	}
	
	// 检查是否有模糊查询
	if *i < len(tokens) && tokens[*i].typ == tokenFuzzy {
		fuzzyStr := strings.TrimPrefix(tokens[*i].value, "~")
		distance := 2 // 默认编辑距离
		if fuzzyStr != "" {
			if d, err := strconv.Atoi(fuzzyStr); err == nil {
				distance = d
			}
		}
		query := NewFuzzyQuery(p.defaultField, term, distance)
		query.SetBoost(boost)
		*i++
		return query
	}
	
	query := NewTermQuery(p.defaultField, term)
	query.SetBoost(boost)
	return query
}

// SimpleQueryParser 简化查询解析器
type SimpleQueryParser struct {
	field string
}

// NewSimpleQueryParser 创建简化查询解析器
func NewSimpleQueryParser(field string) *SimpleQueryParser {
	return &SimpleQueryParser{field: field}
}

// Parse 解析查询字符串
// 简单语法：空格分隔的词，双引号表示短语，+表示必须，-表示排除
func (p *SimpleQueryParser) Parse(queryStr string) (Query, error) {
	queryStr = strings.TrimSpace(queryStr)
	if queryStr == "" {
		return NewMatchAllQuery(), nil
	}
	
	boolQuery := NewBooleanQuery()
	
	// 解析短语
	phraseRe := regexp.MustCompile(`"([^"]*)"`)
	phrases := phraseRe.FindAllStringSubmatch(queryStr, -1)
	for _, phrase := range phrases {
		if len(phrase) > 1 {
			words := strings.Fields(phrase[1])
			if len(words) > 0 {
				boolQuery.AddShould(NewPhraseQuery(p.field, words, 0))
			}
		}
	}
	// 移除短语部分
	queryStr = phraseRe.ReplaceAllString(queryStr, "")
	
	// 解析词项
	words := strings.Fields(queryStr)
	for _, word := range words {
		if strings.HasPrefix(word, "+") {
			// 必须包含
			term := strings.TrimPrefix(word, "+")
			boolQuery.AddMust(NewTermQuery(p.field, term))
		} else if strings.HasPrefix(word, "-") {
			// 必须排除
			term := strings.TrimPrefix(word, "-")
			boolQuery.AddMustNot(NewTermQuery(p.field, term))
		} else {
			// 应该包含
			boolQuery.AddShould(NewTermQuery(p.field, word))
		}
	}
	
	return boolQuery, nil
}
