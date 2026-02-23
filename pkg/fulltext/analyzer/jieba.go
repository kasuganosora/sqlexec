//go:build cgo

package analyzer

import (
	"strings"
	"sync"

	"github.com/yanyiwu/gojieba"
)

// JiebaTokenizer 基于 gojieba 的中文分词器
type JiebaTokenizer struct {
	*BaseTokenizer
	segmenter  *gojieba.Jieba
	useHMM     bool
	searchMode bool
	mu         sync.RWMutex
}

// NewJiebaTokenizer 创建 Jieba 分词器
// dictPath: 主词典路径（可选）
// hmmPath: HMM模型路径（可选）
// userDictPath: 用户词典路径（可选）
// stopWords: 停用词列表
func NewJiebaTokenizer(dictPath, hmmPath, userDictPath string, stopWords []string) (*JiebaTokenizer, error) {
	// 创建 gojieba 分词器
	var seg *gojieba.Jieba

	if dictPath != "" && hmmPath != "" {
		// 使用自定义词典
		seg = gojieba.NewJieba(dictPath, hmmPath, userDictPath, "")
	} else {
		// 使用默认词典
		seg = gojieba.NewJieba()
	}

	tokenizer := &JiebaTokenizer{
		BaseTokenizer: NewBaseTokenizer(stopWords, 1, 50),
		segmenter:     seg,
		useHMM:        true,
		searchMode:    false,
	}

	return tokenizer, nil
}

// Tokenize 精确模式分词
func (j *JiebaTokenizer) Tokenize(text string) ([]Token, error) {
	j.mu.RLock()
	defer j.mu.RUnlock()

	// 使用精确模式
	words := j.segmenter.Cut(text, j.useHMM)
	return j.convertToTokens(words, text)
}

// TokenizeForSearch 搜索模式分词（更细粒度）
func (j *JiebaTokenizer) TokenizeForSearch(text string) ([]Token, error) {
	j.mu.RLock()
	defer j.mu.RUnlock()

	// 使用搜索引擎模式（更细粒度）
	words := j.segmenter.CutForSearch(text, j.useHMM)
	return j.convertToTokens(words, text)
}

// CutAll 全模式分词（所有可能的分词结果）
func (j *JiebaTokenizer) CutAll(text string) ([]Token, error) {
	j.mu.RLock()
	defer j.mu.RUnlock()

	words := j.segmenter.CutAll(text)
	return j.convertToTokens(words, text)
}

// CutForIndex 索引模式分词（适合构建索引）
// 对长词进行额外切分，生成前缀词
func (j *JiebaTokenizer) CutForIndex(text string) ([]Token, error) {
	j.mu.RLock()
	defer j.mu.RUnlock()

	// 先进行精确模式分词
	words := j.segmenter.Cut(text, j.useHMM)

	var allWords []string
	for _, word := range words {
		allWords = append(allWords, word)

		// 对长词（>=3字）生成前缀
		runes := []rune(word)
		if len(runes) >= 3 {
			// 生成2字前缀
			if len(runes) >= 2 {
				prefix2 := string(runes[:2])
				allWords = append(allWords, prefix2)
			}
			// 生成3字前缀
			if len(runes) >= 3 {
				prefix3 := string(runes[:3])
				allWords = append(allWords, prefix3)
			}
		}
	}

	return j.convertToTokens(allWords, text)
}

// Tag 词性标注
func (j *JiebaTokenizer) Tag(text string) ([]Token, error) {
	j.mu.RLock()
	defer j.mu.RUnlock()

	tags := j.segmenter.Tag(text)

	tokens := make([]Token, 0, len(tags))
	position := 0
	offset := 0

	for _, tag := range tags {
		word := tag
		pos := "x" // 词性

		// gojieba.Tag 返回格式: "词/词性"
		if idx := strings.Index(tag, "/"); idx != -1 {
			word = tag[:idx]
			pos = tag[idx+1:]
		}

		if filtered, ok := j.FilterToken(word); ok {
			start := strings.Index(text[offset:], word)
			if start != -1 {
				start += offset
				end := start + len(word)
				tokens = append(tokens, Token{
					Text:     filtered,
					Position: position,
					Start:    start,
					End:      end,
					Type:     pos,
				})
				position++
				offset = end
			}
		}
	}

	return tokens, nil
}

// Extract 关键词提取（TF-IDF）
func (j *JiebaTokenizer) Extract(text string, topK int) ([]Token, error) {
	j.mu.RLock()
	defer j.mu.RUnlock()

	keywords := j.segmenter.Extract(text, topK)

	tokens := make([]Token, 0, len(keywords))
	for i, kw := range keywords {
		if filtered, ok := j.FilterToken(kw); ok {
			tokens = append(tokens, Token{
				Text:     filtered,
				Position: i,
				Type:     "keyword",
			})
		}
	}

	return tokens, nil
}

// AddWord 动态添加词到词典
func (j *JiebaTokenizer) AddWord(word string) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.segmenter.AddWord(word)
}

// AddWordEx 动态添加词到词典（带词频和词性）
func (j *JiebaTokenizer) AddWordEx(word string, freq int, tag string) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.segmenter.AddWordEx(word, freq, tag)
}

// RemoveWord 从词典中删除词
func (j *JiebaTokenizer) RemoveWord(word string) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.segmenter.RemoveWord(word)
}

// HasWord 检查词是否在词典中
func (j *JiebaTokenizer) HasWord(word string) bool {
	j.mu.RLock()
	defer j.mu.RUnlock()
	// gojieba 没有直接的 LookupWord 方法，使用 Cut 来检查
	words := j.segmenter.Cut(word, false)
	return len(words) == 1 && words[0] == word
}

// SetSearchMode 设置搜索模式
func (j *JiebaTokenizer) SetSearchMode(mode bool) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.searchMode = mode
}

// SetHMM 设置HMM模式
func (j *JiebaTokenizer) SetHMM(useHMM bool) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.useHMM = useHMM
}

// Free 释放资源
func (j *JiebaTokenizer) Free() {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.segmenter != nil {
		j.segmenter.Free()
	}
}

// convertToTokens 将分词结果转换为 Token 列表
func (j *JiebaTokenizer) convertToTokens(words []string, originalText string) ([]Token, error) {
	tokens := make([]Token, 0, len(words))
	position := 0
	offset := 0

	for _, word := range words {
		if filtered, ok := j.FilterToken(word); ok {
			// 查找词在原文中的位置
			start := strings.Index(originalText[offset:], word)
			if start == -1 {
				// 如果找不到，可能是 Unicode 字符
				start = j.findRuneIndex(originalText[offset:], word)
			}

			if start != -1 {
				start += offset
				end := start + len(word)
				tokens = append(tokens, Token{
					Text:     filtered,
					Position: position,
					Start:    start,
					End:      end,
					Type:     "word",
				})
				position++
				offset = end
			}
		}
	}

	return tokens, nil
}

// findRuneIndex 在 rune 级别查找子串位置
func (j *JiebaTokenizer) findRuneIndex(s, substr string) int {
	runes := []rune(s)
	subRunes := []rune(substr)

	for i := 0; i <= len(runes)-len(subRunes); i++ {
		match := true
		for j := 0; j < len(subRunes); j++ {
			if runes[i+j] != subRunes[j] {
				match = false
				break
			}
		}
		if match {
			// 计算字节位置
			return len(string(runes[:i]))
		}
	}
	return -1
}
