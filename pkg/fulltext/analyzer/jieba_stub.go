//go:build !cgo

package analyzer

import "fmt"

// NewJiebaTokenizer returns an error when CGO is not available.
// gojieba requires CGO (C library). Build with CGO_ENABLED=1 to enable Jieba tokenizer.
func NewJiebaTokenizer(dictPath, hmmPath, userDictPath string, stopWords []string) (*StandardTokenizer, error) {
	return nil, fmt.Errorf("jieba tokenizer requires CGO; build with CGO_ENABLED=1")
}
