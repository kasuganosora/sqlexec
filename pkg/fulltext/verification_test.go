package fulltext

import (
	"fmt"
	"testing"
	
	"github.com/kasuganosora/sqlexec/pkg/fulltext/analyzer"
)

// TestJiebaAndHybrid 验证 Jieba 分词器和混合搜索功能
func TestJiebaAndHybrid(t *testing.T) {
	// 测试 Jieba 分词器
	tokenizer, err := analyzer.TokenizerFactory(analyzer.TokenizerTypeJieba, nil)
	if err != nil {
		t.Fatalf("Failed to create Jieba tokenizer: %v", err)
	}
	
	// 测试中文分词
	text := "我爱北京天安门"
	tokens, err := tokenizer.Tokenize(text)
	if err != nil {
		t.Fatalf("Failed to tokenize: %v", err)
	}
	
	fmt.Printf("Jieba tokenization result: %d tokens\n", len(tokens))
	for i, token := range tokens {
		fmt.Printf("  %d: %s\n", i, token.Text)
	}
	
	if len(tokens) == 0 {
		t.Error("Jieba tokenizer produced no tokens")
	}
	
	// 测试混合搜索引擎
	engine := NewEngine(DefaultConfig)
	
	// 索引一些文档
	docs := []*Document{
		{ID: 1, Content: "我爱北京天安门，天安门上太阳升"},
		{ID: 2, Content: "机器学习是人工智能的重要分支"},
		{ID: 3, Content: "深度学习在图像识别中的应用"},
		{ID: 4, Content: "自然语言处理是人工智能的前沿领域"},
	}
	
	for _, doc := range docs {
		if err := engine.IndexDocument(doc); err != nil {
			t.Fatalf("Failed to index document: %v", err)
		}
	}
	
	// 创建混合搜索引擎
	hybrid := NewHybridEngine(engine, 0.7, 0.3)
	
	// 测试 RRF 融合
	hybrid.SetRRF(60)
	results, err := hybrid.SearchHybrid("机器学习", 3)
	if err != nil {
		t.Fatalf("Hybrid search failed: %v", err)
	}
	
	fmt.Printf("\nHybrid search (RRF) results: %d\n", len(results))
	for i, result := range results {
		fmt.Printf("  %d. DocID=%d, HybridScore=%.4f, FTScore=%.4f, VecScore=%.4f\n",
			i+1, result.DocID, result.HybridScore, result.FTScore, result.VecScore)
	}
	
	// 测试加权融合
	hybrid.SetWeightedFusion(0.6, 0.4)
	weightedResults, err := hybrid.SearchHybrid("人工智能", 3)
	if err != nil {
		t.Fatalf("Weighted hybrid search failed: %v", err)
	}
	
	fmt.Printf("\nHybrid search (Weighted) results: %d\n", len(weightedResults))
	for i, result := range weightedResults {
		fmt.Printf("  %d. DocID=%d, HybridScore=%.4f (FT:%.4f, Vec:%.4f)\n",
			i+1, result.DocID, result.HybridScore, result.FTScore, result.VecScore)
	}
	
	// 测试自动向量转换
	fmt.Println("\nTesting auto vector conversion:")
	for _, doc := range docs {
		vector, err := hybrid.AutoConvertToVector(doc)
		if err != nil {
			t.Fatalf("AutoConvertToVector failed: %v", err)
		}
		fmt.Printf("  Doc %d: %d terms in vector\n", doc.ID, len(vector))
		if len(vector) == 0 {
			t.Errorf("Doc %d has empty vector", doc.ID)
		}
	}
	
	fmt.Println("\n✅ All tests passed!")
}

// TestQueryTypes 验证查询类型
func TestQueryTypes(t *testing.T) {
	engine := NewEngine(DefaultConfig)
	
	// 索引测试文档
	docs := []*Document{
		{ID: 1, Content: "The quick brown fox jumps over the lazy dog"},
		{ID: 2, Content: "The fast orange fox leaps over the sleepy dog"},
		{ID: 3, Content: "A brown dog runs in the park"},
	}
	
	for _, doc := range docs {
		if err := engine.IndexDocument(doc); err != nil {
			t.Fatalf("Failed to index: %v", err)
		}
	}
	
	// 测试不同查询类型
	tests := []struct {
		name   string
		query  string
		minRes int
	}{
		{"Term query", "fox", 2},
		{"Phrase query (exact)", "quick brown fox", 1},
		{"Partial match", "lazy", 1},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			results, err := engine.Search(tt.query, 10)
			if err != nil {
				t.Fatalf("Search failed: %v", err)
			}
			
			fmt.Printf("%s: found %d results for '%s'\n", tt.name, len(results), tt.query)
			for i, r := range results {
				if i < 3 { // 只显示前3个
					fmt.Printf("  %d. Doc%d (score: %.4f)\n", i+1, r.DocID, r.Score)
				}
			}
			
			if len(results) < tt.minRes {
				t.Errorf("Expected at least %d results, got %d", tt.minRes, len(results))
			}
		})
	}
}
