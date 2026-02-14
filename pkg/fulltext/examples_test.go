package fulltext

import (
	"fmt"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/fulltext/analyzer"
	"github.com/kasuganosora/sqlexec/pkg/fulltext/query"
)

// Example 演示全文搜索基本用法
func Example() {
	// 创建全文引擎（使用标准分词器）
	engine := NewEngine(&Config{
		BM25Params: DefaultBM25Params,
	})
	
	// 索引文档
	documents := []string{
		"The quick brown fox jumps over the lazy dog",
		"A quick brown dog outpaces a swift fox",
		"The lazy dog sleeps all day",
		"Swift foxes are known for their speed",
	}
	
	for i, content := range documents {
		doc := &Document{
			ID:      int64(i + 1),
			Content: content,
		}
		if err := engine.IndexDocument(doc); err != nil {
			fmt.Printf("Index error: %v\n", err)
			return
		}
	}
	
	// 搜索
	results, err := engine.Search("quick fox", 10)
	if err != nil {
		fmt.Printf("Search error: %v\n", err)
		return
	}
	
	fmt.Printf("Found %d results\n", len(results))
	for _, r := range results {
		fmt.Printf("Doc %d: score=%.4f\n", r.DocID, r.Score)
	}
	
	// Output:
	// Found 2 results
	// Doc 1: score=0.0000
	// Doc 2: score=0.0000
}

// Example_chineseSearch 演示中文搜索
func Example_chineseSearch() {
	// 创建使用N-gram分词器的引擎（适合中文）
	engine := NewEngine(DefaultConfig)
	
	// 使用N-gram分词器（bigram）
	tokenizer := analyzer.NewNgramTokenizer(2, 3, false, analyzer.DefaultChineseStopWords)
	engine.SetTokenizer(tokenizer)
	
	// 索引中文文档
	documents := []string{
		"全文搜索引擎是信息检索的核心组件",
		"高性能搜索需要优化索引结构",
		"倒排索引是搜索引擎的基础数据结构",
	}
	
	for i, content := range documents {
		doc := &Document{
			ID:      int64(i + 1),
			Content: content,
		}
		if err := engine.IndexDocument(doc); err != nil {
			fmt.Printf("Index error: %v\n", err)
			return
		}
	}
	
	// 搜索
	results, err := engine.Search("搜索引擎", 10)
	if err != nil {
		fmt.Printf("Search error: %v\n", err)
		return
	}
	
	fmt.Printf("Found %d results for '搜索引擎'\n", len(results))

	// Output:
	// Found 3 results for '搜索引擎'
}

// Example_booleanQuery 演示布尔查询
func Example_booleanQuery() {
	engine := NewEngine(DefaultConfig)
	
	// 索引文档
	documents := []string{
		"Apple iPhone smartphone",
		"Samsung Android smartphone",
		"Apple MacBook laptop",
		"Dell Windows laptop",
	}
	
	for i, content := range documents {
		doc := &Document{
			ID:      int64(i + 1),
			Content: content,
		}
		engine.IndexDocument(doc)
	}
	
	// 构建布尔查询: (apple AND smartphone) OR (samsung)
	boolQuery := query.NewBooleanQuery()
	
	// Must: 必须包含 apple 和 smartphone
	mustQuery := query.NewBooleanQuery()
	mustQuery.AddMust(query.NewTermQuery("content", "apple"))
	mustQuery.AddMust(query.NewTermQuery("content", "smartphone"))
	boolQuery.AddShould(mustQuery)
	
	// Should: 可以包含 samsung
	boolQuery.AddShould(query.NewTermQuery("content", "samsung"))
	
	// 执行查询
	results, _ := engine.SearchWithQuery(boolQuery, 10)
	
	fmt.Printf("Boolean query found %d results\n", len(results))
	
	// Output:
	// Boolean query found 2 results
}

// Example_phraseSearch 演示短语搜索
func Example_phraseSearch() {
	engine := NewEngine(DefaultConfig)
	
	// 索引文档
	documents := []string{
		"machine learning is a subset of artificial intelligence",
		"deep learning is a type of machine learning",
		"artificial intelligence and machine learning are related",
	}
	
	for i, content := range documents {
		doc := &Document{
			ID:      int64(i + 1),
			Content: content,
		}
		engine.IndexDocument(doc)
	}
	
	// 短语搜索 "machine learning"
	results, _ := engine.SearchPhrase("machine learning", 0, 10)
	
	fmt.Printf("Phrase search found %d results\n", len(results))
	
	// Output:
	// Phrase search found 3 results
}

// Example_highlight 演示高亮搜索
func Example_highlight() {
	engine := NewEngine(DefaultConfig)
	
	// 索引文档
	doc := &Document{
		ID:      1,
		Content: "The quick brown fox jumps over the lazy dog. The quick brown fox is very quick.",
	}
	engine.IndexDocument(doc)
	
	// 带高亮的搜索
	results, _ := engine.SearchWithHighlight("quick fox", 10, "<mark>", "</mark>")

	fmt.Printf("Found %d results with highlights\n", len(results))

	// Output:
	// Found 1 results with highlights
}

// BenchmarkEngine_Index 索引性能基准测试
func BenchmarkEngine_Index(b *testing.B) {
	engine := NewEngine(DefaultConfig)
	
	content := "The quick brown fox jumps over the lazy dog. This is a sample document for benchmarking."
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		doc := &Document{
			ID:      int64(i),
			Content: content,
		}
		engine.IndexDocument(doc)
	}
}

// BenchmarkEngine_Search 搜索性能基准测试
func BenchmarkEngine_Search(b *testing.B) {
	engine := NewEngine(DefaultConfig)
	
	// 索引一些文档
	for i := 0; i < 1000; i++ {
		doc := &Document{
			ID:      int64(i),
			Content: "The quick brown fox jumps over the lazy dog",
		}
		engine.IndexDocument(doc)
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		engine.Search("quick fox", 10)
	}
}

// BenchmarkEngine_SearchBM25 BM25搜索性能基准测试
func BenchmarkEngine_SearchBM25(b *testing.B) {
	engine := NewEngine(DefaultConfig)
	
	// 索引一些文档
	for i := 0; i < 1000; i++ {
		doc := &Document{
			ID:      int64(i),
			Content: "The quick brown fox jumps over the lazy dog",
		}
		engine.IndexDocument(doc)
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		engine.SearchBM25("quick fox", 10)
	}
}

// TestEngine_CompleteWorkflow 完整工作流测试
func TestEngine_CompleteWorkflow(t *testing.T) {
	engine := NewEngine(DefaultConfig)
	
	// 1. 索引文档
	documents := []struct {
		id      int64
		content string
	}{
		{1, "Go is a programming language developed by Google"},
		{2, "Python is great for data science and machine learning"},
		{3, "JavaScript is the language of the web"},
		{4, "Rust provides memory safety without garbage collection"},
		{5, "Go and Rust are both modern systems programming languages"},
	}
	
	for _, d := range documents {
		doc := &Document{
			ID:      d.id,
			Content: d.content,
		}
		if err := engine.IndexDocument(doc); err != nil {
			t.Fatalf("Failed to index document: %v", err)
		}
	}
	
	// 2. 验证文档数量
	if count := engine.DocumentCount(); count != int64(len(documents)) {
		t.Errorf("Expected %d documents, got %d", len(documents), count)
	}
	
	// 3. 搜索测试
	results, err := engine.Search("programming language", 10)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}
	
	if len(results) == 0 {
		t.Error("Expected some results, got none")
	}
	
	// 4. 短语搜索测试
	phraseResults, err := engine.SearchPhrase("programming language", 0, 10)
	if err != nil {
		t.Fatalf("Phrase search failed: %v", err)
	}
	
	if len(phraseResults) == 0 {
		t.Error("Expected some phrase results, got none")
	}
	
	// 5. 获取文档测试
	doc := engine.GetDocument(1)
	if doc == nil {
		t.Error("Failed to get document")
	} else if doc.ID != 1 {
		t.Errorf("Expected doc ID 1, got %d", doc.ID)
	}
	
	// 6. 清空测试
	engine.Clear()
	if count := engine.DocumentCount(); count != 0 {
		t.Errorf("Expected 0 documents after clear, got %d", count)
	}
}

// TestTokenizer_Types 测试各种分词器
func TestTokenizer_Types(t *testing.T) {
	text := "Hello World 中文测试"
	
	// 测试标准分词器
	stdTokenizer := analyzer.NewStandardTokenizer(nil)
	tokens, err := stdTokenizer.Tokenize(text)
	if err != nil {
		t.Fatalf("Standard tokenizer failed: %v", err)
	}
	t.Logf("Standard tokens: %v", tokens)
	
	// 测试N-gram分词器
	ngramTokenizer := analyzer.NewNgramTokenizer(2, 3, false, nil)
	tokens, err = ngramTokenizer.Tokenize(text)
	if err != nil {
		t.Fatalf("N-gram tokenizer failed: %v", err)
	}
	t.Logf("N-gram tokens: %v", tokens)
	
	// 测试英文分词器
	engTokenizer := analyzer.NewEnglishTokenizer(nil)
	tokens, err = engTokenizer.Tokenize(text)
	if err != nil {
		t.Fatalf("English tokenizer failed: %v", err)
	}
	t.Logf("English tokens: %v", tokens)
}

// TestQuery_Parsing 测试查询解析
func TestQuery_Parsing(t *testing.T) {
	parser := query.NewSimpleQueryParser("content")
	
	testCases := []struct {
		query    string
		expected string
	}{
		{"hello world", "boolean"},
		{"+must -exclude", "boolean"},
		{"\"exact phrase\"", "boolean"},
	}
	
	for _, tc := range testCases {
		q, err := parser.Parse(tc.query)
		if err != nil {
			t.Errorf("Failed to parse '%s': %v", tc.query, err)
			continue
		}
		if q == nil {
			t.Errorf("Query for '%s' is nil", tc.query)
		}
	}
}
