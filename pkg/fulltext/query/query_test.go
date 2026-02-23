package query

import (
	"fmt"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/fulltext/bm25"
	"github.com/kasuganosora/sqlexec/pkg/fulltext/index"
)

// TestRangeQuery 测试范围查询
func TestRangeQuery(t *testing.T) {
	// 创建索引
	stats := bm25.NewCollectionStats()
	scorer := bm25.NewScorer(bm25.DefaultParams, stats)
	idx := index.NewInvertedIndex(scorer)

	// 添加测试文档
	docs := []*index.Document{
		{ID: 1, Content: "Product A", Fields: map[string]interface{}{"price": 100, "rating": 4.5, "date": "2024-01-01"}},
		{ID: 2, Content: "Product B", Fields: map[string]interface{}{"price": 200, "rating": 3.5, "date": "2024-02-01"}},
		{ID: 3, Content: "Product C", Fields: map[string]interface{}{"price": 150, "rating": 5.0, "date": "2024-03-01"}},
		{ID: 4, Content: "Product D", Fields: map[string]interface{}{"price": 300, "rating": 2.0, "date": "2024-04-01"}},
	}

	// 索引文档（简化，不使用分词）
	for _, doc := range docs {
		idx.GetDocStore()[doc.ID] = doc
		stats.TotalDocs++
	}

	// 测试整数范围查询
	t.Run("IntegerRange", func(t *testing.T) {
		query := NewRangeQuery("price", 100, 200, true, true)
		results := query.Execute(idx)

		fmt.Printf("Integer range query results: %d\n", len(results))
		for _, r := range results {
			fmt.Printf("  Doc %d: price=%v\n", r.DocID, r.Doc.Fields["price"])
		}

		if len(results) != 3 {
			t.Errorf("Expected 2 results, got %d", len(results))
		}
	})

	// 测试浮点数范围查询
	t.Run("FloatRange", func(t *testing.T) {
		query := NewRangeQuery("rating", 3.0, 5.0, true, true)
		results := query.Execute(idx)

		fmt.Printf("Float range query results: %d\n", len(results))
		for _, r := range results {
			fmt.Printf("  Doc %d: rating=%v\n", r.DocID, r.Doc.Fields["rating"])
		}

		if len(results) != 3 {
			t.Errorf("Expected 2 results, got %d", len(results))
		}
	})

	// 测试字符串范围查询
	t.Run("StringRange", func(t *testing.T) {
		query := NewRangeQuery("date", "2024-02-01", "2024-03-01", true, true)
		results := query.Execute(idx)

		fmt.Printf("String range query results: %d\n", len(results))
		for _, r := range results {
			fmt.Printf("  Doc %d: date=%v\n", r.DocID, r.Doc.Fields["date"])
		}

		if len(results) != 2 {
			t.Errorf("Expected 2 results, got %d", len(results))
		}
	})
}

// TestFuzzyQuery 测试模糊查询
func TestFuzzyQuery(t *testing.T) {
	// 创建索引
	stats := bm25.NewCollectionStats()
	scorer := bm25.NewScorer(bm25.DefaultParams, stats)
	idx := index.NewInvertedIndex(scorer)

	// 添加测试文档
	docs := []*index.Document{
		{ID: 1, Content: "hello world", Fields: map[string]interface{}{"text": "hello world"}},
		{ID: 2, Content: "helo world", Fields: map[string]interface{}{"text": "helo world"}}, // 编辑距离1
		{ID: 3, Content: "help world", Fields: map[string]interface{}{"text": "help world"}}, // 编辑距离2
		{ID: 4, Content: "hell world", Fields: map[string]interface{}{"text": "hell world"}}, // 编辑距离1
		{ID: 5, Content: "helicopter", Fields: map[string]interface{}{"text": "helicopter"}}, // 编辑距离4 (太远)
	}

	// 索引文档 (只需要添加到 docStore)
	for _, doc := range docs {
		idx.GetDocStore()[doc.ID] = doc
		stats.TotalDocs++
	}

	// 测试模糊查询
	t.Run("FuzzyDistance1", func(t *testing.T) {
		query := NewFuzzyQuery("text", "hello", 1)
		results := query.Execute(idx)

		fmt.Printf("Fuzzy query (distance=1) results: %d\n", len(results))
		for _, r := range results {
			fmt.Printf("  Doc %d: %v (score=%.4f)\n", r.DocID, r.Doc.Fields["text"], r.Score)
		}

		if len(results) < 2 {
			t.Errorf("Expected at least 2 results, got %d", len(results))
		}
	})

	// 测试更大的编辑距离
	t.Run("FuzzyDistance2", func(t *testing.T) {
		query := NewFuzzyQuery("text", "hello", 2)
		results := query.Execute(idx)

		fmt.Printf("Fuzzy query (distance=2) results: %d\n", len(results))
		for _, r := range results {
			fmt.Printf("  Doc %d: %v (score=%.4f)\n", r.DocID, r.Doc.Fields["text"], r.Score)
		}

		if len(results) < 3 {
			t.Errorf("Expected at least 3 results, got %d", len(results))
		}
	})
}

// TestRegexQuery 测试正则查询
func TestRegexQuery(t *testing.T) {
	// 创建索引
	stats := bm25.NewCollectionStats()
	scorer := bm25.NewScorer(bm25.DefaultParams, stats)
	idx := index.NewInvertedIndex(scorer)

	// 添加测试文档
	docs := []*index.Document{
		{ID: 1, Content: "test", Fields: map[string]interface{}{"email": "user1@example.com"}},
		{ID: 2, Content: "test", Fields: map[string]interface{}{"email": "user3@sample.com"}},
		{ID: 3, Content: "test", Fields: map[string]interface{}{"email": "user2@example.com"}},
		{ID: 4, Content: "test", Fields: map[string]interface{}{"phone": "123-456-7890"}},
		{ID: 5, Content: "test", Fields: map[string]interface{}{"code": "ABC-123-XYZ"}},
	}

	// 索引文档
	for _, doc := range docs {
		idx.GetDocStore()[doc.ID] = doc
		stats.TotalDocs++
	}

	// 测试邮箱正则
	t.Run("EmailRegex", func(t *testing.T) {
		query := NewRegexQuery("email", `^[a-zA-Z0-9._%+-]+@example\.com$`)

		results := query.Execute(idx)

		fmt.Printf("Email regex query results: %d\n", len(results))
		for _, r := range results {
			fmt.Printf("  Doc %d: %v\n", r.DocID, r.Doc.Fields["email"])
		}

		if len(results) != 2 {
			t.Errorf("Expected 2 results, got %d", len(results))
		}
	})

	// 测试通配符查询
	t.Run("WildcardQuery", func(t *testing.T) {
		// 测试 * 通配符
		query := NewWildcardQuery("email", "*@*.com")

		results := query.Execute(idx)

		fmt.Printf("Wildcard query results: %d\n", len(results))
		for _, r := range results {
			fmt.Printf("  Doc %d: %v\n", r.DocID, r.Doc.Fields["email"])
		}

		if len(results) != 3 {
			t.Errorf("Expected 2 results, got %d", len(results))
		}
	})

	// 测试复杂正则
	t.Run("ComplexRegex", func(t *testing.T) {
		// 匹配格式：XXX-XXX-XXXX
		query := NewRegexQuery("code", `^[A-Z]{3}-\d{3}-[A-Z]{3}$`)

		results := query.Execute(idx)

		fmt.Printf("Complex regex query results: %d\n", len(results))
		for _, r := range results {
			fmt.Printf("  Doc %d: %v\n", r.DocID, r.Doc.Fields["code"])
		}

		if len(results) != 1 {
			t.Errorf("Expected 1 result, got %d", len(results))
		}
	})
}

// TestCombinedQueries 测试组合查询
func TestCombinedQueries(t *testing.T) {
	// 创建索引
	stats := bm25.NewCollectionStats()
	scorer := bm25.NewScorer(bm25.DefaultParams, stats)
	idx := index.NewInvertedIndex(scorer)

	// 添加测试文档
	docs := []*index.Document{
		{ID: 1, Content: "test", Fields: map[string]interface{}{"name": "apple", "price": 10, "category": "fruit"}},
		{ID: 2, Content: "test", Fields: map[string]interface{}{"name": "apricot", "price": 15, "category": "fruit"}},
		{ID: 3, Content: "test", Fields: map[string]interface{}{"name": "banana", "price": 20, "category": "fruit"}},
		{ID: 4, Content: "test", Fields: map[string]interface{}{"name": "apple", "price": 25, "category": "premium"}},
	}

	// 索引文档
	for _, doc := range docs {
		idx.GetDocStore()[doc.ID] = doc
		stats.TotalDocs++
	}

	// 测试组合：范围 + 模糊
	t.Run("RangeAndFuzzy", func(t *testing.T) {
		// 价格范围 10-20
		rangeQuery := NewRangeQuery("price", 10, 20, true, true)

		// 名称模糊匹配 "appel" (拼写错误)
		// "appel" 到 "apple" 的编辑距离是 2 (交换)，所以需要 distance=2
		fuzzyQuery := NewFuzzyQuery("name", "appel", 2)

		// 布尔查询
		boolQuery := NewBooleanQuery()
		boolQuery.AddMust(rangeQuery)
		boolQuery.AddMust(fuzzyQuery)

		results := boolQuery.Execute(idx)

		fmt.Printf("Range + Fuzzy query results: %d\n", len(results))
		for _, r := range results {
			fmt.Printf("  Doc %d: name=%v, price=%v\n", r.DocID, r.Doc.Fields["name"], r.Doc.Fields["price"])
		}

		// 只有 Doc 1 满足条件: name="apple" (匹配 "appel" 的模糊查询) 且 price=10 (在范围内)
		if len(results) != 1 {
			t.Errorf("Expected 1 result, got %d", len(results))
		}
	})
}
