package fulltext

import (
	"math"
	"sort"
	"sync"
)

// HybridResult 混合搜索结果
type HybridResult struct {
	DocID       int64
	FTScore     float64 // 全文分数
	VecScore    float64 // 向量分数
	HybridScore float64 // 混合分数
	Doc         *Document
}

// HybridEngine 混合搜索引擎
type HybridEngine struct {
	ftEngine       *Engine
	ftWeight       float64
	vecWeight      float64
	k              int // RRF参数
	enableRRF      bool
	enableWeighted bool
	mu             sync.RWMutex
}

// NewHybridEngine 创建混合搜索引擎
func NewHybridEngine(ftEngine *Engine, ftWeight, vecWeight float64) *HybridEngine {
	if ftWeight <= 0 {
		ftWeight = 0.7
	}
	if vecWeight <= 0 {
		vecWeight = 0.3
	}

	return &HybridEngine{
		ftEngine:       ftEngine,
		ftWeight:       ftWeight,
		vecWeight:      vecWeight,
		k:              60, // RRF默认参数
		enableRRF:      false,
		enableWeighted: false,
	}
}

// SetRRF 启用RRF融合
func (h *HybridEngine) SetRRF(k int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.k = k
	h.enableRRF = true
	h.enableWeighted = false
}

// SetWeightedFusion 启用加权融合
func (h *HybridEngine) SetWeightedFusion(ftWeight, vecWeight float64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.ftWeight = ftWeight
	h.vecWeight = vecWeight
	h.enableRRF = false
	h.enableWeighted = true
}

// SearchHybrid 混合搜索
func (h *HybridEngine) SearchHybrid(query string, topK int) ([]HybridResult, error) {
	// 执行全文搜索
	ftResults, err := h.ftEngine.SearchBM25(query, topK)
	if err != nil {
		return nil, err
	}

	// 执行向量搜索（模拟）
	vecResults := h.vectorSearch(query, topK)

	// 融合结果
	if h.enableRRF {
		return h.RRF(ftResults, vecResults, h.k), nil
	}

	return h.WeightedFusion(ftResults, vecResults), nil
}

// vectorSearch 向量搜索（模拟实现）
// 实际应该调用真实的向量索引
func (h *HybridEngine) vectorSearch(query string, topK int) []SearchResult {
	// 将查询文本转换为向量（这里使用简单的词向量平均）
	// 实际应该使用embedding模型

	tokens, err := h.ftEngine.tokenizer.Tokenize(query)
	if err != nil {
		return nil
	}

	// 模拟向量搜索结果
	// 在实际实现中，这里应该调用向量索引的Search方法
	var results []SearchResult

	// 获取所有文档ID
	docIDs := h.ftEngine.invertedIdx.GetAllDocIDs()

	for _, docID := range docIDs {
		// 模拟计算文档与查询的相似度
		similarity := h.calculateSimilarity(docID, tokens)

		if doc := h.ftEngine.GetDocument(docID); doc != nil {
			results = append(results, SearchResult{
				DocID: docID,
				Score: similarity,
				Doc:   doc,
			})
		}
	}

	// 按分数排序
	sort.Slice(results, func(i, j int) bool {
		return results[i].Score > results[j].Score
	})

	// 限制结果数量
	if topK > 0 && len(results) > topK {
		results = results[:topK]
	}

	return results
}

// calculateSimilarity 计算文档与查询的相似度（模拟）
func (h *HybridEngine) calculateSimilarity(docID int64, queryTokens []Token) float64 {
	// 实际应该计算向量余弦相似度
	// 这里使用简单的词匹配作为模拟

	doc := h.ftEngine.GetDocument(docID)
	if doc == nil {
		return 0
	}

	// 获取文档的词向量
	docVector := h.ftEngine.invertedIdx.GetDocVector(docID)
	if docVector == nil {
		return 0
	}

	// 构建查询向量（使用hashString保持与索引一致）
	queryVector := make(map[int64]float64)
	for _, token := range queryTokens {
		termID := hashString(token.Text)
		if weight, exists := queryVector[termID]; exists {
			queryVector[termID] = weight + 1.0
		} else {
			queryVector[termID] = 1.0
		}
	}

	// 计算点积（模拟余弦相似度）
	var dotProduct float64
	var queryNorm float64
	var docNorm float64

	// Compute full document vector norm
	for _, dWeight := range docVector.Terms {
		docNorm += dWeight * dWeight
	}

	for termID, qWeight := range queryVector {
		queryNorm += qWeight * qWeight

		if dWeight, exists := docVector.Get(termID); exists {
			dotProduct += qWeight * dWeight
		}
	}

	if queryNorm == 0 || docNorm == 0 {
		return 0
	}

	// 归一化
	cosineSimilarity := dotProduct / (math.Sqrt(queryNorm) * math.Sqrt(docNorm))

	// 将余弦相似度转换为分数（0-1范围）
	return (cosineSimilarity + 1) / 2
}

// RRF RRF融合算法
func (h *HybridEngine) RRF(ftResults, vecResults []SearchResult, k int) []HybridResult {
	scores := make(map[int64]float64)
	ftScoreMap := make(map[int64]float64)
	vecScoreMap := make(map[int64]float64)

	// 合并结果
	for i, result := range ftResults {
		scores[result.DocID] += 1.0 / float64(k+i+1)
		ftScoreMap[result.DocID] = result.Score
	}

	for i, result := range vecResults {
		scores[result.DocID] += 1.0 / float64(k+i+1)
		vecScoreMap[result.DocID] = result.Score
	}

	// 转换为结果列表
	ranked := make([]HybridResult, 0, len(scores))
	for docID, score := range scores {
		ranked = append(ranked, HybridResult{
			DocID:       docID,
			HybridScore: score,
			FTScore:     ftScoreMap[docID],
			VecScore:    vecScoreMap[docID],
			Doc:         h.ftEngine.GetDocument(docID),
		})
	}

	// 按RRF分数排序
	sort.Slice(ranked, func(i, j int) bool {
		return ranked[i].HybridScore > ranked[j].HybridScore
	})

	return ranked
}

// WeightedFusion 加权融合
func (h *HybridEngine) WeightedFusion(ftResults, vecResults []SearchResult) []HybridResult {
	merged := make(map[int64]*HybridResult)

	// 归一化全文分数
	ftMaxScore := 0.0001
	if len(ftResults) > 0 {
		ftMaxScore = ftResults[0].Score
		if ftMaxScore == 0 {
			ftMaxScore = 0.0001
		}
	}

	// 添加全文结果
	for _, result := range ftResults {
		merged[result.DocID] = &HybridResult{
			DocID:   result.DocID,
			FTScore: result.Score / ftMaxScore,
			Doc:     result.Doc,
		}
	}

	// 归一化向量分数
	vecMaxScore := 0.0001
	if len(vecResults) > 0 {
		vecMaxScore = vecResults[0].Score
		if vecMaxScore == 0 {
			vecMaxScore = 0.0001
		}
	}

	// 合并向量结果
	for _, result := range vecResults {
		if r, exists := merged[result.DocID]; exists {
			r.VecScore = result.Score / vecMaxScore
		} else {
			merged[result.DocID] = &HybridResult{
				DocID:    result.DocID,
				FTScore:  0,
				VecScore: result.Score / vecMaxScore,
				Doc:      result.Doc,
			}
		}
	}

	// 计算混合分数
	results := make([]HybridResult, 0, len(merged))
	for _, r := range merged {
		r.HybridScore = r.FTScore*h.ftWeight + r.VecScore*h.vecWeight
		results = append(results, *r)
	}

	// 按混合分数排序
	sort.Slice(results, func(i, j int) bool {
		return results[i].HybridScore > results[j].HybridScore
	})

	return results
}

// SearchBM25AndVector 分别执行BM25和向量搜索
func (h *HybridEngine) SearchBM25AndVector(query string, topK int) (ftResults, vecResults []SearchResult, err error) {
	ftResults, err = h.ftEngine.SearchBM25(query, topK)
	if err != nil {
		return nil, nil, err
	}

	vecResults = h.vectorSearch(query, topK)

	return ftResults, vecResults, nil
}

// AutoConvertToVector 自动将文档转换为稀疏向量（Milvus特性）
func (h *HybridEngine) AutoConvertToVector(doc *Document) (map[int64]float64, error) {
	// 分词
	tokens, err := h.ftEngine.tokenizer.Tokenize(doc.Content)
	if err != nil {
		return nil, err
	}

	// 创建稀疏向量
	sparseVector := make(map[int64]float64)

	// 更新词汇表统计（使用hashString保持与索引一致）
	for _, token := range tokens {
		termID := hashString(token.Text)

		// 更新文档频率
		h.ftEngine.invertedIdx.UpdateDocFreq(termID)
	}

	// 计算BM25权重
	stats := h.ftEngine.GetStats()
	params := h.ftEngine.config.BM25Params

	for _, token := range tokens {
		termID := hashString(token.Text)

		// 计算TF
		tf := 0.0
		for _, t := range tokens {
			if t.Text == token.Text {
				tf++
			}
		}

		// 计算IDF
		df := stats.GetDocFreq(termID)
		if df == 0 {
			df = 1
		}
		idf := math.Log((float64(stats.GetTotalDocs()) - float64(df) + 0.5) / (float64(df) + 0.5))

		// 计算BM25分数
		docLength := float64(len(tokens))
		avgDocLength := stats.GetAvgDocLength()
		if avgDocLength == 0 {
			avgDocLength = 1
		}

		numerator := tf * (params.K1 + 1)
		denominator := tf + params.K1*(1-params.B+params.B*docLength/avgDocLength)

		if denominator > 0 {
			sparseVector[termID] = idf * (numerator / denominator)
		}
	}

	return sparseVector, nil
}

// BatchAutoConvert 批量自动转换
func (h *HybridEngine) BatchAutoConvert(docs []*Document) (map[int64]map[int64]float64, error) {
	results := make(map[int64]map[int64]float64)

	for _, doc := range docs {
		vector, err := h.AutoConvertToVector(doc)
		if err != nil {
			return nil, err
		}
		results[doc.ID] = vector
	}

	return results, nil
}

// OptimizeVectorIndex 优化向量索引
func (h *HybridEngine) OptimizeVectorIndex() error {
	// 实际应该调用向量索引的优化方法
	// 这里仅作为接口定义
	return nil
}

// EstimateOptimalWeights 估计最优权重
func (h *HybridEngine) EstimateOptimalWeights(querySet []string) (float64, float64) {
	// 使用简单的启发式方法估计权重
	// 实际应该使用交叉验证或网格搜索

	totalFTScore := 0.0
	totalVecScore := 0.0
	count := 0

	for _, query := range querySet {
		ftResults, vecResults, err := h.SearchBM25AndVector(query, 10)
		if err != nil {
			continue
		}

		// 计算平均分数
		if len(ftResults) > 0 {
			ftAvg := 0.0
			for _, r := range ftResults {
				ftAvg += r.Score
			}
			ftAvg /= float64(len(ftResults))
			totalFTScore += ftAvg
		}

		if len(vecResults) > 0 {
			vecAvg := 0.0
			for _, r := range vecResults {
				vecAvg += r.Score
			}
			vecAvg /= float64(len(vecResults))
			totalVecScore += vecAvg
		}

		count++
	}

	if count == 0 {
		return 0.7, 0.3
	}

	// 归一化
	ftAvg := totalFTScore / float64(count)
	vecAvg := totalVecScore / float64(count)

	total := ftAvg + vecAvg
	if total == 0 {
		return 0.7, 0.3
	}

	return ftAvg / total, vecAvg / total
}
