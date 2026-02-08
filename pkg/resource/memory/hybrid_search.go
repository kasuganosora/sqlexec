package memory

import (
	"context"
	"sort"
	"sync"

	"github.com/kasuganosora/sqlexec/pkg/fulltext"
)

// HybridResult 混合搜索结果
type HybridResult struct {
	DocID         int64
	BM25Score     float64
	VectorScore   float64
	RRFScore      float64
	WeightedScore float64
	Doc           *fulltext.Document
}

// HybridSearcher 混合搜索器（全文 + 向量）
type HybridSearcher struct {
	fulltextIndex *AdvancedFullTextIndex
	vectorIndex   VectorIndex
	ftWeight      float64
	vecWeight     float64
	rrfK          int
}

// NewHybridSearcher 创建混合搜索器
func NewHybridSearcher(
	ftIndex *AdvancedFullTextIndex,
	vecIndex VectorIndex,
	ftWeight, vecWeight float64,
) *HybridSearcher {
	return &HybridSearcher{
		fulltextIndex: ftIndex,
		vectorIndex:   vecIndex,
		ftWeight:      ftWeight,
		vecWeight:     vecWeight,
		rrfK:          60, // RRF默认参数
	}
}

// SetRRFK 设置RRF参数
func (hs *HybridSearcher) SetRRFK(k int) {
	hs.rrfK = k
}

// Search 执行混合搜索
func (hs *HybridSearcher) Search(
	textQuery string,
	vectorQuery []float32,
	topK int,
) ([]HybridResult, error) {
	// 并行执行全文搜索和向量搜索
	var ftResults []fulltext.SearchResult
	var vecResult *VectorSearchResult
	var ftErr, vecErr error
	
	var wg sync.WaitGroup
	wg.Add(2)
	
	// 全文搜索
	go func() {
		defer wg.Done()
		ftResults, ftErr = hs.fulltextIndex.SearchBM25(textQuery, topK*2)
	}()
	
	// 向量搜索
	go func() {
		defer wg.Done()
		ctx := context.Background()
		vecResult, vecErr = hs.vectorIndex.Search(ctx, vectorQuery, topK*2, nil)
	}()
	
	wg.Wait()
	
	if ftErr != nil {
		return nil, ftErr
	}
	if vecErr != nil {
		return nil, vecErr
	}
	
	// 使用RRF融合结果
	return hs.fuseWithRRF(ftResults, vecResult, topK)
}

// SearchWithWeightedFusion 使用加权融合搜索
func (hs *HybridSearcher) SearchWithWeightedFusion(
	textQuery string,
	vectorQuery []float32,
	topK int,
) ([]HybridResult, error) {
	// 并行执行搜索
	var ftResults []fulltext.SearchResult
	var vecResult *VectorSearchResult
	var ftErr, vecErr error
	
	var wg sync.WaitGroup
	wg.Add(2)
	
	go func() {
		defer wg.Done()
		ftResults, ftErr = hs.fulltextIndex.SearchBM25(textQuery, topK*2)
	}()
	
	go func() {
		defer wg.Done()
		ctx := context.Background()
		vecResult, vecErr = hs.vectorIndex.Search(ctx, vectorQuery, topK*2, nil)
	}()
	
	wg.Wait()
	
	if ftErr != nil {
		return nil, ftErr
	}
	if vecErr != nil {
		return nil, vecErr
	}
	
	return hs.fuseWithWeighted(ftResults, vecResult, topK)
}

// fuseWithRRF 使用RRF (Reciprocal Rank Fusion) 融合结果
func (hs *HybridSearcher) fuseWithRRF(
	ftResults []fulltext.SearchResult,
	vecResult *VectorSearchResult,
	topK int,
) ([]HybridResult, error) {
	scores := make(map[int64]*HybridResult)
	k := float64(hs.rrfK)
	
	// 融合全文搜索结果
	for rank, result := range ftResults {
		docID := result.DocID
		if _, exists := scores[docID]; !exists {
			scores[docID] = &HybridResult{
				DocID:     docID,
				BM25Score: result.Score,
				Doc:       result.Doc,
			}
		}
		scores[docID].RRFScore += 1.0 / (k + float64(rank+1))
	}
	
	// 融合向量搜索结果
	if vecResult != nil {
		for rank, docID := range vecResult.IDs {
			if _, exists := scores[docID]; !exists {
				scores[docID] = &HybridResult{
					DocID: docID,
				}
			}
			// 向量距离需要反转（越小越好 -> 越大越好）
			if rank < len(vecResult.Distances) {
				scores[docID].VectorScore = float64(vecResult.Distances[rank])
			}
			scores[docID].RRFScore += 1.0 / (k + float64(rank+1))
		}
	}
	
	// 转换为结果列表
	results := make([]HybridResult, 0, len(scores))
	for _, result := range scores {
		results = append(results, *result)
	}
	
	// 按RRF分数降序排序
	sort.Slice(results, func(i, j int) bool {
		return results[i].RRFScore > results[j].RRFScore
	})
	
	// 限制结果数量
	if len(results) > topK {
		results = results[:topK]
	}
	
	return results, nil
}

// fuseWithWeighted 使用加权融合结果
func (hs *HybridSearcher) fuseWithWeighted(
	ftResults []fulltext.SearchResult,
	vecResult *VectorSearchResult,
	topK int,
) ([]HybridResult, error) {
	scores := make(map[int64]*HybridResult)
	
	// 归一化分数
	var ftMaxScore, vecMaxScore float64
	
	if len(ftResults) > 0 {
		ftMaxScore = ftResults[0].Score
	}
	if vecResult != nil && len(vecResult.Distances) > 0 {
		vecMaxScore = float64(vecResult.Distances[0])
	}
	
	// 融合全文搜索结果
	for _, result := range ftResults {
		docID := result.DocID
		normalizedScore := 0.0
		if ftMaxScore > 0 {
			normalizedScore = result.Score / ftMaxScore
		}
		
		if _, exists := scores[docID]; !exists {
			scores[docID] = &HybridResult{
				DocID:     docID,
				BM25Score: result.Score,
				Doc:       result.Doc,
			}
		}
		scores[docID].BM25Score = normalizedScore
	}
	
	// 融合向量搜索结果
	if vecResult != nil {
		for i, docID := range vecResult.IDs {
			normalizedScore := 0.0
			if vecMaxScore > 0 && i < len(vecResult.Distances) {
				// 向量距离需要反转（越小越好 -> 越大越好）
				normalizedScore = 1.0 - (float64(vecResult.Distances[i]) / vecMaxScore)
			}
			
			if _, exists := scores[docID]; !exists {
				scores[docID] = &HybridResult{
					DocID: docID,
				}
			}
			scores[docID].VectorScore = normalizedScore
		}
	}
	
	// 计算加权分数
	results := make([]HybridResult, 0, len(scores))
	for _, result := range scores {
		result.WeightedScore = result.BM25Score*hs.ftWeight + result.VectorScore*hs.vecWeight
		results = append(results, *result)
	}
	
	// 按加权分数降序排序
	sort.Slice(results, func(i, j int) bool {
		return results[i].WeightedScore > results[j].WeightedScore
	})
	
	// 限制结果数量
	if len(results) > topK {
		results = results[:topK]
	}
	
	return results, nil
}

// HybridSearchConfig 混合搜索配置
type HybridSearchConfig struct {
	FulltextWeight float64
	VectorWeight   float64
	RRFK           int
}

// DefaultHybridSearchConfig 默认混合搜索配置
var DefaultHybridSearchConfig = &HybridSearchConfig{
	FulltextWeight: 0.5,
	VectorWeight:   0.5,
	RRFK:           60,
}
