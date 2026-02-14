package index

import (
	"sort"
	"sync"

	"github.com/kasuganosora/sqlexec/pkg/fulltext/analyzer"
	"github.com/kasuganosora/sqlexec/pkg/fulltext/bm25"
	"github.com/kasuganosora/sqlexec/pkg/utils"
)

// Document 文档（本地定义避免导入循环）
type Document struct {
	ID      int64
	Content string
	Fields  map[string]interface{}
}

// SearchResult 搜索结果（本地定义）
type SearchResult struct {
	DocID int64
	Score float64
	Doc   *Document
}

// AnalyzerToken 分词结果（导出供query包使用）
type AnalyzerToken = analyzer.Token

// InvertedIndex 倒排索引
type InvertedIndex struct {
	postings   map[int64]*PostingsList // termID -> PostingsList
	docStore   map[int64]*Document
	docVectors map[int64]*bm25.SparseVector // docID -> 文档向量
	stats      *bm25.CollectionStats
	scorer     *bm25.Scorer
	mu         sync.RWMutex
}

// NewInvertedIndex 创建倒排索引
func NewInvertedIndex(scorer *bm25.Scorer) *InvertedIndex {
	return &InvertedIndex{
		postings:   make(map[int64]*PostingsList),
		docStore:   make(map[int64]*Document),
		docVectors: make(map[int64]*bm25.SparseVector),
		stats:      bm25.NewCollectionStats(),
		scorer:     scorer,
	}
}

// AddDocument 添加文档
func (idx *InvertedIndex) AddDocument(doc *Document, tokens []analyzer.Token) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	
	// 存储文档
	idx.docStore[doc.ID] = doc
	
	// 统计词频
	termFreqs := make(map[int64]int)
	termPositions := make(map[int64][]int)
	
	for _, token := range tokens {
		// 这里假设token.Text已经转换为termID
		// 实际实现需要词汇表管理
		termID := hashString(token.Text) // 简化处理，实际应该用词汇表
		termFreqs[termID]++
		termPositions[termID] = append(termPositions[termID], token.Position)
	}
	
	// 计算文档向量
	vector := idx.scorer.ComputeDocumentVector(termFreqs, len(tokens))
	idx.docVectors[doc.ID] = vector
	
	// 更新倒排索引
	for termID, freq := range termFreqs {
		postingsList, exists := idx.postings[termID]
		if !exists {
			postingsList = NewPostingsList(termID)
			idx.postings[termID] = postingsList
		}
		
		// 计算BM25分数
		score := idx.scorer.Score(termID, freq, len(tokens))
		
		posting := Posting{
			DocID:     doc.ID,
			Frequency: freq,
			Positions: termPositions[termID],
			BM25Score: score,
		}
		
		postingsList.AddPosting(posting)
		idx.stats.IncrementDocFreq(termID)
	}
	
	// 更新统计信息
	idx.stats.TotalDocs++
	idx.stats.TotalDocLength += int64(len(tokens))
	idx.stats.UpdateAvgDocLength()
	
	return nil
}

// RemoveDocument 从倒排索引中移除文档
func (idx *InvertedIndex) RemoveDocument(docID int64) bool {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	doc, exists := idx.docStore[docID]
	if !exists {
		return false
	}

	// Remove from all postings lists
	for _, pl := range idx.postings {
		pl.RemovePosting(docID)
	}

	// Remove document vector
	delete(idx.docVectors, docID)

	// Remove from doc store
	delete(idx.docStore, docID)

	// Update stats
	idx.stats.TotalDocs--
	// Approximate doc length reduction
	_ = doc
	if idx.stats.TotalDocs > 0 {
		idx.stats.UpdateAvgDocLength()
	}

	return true
}

// Search 基础搜索（合并所有查询词的倒排列表）
func (idx *InvertedIndex) Search(queryVector *bm25.SparseVector) []SearchResult {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	
	if queryVector.IsEmpty() {
		return nil
	}
	
	// 收集候选文档分数
	scores := make(map[int64]float64)
	
	for termID, queryWeight := range queryVector.Terms {
		if postingsList, exists := idx.postings[termID]; exists {
			for _, posting := range postingsList.Postings {
				scores[posting.DocID] += queryWeight * posting.BM25Score
			}
		}
	}
	
	// 转换为结果列表
	results := make([]SearchResult, 0, len(scores))
	for docID, score := range scores {
		results = append(results, SearchResult{
			DocID: docID,
			Score: score,
			Doc:   idx.docStore[docID],
		})
	}
	
	// 按分数降序排序
	sort.Slice(results, func(i, j int) bool {
		return results[i].Score > results[j].Score
	})
	
	return results
}

// SearchTopK 搜索Top-K结果（使用MAXSCORE优化）
func (idx *InvertedIndex) SearchTopK(queryVector *bm25.SparseVector, topK int) []SearchResult {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	
	if queryVector.IsEmpty() || topK <= 0 {
		return nil
	}
	
	// 获取查询词列表，按MaxScore降序排序
	type termInfo struct {
		termID      int64
		queryWeight float64
		maxScore    float64
		postings    *PostingsList
	}
	
	var terms []termInfo
	for termID, queryWeight := range queryVector.Terms {
		if postingsList, exists := idx.postings[termID]; exists {
			terms = append(terms, termInfo{
				termID:      termID,
				queryWeight: queryWeight,
				maxScore:    postingsList.MaxScore,
				postings:    postingsList,
			})
		}
	}
	
	if len(terms) == 0 {
		return nil
	}
	
	// 按maxScore降序排序
	sort.Slice(terms, func(i, j int) bool {
		return terms[i].maxScore > terms[j].maxScore
	})
	
	// 使用最小堆维护Top-K
	heap := newMinHeap(topK)
	
	// DAAT (Document-At-A-Time) 算法
	// 只遍历第一个词的倒排列表（分数最高的词）
	firstTerm := terms[0]
	
	for _, posting := range firstTerm.postings.Postings {
		docID := posting.DocID
		score := firstTerm.queryWeight * posting.BM25Score
		
		// 累加其他词的分数
		for i := 1; i < len(terms); i++ {
			if p := terms[i].postings.FindPosting(docID); p != nil {
				score += terms[i].queryWeight * p.BM25Score
			}
		}
		
		// 更新堆
		heap.tryAdd(docID, score)
	}
	
	// 转换为结果
	results := heap.toResults(idx)
	
	return results
}

// SearchPhrase 短语搜索
func (idx *InvertedIndex) SearchPhrase(phraseTokens []analyzer.Token, slop int) []SearchResult {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	
	if len(phraseTokens) == 0 {
		return nil
	}
	
	// 获取第一个词的倒排列表
	firstTermID := hashString(phraseTokens[0].Text)
	firstList, exists := idx.postings[firstTermID]
	if !exists {
		return nil
	}
	
	var results []SearchResult
	
	// 对每个候选文档检查短语匹配
	for _, posting := range firstList.Postings {
		if idx.matchPhrase(posting.DocID, phraseTokens, slop) {
			results = append(results, SearchResult{
				DocID: posting.DocID,
				Score: posting.BM25Score,
				Doc:   idx.docStore[posting.DocID],
			})
		}
	}
	
	return results
}

// matchPhrase 检查文档是否包含短语
func (idx *InvertedIndex) matchPhrase(docID int64, tokens []analyzer.Token, slop int) bool {
	if len(tokens) == 0 {
		return false
	}
	
	// 获取所有词的倒排列表
	var positions [][]int
	for _, token := range tokens {
		termID := hashString(token.Text)
		postingsList, exists := idx.postings[termID]
		if !exists {
			return false
		}
		
		posting := postingsList.FindPosting(docID)
		if posting == nil {
			return false
		}
		
		positions = append(positions, posting.Positions)
	}
	
	// 检查位置匹配（考虑slop）
	return checkPositionsWithSlop(positions, slop)
}

// checkPositionsWithSlop 检查位置是否满足slop约束
func checkPositionsWithSlop(positions [][]int, slop int) bool {
	if len(positions) < 2 {
		return true
	}
	
	// 简化实现：检查第一个词的每个位置
	for _, pos1 := range positions[0] {
		if matchRemainingPositions(positions, 1, pos1, slop) {
			return true
		}
	}
	
	return false
}

// matchRemainingPositions 递归匹配剩余位置
func matchRemainingPositions(positions [][]int, idx int, lastPos int, slop int) bool {
	if idx >= len(positions) {
		return true
	}
	
	expectedPos := lastPos + 1
	for _, pos := range positions[idx] {
		if utils.AbsInt(pos-expectedPos) <= slop {
			if matchRemainingPositions(positions, idx+1, pos, slop) {
				return true
			}
		}
	}
	
	return false
}

// GetDocument 获取文档
func (idx *InvertedIndex) GetDocument(docID int64) *Document {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.docStore[docID]
}

// GetStats 获取统计信息
func (idx *InvertedIndex) GetStats() *bm25.CollectionStats {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.stats
}

// GetAllDocIDs 获取所有文档ID
func (idx *InvertedIndex) GetAllDocIDs() []int64 {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	
	docIDs := make([]int64, 0, len(idx.docStore))
	for docID := range idx.docStore {
		docIDs = append(docIDs, docID)
	}
	
	return docIDs
}

// GetDocVector 获取文档向量
func (idx *InvertedIndex) GetDocVector(docID int64) *bm25.SparseVector {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	
	return idx.docVectors[docID]
}

// GetDocStore 获取文档存储（用于测试）
func (idx *InvertedIndex) GetDocStore() map[int64]*Document {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.docStore
}

// UpdateDocFreq 更新文档频率
func (idx *InvertedIndex) UpdateDocFreq(termID int64) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	
	idx.stats.IncrementDocFreq(termID)
}

// UpdateVocabulary 更新词汇表（仅用于混合搜索的词汇表管理）
func (idx *InvertedIndex) UpdateVocabulary(termID int64) {
	// 这个方法在当前的架构中由外部词汇表管理
	// 这里保留为接口
}

// 辅助函数
func hashString(s string) int64 {
	// 简化实现，实际应该使用词汇表管理
	h := int64(0)
	for _, c := range s {
		h = h*31 + int64(c)
	}
	return h
}

// minHeap 最小堆（用于维护Top-K）
type minHeap struct {
	items   []heapItem
	maxSize int
}

type heapItem struct {
	docID int64
	score float64
}

func newMinHeap(maxSize int) *minHeap {
	return &minHeap{
		items:   make([]heapItem, 0, maxSize),
		maxSize: maxSize,
	}
}

func (h *minHeap) tryAdd(docID int64, score float64) {
	if len(h.items) < h.maxSize {
		h.items = append(h.items, heapItem{docID: docID, score: score})
		h.up(len(h.items) - 1)
	} else if score > h.items[0].score {
		h.items[0] = heapItem{docID: docID, score: score}
		h.down(0)
	}
}

func (h *minHeap) minScore() float64 {
	if len(h.items) == 0 {
		return 0
	}
	return h.items[0].score
}

func (h *minHeap) toResults(idx *InvertedIndex) []SearchResult {
	results := make([]SearchResult, len(h.items))
	for i, item := range h.items {
		results[i] = SearchResult{
			DocID: item.docID,
			Score: item.score,
			Doc:   idx.docStore[item.docID],
		}
	}
	
	// 按分数降序排序
	sort.Slice(results, func(i, j int) bool {
		return results[i].Score > results[j].Score
	})
	
	return results
}

func (h *minHeap) up(i int) {
	for i > 0 {
		parent := (i - 1) / 2
		if h.items[parent].score <= h.items[i].score {
			break
		}
		h.items[parent], h.items[i] = h.items[i], h.items[parent]
		i = parent
	}
}

func (h *minHeap) down(i int) {
	n := len(h.items)
	for {
		minIdx := i
		left := 2*i + 1
		right := 2*i + 2
		
		if left < n && h.items[left].score < h.items[minIdx].score {
			minIdx = left
		}
		if right < n && h.items[right].score < h.items[minIdx].score {
			minIdx = right
		}
		
		if minIdx == i {
			break
		}
		
		h.items[i], h.items[minIdx] = h.items[minIdx], h.items[i]
		i = minIdx
	}
}
