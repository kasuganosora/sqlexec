package bm25

import (
	"math"
	"sync"
)

// CollectionStats 集合统计信息
type CollectionStats struct {
	TotalDocs      int64
	TotalDocLength int64
	AvgDocLength   float64
	DocFreqs       map[int64]int64 // termID -> 包含该词的文档数 (DF)
	mu             sync.RWMutex
}

// NewCollectionStats 创建集合统计
func NewCollectionStats() *CollectionStats {
	return &CollectionStats{
		DocFreqs: make(map[int64]int64),
	}
}

// UpdateAvgDocLength 更新平均文档长度
// Caller must hold s.mu write lock or ensure exclusive access.
func (s *CollectionStats) UpdateAvgDocLength() {
	if s.TotalDocs > 0 {
		s.AvgDocLength = float64(s.TotalDocLength) / float64(s.TotalDocs)
	}
}

// GetTotalDocs 获取文档总数（线程安全）
func (s *CollectionStats) GetTotalDocs() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.TotalDocs
}

// GetAvgDocLength 获取平均文档长度（线程安全）
func (s *CollectionStats) GetAvgDocLength() float64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.AvgDocLength
}

// AddDocStats 记录新增文档的统计信息（线程安全）
func (s *CollectionStats) AddDocStats(docLength int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.TotalDocs++
	s.TotalDocLength += docLength
	s.UpdateAvgDocLength()
}

// RemoveDocStats 记录移除文档的统计信息（线程安全）
func (s *CollectionStats) RemoveDocStats(docLength int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.TotalDocs--
	s.TotalDocLength -= docLength
	if s.TotalDocLength < 0 {
		s.TotalDocLength = 0
	}
	if s.TotalDocs > 0 {
		s.UpdateAvgDocLength()
	} else {
		s.AvgDocLength = 0
	}
}

// GetDocFreq 获取文档频率
func (s *CollectionStats) GetDocFreq(termID int64) int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.DocFreqs[termID]
}

// IncrementDocFreq 增加文档频率
func (s *CollectionStats) IncrementDocFreq(termID int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.DocFreqs[termID]++
}

// Params BM25参数
type Params struct {
	K1 float64 // 词频饱和参数 (1.2-2.0)
	B  float64 // 长度归一化参数 (0-1)
}

// DefaultParams 默认BM25参数
var DefaultParams = Params{
	K1: 1.2,
	B:  0.75,
}

// Scorer BM25评分器
type Scorer struct {
	params Params
	stats  *CollectionStats
}

// NewScorer 创建BM25评分器
func NewScorer(params Params, stats *CollectionStats) *Scorer {
	return &Scorer{
		params: params,
		stats:  stats,
	}
}

// CalculateIDF 计算逆文档频率
func (s *Scorer) CalculateIDF(termID int64) float64 {
	df := s.stats.GetDocFreq(termID)
	N := float64(s.stats.GetTotalDocs())

	if df == 0 || N == 0 {
		return 0
	}

	// IDF = log((N - df + 0.5) / (df + 0.5))
	return math.Log((N - float64(df) + 0.5) / (float64(df) + 0.5))
}

// CalculateTF 计算词频分数
func (s *Scorer) CalculateTF(freq int, docLength int) float64 {
	k1, b := s.params.K1, s.params.B
	avgdl := s.stats.GetAvgDocLength()

	if avgdl == 0 {
		avgdl = 1
	}

	// TF = (f * (k1 + 1)) / (f + k1 * (1 - b + b * |D| / avgdl))
	numerator := float64(freq) * (k1 + 1)
	denominator := float64(freq) + k1*(1-b+b*float64(docLength)/avgdl)

	return numerator / denominator
}

// Score 计算单个词的BM25分数
func (s *Scorer) Score(termID int64, freq int, docLength int) float64 {
	idf := s.CalculateIDF(termID)
	tf := s.CalculateTF(freq, docLength)
	return idf * tf
}

// Document 文档（用于评分计算）
type Document struct {
	ID         int64
	TermFreqs  map[int64]int // termID -> frequency
	Length     int
	Vector     *SparseVector
}

// ComputeDocumentVector 计算文档的BM25稀疏向量
func (s *Scorer) ComputeDocumentVector(termFreqs map[int64]int, docLength int) *SparseVector {
	vector := NewSparseVector()
	
	for termID, freq := range termFreqs {
		score := s.Score(termID, freq, docLength)
		if score > 0 {
			vector.Set(termID, score)
		}
	}
	
	return vector
}

// QueryScorer 查询评分器
type QueryScorer struct {
	scorer       *Scorer
	queryVector  *SparseVector
}

// NewQueryScorer 创建查询评分器
func NewQueryScorer(scorer *Scorer, queryTerms map[int64]float64) *QueryScorer {
	vector := NewSparseVector()
	for termID, weight := range queryTerms {
		vector.Set(termID, weight)
	}
	
	return &QueryScorer{
		scorer:      scorer,
		queryVector: vector,
	}
}

// Score 计算文档与查询的相关性分数
func (qs *QueryScorer) Score(docVector *SparseVector) float64 {
	return qs.queryVector.DotProduct(docVector)
}

// ScoreWithDetails 计算分数并返回详细信息
func (qs *QueryScorer) ScoreWithDetails(docVector *SparseVector) (float64, map[int64]float64) {
	var totalScore float64
	details := make(map[int64]float64)
	
	for termID, queryWeight := range qs.queryVector.Terms {
		if docWeight, ok := docVector.Terms[termID]; ok {
			score := queryWeight * docWeight
			totalScore += score
			details[termID] = score
		}
	}
	
	return totalScore, details
}
