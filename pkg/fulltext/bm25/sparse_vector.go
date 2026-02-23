package bm25

import (
	"math"
	"sort"
)

// SparseVector 稀疏向量（termID -> weight）
type SparseVector struct {
	Terms map[int64]float64 // termID -> weight
	Norm  float64           // 向量范数
}

// NewSparseVector 创建稀疏向量
func NewSparseVector() *SparseVector {
	return &SparseVector{
		Terms: make(map[int64]float64),
	}
}

// Set 设置权重
func (v *SparseVector) Set(termID int64, weight float64) {
	v.Terms[termID] = weight
	v.Norm = 0 // 需要重新计算
}

// Get 获取权重
func (v *SparseVector) Get(termID int64) (float64, bool) {
	weight, ok := v.Terms[termID]
	return weight, ok
}

// CalculateNorm 计算向量范数
func (v *SparseVector) CalculateNorm() float64 {
	if v.Norm > 0 {
		return v.Norm
	}

	var sum float64
	for _, weight := range v.Terms {
		sum += weight * weight
	}
	v.Norm = math.Sqrt(sum)
	return v.Norm
}

// Normalize 归一化
func (v *SparseVector) Normalize() {
	norm := v.CalculateNorm()
	if norm == 0 {
		return
	}

	for termID := range v.Terms {
		v.Terms[termID] /= norm
	}
	v.Norm = 1.0
}

// DotProduct 计算点积
func (v *SparseVector) DotProduct(other *SparseVector) float64 {
	var result float64

	// 遍历较小的向量以提高效率
	if len(v.Terms) > len(other.Terms) {
		v, other = other, v
	}

	for termID, weight := range v.Terms {
		if otherWeight, ok := other.Terms[termID]; ok {
			result += weight * otherWeight
		}
	}

	return result
}

// CosineSimilarity 计算余弦相似度
func (v *SparseVector) CosineSimilarity(other *SparseVector) float64 {
	norm1 := v.CalculateNorm()
	norm2 := other.CalculateNorm()

	if norm1 == 0 || norm2 == 0 {
		return 0
	}

	return v.DotProduct(other) / (norm1 * norm2)
}

// Multiply 标量乘法
func (v *SparseVector) Multiply(scalar float64) *SparseVector {
	result := NewSparseVector()
	for termID, weight := range v.Terms {
		result.Set(termID, weight*scalar)
	}
	return result
}

// Add 向量加法
func (v *SparseVector) Add(other *SparseVector) *SparseVector {
	result := NewSparseVector()

	// 复制当前向量
	for termID, weight := range v.Terms {
		result.Set(termID, weight)
	}

	// 添加另一个向量
	for termID, weight := range other.Terms {
		if existing, ok := result.Terms[termID]; ok {
			result.Set(termID, existing+weight)
		} else {
			result.Set(termID, weight)
		}
	}

	return result
}

// GetSortedTerms 获取按权重排序的词项列表
func (v *SparseVector) GetSortedTerms() []TermWeight {
	terms := make([]TermWeight, 0, len(v.Terms))
	for termID, weight := range v.Terms {
		terms = append(terms, TermWeight{
			TermID: termID,
			Weight: weight,
		})
	}

	sort.Slice(terms, func(i, j int) bool {
		return terms[i].Weight > terms[j].Weight
	})

	return terms
}

// TermWeight 词项权重
type TermWeight struct {
	TermID int64
	Weight float64
}

// GetTopK 获取Top-K词项
func (v *SparseVector) GetTopK(k int) []TermWeight {
	terms := v.GetSortedTerms()
	if len(terms) <= k {
		return terms
	}
	return terms[:k]
}

// Clone 克隆向量
func (v *SparseVector) Clone() *SparseVector {
	result := NewSparseVector()
	for termID, weight := range v.Terms {
		result.Set(termID, weight)
	}
	result.Norm = v.Norm
	return result
}

// IsEmpty 检查是否为空
func (v *SparseVector) IsEmpty() bool {
	return len(v.Terms) == 0
}

// Size 返回词项数量
func (v *SparseVector) Size() int {
	return len(v.Terms)
}
