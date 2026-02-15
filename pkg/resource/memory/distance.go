package memory

import (
	"fmt"
	"math"
	"sync"
)

// DistanceFunc 距离函数接口
type DistanceFunc interface {
	Name() string
	Compute(v1, v2 []float32) float32
}

// distanceRegistry 距离函数注册中心
var distanceRegistry = struct {
	mu    sync.RWMutex
	funcs map[string]DistanceFunc
}{funcs: make(map[string]DistanceFunc)}

// RegisterDistance 注册距离函数
func RegisterDistance(name string, fn DistanceFunc) {
	distanceRegistry.mu.Lock()
	defer distanceRegistry.mu.Unlock()
	distanceRegistry.funcs[name] = fn
}

// GetDistance 获取距离函数
func GetDistance(name string) (DistanceFunc, error) {
	distanceRegistry.mu.RLock()
	defer distanceRegistry.mu.RUnlock()
	fn, ok := distanceRegistry.funcs[name]
	if !ok {
		return nil, fmt.Errorf("unknown distance function: %s", name)
	}
	return fn, nil
}

// MustGetDistance 获取距离函数（panic if not found）
func MustGetDistance(name string) DistanceFunc {
	fn, err := GetDistance(name)
	if err != nil {
		panic(err)
	}
	return fn
}

// CosineDistance 计算余弦距离
func CosineDistance(v1, v2 []float32) float32 {
	return MustGetDistance("cosine").Compute(v1, v2)
}

// L2Distance 计算L2距离
func L2Distance(v1, v2 []float32) float32 {
	return MustGetDistance("l2").Compute(v1, v2)
}

// InnerProductDistance 计算内积距离
func InnerProductDistance(v1, v2 []float32) float32 {
	return MustGetDistance("inner_product").Compute(v1, v2)
}

// ==================== 具体实现 ====================

// cosineDistance 余弦距离实现
type cosineDistance struct{}

func (c *cosineDistance) Name() string { return "cosine" }

func (c *cosineDistance) Compute(v1, v2 []float32) float32 {
	if len(v1) != len(v2) {
		return 1.0
	}
	n := len(v1)
	var dot, norm1, norm2 float32

	// Process 4 elements at a time to help the compiler vectorize
	i := 0
	for ; i <= n-4; i += 4 {
		a0, a1, a2, a3 := v1[i], v1[i+1], v1[i+2], v1[i+3]
		b0, b1, b2, b3 := v2[i], v2[i+1], v2[i+2], v2[i+3]
		dot += a0*b0 + a1*b1 + a2*b2 + a3*b3
		norm1 += a0*a0 + a1*a1 + a2*a2 + a3*a3
		norm2 += b0*b0 + b1*b1 + b2*b2 + b3*b3
	}
	for ; i < n; i++ {
		dot += v1[i] * v2[i]
		norm1 += v1[i] * v1[i]
		norm2 += v2[i] * v2[i]
	}

	if norm1 == 0 || norm2 == 0 {
		return 1.0
	}
	return 1.0 - dot/float32(math.Sqrt(float64(norm1)*float64(norm2)))
}

// l2Distance L2距离实现
type l2Distance struct{}

func (l *l2Distance) Name() string { return "l2" }

func (l *l2Distance) Compute(v1, v2 []float32) float32 {
	if len(v1) != len(v2) {
		return float32(math.MaxFloat32)
	}
	var sum float64
	for i := 0; i < len(v1); i++ {
		diff := float64(v1[i] - v2[i])
		sum += diff * diff
	}
	return float32(math.Sqrt(sum))
}

// innerProductDistance 内积距离实现
type innerProductDistance struct{}

func (i *innerProductDistance) Name() string { return "inner_product" }

func (i *innerProductDistance) Compute(v1, v2 []float32) float32 {
	if len(v1) != len(v2) {
		return 0
	}
	var dot float64
	for i := 0; i < len(v1); i++ {
		dot += float64(v1[i] * v2[i])
	}
	return float32(-dot) // 返回负值，因为内积越大越相似
}

// init 注册默认距离函数
func init() {
	RegisterDistance("cosine", &cosineDistance{})
	RegisterDistance("l2", &l2Distance{})
	RegisterDistance("inner_product", &innerProductDistance{})
}
