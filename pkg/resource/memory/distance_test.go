package memory

import (
	"math"
	"testing"
)

// TestRegisterDistance 测试距离函数注册
func TestRegisterDistance(t *testing.T) {
	// 测试注册自定义距离函数
	customDist := &testDistanceFunc{name: "custom_test"}
	RegisterDistance("custom_test", customDist)

	// 验证注册成功
	fn, err := GetDistance("custom_test")
	if err != nil {
		t.Errorf("GetDistance failed: %v", err)
	}
	if fn.Name() != "custom_test" {
		t.Errorf("Expected name 'custom_test', got %s", fn.Name())
	}

	// 测试获取不存在的距离函数
	_, err = GetDistance("nonexistent")
	if err == nil {
		t.Error("GetDistance should return error for nonexistent function")
	}
}

// TestMustGetDistance 测试 MustGetDistance
func TestMustGetDistance(t *testing.T) {
	// 测试正常获取
	fn := MustGetDistance("cosine")
	if fn == nil {
		t.Error("MustGetDistance should not return nil for existing function")
	}

	// 测试获取不存在的函数会panic
	defer func() {
		if r := recover(); r == nil {
			t.Error("MustGetDistance should panic for nonexistent function")
		}
	}()
	MustGetDistance("nonexistent_panic")
}

// TestCosineDistanceVariations 测试余弦距离的各种情况
func TestCosineDistanceVariations(t *testing.T) {
	tests := []struct {
		name     string
		v1       []float32
		v2       []float32
		expected float32
		delta    float32
	}{
		{
			name:     "identical_vectors",
			v1:       []float32{1.0, 2.0, 3.0},
			v2:       []float32{1.0, 2.0, 3.0},
			expected: 0.0,
			delta:    0.0001,
		},
		{
			name:     "opposite_vectors",
			v1:       []float32{1.0, 2.0, 3.0},
			v2:       []float32{-1.0, -2.0, -3.0},
			expected: 2.0,
			delta:    0.0001,
		},
		{
			name:     "orthogonal_vectors",
			v1:       []float32{1.0, 0.0},
			v2:       []float32{0.0, 1.0},
			expected: 1.0,
			delta:    0.0001,
		},
		{
			name:     "zero_vector",
			v1:       []float32{0.0, 0.0, 0.0},
			v2:       []float32{1.0, 2.0, 3.0},
			expected: 1.0,
			delta:    0.0001,
		},
		{
			name:     "single_element",
			v1:       []float32{5.0},
			v2:       []float32{3.0},
			expected: 0.0,
			delta:    0.0001,
		},
		{
			name:     "large_dimensions",
			v1:       make([]float32, 768),
			v2:       make([]float32, 768),
			expected: 0.0,
			delta:    0.0001,
		},
	}

	// 填充大维度测试数据
	for i := range tests[5].v1 {
		tests[5].v1[i] = float32(i) * 0.01
		tests[5].v2[i] = float32(i) * 0.01
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CosineDistance(tt.v1, tt.v2)
			if math.Abs(float64(result-tt.expected)) > float64(tt.delta) {
				t.Errorf("CosineDistance() = %v, expected %v", result, tt.expected)
			}
		})
	}
}

// TestL2DistanceVariations 测试L2距离的各种情况
func TestL2DistanceVariations(t *testing.T) {
	tests := []struct {
		name     string
		v1       []float32
		v2       []float32
		expected float32
		delta    float32
	}{
		{
			name:     "identical_vectors",
			v1:       []float32{1.0, 2.0, 3.0},
			v2:       []float32{1.0, 2.0, 3.0},
			expected: 0.0,
			delta:    0.0001,
		},
		{
			name:     "simple_case",
			v1:       []float32{0.0, 0.0},
			v2:       []float32{3.0, 4.0},
			expected: 5.0,
			delta:    0.0001,
		},
		{
			name:     "negative_values",
			v1:       []float32{-1.0, -2.0, -3.0},
			v2:       []float32{1.0, 2.0, 3.0},
			expected: 7.483315,
			delta:    0.001,
		},
		{
			name:     "single_element",
			v1:       []float32{5.0},
			v2:       []float32{2.0},
			expected: 3.0,
			delta:    0.0001,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := L2Distance(tt.v1, tt.v2)
			if math.Abs(float64(result-tt.expected)) > float64(tt.delta) {
				t.Errorf("L2Distance() = %v, expected %v", result, tt.expected)
			}
		})
	}
}

// TestInnerProductVariations 测试内积的各种情况
func TestInnerProductVariations(t *testing.T) {
	tests := []struct {
		name     string
		v1       []float32
		v2       []float32
		expected float32
		delta    float32
	}{
		{
			name:     "simple_case",
			v1:       []float32{1.0, 2.0, 3.0},
			v2:       []float32{1.0, 2.0, 3.0},
			expected: -14.0, // 负的内积（距离越小越相似）
			delta:    0.0001,
		},
		{
			name:     "orthogonal",
			v1:       []float32{1.0, 0.0},
			v2:       []float32{0.0, 1.0},
			expected: 0.0,
			delta:    0.0001,
		},
		{
			name:     "zero_vector",
			v1:       []float32{0.0, 0.0, 0.0},
			v2:       []float32{1.0, 2.0, 3.0},
			expected: 0.0,
			delta:    0.0001,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := InnerProductDistance(tt.v1, tt.v2)
			if math.Abs(float64(result-tt.expected)) > float64(tt.delta) {
				t.Errorf("InnerProductDistance() = %v, expected %v", result, tt.expected)
			}
		})
	}
}

// TestMismatchedDimensions 测试维度不匹配的情况
func TestMismatchedDimensions(t *testing.T) {
	v1 := []float32{1.0, 2.0, 3.0}
	v2 := []float32{1.0, 2.0}

	t.Run("cosine_mismatch", func(t *testing.T) {
		result := CosineDistance(v1, v2)
		if result != 1.0 {
			t.Errorf("Expected 1.0 for mismatched dimensions, got %f", result)
		}
	})

	t.Run("l2_mismatch", func(t *testing.T) {
		result := L2Distance(v1, v2)
		if result != float32(math.MaxFloat32) {
			t.Errorf("Expected MaxFloat32 for mismatched dimensions, got %f", result)
		}
	})

	t.Run("inner_product_mismatch", func(t *testing.T) {
		result := InnerProductDistance(v1, v2)
		if result != 0.0 {
			t.Errorf("Expected 0 for mismatched dimensions, got %f", result)
		}
	})
}

// TestDistanceFuncImplementations 测试具体实现
func TestDistanceFuncImplementations(t *testing.T) {
	t.Run("cosine_implementation", func(t *testing.T) {
		cosine := &cosineDistance{}
		if cosine.Name() != "cosine" {
			t.Errorf("Expected name 'cosine', got %s", cosine.Name())
		}

		v1 := []float32{1.0, 0.0}
		v2 := []float32{0.0, 1.0}
		result := cosine.Compute(v1, v2)
		if math.Abs(float64(result-1.0)) > 0.0001 {
			t.Errorf("Expected ~1.0 for orthogonal vectors, got %f", result)
		}
	})

	t.Run("l2_implementation", func(t *testing.T) {
		l2 := &l2Distance{}
		if l2.Name() != "l2" {
			t.Errorf("Expected name 'l2', got %s", l2.Name())
		}

		v1 := []float32{0.0, 0.0}
		v2 := []float32{3.0, 4.0}
		result := l2.Compute(v1, v2)
		if math.Abs(float64(result-5.0)) > 0.0001 {
			t.Errorf("Expected 5.0, got %f", result)
		}
	})

	t.Run("inner_product_implementation", func(t *testing.T) {
		ip := &innerProductDistance{}
		if ip.Name() != "inner_product" {
			t.Errorf("Expected name 'inner_product', got %s", ip.Name())
		}

		v1 := []float32{1.0, 2.0}
		v2 := []float32{3.0, 4.0}
		result := ip.Compute(v1, v2)
		expected := float32(-11.0) // -(1*3 + 2*4)
		if result != expected {
			t.Errorf("Expected %f, got %f", expected, result)
		}
	})
}

// TestConcurrentAccess 测试并发访问
func TestConcurrentAccess(t *testing.T) {
	// 测试并发读取注册表
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func() {
			_, _ = GetDistance("cosine")
			_, _ = GetDistance("l2")
			_, _ = GetDistance("inner_product")
			done <- true
		}()
	}

	for i := 0; i < 10; i++ {
		<-done
	}
}

// testDistanceFunc 用于测试的自定义距离函数
type testDistanceFunc struct {
	name string
}

func (t *testDistanceFunc) Name() string {
	return t.name
}

func (t *testDistanceFunc) Compute(v1, v2 []float32) float32 {
	return 0.0
}
