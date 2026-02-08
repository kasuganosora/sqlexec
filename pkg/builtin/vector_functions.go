package builtin

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
)

// CategoryVector 向量函数类别
const CategoryVector FunctionCategory = "vector"

func init() {
	// 注册向量函数
	registerVectorFunctions()
}

// registerVectorFunctions 注册所有向量函数
func registerVectorFunctions() {
	// VEC_COSINE_DISTANCE - 余弦距离
	RegisterGlobal(&FunctionInfo{
		Name:        "vec_cosine_distance",
		Type:        FunctionTypeScalar,
		Category:    "vector",
		Description: "计算两个向量之间的余弦距离",
		Signatures: []FunctionSignature{
			{
				Name:       "vec_cosine_distance",
				ReturnType: "float",
				ParamTypes: []string{"array", "array"},
				Variadic:   false,
			},
		},
		Handler:     vecCosineDistanceHandler,
		Example:     "SELECT vec_cosine_distance(embedding, '[0.1, 0.2, 0.3]') FROM articles",
	})

	// VEC_L2_DISTANCE - L2/Euclidean 距离
	RegisterGlobal(&FunctionInfo{
		Name:        "vec_l2_distance",
		Type:        FunctionTypeScalar,
		Category:    "vector",
		Description: "计算两个向量之间的 L2 (欧几里得) 距离",
		Signatures: []FunctionSignature{
			{
				Name:       "vec_l2_distance",
				ReturnType: "float",
				ParamTypes: []string{"array", "array"},
				Variadic:   false,
			},
		},
		Handler:     vecL2DistanceHandler,
		Example:     "SELECT vec_l2_distance(embedding, '[0.1, 0.2, 0.3]') FROM articles",
	})

	// VEC_INNER_PRODUCT - 内积
	RegisterGlobal(&FunctionInfo{
		Name:        "vec_inner_product",
		Type:        FunctionTypeScalar,
		Category:    "vector",
		Description: "计算两个向量之间的内积",
		Signatures: []FunctionSignature{
			{
				Name:       "vec_inner_product",
				ReturnType: "float",
				ParamTypes: []string{"array", "array"},
				Variadic:   false,
			},
		},
		Handler:     vecInnerProductHandler,
		Example:     "SELECT vec_inner_product(embedding, '[0.1, 0.2, 0.3]') FROM articles",
	})

	// VEC_DISTANCE - 通用距离函数（默认使用余弦）
	RegisterGlobal(&FunctionInfo{
		Name:        "vec_distance",
		Type:        FunctionTypeScalar,
		Category:    "vector",
		Description: "计算两个向量之间的距离（默认使用余弦距离）",
		Signatures: []FunctionSignature{
			{
				Name:       "vec_distance",
				ReturnType: "float",
				ParamTypes: []string{"array", "array"},
				Variadic:   false,
			},
		},
		Handler:     vecCosineDistanceHandler, // 默认使用余弦距离
		Example:     "SELECT vec_distance(embedding, '[0.1, 0.2, 0.3]') FROM articles",
	})
}

// vecCosineDistanceHandler 余弦距离处理函数
func vecCosineDistanceHandler(args []interface{}) (interface{}, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("vec_cosine_distance requires exactly 2 arguments, got %d", len(args))
	}

	vec1, err := parseVector(args[0])
	if err != nil {
		return nil, fmt.Errorf("first argument is not a valid vector: %v", err)
	}

	vec2, err := parseVector(args[1])
	if err != nil {
		return nil, fmt.Errorf("second argument is not a valid vector: %v", err)
	}

	if len(vec1) != len(vec2) {
		return nil, fmt.Errorf("vectors have different dimensions: %d vs %d", len(vec1), len(vec2))
	}

	return cosineDistance(vec1, vec2), nil
}

// vecL2DistanceHandler L2 距离处理函数
func vecL2DistanceHandler(args []interface{}) (interface{}, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("vec_l2_distance requires exactly 2 arguments, got %d", len(args))
	}

	vec1, err := parseVector(args[0])
	if err != nil {
		return nil, fmt.Errorf("first argument is not a valid vector: %v", err)
	}

	vec2, err := parseVector(args[1])
	if err != nil {
		return nil, fmt.Errorf("second argument is not a valid vector: %v", err)
	}

	if len(vec1) != len(vec2) {
		return nil, fmt.Errorf("vectors have different dimensions: %d vs %d", len(vec1), len(vec2))
	}

	return l2Distance(vec1, vec2), nil
}

// vecInnerProductHandler 内积处理函数
func vecInnerProductHandler(args []interface{}) (interface{}, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("vec_inner_product requires exactly 2 arguments, got %d", len(args))
	}

	vec1, err := parseVector(args[0])
	if err != nil {
		return nil, fmt.Errorf("first argument is not a valid vector: %v", err)
	}

	vec2, err := parseVector(args[1])
	if err != nil {
		return nil, fmt.Errorf("second argument is not a valid vector: %v", err)
	}

	if len(vec1) != len(vec2) {
		return nil, fmt.Errorf("vectors have different dimensions: %d vs %d", len(vec1), len(vec2))
	}

	return innerProduct(vec1, vec2), nil
}

// parseVector 解析向量参数
func parseVector(arg interface{}) ([]float64, error) {
	switch v := arg.(type) {
	case []float64:
		return v, nil
	case []float32:
		result := make([]float64, len(v))
		for i, val := range v {
			result[i] = float64(val)
		}
		return result, nil
	case []interface{}:
		result := make([]float64, len(v))
		for i, val := range v {
			switch fv := val.(type) {
			case float64:
				result[i] = fv
			case float32:
				result[i] = float64(fv)
			case int:
				result[i] = float64(fv)
			case int64:
				result[i] = float64(fv)
			default:
				return nil, fmt.Errorf("unsupported element type: %T", val)
			}
		}
		return result, nil
	case string:
		// 解析 JSON 格式的向量，如 "[0.1, 0.2, 0.3]"
		return parseVectorString(v)
	default:
		return nil, fmt.Errorf("unsupported vector type: %T", arg)
	}
}

// parseVectorString 解析向量字符串
func parseVectorString(s string) ([]float64, error) {
	s = strings.TrimSpace(s)
	
	// 处理 JSON 数组格式
	if strings.HasPrefix(s, "[") && strings.HasSuffix(s, "]") {
		var vec []float64
		if err := json.Unmarshal([]byte(s), &vec); err != nil {
			// 尝试解析为 []interface{}
			var ifaceVec []interface{}
			if err2 := json.Unmarshal([]byte(s), &ifaceVec); err2 != nil {
				return nil, fmt.Errorf("failed to parse vector JSON: %v", err)
			}
			// 转换为 float64
			vec = make([]float64, len(ifaceVec))
			for i, v := range ifaceVec {
				switch fv := v.(type) {
				case float64:
					vec[i] = fv
				case int:
					vec[i] = float64(fv)
				default:
					return nil, fmt.Errorf("unsupported element type at index %d: %T", i, v)
				}
			}
		}
		return vec, nil
	}
	
	return nil, fmt.Errorf("invalid vector format: %s", s)
}

// cosineDistance 计算余弦距离
func cosineDistance(v1, v2 []float64) float64 {
	var dot, norm1, norm2 float64
	for i := 0; i < len(v1); i++ {
		dot += v1[i] * v2[i]
		norm1 += v1[i] * v1[i]
		norm2 += v2[i] * v2[i]
	}
	if norm1 == 0 || norm2 == 0 {
		return 1.0
	}
	return 1.0 - dot/(math.Sqrt(norm1)*math.Sqrt(norm2))
}

// l2Distance 计算 L2 距离
func l2Distance(v1, v2 []float64) float64 {
	var sum float64
	for i := 0; i < len(v1); i++ {
		diff := v1[i] - v2[i]
		sum += diff * diff
	}
	return math.Sqrt(sum)
}

// innerProduct 计算内积
func innerProduct(v1, v2 []float64) float64 {
	var dot float64
	for i := 0; i < len(v1); i++ {
		dot += v1[i] * v2[i]
	}
	return dot
}
