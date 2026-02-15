package optimizer

import (
	"context"
	"fmt"
	"strings"

	"github.com/kasuganosora/sqlexec/pkg/parser"
)

// VectorIndexRule 向量索引优化规则
// 将 Sort + Limit + 向量距离函数 转换为 VectorScan
type VectorIndexRule struct {
	indexManager interface {
		HasVectorIndex(tableName, columnName string) bool
		GetVectorIndex(tableName, columnName string) (interface{}, error)
	}
}

// NewVectorIndexRule 创建向量索引规则
func NewVectorIndexRule() *VectorIndexRule {
	return &VectorIndexRule{}
}

// Name 返回规则名称
func (r *VectorIndexRule) Name() string {
	return "VectorIndex"
}

// Match 检查规则是否匹配
// 匹配条件：
// 1. 有 ORDER BY 子句且包含向量距离函数
// 2. 有 LIMIT 子句
// 3. 表上有对应的向量索引
func (r *VectorIndexRule) Match(plan LogicalPlan) bool {
	// 检查是否为 Sort 节点
	sort, ok := plan.(*LogicalSort)
	if !ok {
		return false
	}

	// 检查是否有 LIMIT
	hasLimit := false
	children := sort.Children()
	if len(children) > 0 {
		if _, ok := children[0].(*LogicalLimit); ok {
			hasLimit = true
		}
	}

	// 检查 ORDER BY 是否包含向量距离函数
	hasVectorFunc := false
	for _, item := range sort.OrderBy() {
		expr := &item.Expr // 取地址
		if isVectorDistanceFunction(expr) {
			hasVectorFunc = true
			break
		}
	}

	return hasLimit && hasVectorFunc
}

// Apply 应用向量索引优化规则
func (r *VectorIndexRule) Apply(ctx context.Context, plan LogicalPlan, optCtx *OptimizationContext) (LogicalPlan, error) {
	sort, ok := plan.(*LogicalSort)
	if !ok {
		return plan, nil
	}

	// 获取 LIMIT 信息
	var limitVal, offsetVal int64
	children := sort.Children()
	if len(children) == 0 {
		return plan, nil
	}

	// 获取数据源
	var dataSource *LogicalDataSource
	if limit, ok := children[0].(*LogicalLimit); ok {
		limitVal = limit.GetLimit()
		offsetVal = limit.GetOffset()
		// 获取 Limit 的子节点
		limitChildren := limit.Children()
		if len(limitChildren) > 0 {
			if ds, ok := limitChildren[0].(*LogicalDataSource); ok {
				dataSource = ds
			}
		}
	} else if ds, ok := children[0].(*LogicalDataSource); ok {
		dataSource = ds
	}

	if dataSource == nil {
		return plan, nil
	}

	// 从 ORDER BY 中提取向量搜索信息
	vectorInfo := r.extractVectorSearchInfo(sort)
	if vectorInfo == nil {
		return plan, nil
	}

	// 检查是否有对应的向量索引
	if r.indexManager != nil && !r.indexManager.HasVectorIndex(dataSource.TableName, vectorInfo.ColumnName) {
		// 没有找到向量索引，但也可以尝试优化
		debugf("  [DEBUG] VectorIndexRule: 表 %s 列 %s 没有向量索引\n", 
			dataSource.TableName, vectorInfo.ColumnName)
	}

	// 创建 VectorScan 逻辑节点
	vectorScan := NewLogicalVectorScan(
		dataSource.TableName,
		vectorInfo.ColumnName,
		vectorInfo.QueryVector,
		int(limitVal),
		vectorInfo.MetricType,
	)

	// 如果有 offset，添加 offset 信息
	if offsetVal > 0 {
		vectorScan.Offset = int(offsetVal)
	}

	debugf("  [DEBUG] VectorIndexRule: 将 Sort+Limit 转换为 VectorScan, 表=%s, 列=%s, k=%d\n",
		dataSource.TableName, vectorInfo.ColumnName, limitVal)

	return vectorScan, nil
}

// VectorSearchInfo 向量搜索信息
type VectorSearchInfo struct {
	ColumnName  string
	QueryVector []float32
	MetricType  string
}

// extractVectorSearchInfo 从 Sort 节点提取向量搜索信息
func (r *VectorIndexRule) extractVectorSearchInfo(sort *LogicalSort) *VectorSearchInfo {
	for _, item := range sort.OrderBy() {
		expr := &item.Expr // 取地址

		// 检查是否为向量距离函数
		if isVectorDistanceFunction(expr) {
			return r.parseVectorDistanceExpr(expr)
		}
	}
	return nil
}

// isVectorDistanceFunction 检查表达式是否为向量距离函数
func isVectorDistanceFunction(expr *parser.Expression) bool {
	if expr == nil {
		return false
	}

	if expr.Type != parser.ExprTypeFunction {
		return false
	}

	funcName := strings.ToLower(expr.Function)
	return funcName == "vec_cosine_distance" ||
		funcName == "vec_l2_distance" ||
		funcName == "vec_inner_product" ||
		funcName == "vec_distance"
}

// parseVectorDistanceExpr 解析向量距离函数表达式
func (r *VectorIndexRule) parseVectorDistanceExpr(expr *parser.Expression) *VectorSearchInfo {
	if expr == nil || len(expr.Args) < 2 {
		return nil
	}

	// 第一个参数应该是列引用
	colArg := expr.Args[0]
	if colArg.Type != parser.ExprTypeColumn {
		return nil
	}

	// 第二个参数应该是查询向量
	vecArg := expr.Args[1]
	queryVector := extractVectorFromExpr(vecArg)
	if queryVector == nil {
		return nil
	}

	// 确定距离度量类型
	metricType := "cosine" // 默认
	funcName := strings.ToLower(expr.Function)
	switch funcName {
	case "vec_cosine_distance":
		metricType = "cosine"
	case "vec_l2_distance":
		metricType = "l2"
	case "vec_inner_product":
		metricType = "inner_product"
	}

	return &VectorSearchInfo{
		ColumnName:  colArg.Column,
		QueryVector: queryVector,
		MetricType:  metricType,
	}
}

// extractVectorFromExpr 从表达式中提取向量值
func extractVectorFromExpr(expr parser.Expression) []float32 {
	// 处理字符串值（JSON格式）
	if expr.Type == parser.ExprTypeValue && expr.Value != nil {
		switch v := expr.Value.(type) {
		case string:
			// 解析 JSON 向量字符串
			return parseVectorString(v)
		case []float32:
			return v
		case []float64:
			result := make([]float32, len(v))
			for i, val := range v {
				result[i] = float32(val)
			}
			return result
		case []interface{}:
			result := make([]float32, len(v))
			for i, val := range v {
				switch fv := val.(type) {
				case float64:
					result[i] = float32(fv)
				case float32:
					result[i] = fv
				case int:
					result[i] = float32(fv)
				default:
					return nil
				}
			}
			return result
		}
	}
	return nil
}

// parseVectorString 解析向量字符串
func parseVectorString(s string) []float32 {
	// 简单解析 "[0.1, 0.2, 0.3]" 格式
	s = strings.TrimSpace(s)
	s = strings.Trim(s, "[]")
	parts := strings.Split(s, ",")
	
	result := make([]float32, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		var val float64
		if _, err := fmt.Sscanf(part, "%f", &val); err == nil {
			result = append(result, float32(val))
		}
	}
	
	if len(result) == 0 {
		return nil
	}
	return result
}

// ==================== LogicalVectorScan 定义 ====================

// LogicalVectorScan 向量扫描逻辑节点
type LogicalVectorScan struct {
	TableName   string
	ColumnName  string
	QueryVector []float32
	K           int
	MetricType  string
	Offset      int
	children    []LogicalPlan
}

// NewLogicalVectorScan 创建向量扫描逻辑节点
func NewLogicalVectorScan(tableName, columnName string, queryVector []float32, k int, metricType string) *LogicalVectorScan {
	return &LogicalVectorScan{
		TableName:   tableName,
		ColumnName:  columnName,
		QueryVector: queryVector,
		K:           k,
		MetricType:  metricType,
	}
}

// Explain 返回节点说明
func (l *LogicalVectorScan) Explain() string {
	return fmt.Sprintf("VectorScan[%s.%s, k=%d, metric=%s]", 
		l.TableName, l.ColumnName, l.K, l.MetricType)
}

// Schema 返回输出 schema
func (l *LogicalVectorScan) Schema() []ColumnInfo {
	// 返回包含距离列的 schema
	return []ColumnInfo{
		{Name: l.ColumnName, Type: "VECTOR"},
		{Name: "_distance", Type: "FLOAT"},
	}
}

// Children 获取子节点
func (l *LogicalVectorScan) Children() []LogicalPlan {
	return l.children
}

// SetChildren 设置子节点
func (l *LogicalVectorScan) SetChildren(children ...LogicalPlan) {
	l.children = children
}

// EstimateCardinality 估算基数
func (l *LogicalVectorScan) EstimateCardinality() int64 {
	return int64(l.K)
}
