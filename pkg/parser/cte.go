package parser

import (
	"fmt"
)

// CTEInfo CTE(公用表表达式)信息
type CTEInfo struct {
	Name      string           // CTE名称
	Alias     string           // CTE别名
	Subquery  *SelectStatement // CTE子查询
	Columns   []string         // 列别名(可选)
	Recursive bool             // 是否为递归CTE
}

// WithClause WITH子句(CTE定义)
type WithClause struct {
	CTEs        []*CTEInfo // CTE列表
	IsRecursive bool       // 是否递归
}

// 解析CTE相关函数
// 注意: 由于TiDB Parser的限制,这里提供辅助函数

// NewWithClause 创建WITH子句
func NewWithClause(isRecursive bool) *WithClause {
	return &WithClause{
		CTEs:        make([]*CTEInfo, 0),
		IsRecursive: isRecursive,
	}
}

// AddCTE 添加CTE
func (wc *WithClause) AddCTE(name string, subquery *SelectStatement, columns ...string) {
	cte := &CTEInfo{
		Name:      name,
		Subquery:  subquery,
		Columns:   columns,
		Recursive: wc.IsRecursive,
	}
	wc.CTEs = append(wc.CTEs, cte)
}

// GetCTE 获取CTE
func (wc *WithClause) GetCTE(name string) *CTEInfo {
	for _, cte := range wc.CTEs {
		if cte.Name == name {
			return cte
		}
	}
	return nil
}

// HasCTE 检查是否存在CTE
func (wc *WithClause) HasCTE(name string) bool {
	return wc.GetCTE(name) != nil
}

// GetCTENames 获取所有CTE名称
func (wc *WithClause) GetCTENames() []string {
	names := make([]string, 0, len(wc.CTEs))
	for _, cte := range wc.CTEs {
		names = append(names, cte.Name)
	}
	return names
}

// ParseCTEFromTiDB 从TiDB Parser的AST解析CTE
// 注意: TiDB Parser不完全支持CTE解析,这里提供手动构建接口
func ParseCTEFromTiDB(astNode interface{}) (*WithClause, error) {
	// 如果TiDB Parser有CTE支持,可以在这里实现
	// 目前提供手动构建接口

	// 示例: 手动构建CTE
	/*
		SELECT * FROM t WHERE id IN (WITH cte AS (SELECT id FROM t2) SELECT * FROM cte)

		构建方式:
		wc := parser.NewWithClause(false)
		wc.AddCTE("cte", subqueryStmt)
	*/

	return nil, fmt.Errorf("CTE parsing from TiDB AST not yet implemented, use manual construction")
}

// CTEOptimizer CTE优化器
type CTEOptimizer struct {
	// 优化配置
	InlineThreshold int  // 内联阈值(行数)
	CacheEnabled    bool // 是否启用缓存
}

// NewCTEOptimizer 创建CTE优化器
func NewCTEOptimizer() *CTEOptimizer {
	return &CTEOptimizer{
		InlineThreshold: 1000,
		CacheEnabled:    true,
	}
}

// Optimize 优化CTE
// 策略:
// 1. CTE只引用一次: 内联(Inline)
// 2. CTE多次引用: 物化(Materialize)并缓存
// 3. 递归CTE: 强制物化
func (opt *CTEOptimizer) Optimize(withClause *WithClause, selectStmt *SelectStatement) (*SelectStatement, error) {
	if withClause == nil || len(withClause.CTEs) == 0 {
		return selectStmt, nil
	}

	// 分析每个CTE的引用次数
	refCounts := opt.analyzeCTEReferences(selectStmt)

	// 优化每个CTE
	for _, cte := range withClause.CTEs {
		count := refCounts[cte.Name]

		if cte.Recursive {
			// 递归CTE必须物化
			cte.Subquery = opt.materializeCTE(cte.Subquery)
		} else if count == 1 {
			// 只引用一次,直接内联
			opt.inlineCTE(selectStmt, cte)
		} else if count > 1 && opt.CacheEnabled {
			// 多次引用,物化并缓存
			cte.Subquery = opt.materializeCTE(cte.Subquery)
		}
	}

	return selectStmt, nil
}

// analyzeCTEReferences 分析CTE引用次数
func (opt *CTEOptimizer) analyzeCTEReferences(stmt *SelectStatement) map[string]int {
	refCounts := make(map[string]int)
	opt.collectReferences(stmt, refCounts)
	return refCounts
}

// collectReferences 递归收集CTE引用
func (opt *CTEOptimizer) collectReferences(stmt *SelectStatement, refCounts map[string]int) {
	if stmt == nil {
		return
	}

	// 检查FROM子句中的CTE引用
	// 注意: 当前SelectStatement.From是string类型,不是表列表
	// 需要解析FROM字符串来识别CTE引用
	if stmt.From != "" {
		// 简化的实现: 假设FROM中可能有CTE引用
		// 实际需要更复杂的SQL解析
		// 这里暂时不实现,因为当前SelectStatement结构不支持表列表
	}

	// 检查JOIN子句中的CTE引用
	for _, join := range stmt.Joins {
		if join.Alias != "" {
			refCounts[join.Alias]++
		}
	}
}

// inlineCTE 内联CTE到主查询
func (opt *CTEOptimizer) inlineCTE(mainStmt *SelectStatement, cte *CTEInfo) {
	// 将CTE子查询替换到主查询中引用CTE的位置
	// 这是一个简化的实现

	// 实际实现需要:
	// 1. 找到所有引用CTE的表引用
	// 2. 将表引用替换为CTE的子查询
	// 3. 可能需要添加子查询别名
}

// materializeCTE 物化CTE
func (opt *CTEOptimizer) materializeCTE(subquery *SelectStatement) *SelectStatement {
	// 为CTE添加物化标记
	// 实际实现会创建临时表或使用内存缓存

	// 注意: SelectStatement当前没有Hints字段
	// 物化标记应该通过其他方式传递,例如在执行上下文中
	// 这里只是一个占位符实现
	return subquery
}

// CTEContext CTE执行上下文
type CTEContext struct {
	// CTE缓存
	CTEResults map[string][]map[string]interface{}

	// CTE是否已物化
	CTEMaterialized map[string]bool
}

// NewCTEContext 创建CTE执行上下文
func NewCTEContext() *CTEContext {
	return &CTEContext{
		CTEResults:      make(map[string][]map[string]interface{}),
		CTEMaterialized: make(map[string]bool),
	}
}

// GetCTEResult 获取CTE结果
func (ctx *CTEContext) GetCTEResult(cteName string) ([]map[string]interface{}, bool) {
	result, exists := ctx.CTEResults[cteName]
	return result, exists
}

// SetCTEResult 设置CTE结果
func (ctx *CTEContext) SetCTEResult(cteName string, result []map[string]interface{}) {
	ctx.CTEResults[cteName] = result
	ctx.CTEMaterialized[cteName] = true
}

// IsCTEMaterialized 检查CTE是否已物化
func (ctx *CTEContext) IsCTEMaterialized(cteName string) bool {
	return ctx.CTEMaterialized[cteName]
}

// ClearCTE 清除指定CTE的缓存
func (ctx *CTEContext) ClearCTE(cteName string) {
	delete(ctx.CTEResults, cteName)
	delete(ctx.CTEMaterialized, cteName)
}

// ClearAll 清除所有CTE缓存
func (ctx *CTEContext) ClearAll() {
	ctx.CTEResults = make(map[string][]map[string]interface{})
	ctx.CTEMaterialized = make(map[string]bool)
}
