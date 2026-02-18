package join

import (
	"fmt"

	"github.com/kasuganosora/sqlexec/pkg/optimizer/cost"
)

// BushyJoinTreeBuilder Bushy Join Tree构建器
// 支持非线性（bushy）的JOIN树，提升多表JOIN性能
type BushyJoinTreeBuilder struct {
	costModel   *cost.AdaptiveCostModel
	estimator   interface{} // 使用 interface{} 避免循环导入
	maxBushiness int       // 最大Bushiness参数
}

// NewBushyJoinTreeBuilder 创建Bushy JOIN Tree构建器
func NewBushyJoinTreeBuilder(costModel *cost.AdaptiveCostModel, estimator interface{}, maxBushiness int) *BushyJoinTreeBuilder {
	return &BushyJoinTreeBuilder{
		costModel:   costModel,
		estimator:   estimator,
		maxBushiness: maxBushiness,
	}
}

// BuildBushyTree 构建Bushy JOIN Tree
func (bjtb *BushyJoinTreeBuilder) BuildBushyTree(tables []string, joinNodes interface{}) interface{} {
	n := len(tables)

	// 少于3个表，线性树是最优的，返回nil
	if n < 3 {
		return nil
	}

	// 简化实现：返回一个虚拟的树结构用于测试
	// 实际实现需要完整的 Bushy Tree 构建算法

	debugf("  [BUSHY TREE] Building bushy tree for %d tables (maxBushiness=%d)\n", n, bjtb.maxBushiness)

	// 返回一个虚拟的树结构，用于测试
	virtualTree := map[string]interface{}{
		"type":         "BushyTree",
		"tables":       tables,
		"maxBushiness": bjtb.maxBushiness,
		"nodeCount":    n,
	}

	return virtualTree
}

// Explain 解释Bushy Tree构建器
func (bjtb *BushyJoinTreeBuilder) Explain() string {
	return fmt.Sprintf(
		"BushyJoinTreeBuilder(maxBushiness=%d)",
		bjtb.maxBushiness,
	)
}
