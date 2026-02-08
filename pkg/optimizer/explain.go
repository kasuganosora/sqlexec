package optimizer

import (
	"strings"

	"github.com/kasuganosora/sqlexec/pkg/optimizer/plan"
)

// ExplainPlan 解释执行计划（统一调用 ExplainPlanV2）
// 注意：PhysicalPlan 接口已被弃用，请使用 *plan.Plan
func ExplainPlan(physicalPlan PhysicalPlan) string {
	// 简单输出当前节点的 Explain 信息
	var builder strings.Builder
	builder.WriteString(physicalPlan.Explain())
	builder.WriteString("\n")
	return builder.String()
}

// ExplainPlanV2 解释新架构的执行计划（plan.Plan）
func ExplainPlanV2(p *plan.Plan) string {
	return explainPlanV2(p, 0)
}

// explainPlanV2 递归解释新架构的计划
func explainPlanV2(p *plan.Plan, depth int) string {
	if p == nil {
		return ""
	}

	var builder strings.Builder

	for i := 0; i < depth; i++ {
		builder.WriteString("  ")
	}

	builder.WriteString(p.ID)
	builder.WriteString(" [")
	builder.WriteString(string(p.Type))
	builder.WriteString("]")
	builder.WriteString("\n")

	for _, child := range p.Children {
		builder.WriteString(explainPlanV2(child, depth+1))
	}

	return builder.String()
}
