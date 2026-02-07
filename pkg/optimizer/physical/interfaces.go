package physical

import (
	"github.com/kasuganosora/sqlexec/pkg/optimizer"
)

// PhysicalOperator 物理算子接口，所有物理算子必须实现此接口
type PhysicalOperator interface {
	// Children 获取子节点
	Children() []PhysicalOperator

	// SetChildren 设置子节点
	SetChildren(children ...PhysicalOperator)

	// Schema 返回输出列
	Schema() []optimizer.ColumnInfo

	// Cost 返回执行成本
	Cost() float64

	// Explain 返回计划说明
	Explain() string
}
