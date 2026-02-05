package optimizer

import (
	"github.com/kasuganosora/sqlexec/pkg/parser"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// LogicalDataSource 逻辑数据源（表扫描）
type LogicalDataSource struct {
	TableName            string
	Columns              []ColumnInfo
	TableInfo            *domain.TableInfo
	Statistics           *Statistics
	children             []LogicalPlan
	pushedDownPredicates []*parser.Expression // 下推的谓词条件
	pushedDownLimit      *LimitInfo           // 下推的Limit信息
	pushedDownTopN      *TopNInfo            // 下推的TopN信息
}

// TopNInfo contains TopN pushdown information
type TopNInfo struct {
	SortItems []*parser.OrderItem // Sort items
	Limit     int64            // Limit count
	Offset    int64             // Offset count
}

// NewLogicalDataSource 创建逻辑数据源
func NewLogicalDataSource(tableName string, tableInfo *domain.TableInfo) *LogicalDataSource {
	columns := make([]ColumnInfo, 0, len(tableInfo.Columns))
	for _, col := range tableInfo.Columns {
		columns = append(columns, ColumnInfo{
			Name:     col.Name,
			Type:     col.Type,
			Nullable: col.Nullable,
		})
	}

	return &LogicalDataSource{
		TableName: tableName,
		Columns:   columns,
		TableInfo: tableInfo,
		children:  []LogicalPlan{},
	}
}

// Children 获取子节点
func (p *LogicalDataSource) Children() []LogicalPlan {
	return p.children
}

// SetChildren 设置子节点
func (p *LogicalDataSource) SetChildren(children ...LogicalPlan) {
	p.children = children
}

// Schema 返回输出列
func (p *LogicalDataSource) Schema() []ColumnInfo {
	return p.Columns
}

// RowCount 返回预估行数
func (p *LogicalDataSource) RowCount() int64 {
	if p.Statistics != nil {
		return p.Statistics.RowCount
	}
	return 1000 // 默认估计
}

// Table 返回表名
func (p *LogicalDataSource) Table() string {
	return p.TableName
}

// Explain 返回计划说明
func (p *LogicalDataSource) Explain() string {
	return "DataSource(" + p.TableName + ")"
}

// PushDownPredicates 添加下推的谓词条件
func (p *LogicalDataSource) PushDownPredicates(conditions []*parser.Expression) {
	p.pushedDownPredicates = append(p.pushedDownPredicates, conditions...)
}

// GetPushedDownPredicates 获取下推的谓词条件
func (p *LogicalDataSource) GetPushedDownPredicates() []*parser.Expression {
	return p.pushedDownPredicates
}

// PushDownLimit 添加下推的Limit
func (p *LogicalDataSource) PushDownLimit(limit, offset int64) {
	p.pushedDownLimit = &LimitInfo{
		Limit:  limit,
		Offset: offset,
	}
}

// GetPushedDownLimit 获取下推的Limit
func (p *LogicalDataSource) GetPushedDownLimit() *LimitInfo {
	return p.pushedDownLimit
}

// SetPushDownTopN sets the TopN pushdown information
func (p *LogicalDataSource) SetPushDownTopN(items []*parser.OrderItem, limit, offset int64) {
	p.pushedDownTopN = &TopNInfo{
		SortItems: items,
		Limit:     limit,
		Offset:    offset,
	}
}

// GetPushedDownTopN gets the TopN pushdown information
func (p *LogicalDataSource) GetPushedDownTopN() *TopNInfo {
	return p.pushedDownTopN
}
