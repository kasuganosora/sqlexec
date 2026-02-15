package memory

import (
	"fmt"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/resource/util"
)

// ScanMethod 扫描方法
type ScanMethod string

const (
	ScanMethodFull  ScanMethod = "full"  // 全表扫描
	ScanMethodIndex ScanMethod = "index" // 索引扫描
	ScanMethodRange ScanMethod = "range" // 范围扫描
)

// QueryPlan 查询计划
type QueryPlan struct {
	TableName string
	Filters   []domain.Filter
	Options   *domain.QueryOptions
	Method    ScanMethod
	Index     *IndexInfo
}

// QueryPlanner 查询优化器
type QueryPlanner struct {
	indexManager *IndexManager
}

// NewQueryPlanner 创建查询优化器
func NewQueryPlanner(indexManager *IndexManager) *QueryPlanner {
	return &QueryPlanner{
		indexManager: indexManager,
	}
}

// PlanQuery 优化查询计划
func (p *QueryPlanner) PlanQuery(tableName string, filters []domain.Filter, options *domain.QueryOptions) (*QueryPlan, error) {
	plan := &QueryPlan{
		TableName: tableName,
		Filters:   filters,
		Options:   options,
		Method:    ScanMethodFull,
		Index:     nil,
	}

	// 检查是否可以使用索引（等值查询）
	if len(filters) == 1 && filters[0].Operator == "=" {
		index, err := p.indexManager.GetIndex(tableName, filters[0].Field)
		if err == nil && index != nil {
			// 使用索引
			indexInfo := index.GetIndexInfo()
			if indexInfo.Type == IndexTypeBTree || indexInfo.Type == IndexTypeHash {
				plan.Method = ScanMethodIndex
				plan.Index = indexInfo
				return plan, nil
			}
		}
	}

	return plan, nil
}

// ExecutePlan 执行查询计划
func (p *QueryPlanner) ExecutePlan(plan *QueryPlan, tableData *TableData) (*domain.QueryResult, error) {
	switch plan.Method {
	case ScanMethodFull:
		// 全表扫描
		return p.fullScan(tableData, plan)
	case ScanMethodIndex:
		// 索引查询
		return p.indexScan(tableData, plan)
	default:
		return nil, fmt.Errorf("unknown scan method: %s", plan.Method)
	}
}

// fullScan 全表扫描
func (p *QueryPlanner) fullScan(tableData *TableData, plan *QueryPlan) (*domain.QueryResult, error) {
	// 使用现有的过滤逻辑
	filteredRows := make([]domain.Row, 0)
	for _, row := range tableData.Rows() {
		matches := true
		for _, filter := range plan.Filters {
			// 使用MatchFilter进行正确的值比较
			if !util.MatchFilter(row, filter) {
				matches = false
				break
			}
		}

		if matches {
			filteredRows = append(filteredRows, row)
		}
	}

	return &domain.QueryResult{
		Columns: tableData.schema.Columns,
		Rows:    filteredRows,
		Total:   int64(len(filteredRows)),
	}, nil
}

// indexScan 索引查询
func (p *QueryPlanner) indexScan(tableData *TableData, plan *QueryPlan) (*domain.QueryResult, error) {
	if len(plan.Filters) == 0 {
		return p.fullScan(tableData, plan)
	}

	// 获取索引
	index, err := p.indexManager.GetIndex(plan.TableName, plan.Filters[0].Field)
	if err != nil || index == nil {
		return p.fullScan(tableData, plan)
	}

	// 执行点查询
	rowIDs, found := index.Find(plan.Filters[0].Value)
	if !found || len(rowIDs) == 0 {
		return &domain.QueryResult{
			Columns: tableData.schema.Columns,
			Rows:    []domain.Row{},
			Total:   0,
		}, nil
	}

	// 根据rowID获取行数据
	filteredRows := make([]domain.Row, 0, len(rowIDs))
	for _, rowID := range rowIDs {
		if int(rowID) <= tableData.RowCount() {
			filteredRows = append(filteredRows, tableData.rows.Get(int(rowID-1)))
		}
	}

	// 应用其他过滤条件
	for _, filter := range plan.Filters[1:] {
		newFiltered := make([]domain.Row, 0)
		for _, row := range filteredRows {
			matches := true
			_, exists := row[filter.Field]
			if !exists {
				matches = false
			}
			if matches {
				newFiltered = append(newFiltered, row)
			}
		}
		filteredRows = newFiltered
	}

	return &domain.QueryResult{
		Columns: tableData.schema.Columns,
		Rows:    filteredRows,
		Total:   int64(len(filteredRows)),
	}, nil
}

// rangeScan 范围查询
func (p *QueryPlanner) rangeScan(tableData *TableData, plan *QueryPlan) (*domain.QueryResult, error) {
	// 暂不实现，简化为全表扫描
	return p.fullScan(tableData, plan)
}
