package executor

import (
	"fmt"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// Aggregator 结果聚合器
type Aggregator struct {
	results []*domain.QueryResult
}

// NewAggregator 创建结果聚合器
func NewAggregator() *Aggregator {
	return &Aggregator{
		results: make([]*domain.QueryResult, 0),
	}
}

// AddResult 添加结果
func (a *Aggregator) AddResult(result *domain.QueryResult) {
	a.results = append(a.results, result)
}

// Aggregate 聚合所有结果
func (a *Aggregator) Aggregate() (*domain.QueryResult, error) {
	if len(a.results) == 0 {
		return nil, fmt.Errorf("no results to aggregate")
	}

	if len(a.results) == 1 {
		return a.results[0], nil
	}

	// 合并多个结果
	merged := &domain.QueryResult{
		Columns: a.results[0].Columns,
		Rows:    make([]domain.Row, 0),
	}

	for _, result := range a.results {
		merged.Rows = append(merged.Rows, result.Rows...)
	}

	return merged, nil
}

// Clear 清空聚合器
func (a *Aggregator) Clear() {
	a.results = make([]*domain.QueryResult, 0)
}

// Count 返回结果数量
func (a *Aggregator) Count() int {
	return len(a.results)
}
