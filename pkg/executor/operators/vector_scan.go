package operators

import (
	"context"
	"fmt"

	"github.com/kasuganosora/sqlexec/pkg/dataaccess"
	"github.com/kasuganosora/sqlexec/pkg/optimizer/plan"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/resource/memory"
)

// VectorScanOperator 向量扫描算子
type VectorScanOperator struct {
	*BaseOperator
	config *plan.VectorScanConfig
	idxMgr *memory.IndexManager
}

// NewVectorScanOperator 创建向量扫描算子
func NewVectorScanOperator(p *plan.Plan, das dataaccess.Service, idxMgr *memory.IndexManager) (*VectorScanOperator, error) {
	config, ok := p.Config.(*plan.VectorScanConfig)
	if !ok {
		return nil, fmt.Errorf("invalid config type for VectorScanOperator")
	}
	return &VectorScanOperator{
		BaseOperator: NewBaseOperator(p, das),
		config:       config,
		idxMgr:       idxMgr,
	}, nil
}

// Execute 执行向量扫描
func (v *VectorScanOperator) Execute(ctx context.Context) (*domain.QueryResult, error) {
	// 1. 获取向量索引
	vectorIdx, err := v.idxMgr.GetVectorIndex(v.config.TableName, v.config.ColumnName)
	if err != nil {
		return nil, fmt.Errorf("get vector index failed: %w", err)
	}

	// 2. 执行向量搜索
	result, err := vectorIdx.Search(ctx, v.config.QueryVector, v.config.K, nil)
	if err != nil {
		return nil, fmt.Errorf("vector search failed: %w", err)
	}

	// 3. 根据ID获取完整行数据
	rows, err := v.fetchRowsByIDs(ctx, result.IDs)
	if err != nil {
		return nil, fmt.Errorf("fetch rows failed: %w", err)
	}

	// 4. 添加距离列
	for i, row := range rows {
		if i < len(result.Distances) {
			row["_distance"] = result.Distances[i]
		}
	}

	// 5. 获取列信息
	columns := v.GetSchema()
	// 添加_distance列
	hasDistanceCol := false
	for _, col := range columns {
		if col.Name == "_distance" {
			hasDistanceCol = true
			break
		}
	}
	if !hasDistanceCol {
		columns = append(columns, domain.ColumnInfo{
			Name:     "_distance",
			Type:     "float",
			Nullable: true,
		})
	}

	return &domain.QueryResult{
		Columns: columns,
		Rows:    rows,
		Total:   int64(len(rows)),
	}, nil
}

// fetchRowsByIDs 根据ID列表获取行数据
func (v *VectorScanOperator) fetchRowsByIDs(ctx context.Context, ids []int64) ([]domain.Row, error) {
	rows := make([]domain.Row, 0, len(ids))

	for _, id := range ids {
		// 使用Filter接口获取行数据
		filter := domain.Filter{
			Field:    "id",
			Operator: "=",
			Value:    id,
		}

		result, _, err := v.dataAccessService.Filter(ctx, v.config.TableName, filter, 0, 1)
		if err != nil || len(result) == 0 {
			continue
		}

		rows = append(rows, result[0])
	}

	return rows, nil
}
