package operators

import (
	"context"
	"fmt"

	"github.com/kasuganosora/sqlexec/pkg/dataaccess"
	"github.com/kasuganosora/sqlexec/pkg/optimizer/plan"
	"github.com/kasuganosora/sqlexec/pkg/parser"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// InsertOperator INSERT算子
type InsertOperator struct {
	*BaseOperator
	config *plan.InsertConfig
}

// NewInsertOperator 创建INSERT算子
func NewInsertOperator(p *plan.Plan, das dataaccess.Service) (*InsertOperator, error) {
	config, ok := p.Config.(*plan.InsertConfig)
	if !ok {
		return nil, fmt.Errorf("invalid config type for Insert: %T", p.Config)
	}

	base := NewBaseOperator(p, das)
	return &InsertOperator{
		BaseOperator: base,
		config:       config,
	}, nil
}

// Execute 执行INSERT
func (op *InsertOperator) Execute(ctx context.Context) (*domain.QueryResult, error) {
	var rowsAffected int64
	var lastInsertID int64
	var err error

	// 处理 INSERT ... SELECT 的情况
	if len(op.plan.Children) > 0 {
		// 先执行SELECT获取数据
		child := op.children[0]
		selectResult, err := child.Execute(ctx)
		if err != nil {
			return nil, fmt.Errorf("execute SELECT for INSERT failed: %w", err)
		}

		// 将查询结果转换为值列表
		values := make([][]interface{}, 0, len(selectResult.Rows))
		for _, row := range selectResult.Rows {
			// 按列顺序提取值
			rowValues := make([]interface{}, 0, len(op.config.Columns))
			if len(op.config.Columns) > 0 {
				for _, col := range op.config.Columns {
					rowValues = append(rowValues, row[col])
				}
			} else {
				// 如果没有指定列，按SelectResult的列顺序
				for _, col := range selectResult.Columns {
					rowValues = append(rowValues, row[col.Name])
				}
			}
			values = append(values, rowValues)
		}

		// 插入数据
		rowsAffected, err = op.insertValues(ctx, values)
		if err != nil {
			return nil, err
		}
	} else {
		// 直接值插入
		// 转换parser.Expression为interface{}
		values := make([][]interface{}, len(op.config.Values))
		for i, row := range op.config.Values {
			values[i] = make([]interface{}, len(row))
			for j, expr := range row {
				values[i][j] = op.evaluateExpression(expr)
			}
		}

		rowsAffected, err = op.insertValues(ctx, values)
		if err != nil {
			return nil, err
		}
	}

	// 构建结果（返回影响的行数和最后的插入ID）
	result := &domain.QueryResult{
		Columns: []domain.ColumnInfo{
			{Name: "rows_affected", Type: "int", Nullable: false},
			{Name: "last_insert_id", Type: "int", Nullable: true},
		},
		Rows: []domain.Row{
			{"rows_affected": rowsAffected, "last_insert_id": lastInsertID},
		},
		Total: 1,
	}

	return result, nil
}

// insertValues 插入值列表
func (op *InsertOperator) insertValues(ctx context.Context, values [][]interface{}) (int64, error) {
	for i, row := range values {
		err := op.insertRow(ctx, row)
		if err != nil {
			return int64(i), fmt.Errorf("insert row %d failed: %w", i, err)
		}
	}
	return int64(len(values)), nil
}

// insertRow 插入单行
func (op *InsertOperator) insertRow(ctx context.Context, row []interface{}) error {
	// 使用数据访问服务插入数据
	insertData := make(map[string]interface{})
	
	// 如果指定了列，按指定列插入
	if len(op.config.Columns) > 0 {
		for i, col := range op.config.Columns {
			if i < len(row) {
				insertData[col] = row[i]
			}
		}
	} else {
		// 否则插入所有值（假设顺序匹配表的所有列）
		// 这里简化实现，实际应该从表schema获取列名
		for i, val := range row {
			insertData[fmt.Sprintf("col_%d", i)] = val
		}
	}

	err := op.dataAccessService.Insert(ctx, op.config.TableName, insertData)
	if err != nil {
		// 处理 ON DUPLICATE KEY UPDATE：仅在插入失败时才执行更新
		if op.config.OnDuplicate != nil {
			// 构建更新数据
			updateData := make(map[string]interface{})
			for col, expr := range *op.config.OnDuplicate {
				updateData[col] = op.evaluateExpression(expr)
			}

			// 使用插入数据构建过滤条件，只更新匹配行
			subFilters := make([]domain.Filter, 0, len(insertData))
			for k, v := range insertData {
				subFilters = append(subFilters, domain.Filter{
					Field:    k,
					Operator: "=",
					Value:    v,
				})
			}
			filter := &domain.Filter{
				Logic:      "AND",
				SubFilters: subFilters,
			}

			if updateErr := op.dataAccessService.Update(ctx, op.config.TableName, updateData, filter); updateErr != nil {
				return fmt.Errorf("ON DUPLICATE KEY UPDATE failed: %w", updateErr)
			}
			return nil
		}
		return fmt.Errorf("insert data failed: %w", err)
	}

	return nil
}

// evaluateExpression 评估表达式
func (op *InsertOperator) evaluateExpression(expr parser.Expression) interface{} {
	if expr.Type == parser.ExprTypeValue {
		return expr.Value
	}
	// 对于常量表达式，简单处理
	// 实际实现需要完整的表达式求值器
	return nil
}
