package datasource

import (
	"context"
	"fmt"
	"strings"
)

// Executor 查询执行器
type Executor struct {
	config           *Config
	tableProcessor   *TableProcessor
	filter           *Filter
	functionManager  *FunctionManager
	subqueryExecutor *SubqueryExecutor
}

// NewExecutor 创建执行器
func NewExecutor(config *Config) *Executor {
	functionManager := NewFunctionManager()
	executor := &Executor{
		config:          config,
		functionManager: functionManager,
	}
	subqueryExecutor := NewSubqueryExecutor(executor)
	tableProcessor := NewTableProcessor(config)
	filter := NewFilter(functionManager, subqueryExecutor)

	executor.subqueryExecutor = subqueryExecutor
	executor.tableProcessor = tableProcessor
	executor.filter = filter

	return executor
}

// ExecuteQuery 执行查询
func (e *Executor) ExecuteQuery(ctx context.Context, query *Query) ([]Row, error) {
	switch query.Type {
	case QueryTypeSelect:
		return e.executeSelect(ctx, query)
	case QueryTypeInsert:
		return e.executeInsert(ctx, query)
	case QueryTypeUpdate:
		return e.executeUpdate(ctx, query)
	case QueryTypeDelete:
		return e.executeDelete(ctx, query)
	default:
		return nil, fmt.Errorf("unsupported query type: %d", query.Type)
	}
}

// executeSelect 执行 SELECT 查询
func (e *Executor) executeSelect(ctx context.Context, query *Query) ([]Row, error) {
	// 获取主表数据
	mainTable := query.Table
	mainRows, err := e.tableProcessor.ReadTableData(mainTable)
	if err != nil {
		return nil, fmt.Errorf("failed to read main table: %v", err)
	}

	// 获取主表配置
	mainTableConfig, err := e.tableProcessor.GetTableConfig(mainTable)
	if err != nil {
		return nil, fmt.Errorf("failed to get main table config: %v", err)
	}

	// 处理 JOIN
	var rows []Row
	if len(query.Joins) > 0 {
		joinedRows, err := e.tableProcessor.ProcessJoins(mainRows, query.Joins, mainTableConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to process joins: %v", err)
		}
		rows = joinedRows
	} else {
		// 将 [][]interface{} 转换为 []Row
		rows = make([]Row, len(mainRows))
		for i, row := range mainRows {
			rowMap := make(Row)
			for j, field := range mainTableConfig.Fields {
				if j < len(row) {
					rowMap[field.Name] = row[j]
				}
			}
			rows[i] = rowMap
		}
	}

	// 处理 WHERE 条件
	if len(query.Where) > 0 {
		filteredRows := make([]Row, 0)
		for _, row := range rows {
			if e.filter.MatchConditions(row, query.Where, mainTableConfig) {
				filteredRows = append(filteredRows, row)
			}
		}
		rows = filteredRows
	}

	// 处理 GROUP BY
	if len(query.GroupBy) > 0 {
		rows, err = e.processGroupBy(rows, query)
		if err != nil {
			return nil, fmt.Errorf("failed to process group by: %v", err)
		}
	}

	// 处理 HAVING
	if len(query.Having) > 0 {
		filteredRows := make([]Row, 0)
		for _, row := range rows {
			if e.filter.MatchConditions(row, query.Having, mainTableConfig) {
				filteredRows = append(filteredRows, row)
			}
		}
		rows = filteredRows
	}

	// 处理 ORDER BY
	if len(query.OrderBy) > 0 {
		rows, err = e.processOrderBy(rows, query.OrderBy)
		if err != nil {
			return nil, fmt.Errorf("failed to process order by: %v", err)
		}
	}

	// 处理 LIMIT
	if query.Limit > 0 {
		if query.Limit < len(rows) {
			rows = rows[:query.Limit]
		}
	}

	// 处理 SELECT 字段
	if len(query.Fields) > 0 && query.Fields[0] != "*" {
		rows, err = e.processSelectFields(rows, query.Fields)
		if err != nil {
			return nil, fmt.Errorf("failed to process select fields: %v", err)
		}
	}

	return rows, nil
}

// processGroupBy 处理 GROUP BY
func (e *Executor) processGroupBy(rows []Row, query *Query) ([]Row, error) {
	groups := make(map[string][]Row)
	for _, row := range rows {
		key := make([]string, len(query.GroupBy))
		for i, field := range query.GroupBy {
			key[i] = fmt.Sprintf("%v", row[field])
		}
		groupKey := strings.Join(key, "|")
		groups[groupKey] = append(groups[groupKey], row)
	}

	result := make([]Row, 0)
	for _, group := range groups {
		groupRow := make(Row)
		// 复制第一个非聚合字段的值
		for k, v := range group[0] {
			if !strings.Contains(k, "(") {
				groupRow[k] = v
			}
		}
		// 处理聚合字段
		for _, field := range query.Fields {
			if strings.Contains(field, "(") {
				// 解析聚合函数
				funcName := strings.Split(field, "(")[0]
				arg := strings.TrimSuffix(strings.Split(field, "(")[1], ")")
				// 计算聚合值
				var value interface{}
				switch funcName {
				case "COUNT":
					value = len(group)
				case "SUM":
					sum := 0.0
					for _, row := range group {
						if v, ok := row[arg].(float64); ok {
							sum += v
						}
					}
					value = sum
				case "AVG":
					sum := 0.0
					for _, row := range group {
						if v, ok := row[arg].(float64); ok {
							sum += v
						}
					}
					value = sum / float64(len(group))
				case "MAX":
					max := -1.0
					for _, row := range group {
						if v, ok := row[arg].(float64); ok {
							if v > max {
								max = v
							}
						}
					}
					value = max
				case "MIN":
					min := 999999.0
					for _, row := range group {
						if v, ok := row[arg].(float64); ok {
							if v < min {
								min = v
							}
						}
					}
					value = min
				}
				groupRow[field] = value
			}
		}
		result = append(result, groupRow)
	}
	return result, nil
}

// processOrderBy 处理 ORDER BY
func (e *Executor) processOrderBy(rows []Row, orderBy []OrderBy) ([]Row, error) {
	// 实现排序逻辑
	return rows, nil
}

// processSelectFields 处理 SELECT 字段
func (e *Executor) processSelectFields(rows []Row, fields []string) ([]Row, error) {
	result := make([]Row, len(rows))
	for i, row := range rows {
		newRow := make(Row)
		for _, field := range fields {
			if value, exists := row[field]; exists {
				newRow[field] = value
			}
		}
		result[i] = newRow
	}
	return result, nil
}

// executeInsert 执行 INSERT 查询
func (e *Executor) executeInsert(ctx context.Context, query *Query) ([]Row, error) {
	// 实现 INSERT 逻辑
	return nil, nil
}

// executeUpdate 执行 UPDATE 查询
func (e *Executor) executeUpdate(ctx context.Context, query *Query) ([]Row, error) {
	// 实现 UPDATE 逻辑
	return nil, nil
}

// executeDelete 执行 DELETE 查询
func (e *Executor) executeDelete(ctx context.Context, query *Query) ([]Row, error) {
	// 实现 DELETE 逻辑
	return nil, nil
}
