package datasource

import (
	"fmt"
	"sync"
)

// ResultProcessor 结果处理器
type ResultProcessor struct {
	executor    *Executor
	query       *Query
	tableConfig *TableConfig
	chunkSize   int
	objectPool  *sync.Pool
}

// NewResultProcessor 创建结果处理器
func NewResultProcessor(executor *Executor, query *Query, tableConfig *TableConfig) *ResultProcessor {
	return &ResultProcessor{
		executor:    executor,
		query:       query,
		tableConfig: tableConfig,
		chunkSize:   1000,         // 使用固定值
		objectPool:  &sync.Pool{}, // 创建新的对象池
	}
}

// Process 处理结果
func (p *ResultProcessor) Process(resultChan <-chan []interface{}) ([][]interface{}, error) {
	var results [][]interface{}
	chunk := make([][]interface{}, 0, p.chunkSize)

	// 处理字段选择
	if len(p.query.Fields) > 0 {
		for result := range resultChan {
			// 获取所有表的字段
			allFields := make([]Field, 0)
			allFields = append(allFields, p.tableConfig.Fields...)

			// 如果有 JOIN 表，添加 JOIN 表的字段
			if len(p.query.Joins) > 0 {
				for _, join := range p.query.Joins {
					if joinTable, ok := p.executor.config.Tables[join.Table]; ok {
						allFields = append(allFields, joinTable.Fields...)
					}
				}
			}

			// 创建临时 TableConfig 包含所有字段
			tempConfig := &TableConfig{
				Fields: allFields,
			}

			// 选择指定字段
			selected := make([]interface{}, 0, len(p.query.Fields))
			for _, field := range p.query.Fields {
				for i, f := range tempConfig.Fields {
					if f.Name == field && i < len(result) {
						selected = append(selected, result[i])
						break
					}
				}
			}

			if len(selected) > 0 {
				chunk = append(chunk, selected)
			}
			if len(chunk) >= p.chunkSize {
				results = append(results, chunk...)
				chunk = chunk[:0]
			}
		}
		if len(chunk) > 0 {
			results = append(results, chunk...)
		}
	} else {
		for result := range resultChan {
			chunk = append(chunk, result)
			if len(chunk) >= p.chunkSize {
				results = append(results, chunk...)
				chunk = chunk[:0]
			}
		}
		if len(chunk) > 0 {
			results = append(results, chunk...)
		}
	}

	fmt.Printf("[DEBUG] 聚合前原始results: %#v\n", results)

	// 处理GROUP BY
	if len(p.query.GroupBy) > 0 {
		// 按分组字段对结果进行分组
		groups := make(map[string][][]interface{})
		for _, row := range results {
			key := ""
			for _, field := range p.query.GroupBy {
				for i, f := range p.tableConfig.Fields {
					if f.Name == field && i < len(row) {
						key += fmt.Sprintf("%v|", row[i])
						break
					}
				}
			}
			groups[key] = append(groups[key], row)
		}

		// 合并分组结果
		results = make([][]interface{}, 0)
		for _, group := range groups {
			// 计算聚合值
			aggRow := make([]interface{}, len(p.query.Fields))
			for i, field := range p.query.Fields {
				var sum float64
				var count int
				for _, row := range group {
					for j, f := range p.tableConfig.Fields {
						if f.Name == field && j < len(row) {
							val := toFloat64(row[j])
							sum += val
							count++
							break
						}
					}
				}
				if count > 0 {
					aggRow[i] = sum / float64(count)
				}
			}
			results = append(results, aggRow)
		}

		// 调试输出
		fmt.Printf("[DEBUG] GroupBy结果: %#v\n", results)
		// 确保分组后的结果不为空
		if len(results) == 0 {
			return results, nil
		}
	}

	// 处理HAVING
	if len(p.query.Having) > 0 {
		// 将结果转换为 Row 类型以便应用 HAVING 条件
		rows := make([]Row, len(results))
		for i, result := range results {
			row := make(Row)
			for j, field := range p.query.Fields {
				if j < len(result) {
					row[field] = result[j]
				}
			}
			rows[i] = row
		}

		// 应用 HAVING 条件
		var filteredResults [][]interface{}
		for i, row := range rows {
			if p.executor.filter.MatchConditions(row, p.query.Having, p.tableConfig) {
				filteredResults = append(filteredResults, results[i])
			}
		}
		results = filteredResults
	}

	// 处理ORDER BY
	if len(p.query.OrderBy) > 0 {
		// 将结果转换为 Row 类型以便排序
		rows := make([]Row, len(results))
		for i, result := range results {
			row := make(Row)
			for j, field := range p.query.Fields {
				if j < len(result) {
					row[field] = result[j]
				}
			}
			rows[i] = row
		}

		// 排序
		for i := 0; i < len(p.query.OrderBy); i++ {
			orderBy := p.query.OrderBy[i]
			for j := 0; j < len(rows)-1; j++ {
				for k := j + 1; k < len(rows); k++ {
					val1 := toFloat64(rows[j][orderBy.Field])
					val2 := toFloat64(rows[k][orderBy.Field])
					if (orderBy.Direction == "ASC" && val1 > val2) ||
						(orderBy.Direction == "DESC" && val1 < val2) {
						rows[j], rows[k] = rows[k], rows[j]
						results[j], results[k] = results[k], results[j]
					}
				}
			}
		}
	}

	return results, nil
}
