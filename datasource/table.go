package datasource

import (
	"fmt"
	"io"
	"strings"
	"time"
)

// TableProcessor 表处理器
type TableProcessor struct {
	config *Config
}

// NewTableProcessor 创建表处理器
func NewTableProcessor(config *Config) *TableProcessor {
	return &TableProcessor{
		config: config,
	}
}

// ReadTableData 读取表数据
func (p *TableProcessor) ReadTableData(tableName string) ([][]interface{}, error) {
	// 获取表配置
	tableConfig, ok := p.config.Tables[tableName]
	if !ok {
		return nil, fmt.Errorf("表不存在: %s", tableName)
	}

	// 创建读取器
	reader, err := NewReader(tableConfig)
	if err != nil {
		return nil, fmt.Errorf("创建读取器失败: %v", err)
	}
	defer reader.Close()

	// 创建转换器
	converter := NewConverter(tableConfig.Fields)

	// 读取数据
	var results [][]interface{}
	for {
		row, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		// 转换数据
		values, err := converter.Convert(row)
		if err != nil {
			return nil, err
		}

		results = append(results, values)
	}

	return results, nil
}

// GetTableConfig 获取表配置
func (p *TableProcessor) GetTableConfig(tableName string) (*TableConfig, error) {
	tableConfig, ok := p.config.Tables[tableName]
	if !ok {
		return nil, fmt.Errorf("表不存在: %s", tableName)
	}
	return tableConfig, nil
}

// ProcessJoins 处理表连接
func (p *TableProcessor) ProcessJoins(mainRows [][]interface{}, joins []Join, mainTableConfig *TableConfig) ([]Row, error) {
	result := mainRows
	var lastJoinTableConfig *TableConfig

	for _, join := range joins {
		// 获取连接表配置
		joinTableConfig, err := p.GetTableConfig(join.Table)
		if err != nil {
			return nil, err
		}
		lastJoinTableConfig = joinTableConfig

		// 读取连接表数据
		joinRows, err := p.ReadTableData(join.Table)
		if err != nil {
			return nil, err
		}

		// 执行连接
		result = p.performJoin(result, joinRows, join, mainTableConfig, joinTableConfig)
	}

	// 将结果转换为 Row 格式
	rows := make([]Row, len(result))
	for i, row := range result {
		rowMap := make(Row)
		// 添加主表字段
		for j, field := range mainTableConfig.Fields {
			if j < len(row) {
				rowMap[field.Name] = row[j]
			}
		}
		// 添加连接表字段
		if lastJoinTableConfig != nil {
			offset := len(mainTableConfig.Fields)
			for j, field := range lastJoinTableConfig.Fields {
				if offset+j < len(row) {
					rowMap[field.Name] = row[offset+j]
				}
			}
		}
		rows[i] = rowMap
	}

	return rows, nil
}

// performJoin 执行连接操作
func (p *TableProcessor) performJoin(mainRows [][]interface{}, joinRows [][]interface{}, join Join, mainTableConfig *TableConfig, joinTableConfig *TableConfig) [][]interface{} {
	var result [][]interface{}

	switch join.Type {
	case JoinTypeInner:
		// INNER JOIN：只保留匹配的行
		for _, mainRow := range mainRows {
			for _, joinRow := range joinRows {
				if p.matchJoinCondition(mainRow, joinRow, join.Condition, mainTableConfig, joinTableConfig) {
					combined := append(mainRow, joinRow...)
					result = append(result, combined)
				}
			}
		}

	case JoinTypeLeft:
		// LEFT JOIN：保留所有左表行，未匹配则补 NULL
		for _, mainRow := range mainRows {
			matched := false
			for _, joinRow := range joinRows {
				if p.matchJoinCondition(mainRow, joinRow, join.Condition, mainTableConfig, joinTableConfig) {
					combined := append(mainRow, joinRow...)
					result = append(result, combined)
					matched = true
				}
			}
			if !matched {
				// 创建包含 NULL 值的连接行
				nullRow := make([]interface{}, len(joinTableConfig.Fields))
				for i := range nullRow {
					nullRow[i] = nil
				}
				combined := append(mainRow, nullRow...)
				result = append(result, combined)
			}
		}

	case JoinTypeRight:
		// RIGHT JOIN：以右表为主，保留所有右表行，未匹配则补 NULL
		processedJoinRows := make(map[int]bool) // 记录已处理的右表行索引

		// 首先处理所有匹配的行
		for i, joinRow := range joinRows {
			if processedJoinRows[i] {
				continue
			}
			for _, mainRow := range mainRows {
				if p.matchJoinCondition(mainRow, joinRow, join.Condition, mainTableConfig, joinTableConfig) {
					combined := append(mainRow, joinRow...)
					result = append(result, combined)
					processedJoinRows[i] = true
					break
				}
			}
		}

		// 然后处理未匹配的右表行
		for i, joinRow := range joinRows {
			if !processedJoinRows[i] {
				// 创建包含 NULL 值的主表行
				nullRow := make([]interface{}, len(mainTableConfig.Fields))
				for i := range nullRow {
					nullRow[i] = nil
				}
				combined := append(nullRow, joinRow...)
				result = append(result, combined)
				processedJoinRows[i] = true
			}
		}
	}

	return result
}

// matchJoinCondition 检查连接条件
func (p *TableProcessor) matchJoinCondition(mainRow []interface{}, joinRow []interface{}, condition string, mainTableConfig *TableConfig, joinTableConfig *TableConfig) bool {
	// 解析连接条件（例如：main.id = join.main_id）
	parts := strings.Split(condition, "=")
	if len(parts) != 2 {
		return false
	}

	mainField := strings.TrimSpace(parts[0])
	joinField := strings.TrimSpace(parts[1])

	// 获取字段索引
	mainIndex := -1
	for i, field := range mainTableConfig.Fields {
		if field.Name == mainField {
			mainIndex = i
			break
		}
	}

	joinIndex := -1
	for i, field := range joinTableConfig.Fields {
		if field.Name == joinField {
			joinIndex = i
			break
		}
	}

	if mainIndex == -1 || joinIndex == -1 {
		return false
	}

	// 比较值
	return p.equal(mainRow[mainIndex], joinRow[joinIndex])
}

// equal 比较相等
func (p *TableProcessor) equal(a, b interface{}) bool {
	if a == nil || b == nil {
		return a == b
	}

	switch v1 := a.(type) {
	case string:
		if v2, ok := b.(string); ok {
			return v1 == v2
		}
	case int64:
		if v2, ok := b.(int64); ok {
			return v1 == v2
		}
	case float64:
		if v2, ok := b.(float64); ok {
			return v1 == v2
		}
	case bool:
		if v2, ok := b.(bool); ok {
			return v1 == v2
		}
	case time.Time:
		if v2, ok := b.(time.Time); ok {
			return v1.Equal(v2)
		}
	}
	return false
}
