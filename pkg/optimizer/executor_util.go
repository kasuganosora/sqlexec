package optimizer

import (
	"strings"

	"github.com/kasuganosora/sqlexec/pkg/parser"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// filterColumns 过滤列信息
func filterColumns(columns []domain.ColumnInfo, selectCols []parser.SelectColumn) []domain.ColumnInfo {
	result := make([]domain.ColumnInfo, 0, len(selectCols))

	// 构建选择的列名映射
	selectMap := make(map[string]bool)
	for _, col := range selectCols {
		if !col.IsWildcard && col.Name != "" {
			selectMap[col.Name] = true
		}
	}

	// 过滤列
	for _, col := range columns {
		if selectMap[col.Name] {
			result = append(result, col)
		}
	}

	return result
}

// isInformationSchemaTable 检查表是否属于 information_schema
func isInformationSchemaTable(tableName string) bool {
	// Check for information_schema. prefix (case-insensitive)
	if strings.Contains(tableName, ".") {
		parts := strings.SplitN(tableName, ".", 2)
		if len(parts) == 2 && strings.ToLower(parts[0]) == "information_schema" {
			return true
		}
	}

	return false
}

// isInformationSchemaQuery 检查是否是 information_schema 查询
func isInformationSchemaQuery(tableName string, currentDB string, dsManager interface{}) bool {
	// 空表名不是 information_schema 查询（如 SELECT DATABASE()）
	if tableName == "" {
		return false
	}

	// Check for information_schema. prefix (case-insensitive)
	if strings.HasPrefix(strings.ToLower(tableName), "information_schema.") {
		return true
	}

	// 检查当前数据库是否为 information_schema
	if strings.EqualFold(currentDB, "information_schema") {
		return true
	}

	return false
}

// isConfigTable 检查表是否属于 config 虚拟数据库
func isConfigTable(tableName string) bool {
	if strings.Contains(tableName, ".") {
		parts := strings.SplitN(tableName, ".", 2)
		if len(parts) == 2 && strings.ToLower(parts[0]) == "config" {
			return true
		}
	}
	return false
}

// isConfigQuery 检查是否是 config 虚拟数据库查询
func isConfigQuery(tableName string, currentDB string) bool {
	if tableName == "" {
		return false
	}

	if strings.HasPrefix(strings.ToLower(tableName), "config.") {
		return true
	}

	if strings.EqualFold(currentDB, "config") {
		return true
	}

	return false
}
