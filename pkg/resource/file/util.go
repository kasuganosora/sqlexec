package file

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/resource/util"
)

// ==================== 文件操作工具函数 ====================

// GetFileExtension 获取文件扩展名
func GetFileExtension(filePath string) string {
	ext := filepath.Ext(filePath)
	return strings.ToLower(ext)
}

// IsSupportedFileType 检查文件类型是否支持
func IsSupportedFileType(filePath string) bool {
	ext := GetFileExtension(filePath)
	supportedTypes := []string{".csv", ".json", ".xlsx", ".xls", ".parquet"}

	for _, t := range supportedTypes {
		if ext == t {
			return true
		}
	}
	return false
}

// GetDataSourceTypeFromFileExt 根据文件扩展名获取数据源类型
func GetDataSourceTypeFromFileExt(filePath string) domain.DataSourceType {
	ext := GetFileExtension(filePath)

	switch ext {
	case ".csv":
		return domain.DataSourceTypeCSV
	case ".json":
		return domain.DataSourceTypeJSON
	case ".xlsx", ".xls":
		return domain.DataSourceTypeExcel
	case ".parquet":
		return domain.DataSourceTypeParquet
	default:
		return ""
	}
}

// EnsureFileExists 确保文件存在，如果不存在则创建
func EnsureFileExists(filePath string) error {
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		// 创建文件
		dir := filepath.Dir(filePath)
		if err := os.MkdirAll(dir, 0755); err != nil {
			return err
		}
		file, err := os.Create(filePath)
		if err != nil {
			return err
		}
		return file.Close()
	}
	return nil
}

// GetFileSize 获取文件大小
func GetFileSize(filePath string) (int64, error) {
	info, err := os.Stat(filePath)
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

// ReadFileContent 读取文件内容
func ReadFileContent(filePath string) ([]byte, error) {
	return os.ReadFile(filePath)
}

// WriteFileContent 写入文件内容
func WriteFileContent(filePath string, content []byte) error {
	return os.WriteFile(filePath, content, 0644)
}

// ==================== 行处理工具函数 ====================

// FilterRowsByNeededColumns 根据需要的列过滤行
func FilterRowsByNeededColumns(rows []domain.Row, neededColumns []string) []domain.Row {
	if len(neededColumns) == 0 {
		return rows
	}

	result := make([]domain.Row, len(rows))
	for i, row := range rows {
		filtered := make(domain.Row)
		for _, col := range neededColumns {
			if val, ok := row[col]; ok {
				filtered[col] = val
			}
		}
		result[i] = filtered
	}

	return result
}

// ApplyFiltersToRows 对行应用过滤器
func ApplyFiltersToRows(rows []domain.Row, filters []domain.Filter) []domain.Row {
	if len(filters) == 0 {
		return rows
	}

	result := make([]domain.Row, 0)
	for _, row := range rows {
		if util.MatchesFilters(row, filters) {
			result = append(result, row)
		}
	}
	return result
}

// SortRows 对行排序
func SortRows(rows []domain.Row, orderBy string, order string) []domain.Row {
	options := &domain.QueryOptions{
		OrderBy: orderBy,
		Order:   order,
	}
	return util.ApplyOrder(rows, options)
}

// PaginateRows 对行分页
func PaginateRows(rows []domain.Row, offset, limit int) []domain.Row {
	return util.ApplyPagination(rows, offset, limit)
}

// ConvertRowToStringMap 将行转换为字符串映射
func ConvertRowToStringMap(row domain.Row) map[string]string {
	result := make(map[string]string)
	for k, v := range row {
		if v != nil {
			result[k] = fmt.Sprintf("%v", v)
		} else {
			result[k] = ""
		}
	}
	return result
}

// MergeRows 合并多个行集
func MergeRows(rowSets ...[]domain.Row) []domain.Row {
	totalLen := 0
	for _, set := range rowSets {
		totalLen += len(set)
	}

	result := make([]domain.Row, 0, totalLen)
	for _, set := range rowSets {
		result = append(result, set...)
	}

	return result
}
