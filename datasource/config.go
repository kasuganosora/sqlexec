package datasource

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/tealeg/xlsx"
)

// 配置管理器
type ConfigManager struct {
	databases map[string]*DatabaseConfig
	baseDir   string
}

// 数据库配置
type DatabaseConfig struct {
	Name   string                  `json:"name"`
	Tables map[string]*TableConfig `json:"tables"`
}

func NewConfigManager(baseDir string) *ConfigManager {
	return &ConfigManager{
		databases: make(map[string]*DatabaseConfig),
		baseDir:   baseDir,
	}
}

// 加载所有数据库配置
func (cm *ConfigManager) LoadAll() error {
	// 遍历baseDir下的所有目录
	entries, err := os.ReadDir(cm.baseDir)
	if err != nil {
		return fmt.Errorf("读取数据源目录失败: %v", err)
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		dbName := entry.Name()
		dbConfig := &DatabaseConfig{
			Name:   dbName,
			Tables: make(map[string]*TableConfig),
		}

		// 读取数据库目录下的所有文件
		dbPath := filepath.Join(cm.baseDir, dbName)
		files, err := os.ReadDir(dbPath)
		if err != nil {
			return fmt.Errorf("读取数据库目录失败 %s: %v", dbName, err)
		}

		for _, file := range files {
			if file.IsDir() {
				continue
			}

			fileName := file.Name()
			ext := strings.ToLower(filepath.Ext(fileName))
			baseName := strings.TrimSuffix(fileName, ext)

			// 根据文件类型创建表配置
			tableConfig := &TableConfig{
				Name:     baseName,
				FilePath: filepath.Join(dbPath, fileName),
			}

			switch {
			case strings.HasSuffix(fileName, ".api-config"):
				// API配置
				tableConfig.Type = "api"
				if err := cm.loadAPIConfig(tableConfig); err != nil {
					return fmt.Errorf("加载API配置失败 %s: %v", fileName, err)
				}
			case ext == ".json":
				// JSON文件
				tableConfig.Type = "json"
				if err := cm.loadJSONConfig(tableConfig); err != nil {
					return fmt.Errorf("加载JSON配置失败 %s: %v", fileName, err)
				}
			case ext == ".csv":
				// CSV文件
				tableConfig.Type = "csv"
				if err := cm.loadCSVConfig(tableConfig); err != nil {
					return fmt.Errorf("加载CSV配置失败 %s: %v", fileName, err)
				}
			case ext == ".xlsx":
				// Excel文件
				tableConfig.Type = "xlsx"
				if err := cm.loadXLSXConfig(tableConfig); err != nil {
					return fmt.Errorf("加载XLSX配置失败 %s: %v", fileName, err)
				}
			default:
				continue
			}

			dbConfig.Tables[baseName] = tableConfig
		}

		cm.databases[dbName] = dbConfig
	}

	return nil
}

// 加载API配置
func (cm *ConfigManager) loadAPIConfig(tableConfig *TableConfig) error {
	data, err := os.ReadFile(tableConfig.FilePath)
	if err != nil {
		return err
	}

	var apiConfig APIConfig
	if err := json.Unmarshal(data, &apiConfig); err != nil {
		return err
	}

	tableConfig.APIConfig = &apiConfig
	tableConfig.Fields = apiConfig.Fields
	return nil
}

// 加载JSON配置
func (cm *ConfigManager) loadJSONConfig(tableConfig *TableConfig) error {
	data, err := os.ReadFile(tableConfig.FilePath)
	if err != nil {
		return err
	}

	// 读取第一行来确定字段
	var firstRow map[string]interface{}
	if err := json.Unmarshal(data, &firstRow); err != nil {
		return err
	}

	fields := make([]Field, 0)
	for name, value := range firstRow {
		field := Field{
			Name:     name,
			Type:     inferFieldType(value),
			Nullable: true,
		}
		fields = append(fields, field)
	}

	tableConfig.Fields = fields
	return nil
}

// 加载CSV配置
func (cm *ConfigManager) loadCSVConfig(tableConfig *TableConfig) error {
	file, err := os.Open(tableConfig.FilePath)
	if err != nil {
		return err
	}
	defer file.Close()

	reader := csv.NewReader(file)

	// 读取表头
	headers, err := reader.Read()
	if err != nil {
		return err
	}

	// 读取前10行用于类型推断
	samples := make([][]string, 0)
	for i := 0; i < 10; i++ {
		row, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		samples = append(samples, row)
	}

	// 推断字段类型
	fields := make([]Field, len(headers))
	for i, header := range headers {
		field := Field{
			Name:     header,
			Type:     TypeString, // 默认类型
			Nullable: true,
		}

		// 根据样本数据推断类型
		if len(samples) > 0 {
			field.Type = inferCSVFieldType(samples, i)
		}

		fields[i] = field
	}

	tableConfig.Fields = fields
	return nil
}

// 加载XLSX配置
func (cm *ConfigManager) loadXLSXConfig(tableConfig *TableConfig) error {
	xlFile, err := xlsx.OpenFile(tableConfig.FilePath)
	if err != nil {
		return err
	}

	// 获取第一个sheet
	sheet := xlFile.Sheets[0]
	tableConfig.SheetName = sheet.Name

	if len(sheet.Rows) == 0 {
		return fmt.Errorf("sheet %s 为空", sheet.Name)
	}

	// 读取表头
	headers := make([]string, 0)
	for _, cell := range sheet.Rows[0].Cells {
		headers = append(headers, cell.String())
	}

	// 读取前10行用于类型推断
	samples := make([][]string, 0)
	for i := 1; i < min(11, len(sheet.Rows)); i++ {
		row := make([]string, 0)
		for _, cell := range sheet.Rows[i].Cells {
			row = append(row, cell.String())
		}
		samples = append(samples, row)
	}

	// 推断字段类型
	fields := make([]Field, len(headers))
	for i, header := range headers {
		field := Field{
			Name:     header,
			Type:     TypeString, // 默认类型
			Nullable: true,
		}

		// 根据样本数据推断类型
		if len(samples) > 0 {
			field.Type = inferCSVFieldType(samples, i)
		}

		fields[i] = field
	}

	tableConfig.Fields = fields
	return nil
}

// 推断字段类型
func inferFieldType(value interface{}) FieldType {
	switch v := value.(type) {
	case string:
		return TypeString
	case float64:
		if v == float64(int64(v)) {
			return TypeInt
		}
		return TypeFloat
	case bool:
		return TypeBoolean
	case nil:
		return TypeString
	default:
		return TypeString
	}
}

// 推断CSV字段类型
func inferCSVFieldType(samples [][]string, colIndex int) FieldType {
	// 尝试解析为数字
	hasInt := true
	hasFloat := true
	hasBool := true
	hasDate := true

	for _, row := range samples {
		if colIndex >= len(row) {
			continue
		}
		value := row[colIndex]
		if value == "" {
			continue
		}

		// 尝试解析为整数
		if _, err := strconv.ParseInt(value, 10, 64); err != nil {
			hasInt = false
		}

		// 尝试解析为浮点数
		if _, err := strconv.ParseFloat(value, 64); err != nil {
			hasFloat = false
		}

		// 尝试解析为布尔值
		if _, err := strconv.ParseBool(value); err != nil {
			hasBool = false
		}

		// 尝试解析为日期
		if _, err := time.Parse("2006-01-02", value); err != nil {
			if _, err := time.Parse("2006-01-02 15:04:05", value); err != nil {
				hasDate = false
			}
		}
	}

	// 按优先级返回类型
	if hasInt {
		return TypeInt
	}
	if hasFloat {
		return TypeFloat
	}
	if hasBool {
		return TypeBoolean
	}
	if hasDate {
		return TypeDate
	}
	return TypeString
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// 获取数据库配置
func (cm *ConfigManager) GetDatabase(name string) (*DatabaseConfig, bool) {
	db, ok := cm.databases[name]
	return db, ok
}

// 获取表配置
func (cm *ConfigManager) GetTable(dbName, tableName string) (*TableConfig, bool) {
	db, ok := cm.databases[dbName]
	if !ok {
		return nil, false
	}
	table, ok := db.Tables[tableName]
	return table, ok
}
