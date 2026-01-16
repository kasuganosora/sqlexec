package datasource

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/tealeg/xlsx"
)

// Reader 数据读取器接口
type Reader interface {
	// Read 读取下一行数据，如果到达文件末尾返回 io.EOF
	Read() (Row, error)
	// Close 关闭读取器
	Close() error
}

// APIReader API数据读取器
type APIReader struct {
	config     *APIConfig
	client     *http.Client
	rows       []Row
	currentPos int
}

func NewAPIReader(config *APIConfig) (*APIReader, error) {
	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	// 构建请求
	req, err := http.NewRequest(config.Method, config.URL, nil)
	if err != nil {
		return nil, fmt.Errorf("创建请求失败: %v", err)
	}

	// 添加请求头
	for k, v := range config.Headers {
		req.Header.Set(k, v)
	}

	// 添加查询参数
	q := req.URL.Query()
	for k, v := range config.QueryParams {
		q.Set(k, v)
	}
	req.URL.RawQuery = q.Encode()

	// 发送请求
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("发送请求失败: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("API请求失败: %s", resp.Status)
	}

	// 解析响应
	var data []map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return nil, fmt.Errorf("解析响应失败: %v", err)
	}

	// 转换为Row格式
	rows := make([]Row, len(data))
	for i, item := range data {
		rows[i] = Row(item)
	}

	return &APIReader{
		config:     config,
		client:     client,
		rows:       rows,
		currentPos: 0,
	}, nil
}

func (r *APIReader) Read() (Row, error) {
	if r.currentPos >= len(r.rows) {
		return nil, io.EOF
	}
	row := r.rows[r.currentPos]
	r.currentPos++
	return row, nil
}

func (r *APIReader) Close() error {
	return nil
}

// JSONReader JSON文件读取器
// 支持一行一个对象（NDJSON）格式
type JSONReader struct {
	file    *os.File
	scanner *bufio.Scanner
}

func NewJSONReader(filePath string) (*JSONReader, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("打开文件失败: %v", err)
	}
	return &JSONReader{
		file:    file,
		scanner: bufio.NewScanner(file),
	}, nil
}

func (r *JSONReader) Read() (Row, error) {
	if !r.scanner.Scan() {
		if err := r.scanner.Err(); err != nil {
			return nil, err
		}
		return nil, io.EOF
	}
	line := r.scanner.Text()
	var row map[string]interface{}
	if err := json.Unmarshal([]byte(line), &row); err != nil {
		return nil, fmt.Errorf("解析JSON行失败: %v", err)
	}
	return Row(row), nil
}

func (r *JSONReader) Close() error {
	return r.file.Close()
}

// CSVReader CSV文件读取器
type CSVReader struct {
	file     *os.File
	reader   *csv.Reader
	headers  []string
	rowCount int
}

func NewCSVReader(filePath string) (*CSVReader, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("打开文件失败: %v", err)
	}

	reader := csv.NewReader(file)
	headers, err := reader.Read()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("读取CSV表头失败: %v", err)
	}

	return &CSVReader{
		file:     file,
		reader:   reader,
		headers:  headers,
		rowCount: 0,
	}, nil
}

func (r *CSVReader) Read() (Row, error) {
	record, err := r.reader.Read()
	if err != nil {
		return nil, err
	}

	row := make(Row)
	for i, value := range record {
		if i < len(r.headers) {
			row[r.headers[i]] = value
		}
	}

	r.rowCount++
	return row, nil
}

func (r *CSVReader) Close() error {
	return r.file.Close()
}

// XLSXReader Excel文件读取器
type XLSXReader struct {
	file     *xlsx.File
	sheet    *xlsx.Sheet
	headers  []string
	rowIndex int
}

func NewXLSXReader(filePath string, sheetName string) (*XLSXReader, error) {
	file, err := xlsx.OpenFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("打开Excel文件失败: %v", err)
	}

	var sheet *xlsx.Sheet
	if sheetName != "" {
		sheet = file.Sheet[sheetName]
	} else if len(file.Sheets) > 0 {
		sheet = file.Sheets[0]
	} else {
		// xlsx.File 没有 Close 方法，无需关闭
		return nil, fmt.Errorf("Excel文件没有工作表")
	}

	if len(sheet.Rows) == 0 {
		// xlsx.File 没有 Close 方法，无需关闭
		return nil, fmt.Errorf("工作表为空")
	}

	// 读取表头
	headers := make([]string, 0)
	for _, cell := range sheet.Rows[0].Cells {
		headers = append(headers, cell.String())
	}

	return &XLSXReader{
		file:     file,
		sheet:    sheet,
		headers:  headers,
		rowIndex: 1, // 从第二行开始（跳过表头）
	}, nil
}

func (r *XLSXReader) Read() (Row, error) {
	if r.rowIndex >= len(r.sheet.Rows) {
		return nil, io.EOF
	}

	row := make(Row)
	for i, cell := range r.sheet.Rows[r.rowIndex].Cells {
		if i < len(r.headers) {
			row[r.headers[i]] = cell.String()
		}
	}

	r.rowIndex++
	return row, nil
}

func (r *XLSXReader) Close() error {
	return nil
}

// 创建数据读取器
func NewReader(config *TableConfig) (Reader, error) {
	switch config.Type {
	case "api":
		return NewAPIReader(config.APIConfig)
	case "json":
		return NewJSONReader(config.FilePath)
	case "csv":
		return NewCSVReader(config.FilePath)
	case "xlsx":
		return NewXLSXReader(config.FilePath, config.SheetName)
	default:
		return nil, fmt.Errorf("不支持的数据源类型: %s", config.Type)
	}
}
