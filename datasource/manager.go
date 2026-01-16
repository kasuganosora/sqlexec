package datasource

import (
	"encoding/csv"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"sync"
)

type DataSource interface {
	GetData() ([]map[string]interface{}, error)
}

type CSVDataSource struct {
	filePath string
}

type JSONDataSource struct {
	filePath string
}

type HTTPDataSource struct {
	url string
}

type Manager struct {
	sources       map[string]DataSource
	mu            sync.RWMutex
	dataSourceDir string
}

func NewManager() *Manager {
	return &Manager{
		sources: make(map[string]DataSource),
	}
}

func (m *Manager) AddDataSource(name string, source DataSource) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sources[name] = source
}

func (m *Manager) GetDataSource(name string) (DataSource, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	source, ok := m.sources[name]
	return source, ok
}

func NewCSVDataSource(filePath string) *CSVDataSource {
	return &CSVDataSource{filePath: filePath}
}

func (ds *CSVDataSource) GetData() ([]map[string]interface{}, error) {
	file, err := os.Open(ds.filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	headers, err := reader.Read()
	if err != nil {
		return nil, err
	}

	var result []map[string]interface{}
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		row := make(map[string]interface{})
		for i, value := range record {
			row[headers[i]] = value
		}
		result = append(result, row)
	}

	return result, nil
}

func NewJSONDataSource(filePath string) *JSONDataSource {
	return &JSONDataSource{filePath: filePath}
}

func (ds *JSONDataSource) GetData() ([]map[string]interface{}, error) {
	file, err := os.ReadFile(ds.filePath)
	if err != nil {
		return nil, err
	}

	var result []map[string]interface{}
	if err := json.Unmarshal(file, &result); err != nil {
		return nil, err
	}

	return result, nil
}

func NewHTTPDataSource(url string) *HTTPDataSource {
	return &HTTPDataSource{url: url}
}

func (ds *HTTPDataSource) GetData() ([]map[string]interface{}, error) {
	resp, err := http.Get(ds.url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result []map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	return result, nil
}

// SetDataSourceDir 设置数据源目录
func (m *Manager) SetDataSourceDir(dir string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.dataSourceDir = dir
}

// GetDataSourceDir 返回数据源目录
func (m *Manager) GetDataSourceDir() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.dataSourceDir
}
