package xml

import (
	"fmt"
	"os"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// XMLFactory XML 数据源工厂
type XMLFactory struct{}

// NewXMLFactory 创建 XML 数据源工厂
func NewXMLFactory() *XMLFactory {
	return &XMLFactory{}
}

// GetType 实现 DataSourceFactory 接口
func (f *XMLFactory) GetType() domain.DataSourceType {
	return domain.DataSourceTypeXML
}

// GetMetadata 实现 DataSourceFactory 接口
func (f *XMLFactory) GetMetadata() domain.DriverMetadata {
	return domain.DriverMetadata{
		Comment:      "XML directory storage engine with MVCC transaction support",
		Transactions: "YES",
		XA:           "NO",
		Savepoints:   "NO",
	}
}

// Create 实现 DataSourceFactory 接口
func (f *XMLFactory) Create(config *domain.DataSourceConfig) (domain.DataSource, error) {
	if config == nil {
		return nil, fmt.Errorf("xml factory: config cannot be nil")
	}

	rootPath := config.Database
	if config.Options != nil {
		if p, ok := config.Options["path"]; ok {
			if str, ok := p.(string); ok && str != "" {
				rootPath = str
			}
		}
	}

	if rootPath == "" {
		return nil, fmt.Errorf("xml factory: root directory path required (set config.Database or options[\"path\"])")
	}

	// 验证路径是目录
	info, err := os.Stat(rootPath)
	if err != nil {
		return nil, fmt.Errorf("xml factory: failed to access path %q: %w", rootPath, err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("xml factory: path %q is not a directory", rootPath)
	}

	return NewXMLAdapter(config, rootPath), nil
}
