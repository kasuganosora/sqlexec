package http

import (
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// HTTPFactory 实现 domain.DataSourceFactory
type HTTPFactory struct{}

// NewHTTPFactory 创建 HTTP 数据源工厂
func NewHTTPFactory() *HTTPFactory {
	return &HTTPFactory{}
}

// GetType 返回数据源类型
func (f *HTTPFactory) GetType() domain.DataSourceType {
	return domain.DataSourceTypeHTTP
}

// Create 创建 HTTP 数据源实例
func (f *HTTPFactory) Create(config *domain.DataSourceConfig) (domain.DataSource, error) {
	httpCfg, err := ParseHTTPConfig(config)
	if err != nil {
		return nil, err
	}

	return NewHTTPDataSource(config, httpCfg)
}
