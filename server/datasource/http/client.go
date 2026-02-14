package http

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	gohttp "net/http"
	"os"
	"strings"
	"time"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// HTTPClient 封装 HTTP 请求，支持认证、重试、模板头、TLS
type HTTPClient struct {
	client    *gohttp.Client
	config    *HTTPConfig
	dsCfg     *domain.DataSourceConfig
	connected bool
}

// NewHTTPClient 创建 HTTP 客户端
func NewHTTPClient(dsCfg *domain.DataSourceConfig, httpCfg *HTTPConfig) (*HTTPClient, error) {
	transport := &gohttp.Transport{}

	// TLS 配置
	if httpCfg.TLSSkipVerify || httpCfg.TLSCACert != "" {
		tlsConfig := &tls.Config{}
		if httpCfg.TLSSkipVerify {
			tlsConfig.InsecureSkipVerify = true
		}
		if httpCfg.TLSCACert != "" {
			caCert, err := os.ReadFile(httpCfg.TLSCACert)
			if err != nil {
				return nil, fmt.Errorf("failed to read CA cert: %w", err)
			}
			caCertPool := x509.NewCertPool()
			if !caCertPool.AppendCertsFromPEM(caCert) {
				return nil, fmt.Errorf("failed to parse CA cert")
			}
			tlsConfig.RootCAs = caCertPool
		}
		transport.TLSClientConfig = tlsConfig
	}

	client := &gohttp.Client{
		Transport: transport,
		Timeout:   httpCfg.GetTimeout(),
	}

	return &HTTPClient{
		client: client,
		config: httpCfg,
		dsCfg:  dsCfg,
	}, nil
}

// buildURL 构建完整 URL
func (c *HTTPClient) buildURL(pathTemplate string, table string) string {
	base := strings.TrimRight(c.dsCfg.Host, "/")
	path := c.config.BasePath + pathTemplate
	if table != "" {
		path = strings.ReplaceAll(path, "{table}", table)
	}
	return base + path
}

// DoGet 发送 GET 请求
func (c *HTTPClient) DoGet(pathTemplate string, table string, result interface{}) error {
	url := c.buildURL(pathTemplate, table)
	return c.doRequest("GET", url, nil, result)
}

// DoPost 发送 POST 请求
func (c *HTTPClient) DoPost(pathTemplate string, table string, body interface{}, result interface{}) error {
	url := c.buildURL(pathTemplate, table)
	return c.doRequest("POST", url, body, result)
}

// doRequest 执行 HTTP 请求（含认证、模板头、重试）
func (c *HTTPClient) doRequest(method, url string, body interface{}, result interface{}) error {
	var bodyBytes []byte
	var err error

	if body != nil {
		bodyBytes, err = json.Marshal(body)
		if err != nil {
			return fmt.Errorf("failed to marshal request body: %w", err)
		}
	}

	maxAttempts := c.config.RetryCount + 1
	var lastErr error

	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			time.Sleep(c.config.GetRetryDelay())
		}

		lastErr = c.doSingleRequest(method, url, bodyBytes, result)
		if lastErr == nil {
			return nil
		}

		// 只在 5xx 或连接错误时重试
		if httpErr, ok := lastErr.(*HTTPError); ok && httpErr.StatusCode < 500 {
			return lastErr
		}
	}
	return lastErr
}

// doSingleRequest 执行单次 HTTP 请求
func (c *HTTPClient) doSingleRequest(method, url string, bodyBytes []byte, result interface{}) error {
	var bodyReader io.Reader
	if bodyBytes != nil {
		bodyReader = bytes.NewReader(bodyBytes)
	}

	req, err := gohttp.NewRequest(method, url, bodyReader)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	if bodyBytes != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json")

	// 设置认证
	c.setAuth(req)

	// 设置模板头
	c.setTemplateHeaders(req, method, bodyBytes)

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("HTTP request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body: %w", err)
	}

	// 处理错误状态码
	if resp.StatusCode >= 400 {
		var errResp ErrorResponse
		if json.Unmarshal(respBody, &errResp) == nil && errResp.Error.Message != "" {
			return &HTTPError{
				StatusCode: resp.StatusCode,
				Code:       errResp.Error.Code,
				Message:    errResp.Error.Message,
			}
		}
		return &HTTPError{
			StatusCode: resp.StatusCode,
			Message:    string(respBody),
		}
	}

	// 解析响应
	if result != nil {
		if err := json.Unmarshal(respBody, result); err != nil {
			return fmt.Errorf("failed to parse response: %w (body: %s)", err, string(respBody))
		}
	}

	return nil
}

// setAuth 设置认证头
func (c *HTTPClient) setAuth(req *gohttp.Request) {
	switch c.config.AuthType {
	case "bearer":
		if c.config.AuthToken != "" {
			req.Header.Set("Authorization", "Bearer "+c.config.AuthToken)
		}
	case "basic":
		req.SetBasicAuth(c.dsCfg.Username, c.dsCfg.Password)
	case "api_key":
		if c.config.APIKeyValue != "" {
			req.Header.Set(c.config.APIKeyHeader, c.config.APIKeyValue)
		}
	}
}

// setTemplateHeaders 设置模板渲染后的自定义头
func (c *HTTPClient) setTemplateHeaders(req *gohttp.Request, method string, bodyBytes []byte) {
	if len(c.config.Headers) == 0 {
		return
	}

	bodyStr := ""
	if bodyBytes != nil {
		bodyStr = string(bodyBytes)
	}

	// 从 URL 中提取路径
	path := req.URL.Path

	ctx := &TemplateContext{
		Method:    method,
		Path:      path,
		Body:      bodyStr,
		AuthToken: c.config.AuthToken,
	}

	for key, tmpl := range c.config.Headers {
		value := RenderTemplate(tmpl, ctx)
		req.Header.Set(key, value)
	}
}

// HealthCheck 执行健康检查
func (c *HTTPClient) HealthCheck() error {
	var resp HealthResponse
	return c.DoGet(c.config.Paths.Health, "", &resp)
}

// HTTPError HTTP 错误
type HTTPError struct {
	StatusCode int
	Code       string
	Message    string
}

func (e *HTTPError) Error() string {
	if e.Code != "" {
		return fmt.Sprintf("HTTP %d [%s]: %s", e.StatusCode, e.Code, e.Message)
	}
	return fmt.Sprintf("HTTP %d: %s", e.StatusCode, e.Message)
}
