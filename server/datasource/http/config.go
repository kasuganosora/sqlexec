package http

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// 默认路径模板
const (
	DefaultPathTables  = "/_schema/tables"
	DefaultPathSchema  = "/_schema/tables/{table}"
	DefaultPathQuery   = "/_query/{table}"
	DefaultPathInsert  = "/_insert/{table}"
	DefaultPathUpdate  = "/_update/{table}"
	DefaultPathDelete  = "/_delete/{table}"
	DefaultPathHealth  = "/_health"

	DefaultTimeoutMs   = 30000
	DefaultRetryCount  = 0
	DefaultRetryDelayMs = 1000
)

// PathsConfig 路径配置
type PathsConfig struct {
	Tables string `json:"tables,omitempty"`
	Schema string `json:"schema,omitempty"`
	Query  string `json:"query,omitempty"`
	Insert string `json:"insert,omitempty"`
	Update string `json:"update,omitempty"`
	Delete string `json:"delete,omitempty"`
	Health string `json:"health,omitempty"`
}

// ACLConfig ACL 配置
type ACLConfig struct {
	AllowedUsers []string            `json:"allowed_users,omitempty"`
	Permissions  map[string][]string `json:"permissions,omitempty"`
}

// HTTPConfig HTTP 数据源完整配置（从 DataSourceConfig.Options 解析）
type HTTPConfig struct {
	// 路径
	BasePath string       `json:"base_path,omitempty"`
	Paths    *PathsConfig `json:"paths,omitempty"`

	// 认证
	AuthType     string `json:"auth_type,omitempty"`      // bearer, basic, api_key, ""
	AuthToken    string `json:"auth_token,omitempty"`      // Bearer token 或签名密钥
	APIKeyHeader string `json:"api_key_header,omitempty"`  // API Key header 名
	APIKeyValue  string `json:"api_key_value,omitempty"`   // API Key 值

	// 超时与重试
	TimeoutMs    int `json:"timeout_ms,omitempty"`
	RetryCount   int `json:"retry_count,omitempty"`
	RetryDelayMs int `json:"retry_delay_ms,omitempty"`

	// TLS
	TLSSkipVerify bool   `json:"tls_skip_verify,omitempty"`
	TLSCACert     string `json:"tls_ca_cert,omitempty"`

	// 自定义头（支持模板）
	Headers map[string]string `json:"headers,omitempty"`

	// 数据库/表名映射
	Database   string            `json:"database,omitempty"`
	TableAlias map[string]string `json:"table_alias,omitempty"`

	// ACL
	ACL *ACLConfig `json:"acl,omitempty"`
}

// ParseHTTPConfig 从 DataSourceConfig 解析 HTTPConfig
func ParseHTTPConfig(dsCfg *domain.DataSourceConfig) (*HTTPConfig, error) {
	cfg := &HTTPConfig{}

	if dsCfg.Options != nil {
		data, err := json.Marshal(dsCfg.Options)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal options: %w", err)
		}
		if err := json.Unmarshal(data, cfg); err != nil {
			return nil, fmt.Errorf("failed to parse HTTP options: %w", err)
		}
	}

	// 如果 basic auth，使用顶层 username/password
	if cfg.AuthType == "basic" && dsCfg.Username != "" {
		// basic auth 凭证从顶层取
	}

	// 设置默认值
	cfg.applyDefaults(dsCfg)

	return cfg, nil
}

// applyDefaults 设置默认值
func (c *HTTPConfig) applyDefaults(dsCfg *domain.DataSourceConfig) {
	if c.Paths == nil {
		c.Paths = &PathsConfig{}
	}
	if c.Paths.Tables == "" {
		c.Paths.Tables = DefaultPathTables
	}
	if c.Paths.Schema == "" {
		c.Paths.Schema = DefaultPathSchema
	}
	if c.Paths.Query == "" {
		c.Paths.Query = DefaultPathQuery
	}
	if c.Paths.Insert == "" {
		c.Paths.Insert = DefaultPathInsert
	}
	if c.Paths.Update == "" {
		c.Paths.Update = DefaultPathUpdate
	}
	if c.Paths.Delete == "" {
		c.Paths.Delete = DefaultPathDelete
	}
	if c.Paths.Health == "" {
		c.Paths.Health = DefaultPathHealth
	}
	if c.TimeoutMs <= 0 {
		c.TimeoutMs = DefaultTimeoutMs
	}
	if c.RetryDelayMs <= 0 {
		c.RetryDelayMs = DefaultRetryDelayMs
	}
	if c.APIKeyHeader == "" {
		c.APIKeyHeader = "X-API-Key"
	}

	// database 默认使用 config.Name
	if c.Database == "" {
		c.Database = dsCfg.Name
	}
}

// GetTimeout 获取超时时间
func (c *HTTPConfig) GetTimeout() time.Duration {
	return time.Duration(c.TimeoutMs) * time.Millisecond
}

// GetRetryDelay 获取重试延迟
func (c *HTTPConfig) GetRetryDelay() time.Duration {
	return time.Duration(c.RetryDelayMs) * time.Millisecond
}

// ResolveTableName 将 SQL 表名解析为 HTTP 端表名
func (c *HTTPConfig) ResolveTableName(sqlTable string) string {
	if c.TableAlias != nil {
		if httpTable, ok := c.TableAlias[sqlTable]; ok {
			return httpTable
		}
	}
	return sqlTable
}

// CheckACL 检查用户是否有指定操作权限
// 返回 nil 表示允许，否则返回权限拒绝错误
func (c *HTTPConfig) CheckACL(user string, operation string) error {
	if c.ACL == nil {
		return nil
	}

	// 检查 allowed_users
	if len(c.ACL.AllowedUsers) > 0 {
		allowed := false
		for _, u := range c.ACL.AllowedUsers {
			if u == user {
				allowed = true
				break
			}
		}
		if !allowed {
			return fmt.Errorf("access denied for user '%s' to HTTP datasource", user)
		}
	}

	// 检查 permissions
	if c.ACL.Permissions != nil {
		perms, ok := c.ACL.Permissions[user]
		if !ok {
			// 用户在 allowed_users 中但没有 permissions 条目，默认允许所有操作
			return nil
		}
		for _, p := range perms {
			if p == operation || p == "ALL" || p == "ALL PRIVILEGES" {
				return nil
			}
		}
		return fmt.Errorf("access denied: user '%s' does not have %s permission on HTTP datasource", user, operation)
	}

	return nil
}
