package sql

import (
	"encoding/json"
	"fmt"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// SQLConfig holds shared SQL datasource configuration
type SQLConfig struct {
	// Connection pool
	MaxOpenConns    int `json:"max_open_conns,omitempty"`
	MaxIdleConns    int `json:"max_idle_conns,omitempty"`
	ConnMaxLifetime int `json:"conn_max_lifetime,omitempty"`  // seconds
	ConnMaxIdleTime int `json:"conn_max_idle_time,omitempty"` // seconds

	// TLS/SSL
	SSLMode     string `json:"ssl_mode,omitempty"`
	SSLCert     string `json:"ssl_cert,omitempty"`
	SSLKey      string `json:"ssl_key,omitempty"`
	SSLRootCert string `json:"ssl_root_cert,omitempty"`

	// MySQL-specific
	Charset   string `json:"charset,omitempty"`
	Collation string `json:"collation,omitempty"`
	ParseTime *bool  `json:"parse_time,omitempty"`

	// PostgreSQL-specific
	Schema string `json:"schema,omitempty"`

	// General
	ConnectTimeout int `json:"connect_timeout,omitempty"` // seconds
}

// ParseSQLConfig extracts SQLConfig from DataSourceConfig.Options
func ParseSQLConfig(dsCfg *domain.DataSourceConfig) (*SQLConfig, error) {
	cfg := &SQLConfig{}

	if dsCfg.Options != nil {
		data, err := json.Marshal(dsCfg.Options)
		if err != nil {
			return nil, fmt.Errorf("marshal options: %w", err)
		}
		if err := json.Unmarshal(data, cfg); err != nil {
			return nil, fmt.Errorf("unmarshal sql config: %w", err)
		}
	}

	// Apply defaults
	if cfg.MaxOpenConns <= 0 {
		cfg.MaxOpenConns = 25
	}
	if cfg.MaxIdleConns <= 0 {
		cfg.MaxIdleConns = 5
	}
	if cfg.ConnMaxLifetime <= 0 {
		cfg.ConnMaxLifetime = 300
	}
	if cfg.ConnMaxIdleTime <= 0 {
		cfg.ConnMaxIdleTime = 60
	}
	if cfg.ConnectTimeout <= 0 {
		cfg.ConnectTimeout = 10
	}
	if cfg.Charset == "" {
		cfg.Charset = "utf8mb4"
	}
	if cfg.Collation == "" {
		cfg.Collation = "utf8mb4_unicode_ci"
	}
	if cfg.ParseTime == nil {
		t := true
		cfg.ParseTime = &t
	}
	if cfg.Schema == "" {
		cfg.Schema = "public"
	}
	if cfg.SSLMode == "" {
		cfg.SSLMode = "disable"
	}

	return cfg, nil
}
