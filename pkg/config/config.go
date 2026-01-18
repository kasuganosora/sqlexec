package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// Config 应用程序配置
type Config struct {
	Server        ServerConfig        `json:"server"`
	Database      DatabaseConfig      `json:"database"`
	Log           LogConfig           `json:"log"`
	Pool          PoolConfig          `json:"pool"`
	Cache         CacheConfig         `json:"cache"`
	Monitor       MonitorConfig       `json:"monitor"`
	Connection    ConnectionConfig    `json:"connection"`
	MVCC          MVCCConfig          `json:"mvcc"`
	Session       SessionConfig       `json:"session"`
	Optimizer     OptimizerConfig     `json:"optimizer"`
}

// ServerConfig 服务器配置
type ServerConfig struct {
	Host            string        `json:"host"`
	Port            int           `json:"port"`
	ServerVersion   string        `json:"server_version"`
	KeepAlivePeriod time.Duration `json:"keep_alive_period"`
}

// DatabaseConfig 数据库配置
type DatabaseConfig struct {
	MaxConnections int `json:"max_connections"`
	IdleTimeout   int `json:"idle_timeout"` // seconds
}

// LogConfig 日志配置
type LogConfig struct {
	Level  string `json:"level"`
	Format string `json:"format"` // json or text
}

// PoolConfig 池配置
type PoolConfig struct {
	GoroutinePool GoroutinePoolConfig `json:"goroutine_pool"`
	ObjectPool    ObjectPoolConfig    `json:"object_pool"`
}

// GoroutinePoolConfig goroutine池配置
type GoroutinePoolConfig struct {
	MaxWorkers int `json:"max_workers"`
	QueueSize  int `json:"queue_size"`
}

// ObjectPoolConfig 对象池配置
type ObjectPoolConfig struct {
	MaxSize int `json:"max_size"`
	MinIdle int `json:"min_idle"`
	MaxIdle int `json:"max_idle"`
}

// CacheConfig 缓存配置
type CacheConfig struct {
	QueryCache  CachePoolConfig `json:"query_cache"`
	ResultCache CachePoolConfig `json:"result_cache"`
	SchemaCache CachePoolConfig `json:"schema_cache"`
}

// CachePoolConfig 缓存池配置
type CachePoolConfig struct {
	MaxSize int           `json:"max_size"`
	TTL     time.Duration `json:"ttl"`
}

// MonitorConfig 监控配置
type MonitorConfig struct {
	SlowQuery SlowQueryConfig `json:"slow_query"`
}

// SlowQueryConfig 慢查询配置
type SlowQueryConfig struct {
	Threshold  time.Duration `json:"threshold"`
	MaxEntries int           `json:"max_entries"`
}

// ConnectionConfig 连接池配置
type ConnectionConfig struct {
	MaxOpen     int           `json:"max_open"`
	MaxIdle     int           `json:"max_idle"`
	Lifetime    time.Duration `json:"lifetime"`
	IdleTimeout time.Duration `json:"idle_timeout"`
}

// MVCCConfig MVCC配置
type MVCCConfig struct {
	EnableWarning      bool          `json:"enable_warning"`
	AutoDowngrade      bool          `json:"auto_downgrade"`
	GCInterval         time.Duration `json:"gc_interval"`
	GCAgeThreshold     time.Duration `json:"gc_age_threshold"`
	XIDWrapThreshold   uint32        `json:"xid_wrap_threshold"`
	MaxActiveTxns      int           `json:"max_active_txns"`
}

// SessionConfig 会话配置
type SessionConfig struct {
	MaxAge       time.Duration `json:"max_age"`
	GCInterval   time.Duration `json:"gc_interval"`
}

// OptimizerConfig 优化器配置
type OptimizerConfig struct {
	Enabled bool `json:"enabled"`
}

// DefaultConfig 返回默认配置
func DefaultConfig() *Config {
	return &Config{
		Server: ServerConfig{
			Host:            "0.0.0.0",
			Port:            3306,
			ServerVersion:   "SqlExc",
			KeepAlivePeriod: 30 * time.Second,
		},
		Database: DatabaseConfig{
			MaxConnections: 100,
			IdleTimeout:    3600,
		},
		Log: LogConfig{
			Level:  "info",
			Format: "text",
		},
		Pool: PoolConfig{
			GoroutinePool: GoroutinePoolConfig{
				MaxWorkers: 10,
				QueueSize:  1000,
			},
			ObjectPool: ObjectPoolConfig{
				MaxSize: 100,
				MinIdle: 2,
				MaxIdle: 50,
			},
		},
		Cache: CacheConfig{
			QueryCache: CachePoolConfig{
				MaxSize: 1000,
				TTL:     5 * time.Minute,
			},
			ResultCache: CachePoolConfig{
				MaxSize: 1000,
				TTL:     10 * time.Minute,
			},
			SchemaCache: CachePoolConfig{
				MaxSize: 100,
				TTL:     1 * time.Hour,
			},
		},
		Monitor: MonitorConfig{
			SlowQuery: SlowQueryConfig{
				Threshold:  1 * time.Second,
				MaxEntries: 1000,
			},
		},
		Connection: ConnectionConfig{
			MaxOpen:     10,
			MaxIdle:     5,
			Lifetime:    30 * time.Minute,
			IdleTimeout: 5 * time.Minute,
		},
		MVCC: MVCCConfig{
			EnableWarning:      true,
			AutoDowngrade:      true,
			GCInterval:         5 * time.Minute,
			GCAgeThreshold:     1 * time.Hour,
			XIDWrapThreshold:   100000,
			MaxActiveTxns:      10000,
		},
		Session: SessionConfig{
			MaxAge:     24 * time.Hour,
			GCInterval: 1 * time.Minute,
		},
		Optimizer: OptimizerConfig{
			Enabled: true,
		},
	}
}

// LoadConfig 从文件加载配置
func LoadConfig(configPath string) (*Config, error) {
	// 如果没有指定配置文件，使用默认配置
	if configPath == "" {
		return DefaultConfig(), nil
	}

	// 检查配置文件是否存在
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("配置文件不存在: %s", configPath)
	}

	// 读取配置文件
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("读取配置文件失败: %w", err)
	}

	// 解析配置
	config := DefaultConfig()
	if err := json.Unmarshal(data, config); err != nil {
		return nil, fmt.Errorf("解析配置文件失败: %w", err)
	}

	// 验证配置
	if err := validateConfig(config); err != nil {
		return nil, err
	}

	return config, nil
}

// LoadConfigOrDefault 尝试从常见位置加载配置文件
func LoadConfigOrDefault() *Config {
	// 尝试的配置文件路径
	possiblePaths := []string{
		"config.json",
		"./config/config.json",
		"/etc/sqlexec/config.json",
	}

	// 尝试从环境变量获取配置文件路径
	if envPath := os.Getenv("SQLEXEC_CONFIG"); envPath != "" {
		if config, err := LoadConfig(envPath); err == nil {
			return config
		}
	}

	// 尝试从常见位置加载
	for _, path := range possiblePaths {
		if absPath, err := filepath.Abs(path); err == nil {
			if config, err := LoadConfig(absPath); err == nil {
				return config
			}
		}
	}

	// 使用默认配置
	return DefaultConfig()
}

// validateConfig 验证配置
func validateConfig(config *Config) error {
	if config.Server.Port < 1 || config.Server.Port > 65535 {
		return fmt.Errorf("无效的端口号: %d", config.Server.Port)
	}

	if config.Database.MaxConnections < 1 {
		return fmt.Errorf("最大连接数必须大于0")
	}

	if config.Pool.GoroutinePool.MaxWorkers < 1 {
		return fmt.Errorf("Goroutine池最大工作线程数必须大于0")
	}

	if config.Pool.GoroutinePool.QueueSize < 1 {
		return fmt.Errorf("Goroutine池队列大小必须大于0")
	}

	if config.Pool.ObjectPool.MaxSize < 1 {
		return fmt.Errorf("对象池最大大小必须大于0")
	}

	if config.Pool.ObjectPool.MinIdle < 0 {
		return fmt.Errorf("对象池最小空闲数不能为负数")
	}

	if config.Pool.ObjectPool.MaxIdle < 1 {
		return fmt.Errorf("对象池最大空闲数必须大于0")
	}

	if config.Connection.MaxOpen < 1 {
		return fmt.Errorf("连接池最大连接数必须大于0")
	}

	if config.Connection.MaxIdle < 1 {
		return fmt.Errorf("连接池最大空闲连接数必须大于0")
	}

	return nil
}

// GetListenAddress 返回监听地址
func (c *Config) GetListenAddress() string {
	return fmt.Sprintf("%s:%d", c.Server.Host, c.Server.Port)
}
