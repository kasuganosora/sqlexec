package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()

	// 验证服务器配置
	assert.Equal(t, "0.0.0.0", config.Server.Host)
	assert.Equal(t, 3306, config.Server.Port)
	assert.Equal(t, "SqlExc", config.Server.ServerVersion)
	assert.Equal(t, 30*time.Second, config.Server.KeepAlivePeriod)

	// 验证数据库配置
	assert.Equal(t, 100, config.Database.MaxConnections)
	assert.Equal(t, 3600, config.Database.IdleTimeout)
	assert.Equal(t, []string{"memory", "csv", "excel", "json", "mysql", "sqlite", "parquet"}, config.Database.EnabledSources)

	// 验证日志配置
	assert.Equal(t, "info", config.Log.Level)
	assert.Equal(t, "text", config.Log.Format)

	// 验证池配置
	assert.Equal(t, 10, config.Pool.GoroutinePool.MaxWorkers)
	assert.Equal(t, 1000, config.Pool.GoroutinePool.QueueSize)
	assert.Equal(t, 100, config.Pool.ObjectPool.MaxSize)
	assert.Equal(t, 2, config.Pool.ObjectPool.MinIdle)
	assert.Equal(t, 50, config.Pool.ObjectPool.MaxIdle)

	// 验证缓存配置
	assert.Equal(t, 1000, config.Cache.QueryCache.MaxSize)
	assert.Equal(t, 5*time.Minute, config.Cache.QueryCache.TTL)
	assert.Equal(t, 1000, config.Cache.ResultCache.MaxSize)
	assert.Equal(t, 10*time.Minute, config.Cache.ResultCache.TTL)
	assert.Equal(t, 100, config.Cache.SchemaCache.MaxSize)
	assert.Equal(t, 1*time.Hour, config.Cache.SchemaCache.TTL)

	// 验证监控配置
	assert.Equal(t, 1*time.Second, config.Monitor.SlowQuery.Threshold)
	assert.Equal(t, 1000, config.Monitor.SlowQuery.MaxEntries)

	// 验证连接池配置
	assert.Equal(t, 10, config.Connection.MaxOpen)
	assert.Equal(t, 5, config.Connection.MaxIdle)
	assert.Equal(t, 30*time.Minute, config.Connection.Lifetime)
	assert.Equal(t, 5*time.Minute, config.Connection.IdleTimeout)

	// 验证MVCC配置
	assert.True(t, config.MVCC.EnableWarning)
	assert.True(t, config.MVCC.AutoDowngrade)
	assert.Equal(t, 5*time.Minute, config.MVCC.GCInterval)
	assert.Equal(t, 1*time.Hour, config.MVCC.GCAgeThreshold)
	assert.Equal(t, uint32(100000), config.MVCC.XIDWrapThreshold)
	assert.Equal(t, 10000, config.MVCC.MaxActiveTxns)

	// 验证会话配置
	assert.Equal(t, 24*time.Hour, config.Session.MaxAge)
	assert.Equal(t, 1*time.Minute, config.Session.GCInterval)

	// 验证优化器配置
	assert.True(t, config.Optimizer.Enabled)
}

func TestLoadConfig_EmptyPath(t *testing.T) {
	config, err := LoadConfig("")

	assert.NoError(t, err)
	assert.NotNil(t, config)
	assert.Equal(t, 3306, config.Server.Port)
}

func TestLoadConfig_FileNotFound(t *testing.T) {
	config, err := LoadConfig("non_existent_config.json")

	assert.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "配置文件不存在")
}

func TestLoadConfig_InvalidJSON(t *testing.T) {
	// 创建临时文件
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "invalid.json")

	// 写入无效的JSON
	err := os.WriteFile(configPath, []byte("{invalid json"), 0644)
	require.NoError(t, err)

	config, err := LoadConfig(configPath)

	assert.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "解析配置文件失败")
}

func TestLoadConfig_InvalidPort(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.json")

	configData := map[string]interface{}{
		"server": map[string]interface{}{
			"port": 70000, // 无效端口号
		},
	}

	jsonData, _ := json.Marshal(configData)
	err := os.WriteFile(configPath, jsonData, 0644)
	require.NoError(t, err)

	config, err := LoadConfig(configPath)

	assert.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "无效的端口号")
}

func TestLoadConfig_InvalidMaxConnections(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.json")

	configData := map[string]interface{}{
		"database": map[string]interface{}{
			"max_connections": 0, // 无效值
		},
	}

	jsonData, _ := json.Marshal(configData)
	err := os.WriteFile(configPath, jsonData, 0644)
	require.NoError(t, err)

	config, err := LoadConfig(configPath)

	assert.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "最大连接数必须大于0")
}

func TestLoadConfig_InvalidPoolConfig(t *testing.T) {
	tests := []struct {
		name      string
		configKey string
		configVal map[string]interface{}
		errMsg    string
	}{
		{
			name: "invalid max workers",
			configKey: "pool",
			configVal: map[string]interface{}{
				"goroutine_pool": map[string]interface{}{
					"max_workers": 0,
				},
			},
			errMsg: "Goroutine池最大工作线程数必须大于0",
		},
		{
			name: "invalid queue size",
			configKey: "pool",
			configVal: map[string]interface{}{
				"goroutine_pool": map[string]interface{}{
					"queue_size": 0,
				},
			},
			errMsg: "Goroutine池队列大小必须大于0",
		},
		{
			name: "invalid object pool max size",
			configKey: "pool",
			configVal: map[string]interface{}{
				"object_pool": map[string]interface{}{
					"max_size": 0,
				},
			},
			errMsg: "对象池最大大小必须大于0",
		},
		{
			name: "invalid object pool min idle",
			configKey: "pool",
			configVal: map[string]interface{}{
				"object_pool": map[string]interface{}{
					"min_idle": -1,
				},
			},
			errMsg: "对象池最小空闲数不能为负数",
		},
		{
			name: "invalid object pool max idle",
			configKey: "pool",
			configVal: map[string]interface{}{
				"object_pool": map[string]interface{}{
					"max_idle": 0,
				},
			},
			errMsg: "对象池最大空闲数必须大于0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			configPath := filepath.Join(tmpDir, "config.json")

			configData := map[string]interface{}{
				tt.configKey: tt.configVal,
			}

			jsonData, _ := json.Marshal(configData)
			err := os.WriteFile(configPath, jsonData, 0644)
			require.NoError(t, err)

			config, err := LoadConfig(configPath)

			assert.Error(t, err)
			assert.Nil(t, config)
			assert.Contains(t, err.Error(), tt.errMsg)
		})
	}
}

func TestLoadConfig_InvalidConnectionConfig(t *testing.T) {
	tests := []struct {
		name      string
		configKey string
		configVal interface{}
		errMsg    string
	}{
		{
			name:      "invalid max open",
			configKey: "connection",
			configVal: map[string]interface{}{
				"max_open": 0,
			},
			errMsg: "连接池最大连接数必须大于0",
		},
		{
			name:      "invalid max idle",
			configKey: "connection",
			configVal: map[string]interface{}{
				"max_idle": 0,
			},
			errMsg: "连接池最大空闲连接数必须大于0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			configPath := filepath.Join(tmpDir, "config.json")

			configData := map[string]interface{}{
				tt.configKey: tt.configVal,
			}

			jsonData, _ := json.Marshal(configData)
			err := os.WriteFile(configPath, jsonData, 0644)
			require.NoError(t, err)

			config, err := LoadConfig(configPath)

			assert.Error(t, err)
			assert.Nil(t, config)
			assert.Contains(t, err.Error(), tt.errMsg)
		})
	}
}

func TestLoadConfig_ValidConfig(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.json")

	configData := map[string]interface{}{
		"server": map[string]interface{}{
			"host":  "127.0.0.1",
			"port":  5432,
		},
		"database": map[string]interface{}{
			"max_connections": 200,
		},
	}

	jsonData, _ := json.Marshal(configData)
	err := os.WriteFile(configPath, jsonData, 0644)
	require.NoError(t, err)

	config, err := LoadConfig(configPath)

	assert.NoError(t, err)
	assert.NotNil(t, config)
	assert.Equal(t, "127.0.0.1", config.Server.Host)
	assert.Equal(t, 5432, config.Server.Port)
	assert.Equal(t, 200, config.Database.MaxConnections)
	// 其他字段应该使用默认值
	assert.Equal(t, "SqlExc", config.Server.ServerVersion)
}

func TestLoadConfigOrDefault_WithEnvVar(t *testing.T) {
	// 创建临时配置文件
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "test_config.json")

	configData := map[string]interface{}{
		"server": map[string]interface{}{
			"port": 8080,
		},
	}

	jsonData, _ := json.Marshal(configData)
	err := os.WriteFile(configPath, jsonData, 0644)
	require.NoError(t, err)

	// 设置环境变量
	oldEnv := os.Getenv("SQLEXEC_CONFIG")
	t.Cleanup(func() {
		os.Setenv("SQLEXEC_CONFIG", oldEnv)
	})
	os.Setenv("SQLEXEC_CONFIG", configPath)

	config := LoadConfigOrDefault()

	assert.NotNil(t, config)
	assert.Equal(t, 8080, config.Server.Port)
}

func TestLoadConfigOrDefault_WithLocalFile(t *testing.T) {
	// 创建临时配置文件在当前目录
	oldWd, _ := os.Getwd()
	tmpDir := t.TempDir()

	// 切换到临时目录
	os.Chdir(tmpDir)
	t.Cleanup(func() {
		os.Chdir(oldWd)
	})

	configPath := filepath.Join(tmpDir, "config.json")

	configData := map[string]interface{}{
		"server": map[string]interface{}{
			"port": 9999,
		},
	}

	jsonData, _ := json.Marshal(configData)
	err := os.WriteFile(configPath, jsonData, 0644)
	require.NoError(t, err)

	config := LoadConfigOrDefault()

	assert.NotNil(t, config)
	assert.Equal(t, 9999, config.Server.Port)
}

func TestLoadConfigOrDefault_NoConfigFile(t *testing.T) {
	tmpDir := t.TempDir()

	oldWd, _ := os.Getwd()
	os.Chdir(tmpDir)
	t.Cleanup(func() {
		os.Chdir(oldWd)
	})

	config := LoadConfigOrDefault()

	assert.NotNil(t, config)
	assert.Equal(t, 3306, config.Server.Port) // 使用默认值
}

func TestGetListenAddress(t *testing.T) {
	tests := []struct {
		host     string
		port     int
		expected string
	}{
		{
			host:     "0.0.0.0",
			port:     3306,
			expected: "0.0.0.0:3306",
		},
		{
			host:     "127.0.0.1",
			port:     8080,
			expected: "127.0.0.1:8080",
		},
		{
			host:     "localhost",
			port:     5432,
			expected: "localhost:5432",
		},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			config := &Config{
				Server: ServerConfig{
					Host: tt.host,
					Port: tt.port,
				},
			}

			address := config.GetListenAddress()
			assert.Equal(t, tt.expected, address)
		})
	}
}

func TestConfigStructTags(t *testing.T) {
	// 测试配置可以正确序列化为JSON
	config := DefaultConfig()

	jsonData, err := json.Marshal(config)
	assert.NoError(t, err)
	assert.NotEmpty(t, jsonData)

	// 测试可以反序列化回Config
	var parsedConfig Config
	err = json.Unmarshal(jsonData, &parsedConfig)
	assert.NoError(t, err)
	assert.Equal(t, config.Server.Port, parsedConfig.Server.Port)
	assert.Equal(t, config.Server.Host, parsedConfig.Server.Host)
}
