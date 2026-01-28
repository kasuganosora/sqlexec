package extensibility

import (
	"errors"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// MockDataSourcePlugin 模拟数据源插件
type MockDataSourcePlugin struct {
	*BasePlugin
	connected bool
}

func NewMockDataSourcePlugin(name, version string) *MockDataSourcePlugin {
	return &MockDataSourcePlugin{
		BasePlugin: NewBasePlugin(name, version),
		connected:   false,
	}
}

func (m *MockDataSourcePlugin) Connect(connectionString string) (interface{}, error) {
	m.connected = true
	return "mock_connection", nil
}

func (m *MockDataSourcePlugin) Disconnect(conn interface{}) error {
	m.connected = false
	return nil
}

func (m *MockDataSourcePlugin) Query(conn interface{}, query string, params []interface{}) (interface{}, error) {
	return "mock_result", nil
}

func (m *MockDataSourcePlugin) Execute(conn interface{}, command string, params []interface{}) (int64, error) {
	return 1, nil
}

// MockFunctionPlugin 模拟函数插件
type MockFunctionPlugin struct {
	*BasePlugin
	functions map[string]interface{}
}

func NewMockFunctionPlugin(name, version string) *MockFunctionPlugin {
	return &MockFunctionPlugin{
		BasePlugin: NewBasePlugin(name, version),
		functions:  make(map[string]interface{}),
	}
}

func (m *MockFunctionPlugin) Register(name string, fn interface{}) error {
	m.functions[name] = fn
	return nil
}

func (m *MockFunctionPlugin) Unregister(name string) error {
	delete(m.functions, name)
	return nil
}

func (m *MockFunctionPlugin) Call(name string, args []interface{}) (interface{}, error) {
	fn, ok := m.functions[name]
	if !ok {
		return nil, errors.New("function not found")
	}
	return fn, nil
}

func (m *MockFunctionPlugin) GetFunction(name string) (interface{}, error) {
	fn, ok := m.functions[name]
	if !ok {
		return nil, errors.New("function not found")
	}
	return fn, nil
}

func (m *MockFunctionPlugin) ListFunctions() []string {
	names := make([]string, 0, len(m.functions))
	for name := range m.functions {
		names = append(names, name)
	}
	return names
}

// MockMonitorPlugin 模拟监控插件
type MockMonitorPlugin struct {
	*BasePlugin
	metrics map[string]float64
	events  []map[string]interface{}
}

func NewMockMonitorPlugin(name, version string) *MockMonitorPlugin {
	return &MockMonitorPlugin{
		BasePlugin: NewBasePlugin(name, version),
		metrics:    make(map[string]float64),
		events:     make([]map[string]interface{}, 0),
	}
}

func (m *MockMonitorPlugin) RecordMetric(name string, value float64, tags map[string]string) {
	m.metrics[name] = value
}

func (m *MockMonitorPlugin) RecordEvent(name string, data map[string]interface{}) {
	m.events = append(m.events, map[string]interface{}{
		"name": name,
		"data": data,
	})
}

func (m *MockMonitorPlugin) GetMetric(name string) (float64, error) {
	val, ok := m.metrics[name]
	if !ok {
		return 0, errors.New("metric not found")
	}
	return val, nil
}

func (m *MockMonitorPlugin) GetMetrics() map[string]float64 {
	metrics := make(map[string]float64)
	for k, v := range m.metrics {
		metrics[k] = v
	}
	return metrics
}

func TestNewPluginManager(t *testing.T) {
	manager := NewPluginManager()

	assert.NotNil(t, manager)
	assert.NotNil(t, manager.dataSourcePlugins)
	assert.NotNil(t, manager.functionPlugins)
	assert.NotNil(t, manager.monitorPlugins)
}

func TestPluginManager_RegisterDataSourcePlugin(t *testing.T) {
	manager := NewPluginManager()
	plugin := NewMockDataSourcePlugin("mysql_plugin", "1.0.0")

	config := map[string]interface{}{
		"host": "localhost",
		"port": 3306,
	}

	err := manager.RegisterDataSourcePlugin(plugin, config)

	assert.NoError(t, err)
	assert.NotNil(t, manager.dataSourcePlugins["mysql_plugin"])
}

func TestPluginManager_RegisterDataSourcePlugin_Duplicate(t *testing.T) {
	manager := NewPluginManager()
	plugin1 := NewMockDataSourcePlugin("mysql_plugin", "1.0.0")
	plugin2 := NewMockDataSourcePlugin("mysql_plugin", "2.0.0")

	config := map[string]interface{}{}

	err1 := manager.RegisterDataSourcePlugin(plugin1, config)
	err2 := manager.RegisterDataSourcePlugin(plugin2, config)

	assert.NoError(t, err1)
	assert.Error(t, err2)
	assert.Contains(t, err2.Error(), "already registered")
}

func TestPluginManager_RegisterFunctionPlugin(t *testing.T) {
	manager := NewPluginManager()
	plugin := NewMockFunctionPlugin("func_plugin", "1.0.0")

	config := map[string]interface{}{}

	err := manager.RegisterFunctionPlugin(plugin, config)

	assert.NoError(t, err)
	assert.NotNil(t, manager.functionPlugins["func_plugin"])
}

func TestPluginManager_RegisterMonitorPlugin(t *testing.T) {
	manager := NewPluginManager()
	plugin := NewMockMonitorPlugin("monitor_plugin", "1.0.0")

	config := map[string]interface{}{}

	err := manager.RegisterMonitorPlugin(plugin, config)

	assert.NoError(t, err)
	assert.NotNil(t, manager.monitorPlugins["monitor_plugin"])
}

func TestPluginManager_UnregisterPlugin(t *testing.T) {
	manager := NewPluginManager()
	plugin := NewMockDataSourcePlugin("test_plugin", "1.0.0")

	config := map[string]interface{}{}
	manager.RegisterDataSourcePlugin(plugin, config)

	plugin.Start()

	err := manager.UnregisterPlugin("test_plugin")

	assert.NoError(t, err)
	_, ok := manager.dataSourcePlugins["test_plugin"]
	assert.False(t, ok)
	assert.False(t, plugin.IsRunning())
}

func TestPluginManager_UnregisterPlugin_NotFound(t *testing.T) {
	manager := NewPluginManager()

	err := manager.UnregisterPlugin("nonexistent")

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestPluginManager_StartPlugin(t *testing.T) {
	manager := NewPluginManager()
	plugin := NewMockDataSourcePlugin("test_plugin", "1.0.0")

	config := map[string]interface{}{}
	manager.RegisterDataSourcePlugin(plugin, config)

	err := manager.StartPlugin("test_plugin")

	assert.NoError(t, err)
	assert.True(t, plugin.IsRunning())
}

func TestPluginManager_StartPlugin_NotFound(t *testing.T) {
	manager := NewPluginManager()

	err := manager.StartPlugin("nonexistent")

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestPluginManager_StopPlugin(t *testing.T) {
	manager := NewPluginManager()
	plugin := NewMockDataSourcePlugin("test_plugin", "1.0.0")

	config := map[string]interface{}{}
	manager.RegisterDataSourcePlugin(plugin, config)
	plugin.Start()

	err := manager.StopPlugin("test_plugin")

	assert.NoError(t, err)
	assert.False(t, plugin.IsRunning())
}

func TestPluginManager_StopPlugin_NotFound(t *testing.T) {
	manager := NewPluginManager()

	err := manager.StopPlugin("nonexistent")

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestPluginManager_GetDataSourcePlugin(t *testing.T) {
	manager := NewPluginManager()
	plugin := NewMockDataSourcePlugin("test_plugin", "1.0.0")

	config := map[string]interface{}{}
	manager.RegisterDataSourcePlugin(plugin, config)

	retrieved, err := manager.GetDataSourcePlugin("test_plugin")

	assert.NoError(t, err)
	assert.Equal(t, plugin, retrieved)
}

func TestPluginManager_GetDataSourcePlugin_NotFound(t *testing.T) {
	manager := NewPluginManager()

	retrieved, err := manager.GetDataSourcePlugin("nonexistent")

	assert.Error(t, err)
	assert.Nil(t, retrieved)
	assert.Contains(t, err.Error(), "not found")
}

func TestPluginManager_GetFunctionPlugin(t *testing.T) {
	manager := NewPluginManager()
	plugin := NewMockFunctionPlugin("test_plugin", "1.0.0")

	config := map[string]interface{}{}
	manager.RegisterFunctionPlugin(plugin, config)

	retrieved, err := manager.GetFunctionPlugin("test_plugin")

	assert.NoError(t, err)
	assert.Equal(t, plugin, retrieved)
}

func TestPluginManager_GetMonitorPlugin(t *testing.T) {
	manager := NewPluginManager()
	plugin := NewMockMonitorPlugin("test_plugin", "1.0.0")

	config := map[string]interface{}{}
	manager.RegisterMonitorPlugin(plugin, config)

	retrieved, err := manager.GetMonitorPlugin("test_plugin")

	assert.NoError(t, err)
	assert.Equal(t, plugin, retrieved)
}

func TestPluginManager_ListPlugins(t *testing.T) {
	manager := NewPluginManager()

	dsPlugin := NewMockDataSourcePlugin("ds_plugin", "1.0.0")
	funcPlugin := NewMockFunctionPlugin("func_plugin", "1.0.0")
	monitorPlugin := NewMockMonitorPlugin("monitor_plugin", "1.0.0")

	config := map[string]interface{}{}
	manager.RegisterDataSourcePlugin(dsPlugin, config)
	manager.RegisterFunctionPlugin(funcPlugin, config)
	manager.RegisterMonitorPlugin(monitorPlugin, config)

	plugins := manager.ListPlugins()

	assert.Len(t, plugins, 3)
	assert.Contains(t, plugins, "ds_plugin")
	assert.Contains(t, plugins, "func_plugin")
	assert.Contains(t, plugins, "monitor_plugin")
}

func TestPluginManager_StartAllPlugins(t *testing.T) {
	manager := NewPluginManager()

	dsPlugin := NewMockDataSourcePlugin("ds_plugin", "1.0.0")
	funcPlugin := NewMockFunctionPlugin("func_plugin", "1.0.0")
	monitorPlugin := NewMockMonitorPlugin("monitor_plugin", "1.0.0")

	config := map[string]interface{}{}
	manager.RegisterDataSourcePlugin(dsPlugin, config)
	manager.RegisterFunctionPlugin(funcPlugin, config)
	manager.RegisterMonitorPlugin(monitorPlugin, config)

	err := manager.StartAllPlugins()

	assert.NoError(t, err)
	assert.True(t, dsPlugin.IsRunning())
	assert.True(t, funcPlugin.IsRunning())
	assert.True(t, monitorPlugin.IsRunning())
}

func TestPluginManager_StopAllPlugins(t *testing.T) {
	manager := NewPluginManager()

	dsPlugin := NewMockDataSourcePlugin("ds_plugin", "1.0.0")
	funcPlugin := NewMockFunctionPlugin("func_plugin", "1.0.0")
	monitorPlugin := NewMockMonitorPlugin("monitor_plugin", "1.0.0")

	config := map[string]interface{}{}
	manager.RegisterDataSourcePlugin(dsPlugin, config)
	manager.RegisterFunctionPlugin(funcPlugin, config)
	manager.RegisterMonitorPlugin(monitorPlugin, config)

	manager.StartAllPlugins()

	err := manager.StopAllPlugins()

	assert.NoError(t, err)
	assert.False(t, dsPlugin.IsRunning())
	assert.False(t, funcPlugin.IsRunning())
	assert.False(t, monitorPlugin.IsRunning())
}

func TestNewBasePlugin(t *testing.T) {
	plugin := NewBasePlugin("test_plugin", "1.0.0")

	assert.NotNil(t, plugin)
	assert.Equal(t, "test_plugin", plugin.Name())
	assert.Equal(t, "1.0.0", plugin.Version())
	assert.False(t, plugin.IsRunning())
	assert.NotNil(t, plugin.config)
}

func TestBasePlugin_Name(t *testing.T) {
	plugin := NewBasePlugin("test_name", "1.0.0")
	assert.Equal(t, "test_name", plugin.Name())
}

func TestBasePlugin_Version(t *testing.T) {
	plugin := NewBasePlugin("test", "1.5.0")
	assert.Equal(t, "1.5.0", plugin.Version())
}

func TestBasePlugin_Initialize(t *testing.T) {
	plugin := NewBasePlugin("test", "1.0.0")

	config := map[string]interface{}{
		"key1": "value1",
		"key2": 123,
	}

	err := plugin.Initialize(config)

	assert.NoError(t, err)
	retrieved := plugin.GetConfig()
	assert.Equal(t, "value1", retrieved["key1"])
	assert.Equal(t, 123, retrieved["key2"])
}

func TestBasePlugin_Start(t *testing.T) {
	plugin := NewBasePlugin("test", "1.0.0")

	err := plugin.Start()

	assert.NoError(t, err)
	assert.True(t, plugin.IsRunning())
}

func TestBasePlugin_Stop(t *testing.T) {
	plugin := NewBasePlugin("test", "1.0.0")
	plugin.Start()

	err := plugin.Stop()

	assert.NoError(t, err)
	assert.False(t, plugin.IsRunning())
}

func TestBasePlugin_IsRunning(t *testing.T) {
	plugin := NewBasePlugin("test", "1.0.0")

	assert.False(t, plugin.IsRunning())

	plugin.Start()
	assert.True(t, plugin.IsRunning())

	plugin.Stop()
	assert.False(t, plugin.IsRunning())
}

func TestBasePlugin_GetConfig(t *testing.T) {
	plugin := NewBasePlugin("test", "1.0.0")

	config := map[string]interface{}{
		"key": "value",
	}
	plugin.Initialize(config)

	retrieved := plugin.GetConfig()

	assert.Equal(t, "value", retrieved["key"])

	// 修改返回的配置不应该影响原配置
	retrieved["new_key"] = "new_value"
	original := plugin.GetConfig()
	_, ok := original["new_key"]
	assert.False(t, ok)
}

func TestPluginManager_ConcurrentAccess(t *testing.T) {
	manager := NewPluginManager()

	var completed int32
	done := make(chan bool)

	// 并发注册插件
	for i := 0; i < 10; i++ {
		go func(i int) {
			plugin := NewMockDataSourcePlugin("plugin_"+string(rune('0'+i)), "1.0.0")
			config := map[string]interface{}{}
			manager.RegisterDataSourcePlugin(plugin, config)
			completed++
			done <- true
		}(i)
	}

	// 等待所有注册完成
	for i := 0; i < 10; i++ {
		<-done
	}

	assert.Equal(t, int32(10), atomic.LoadInt32(&completed))
}

func TestMockDataSourcePlugin_Connect(t *testing.T) {
	plugin := NewMockDataSourcePlugin("test", "1.0.0")

	conn, err := plugin.Connect("localhost:3306")

	assert.NoError(t, err)
	assert.Equal(t, "mock_connection", conn)
	assert.True(t, plugin.connected)
}

func TestMockDataSourcePlugin_Disconnect(t *testing.T) {
	plugin := NewMockDataSourcePlugin("test", "1.0.0")
	conn, _ := plugin.Connect("localhost:3306")

	err := plugin.Disconnect(conn)

	assert.NoError(t, err)
	assert.False(t, plugin.connected)
}

func TestMockFunctionPlugin_RegisterAndCall(t *testing.T) {
	plugin := NewMockFunctionPlugin("test", "1.0.0")

	testFunc := func() string { return "hello" }
	err := plugin.Register("test_func", testFunc)
	require.NoError(t, err)

	fn, err := plugin.GetFunction("test_func")
	assert.NoError(t, err)
	assert.NotNil(t, fn)

	result, err := plugin.Call("test_func", nil)
	assert.NoError(t, err)
	assert.NotNil(t, result)
}

func TestMockMonitorPlugin_RecordAndGetMetric(t *testing.T) {
	plugin := NewMockMonitorPlugin("test", "1.0.0")

	plugin.RecordMetric("test_metric", 42.0, nil)

	value, err := plugin.GetMetric("test_metric")

	assert.NoError(t, err)
	assert.Equal(t, 42.0, value)
}

func TestMockMonitorPlugin_RecordEvent(t *testing.T) {
	plugin := NewMockMonitorPlugin("test", "1.0.0")

	plugin.RecordEvent("test_event", map[string]interface{}{
		"key": "value",
	})

	assert.Len(t, plugin.events, 1)
	assert.Equal(t, "test_event", plugin.events[0]["name"])
}
