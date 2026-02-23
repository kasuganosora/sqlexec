package extensibility

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewPluginManager(t *testing.T) {
	pm := NewPluginManager()
	assert.NotNil(t, pm)
	assert.NotNil(t, pm.dataSourcePlugins)
	assert.NotNil(t, pm.functionPlugins)
	assert.NotNil(t, pm.monitorPlugins)
}

func TestPluginManager_RegisterDataSourcePlugin(t *testing.T) {
	pm := NewPluginManager()

	plugin := &MockDataSourcePlugin{
		name:    "test_ds",
		version: "1.0.0",
	}

	config := map[string]interface{}{"option": "value"}
	err := pm.RegisterDataSourcePlugin(plugin, config)
	assert.NoError(t, err)

	retrieved, err := pm.GetDataSourcePlugin("test_ds")
	assert.NoError(t, err)
	assert.Equal(t, plugin, retrieved)
}

func TestPluginManager_RegisterDataSourcePlugin_Duplicate(t *testing.T) {
	pm := NewPluginManager()

	plugin1 := &MockDataSourcePlugin{
		name:    "test_ds",
		version: "1.0.0",
	}

	plugin2 := &MockDataSourcePlugin{
		name:    "test_ds",
		version: "2.0.0",
	}

	err := pm.RegisterDataSourcePlugin(plugin1, nil)
	assert.NoError(t, err)

	err = pm.RegisterDataSourcePlugin(plugin2, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "data source plugin 'test_ds' already registered")
}

func TestPluginManager_RegisterFunctionPlugin(t *testing.T) {
	pm := NewPluginManager()

	plugin := &MockFunctionPlugin{
		name:    "test_func",
		version: "1.0.0",
	}

	config := map[string]interface{}{"option": "value"}
	err := pm.RegisterFunctionPlugin(plugin, config)
	assert.NoError(t, err)

	retrieved, err := pm.GetFunctionPlugin("test_func")
	assert.NoError(t, err)
	assert.Equal(t, plugin, retrieved)
}

func TestPluginManager_RegisterFunctionPlugin_Duplicate(t *testing.T) {
	pm := NewPluginManager()

	plugin1 := &MockFunctionPlugin{
		name:    "test_func",
		version: "1.0.0",
	}

	plugin2 := &MockFunctionPlugin{
		name:    "test_func",
		version: "2.0.0",
	}

	err := pm.RegisterFunctionPlugin(plugin1, nil)
	assert.NoError(t, err)

	err = pm.RegisterFunctionPlugin(plugin2, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "function plugin 'test_func' already registered")
}

func TestPluginManager_RegisterMonitorPlugin(t *testing.T) {
	pm := NewPluginManager()

	plugin := &MockMonitorPlugin{
		name:    "test_monitor",
		version: "1.0.0",
	}

	config := map[string]interface{}{"option": "value"}
	err := pm.RegisterMonitorPlugin(plugin, config)
	assert.NoError(t, err)

	retrieved, err := pm.GetMonitorPlugin("test_monitor")
	assert.NoError(t, err)
	assert.Equal(t, plugin, retrieved)
}

func TestPluginManager_RegisterMonitorPlugin_Duplicate(t *testing.T) {
	pm := NewPluginManager()

	plugin1 := &MockMonitorPlugin{
		name:    "test_monitor",
		version: "1.0.0",
	}

	plugin2 := &MockMonitorPlugin{
		name:    "test_monitor",
		version: "2.0.0",
	}

	err := pm.RegisterMonitorPlugin(plugin1, nil)
	assert.NoError(t, err)

	err = pm.RegisterMonitorPlugin(plugin2, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "monitor plugin 'test_monitor' already registered")
}

func TestPluginManager_UnregisterPlugin_DataSource(t *testing.T) {
	pm := NewPluginManager()

	plugin := &MockDataSourcePlugin{
		name:    "test_ds",
		version: "1.0.0",
		running: true,
	}

	err := pm.RegisterDataSourcePlugin(plugin, nil)
	require.NoError(t, err)

	err = pm.UnregisterPlugin("test_ds")
	assert.NoError(t, err)

	_, err = pm.GetDataSourcePlugin("test_ds")
	assert.Error(t, err)
}

func TestPluginManager_UnregisterPlugin_Function(t *testing.T) {
	pm := NewPluginManager()

	plugin := &MockFunctionPlugin{
		name:    "test_func",
		version: "1.0.0",
		running: true,
	}

	err := pm.RegisterFunctionPlugin(plugin, nil)
	require.NoError(t, err)

	err = pm.UnregisterPlugin("test_func")
	assert.NoError(t, err)

	_, err = pm.GetFunctionPlugin("test_func")
	assert.Error(t, err)
}

func TestPluginManager_UnregisterPlugin_Monitor(t *testing.T) {
	pm := NewPluginManager()

	plugin := &MockMonitorPlugin{
		name:    "test_monitor",
		version: "1.0.0",
		running: true,
	}

	err := pm.RegisterMonitorPlugin(plugin, nil)
	require.NoError(t, err)

	err = pm.UnregisterPlugin("test_monitor")
	assert.NoError(t, err)

	_, err = pm.GetMonitorPlugin("test_monitor")
	assert.Error(t, err)
}

func TestPluginManager_UnregisterPlugin_NotFound(t *testing.T) {
	pm := NewPluginManager()

	err := pm.UnregisterPlugin("nonexistent")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "plugin 'nonexistent' not found")
}

func TestPluginManager_StartPlugin_DataSource(t *testing.T) {
	pm := NewPluginManager()

	plugin := &MockDataSourcePlugin{
		name:    "test_ds",
		version: "1.0.0",
		running: false,
	}

	err := pm.RegisterDataSourcePlugin(plugin, nil)
	require.NoError(t, err)

	err = pm.StartPlugin("test_ds")
	assert.NoError(t, err)
	assert.True(t, plugin.IsRunning())
}

func TestPluginManager_StartPlugin_Function(t *testing.T) {
	pm := NewPluginManager()

	plugin := &MockFunctionPlugin{
		name:    "test_func",
		version: "1.0.0",
		running: false,
	}

	err := pm.RegisterFunctionPlugin(plugin, nil)
	require.NoError(t, err)

	err = pm.StartPlugin("test_func")
	assert.NoError(t, err)
	assert.True(t, plugin.IsRunning())
}

func TestPluginManager_StartPlugin_Monitor(t *testing.T) {
	pm := NewPluginManager()

	plugin := &MockMonitorPlugin{
		name:    "test_monitor",
		version: "1.0.0",
		running: false,
	}

	err := pm.RegisterMonitorPlugin(plugin, nil)
	require.NoError(t, err)

	err = pm.StartPlugin("test_monitor")
	assert.NoError(t, err)
	assert.True(t, plugin.IsRunning())
}

func TestPluginManager_StartPlugin_NotFound(t *testing.T) {
	pm := NewPluginManager()

	err := pm.StartPlugin("nonexistent")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "plugin 'nonexistent' not found")
}

func TestPluginManager_StopPlugin_DataSource(t *testing.T) {
	pm := NewPluginManager()

	plugin := &MockDataSourcePlugin{
		name:    "test_ds",
		version: "1.0.0",
		running: true,
	}

	err := pm.RegisterDataSourcePlugin(plugin, nil)
	require.NoError(t, err)

	err = pm.StopPlugin("test_ds")
	assert.NoError(t, err)
	assert.False(t, plugin.IsRunning())
}

func TestPluginManager_StopPlugin_Function(t *testing.T) {
	pm := NewPluginManager()

	plugin := &MockFunctionPlugin{
		name:    "test_func",
		version: "1.0.0",
		running: true,
	}

	err := pm.RegisterFunctionPlugin(plugin, nil)
	require.NoError(t, err)

	err = pm.StopPlugin("test_func")
	assert.NoError(t, err)
	assert.False(t, plugin.IsRunning())
}

func TestPluginManager_StopPlugin_Monitor(t *testing.T) {
	pm := NewPluginManager()

	plugin := &MockMonitorPlugin{
		name:    "test_monitor",
		version: "1.0.0",
		running: true,
	}

	err := pm.RegisterMonitorPlugin(plugin, nil)
	require.NoError(t, err)

	err = pm.StopPlugin("test_monitor")
	assert.NoError(t, err)
	assert.False(t, plugin.IsRunning())
}

func TestPluginManager_StopPlugin_NotFound(t *testing.T) {
	pm := NewPluginManager()

	err := pm.StopPlugin("nonexistent")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "plugin 'nonexistent' not found")
}

func TestPluginManager_GetDataSourcePlugin(t *testing.T) {
	pm := NewPluginManager()

	plugin := &MockDataSourcePlugin{
		name:    "test_ds",
		version: "1.0.0",
	}

	err := pm.RegisterDataSourcePlugin(plugin, nil)
	require.NoError(t, err)

	retrieved, err := pm.GetDataSourcePlugin("test_ds")
	assert.NoError(t, err)
	assert.Equal(t, plugin, retrieved)
}

func TestPluginManager_GetDataSourcePlugin_NotFound(t *testing.T) {
	pm := NewPluginManager()

	_, err := pm.GetDataSourcePlugin("nonexistent")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "data source plugin 'nonexistent' not found")
}

func TestPluginManager_GetFunctionPlugin(t *testing.T) {
	pm := NewPluginManager()

	plugin := &MockFunctionPlugin{
		name:    "test_func",
		version: "1.0.0",
	}

	err := pm.RegisterFunctionPlugin(plugin, nil)
	require.NoError(t, err)

	retrieved, err := pm.GetFunctionPlugin("test_func")
	assert.NoError(t, err)
	assert.Equal(t, plugin, retrieved)
}

func TestPluginManager_GetFunctionPlugin_NotFound(t *testing.T) {
	pm := NewPluginManager()

	_, err := pm.GetFunctionPlugin("nonexistent")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "function plugin 'nonexistent' not found")
}

func TestPluginManager_GetMonitorPlugin(t *testing.T) {
	pm := NewPluginManager()

	plugin := &MockMonitorPlugin{
		name:    "test_monitor",
		version: "1.0.0",
	}

	err := pm.RegisterMonitorPlugin(plugin, nil)
	require.NoError(t, err)

	retrieved, err := pm.GetMonitorPlugin("test_monitor")
	assert.NoError(t, err)
	assert.Equal(t, plugin, retrieved)
}

func TestPluginManager_GetMonitorPlugin_NotFound(t *testing.T) {
	pm := NewPluginManager()

	_, err := pm.GetMonitorPlugin("nonexistent")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "monitor plugin 'nonexistent' not found")
}

func TestPluginManager_ListPlugins(t *testing.T) {
	pm := NewPluginManager()

	dsPlugin := &MockDataSourcePlugin{name: "ds1"}
	funcPlugin := &MockFunctionPlugin{name: "func1"}
	monitorPlugin := &MockMonitorPlugin{name: "mon1"}

	pm.RegisterDataSourcePlugin(dsPlugin, nil)
	pm.RegisterFunctionPlugin(funcPlugin, nil)
	pm.RegisterMonitorPlugin(monitorPlugin, nil)

	plugins := pm.ListPlugins()
	assert.Equal(t, 3, len(plugins))
	assert.Contains(t, plugins, "ds1")
	assert.Contains(t, plugins, "func1")
	assert.Contains(t, plugins, "mon1")
}

func TestPluginManager_StartAllPlugins(t *testing.T) {
	pm := NewPluginManager()

	dsPlugin := &MockDataSourcePlugin{name: "ds1", running: false}
	funcPlugin := &MockFunctionPlugin{name: "func1", running: false}
	monitorPlugin := &MockMonitorPlugin{name: "mon1", running: false}

	pm.RegisterDataSourcePlugin(dsPlugin, nil)
	pm.RegisterFunctionPlugin(funcPlugin, nil)
	pm.RegisterMonitorPlugin(monitorPlugin, nil)

	err := pm.StartAllPlugins()
	assert.NoError(t, err)
	assert.True(t, dsPlugin.IsRunning())
	assert.True(t, funcPlugin.IsRunning())
	assert.True(t, monitorPlugin.IsRunning())
}

func TestPluginManager_StopAllPlugins(t *testing.T) {
	pm := NewPluginManager()

	dsPlugin := &MockDataSourcePlugin{name: "ds1", running: true}
	funcPlugin := &MockFunctionPlugin{name: "func1", running: true}
	monitorPlugin := &MockMonitorPlugin{name: "mon1", running: true}

	pm.RegisterDataSourcePlugin(dsPlugin, nil)
	pm.RegisterFunctionPlugin(funcPlugin, nil)
	pm.RegisterMonitorPlugin(monitorPlugin, nil)

	err := pm.StopAllPlugins()
	assert.NoError(t, err)
	assert.False(t, dsPlugin.IsRunning())
	assert.False(t, funcPlugin.IsRunning())
	assert.False(t, monitorPlugin.IsRunning())
}

func TestPluginManager_StartAllPlugins_WithRunningPlugins(t *testing.T) {
	pm := NewPluginManager()

	dsPlugin := &MockDataSourcePlugin{name: "ds1", running: true}
	pm.RegisterDataSourcePlugin(dsPlugin, nil)

	err := pm.StartAllPlugins()
	assert.NoError(t, err)
}

func TestBasePlugin_Name(t *testing.T) {
	plugin := NewBasePlugin("test_plugin", "1.0.0")
	assert.Equal(t, "test_plugin", plugin.Name())
}

func TestBasePlugin_Version(t *testing.T) {
	plugin := NewBasePlugin("test_plugin", "1.0.0")
	assert.Equal(t, "1.0.0", plugin.Version())
}

func TestBasePlugin_Initialize(t *testing.T) {
	plugin := NewBasePlugin("test_plugin", "1.0.0")

	config := map[string]interface{}{
		"option1": "value1",
		"option2": 42,
	}

	err := plugin.Initialize(config)
	assert.NoError(t, err)

	retrievedConfig := plugin.GetConfig()
	assert.Equal(t, "value1", retrievedConfig["option1"])
	assert.Equal(t, 42, retrievedConfig["option2"])
}

func TestBasePlugin_Start(t *testing.T) {
	plugin := NewBasePlugin("test_plugin", "1.0.0")
	assert.False(t, plugin.IsRunning())

	err := plugin.Start()
	assert.NoError(t, err)
	assert.True(t, plugin.IsRunning())
}

func TestBasePlugin_Stop(t *testing.T) {
	plugin := NewBasePlugin("test_plugin", "1.0.0")
	plugin.running = true
	assert.True(t, plugin.IsRunning())

	err := plugin.Stop()
	assert.NoError(t, err)
	assert.False(t, plugin.IsRunning())
}

func TestBasePlugin_IsRunning(t *testing.T) {
	plugin := NewBasePlugin("test_plugin", "1.0.0")
	assert.False(t, plugin.IsRunning())

	plugin.running = true
	assert.True(t, plugin.IsRunning())
}

func TestBasePlugin_GetConfig(t *testing.T) {
	plugin := NewBasePlugin("test_plugin", "1.0.0")

	config := map[string]interface{}{
		"key": "value",
	}

	err := plugin.Initialize(config)
	require.NoError(t, err)

	retrievedConfig := plugin.GetConfig()
	assert.Equal(t, "value", retrievedConfig["key"])
}

// Mock implementations

type MockDataSourcePlugin struct {
	name    string
	version string
	running bool
}

func (m *MockDataSourcePlugin) Name() string                                   { return m.name }
func (m *MockDataSourcePlugin) Version() string                                { return m.version }
func (m *MockDataSourcePlugin) Initialize(config map[string]interface{}) error { return nil }
func (m *MockDataSourcePlugin) Start() error                                   { m.running = true; return nil }
func (m *MockDataSourcePlugin) Stop() error                                    { m.running = false; return nil }
func (m *MockDataSourcePlugin) IsRunning() bool                                { return m.running }
func (m *MockDataSourcePlugin) Connect(conn string) (interface{}, error)       { return nil, nil }
func (m *MockDataSourcePlugin) Disconnect(conn interface{}) error              { return nil }
func (m *MockDataSourcePlugin) Query(conn interface{}, query string, params []interface{}) (interface{}, error) {
	return nil, nil
}
func (m *MockDataSourcePlugin) Execute(conn interface{}, command string, params []interface{}) (int64, error) {
	return 0, nil
}

type MockFunctionPlugin struct {
	name    string
	version string
	running bool
}

func (m *MockFunctionPlugin) Name() string                                   { return m.name }
func (m *MockFunctionPlugin) Version() string                                { return m.version }
func (m *MockFunctionPlugin) Initialize(config map[string]interface{}) error { return nil }
func (m *MockFunctionPlugin) Start() error                                   { m.running = true; return nil }
func (m *MockFunctionPlugin) Stop() error                                    { m.running = false; return nil }
func (m *MockFunctionPlugin) IsRunning() bool                                { return m.running }
func (m *MockFunctionPlugin) Register(name string, fn interface{}) error     { return nil }
func (m *MockFunctionPlugin) Unregister(name string) error                   { return nil }
func (m *MockFunctionPlugin) Call(name string, args []interface{}) (interface{}, error) {
	return nil, nil
}
func (m *MockFunctionPlugin) GetFunction(name string) (interface{}, error) {
	return nil, nil
}
func (m *MockFunctionPlugin) ListFunctions() []string { return []string{} }

type MockMonitorPlugin struct {
	name    string
	version string
	running bool
}

func (m *MockMonitorPlugin) Name() string                                                    { return m.name }
func (m *MockMonitorPlugin) Version() string                                                 { return m.version }
func (m *MockMonitorPlugin) Initialize(config map[string]interface{}) error                  { return nil }
func (m *MockMonitorPlugin) Start() error                                                    { m.running = true; return nil }
func (m *MockMonitorPlugin) Stop() error                                                     { m.running = false; return nil }
func (m *MockMonitorPlugin) IsRunning() bool                                                 { return m.running }
func (m *MockMonitorPlugin) RecordMetric(name string, value float64, tags map[string]string) {}
func (m *MockMonitorPlugin) RecordEvent(name string, data map[string]interface{})            {}
func (m *MockMonitorPlugin) GetMetric(name string) (float64, error)                          { return 0, nil }
func (m *MockMonitorPlugin) GetMetrics() map[string]float64                                  { return nil }
