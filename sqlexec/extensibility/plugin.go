package extensibility

import (
	"fmt"
	"sync"
)

// Plugin 插件接口
type Plugin interface {
	// Name 返回插件名称
	Name() string
	// Version 返回插件版本
	Version() string
	// Initialize 初始化插件
	Initialize(config map[string]interface{}) error
	// Start 启动插件
	Start() error
	// Stop 停止插件
	Stop() error
	// IsRunning 检查插件是否运行中
	IsRunning() bool
}

// DataSourcePlugin 数据源插件接口
type DataSourcePlugin interface {
	Plugin
	// Connect 连接数据源
	Connect(connectionString string) (interface{}, error)
	// Disconnect 断开连接
	Disconnect(conn interface{}) error
	// Query 执行查询
	Query(conn interface{}, query string, params []interface{}) (interface{}, error)
	// Execute 执行命令
	Execute(conn interface{}, command string, params []interface{}) (int64, error)
}

// FunctionPlugin 函数插件接口
type FunctionPlugin interface {
	Plugin
	// Register 注册函数
	Register(name string, fn interface{}) error
	// Unregister 注销函数
	Unregister(name string) error
	// Call 调用函数
	Call(name string, args []interface{}) (interface{}, error)
	// GetFunction 获取函数
	GetFunction(name string) (interface{}, error)
	// ListFunctions 列出所有函数
	ListFunctions() []string
}

// MonitorPlugin 监控插件接口
type MonitorPlugin interface {
	Plugin
	// RecordMetric 记录指标
	RecordMetric(name string, value float64, tags map[string]string)
	// RecordEvent 记录事件
	RecordEvent(name string, data map[string]interface{})
	// GetMetric 获取指标值
	GetMetric(name string) (float64, error)
	// GetMetrics 获取所有指标
	GetMetrics() map[string]float64
}

// PluginManager 插件管理器
type PluginManager struct {
	dataSourcePlugins map[string]DataSourcePlugin
	functionPlugins   map[string]FunctionPlugin
	monitorPlugins    map[string]MonitorPlugin
	mu                sync.RWMutex
}

// NewPluginManager 创建插件管理器
func NewPluginManager() *PluginManager {
	return &PluginManager{
		dataSourcePlugins: make(map[string]DataSourcePlugin),
		functionPlugins:   make(map[string]FunctionPlugin),
		monitorPlugins:    make(map[string]MonitorPlugin),
	}
}

// RegisterDataSourcePlugin 注册数据源插件
func (pm *PluginManager) RegisterDataSourcePlugin(plugin DataSourcePlugin, config map[string]interface{}) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	name := plugin.Name()
	if _, exists := pm.dataSourcePlugins[name]; exists {
		return fmt.Errorf("data source plugin '%s' already registered", name)
	}

	// 初始化插件
	if err := plugin.Initialize(config); err != nil {
		return fmt.Errorf("failed to initialize plugin '%s': %w", name, err)
	}

	pm.dataSourcePlugins[name] = plugin
	return nil
}

// RegisterFunctionPlugin 注册函数插件
func (pm *PluginManager) RegisterFunctionPlugin(plugin FunctionPlugin, config map[string]interface{}) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	name := plugin.Name()
	if _, exists := pm.functionPlugins[name]; exists {
		return fmt.Errorf("function plugin '%s' already registered", name)
	}

	// 初始化插件
	if err := plugin.Initialize(config); err != nil {
		return fmt.Errorf("failed to initialize plugin '%s': %w", name, err)
	}

	pm.functionPlugins[name] = plugin
	return nil
}

// RegisterMonitorPlugin 注册监控插件
func (pm *PluginManager) RegisterMonitorPlugin(plugin MonitorPlugin, config map[string]interface{}) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	name := plugin.Name()
	if _, exists := pm.monitorPlugins[name]; exists {
		return fmt.Errorf("monitor plugin '%s' already registered", name)
	}

	// 初始化插件
	if err := plugin.Initialize(config); err != nil {
		return fmt.Errorf("failed to initialize plugin '%s': %w", name, err)
	}

	pm.monitorPlugins[name] = plugin
	return nil
}

// UnregisterPlugin 注销插件
func (pm *PluginManager) UnregisterPlugin(name string) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	// 尝试注销数据源插件
	if plugin, ok := pm.dataSourcePlugins[name]; ok {
		if plugin.IsRunning() {
			plugin.Stop()
		}
		delete(pm.dataSourcePlugins, name)
		return nil
	}

	// 尝试注销函数插件
	if plugin, ok := pm.functionPlugins[name]; ok {
		if plugin.IsRunning() {
			plugin.Stop()
		}
		delete(pm.functionPlugins, name)
		return nil
	}

	// 尝试注销监控插件
	if plugin, ok := pm.monitorPlugins[name]; ok {
		if plugin.IsRunning() {
			plugin.Stop()
		}
		delete(pm.monitorPlugins, name)
		return nil
	}

	return fmt.Errorf("plugin '%s' not found", name)
}

// StartPlugin 启动插件
func (pm *PluginManager) StartPlugin(name string) error {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	// 尝试启动数据源插件
	if plugin, ok := pm.dataSourcePlugins[name]; ok {
		return plugin.Start()
	}

	// 尝试启动函数插件
	if plugin, ok := pm.functionPlugins[name]; ok {
		return plugin.Start()
	}

	// 尝试启动监控插件
	if plugin, ok := pm.monitorPlugins[name]; ok {
		return plugin.Start()
	}

	return fmt.Errorf("plugin '%s' not found", name)
}

// StopPlugin 停止插件
func (pm *PluginManager) StopPlugin(name string) error {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	// 尝试停止数据源插件
	if plugin, ok := pm.dataSourcePlugins[name]; ok {
		return plugin.Stop()
	}

	// 尝试停止函数插件
	if plugin, ok := pm.functionPlugins[name]; ok {
		return plugin.Stop()
	}

	// 尝试停止监控插件
	if plugin, ok := pm.monitorPlugins[name]; ok {
		return plugin.Stop()
	}

	return fmt.Errorf("plugin '%s' not found", name)
}

// GetDataSourcePlugin 获取数据源插件
func (pm *PluginManager) GetDataSourcePlugin(name string) (DataSourcePlugin, error) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	plugin, ok := pm.dataSourcePlugins[name]
	if !ok {
		return nil, fmt.Errorf("data source plugin '%s' not found", name)
	}

	return plugin, nil
}

// GetFunctionPlugin 获取函数插件
func (pm *PluginManager) GetFunctionPlugin(name string) (FunctionPlugin, error) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	plugin, ok := pm.functionPlugins[name]
	if !ok {
		return nil, fmt.Errorf("function plugin '%s' not found", name)
	}

	return plugin, nil
}

// GetMonitorPlugin 获取监控插件
func (pm *PluginManager) GetMonitorPlugin(name string) (MonitorPlugin, error) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	plugin, ok := pm.monitorPlugins[name]
	if !ok {
		return nil, fmt.Errorf("monitor plugin '%s' not found", name)
	}

	return plugin, nil
}

// ListPlugins 列出所有插件
func (pm *PluginManager) ListPlugins() []string {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	plugins := make([]string, 0)

	for name := range pm.dataSourcePlugins {
		plugins = append(plugins, name)
	}

	for name := range pm.functionPlugins {
		plugins = append(plugins, name)
	}

	for name := range pm.monitorPlugins {
		plugins = append(plugins, name)
	}

	return plugins
}

// StartAllPlugins 启动所有插件
func (pm *PluginManager) StartAllPlugins() error {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	// 启动所有数据源插件
	for _, plugin := range pm.dataSourcePlugins {
		if err := plugin.Start(); err != nil {
			return fmt.Errorf("failed to start data source plugin '%s': %w", plugin.Name(), err)
		}
	}

	// 启动所有函数插件
	for _, plugin := range pm.functionPlugins {
		if err := plugin.Start(); err != nil {
			return fmt.Errorf("failed to start function plugin '%s': %w", plugin.Name(), err)
		}
	}

	// 启动所有监控插件
	for _, plugin := range pm.monitorPlugins {
		if err := plugin.Start(); err != nil {
			return fmt.Errorf("failed to start monitor plugin '%s': %w", plugin.Name(), err)
		}
	}

	return nil
}

// StopAllPlugins 停止所有插件
func (pm *PluginManager) StopAllPlugins() error {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	// 停止所有数据源插件
	for _, plugin := range pm.dataSourcePlugins {
		if plugin.IsRunning() {
			if err := plugin.Stop(); err != nil {
				return fmt.Errorf("failed to stop data source plugin '%s': %w", plugin.Name(), err)
			}
		}
	}

	// 停止所有函数插件
	for _, plugin := range pm.functionPlugins {
		if plugin.IsRunning() {
			if err := plugin.Stop(); err != nil {
				return fmt.Errorf("failed to stop function plugin '%s': %w", plugin.Name(), err)
			}
		}
	}

	// 停止所有监控插件
	for _, plugin := range pm.monitorPlugins {
		if plugin.IsRunning() {
			if err := plugin.Stop(); err != nil {
				return fmt.Errorf("failed to stop monitor plugin '%s': %w", plugin.Name(), err)
			}
		}
	}

	return nil
}

// BasePlugin 基础插件实现
type BasePlugin struct {
	name    string
	version string
	running bool
	mu      sync.RWMutex
	config  map[string]interface{}
}

// NewBasePlugin 创建基础插件
func NewBasePlugin(name, version string) *BasePlugin {
	return &BasePlugin{
		name:    name,
		version: version,
		running: false,
		config:  make(map[string]interface{}),
	}
}

// Name 返回插件名称
func (bp *BasePlugin) Name() string {
	return bp.name
}

// Version 返回插件版本
func (bp *BasePlugin) Version() string {
	return bp.version
}

// Initialize 初始化插件
func (bp *BasePlugin) Initialize(config map[string]interface{}) error {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	bp.config = config
	return nil
}

// Start 启动插件
func (bp *BasePlugin) Start() error {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	bp.running = true
	return nil
}

// Stop 停止插件
func (bp *BasePlugin) Stop() error {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	bp.running = false
	return nil
}

// IsRunning 检查插件是否运行中
func (bp *BasePlugin) IsRunning() bool {
	bp.mu.RLock()
	defer bp.mu.RUnlock()

	return bp.running
}

// GetConfig 获取配置
func (bp *BasePlugin) GetConfig() map[string]interface{} {
	bp.mu.RLock()
	defer bp.mu.RUnlock()

	config := make(map[string]interface{})
	for k, v := range bp.config {
		config[k] = v
	}
	return config
}
