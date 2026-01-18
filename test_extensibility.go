package main

import (
	"fmt"
	"log"
	"mysql-proxy/mysql/extensibility"
)

// 示例数据源插件
type MySQLDataSourcePlugin struct {
	*extensibility.BasePlugin
}

func NewMySQLDataSourcePlugin() *MySQLDataSourcePlugin {
	base := extensibility.NewBasePlugin("mysql-datasource", "1.0.0")
	return &MySQLDataSourcePlugin{
		BasePlugin: base,
	}
}

func (p *MySQLDataSourcePlugin) Connect(connectionString string) (interface{}, error) {
	fmt.Printf("连接MySQL: %s\n", connectionString)
	return "mysql-connection", nil
}

func (p *MySQLDataSourcePlugin) Disconnect(conn interface{}) error {
	fmt.Printf("断开MySQL连接: %v\n", conn)
	return nil
}

func (p *MySQLDataSourcePlugin) Query(conn interface{}, query string, params []interface{}) (interface{}, error) {
	fmt.Printf("执行查询: %s, 参数: %v\n", query, params)
	return []map[string]interface{}{
		{"id": 1, "name": "test"},
	}, nil
}

func (p *MySQLDataSourcePlugin) Execute(conn interface{}, command string, params []interface{}) (int64, error) {
	fmt.Printf("执行命令: %s, 参数: %v\n", command, params)
	return 1, nil
}

// 示例函数插件
type CustomFunctionPlugin struct {
	*extensibility.BasePlugin
	functions map[string]interface{}
}

func NewCustomFunctionPlugin() *CustomFunctionPlugin {
	base := extensibility.NewBasePlugin("custom-functions", "1.0.0")
	return &CustomFunctionPlugin{
		BasePlugin: base,
		functions: make(map[string]interface{}),
	}
}

func (p *CustomFunctionPlugin) Register(name string, fn interface{}) error {
	p.functions[name] = fn
	fmt.Printf("注册函数: %s\n", name)
	return nil
}

func (p *CustomFunctionPlugin) Unregister(name string) error {
	delete(p.functions, name)
	fmt.Printf("注销函数: %s\n", name)
	return nil
}

func (p *CustomFunctionPlugin) Call(name string, args []interface{}) (interface{}, error) {
	if _, ok := p.functions[name]; !ok {
		return nil, fmt.Errorf("function '%s' not found", name)
	}

	// 简化实现，实际需要反射调用
	fmt.Printf("调用函数: %s, 参数: %v\n", name, args)
	return fmt.Sprintf("result of %s", name), nil
}

func (p *CustomFunctionPlugin) GetFunction(name string) (interface{}, error) {
	fn, ok := p.functions[name]
	if !ok {
		return nil, fmt.Errorf("function '%s' not found", name)
	}
	return fn, nil
}

func (p *CustomFunctionPlugin) ListFunctions() []string {
	functions := make([]string, 0, len(p.functions))
	for name := range p.functions {
		functions = append(functions, name)
	}
	return functions
}

// 示例监控插件
type MetricsMonitorPlugin struct {
	*extensibility.BasePlugin
	metrics map[string]float64
	events  map[string][]map[string]interface{}
}

func NewMetricsMonitorPlugin() *MetricsMonitorPlugin {
	base := extensibility.NewBasePlugin("metrics-monitor", "1.0.0")
	return &MetricsMonitorPlugin{
		BasePlugin: base,
		metrics:    make(map[string]float64),
		events:     make(map[string][]map[string]interface{}),
	}
}

func (p *MetricsMonitorPlugin) RecordMetric(name string, value float64, tags map[string]string) {
	key := name
	if len(tags) > 0 {
		key += fmt.Sprintf("%v", tags)
	}
	p.metrics[key] = value
	fmt.Printf("记录指标: %s = %f\n", key, value)
}

func (p *MetricsMonitorPlugin) RecordEvent(name string, data map[string]interface{}) {
	if p.events[name] == nil {
		p.events[name] = make([]map[string]interface{}, 0)
	}
	p.events[name] = append(p.events[name], data)
	fmt.Printf("记录事件: %s, 数据: %v\n", name, data)
}

func (p *MetricsMonitorPlugin) GetMetric(name string) (float64, error) {
	value, ok := p.metrics[name]
	if !ok {
		return 0, fmt.Errorf("metric '%s' not found", name)
	}
	return value, nil
}

func (p *MetricsMonitorPlugin) GetMetrics() map[string]float64 {
	metrics := make(map[string]float64)
	for k, v := range p.metrics {
		metrics[k] = v
	}
	return metrics
}

func main() {
	fmt.Println("=== 阶段7可扩展性测试 ===\n")

	testPluginManager()
	testDataSourcePlugin()
	testFunctionPlugin()
	testMonitorPlugin()

	fmt.Println("\n=== 所有可能性测试完成 ===")
}

func testPluginManager() {
	fmt.Println("1. 插件管理器测试")
	fmt.Println("-------------------------------")

	pm := extensibility.NewPluginManager()

	// 注册数据源插件
	mysqlPlugin := NewMySQLDataSourcePlugin()
	err := pm.RegisterDataSourcePlugin(mysqlPlugin, map[string]interface{}{
		"host":     "localhost",
		"port":     3306,
		"database": "test",
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("数据源插件注册成功")

	// 注册函数插件
	functionPlugin := NewCustomFunctionPlugin()
	err = pm.RegisterFunctionPlugin(functionPlugin, map[string]interface{}{
		"enabled": true,
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("函数插件注册成功")

	// 注册监控插件
	monitorPlugin := NewMetricsMonitorPlugin()
	err = pm.RegisterMonitorPlugin(monitorPlugin, map[string]interface{}{
		"interval": 60,
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("监控插件注册成功")

	// 列出所有插件
	plugins := pm.ListPlugins()
	fmt.Printf("所有插件: %v\n", plugins)

	// 启动所有插件
	err = pm.StartAllPlugins()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("所有插件启动成功")

	// 停止所有插件
	err = pm.StopAllPlugins()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("所有插件停止成功")

	fmt.Println()
}

func testDataSourcePlugin() {
	fmt.Println("2. 数据源插件测试")
	fmt.Println("-------------------------------")

	pm := extensibility.NewPluginManager()

	// 注册并启动插件
	mysqlPlugin := NewMySQLDataSourcePlugin()
	err := pm.RegisterDataSourcePlugin(mysqlPlugin, map[string]interface{}{})
	if err != nil {
		log.Fatal(err)
	}

	err = pm.StartPlugin("mysql-datasource")
	if err != nil {
		log.Fatal(err)
	}

	// 获取插件
	plugin, err := pm.GetDataSourcePlugin("mysql-datasource")
	if err != nil {
		log.Fatal(err)
	}

	// 连接数据源
	conn, err := plugin.Connect("localhost:3306/testdb")
	if err != nil {
		log.Fatal(err)
	}

	// 执行查询
	result, err := plugin.Query(conn, "SELECT * FROM users WHERE id = ?", []interface{}{1})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("查询结果: %v\n", result)

	// 执行命令
	rowsAffected, err := plugin.Execute(conn, "INSERT INTO users (name) VALUES (?)", []interface{}{"John"})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("影响的行数: %d\n", rowsAffected)

	// 断开连接
	err = plugin.Disconnect(conn)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println()
}

func testFunctionPlugin() {
	fmt.Println("3. 函数插件测试")
	fmt.Println("-------------------------------")

	pm := extensibility.NewPluginManager()

	// 注册并启动插件
	functionPlugin := NewCustomFunctionPlugin()
	err := pm.RegisterFunctionPlugin(functionPlugin, map[string]interface{}{})
	if err != nil {
		log.Fatal(err)
	}

	err = pm.StartPlugin("custom-functions")
	if err != nil {
		log.Fatal(err)
	}

	// 获取插件
	plugin, err := pm.GetFunctionPlugin("custom-functions")
	if err != nil {
		log.Fatal(err)
	}

	// 注册函数
	err = plugin.Register("add", func(a, b int) int { return a + b })
	if err != nil {
		log.Fatal(err)
	}

	err = plugin.Register("multiply", func(a, b int) int { return a * b })
	if err != nil {
		log.Fatal(err)
	}

	// 列出所有函数
	functions := plugin.ListFunctions()
	fmt.Printf("所有函数: %v\n", functions)

	// 调用函数
	result, err := plugin.Call("add", []interface{}{10, 20})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("add(10, 20) = %v\n", result)

	result, err = plugin.Call("multiply", []interface{}{5, 6})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("multiply(5, 6) = %v\n", result)

	// 注销函数
	err = plugin.Unregister("multiply")
	if err != nil {
		log.Fatal(err)
	}

	functions = plugin.ListFunctions()
	fmt.Printf("注销后函数列表: %v\n", functions)

	fmt.Println()
}

func testMonitorPlugin() {
	fmt.Println("4. 监控插件测试")
	fmt.Println("-------------------------------")

	pm := extensibility.NewPluginManager()

	// 注册并启动插件
	monitorPlugin := NewMetricsMonitorPlugin()
	err := pm.RegisterMonitorPlugin(monitorPlugin, map[string]interface{}{})
	if err != nil {
		log.Fatal(err)
	}

	err = pm.StartPlugin("metrics-monitor")
	if err != nil {
		log.Fatal(err)
	}

	// 获取插件
	plugin, err := pm.GetMonitorPlugin("metrics-monitor")
	if err != nil {
		log.Fatal(err)
	}

	// 记录指标
	plugin.RecordMetric("query_count", 100, map[string]string{
		"database": "mydb",
		"table":     "users",
	})

	plugin.RecordMetric("query_time", 45.5, map[string]string{
		"type": "select",
	})

	plugin.RecordMetric("error_count", 2, nil)

	// 记录事件
	plugin.RecordEvent("query_start", map[string]interface{}{
		"query": "SELECT * FROM users",
		"time":  "2024-01-01T00:00:00Z",
	})

	plugin.RecordEvent("query_end", map[string]interface{}{
		"query":     "SELECT * FROM users",
		"duration":  45.5,
		"rows":      10,
	})

	// 获取指标值
	queryCount, err := plugin.GetMetric("query_count[map[database:mydb table:users]]")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("query_count = %f\n", queryCount)

	queryTime, err := plugin.GetMetric("query_time[map[type:select]]")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("query_time = %f\n", queryTime)

	// 获取所有指标
	metrics := plugin.GetMetrics()
	fmt.Printf("所有指标: %d 个\n", len(metrics))
	for name, value := range metrics {
		fmt.Printf("  %s = %f\n", name, value)
	}

	fmt.Println()
}
