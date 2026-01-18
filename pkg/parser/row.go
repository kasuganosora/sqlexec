package parser

// Row 行数据的类型别名，与 resource.Row 保持一致
// 为了避免循环导入，这里使用 map[string]interface{} 表示行数据
type Row map[string]interface{}
