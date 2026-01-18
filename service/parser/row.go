package parser

// Row 行数据的类型别名，与 resource.Row 保持一�?
// 为了避免循环导入，这里使�?map[string]interface{} 表示行数�?
type Row map[string]interface{}
