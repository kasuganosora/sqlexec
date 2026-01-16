package datasource

import (
	"testing"
	"time"
)

func TestConverter_Convert(t *testing.T) {
	fields := []Field{
		{Name: "id", Type: TypeInt},
		{Name: "name", Type: TypeString},
		{Name: "salary", Type: TypeFloat},
		{Name: "active", Type: TypeBoolean},
		{Name: "created_at", Type: TypeDate},
	}
	converter := NewConverter(fields)
	tm := time.Now()
	row := Row{
		"id":         "123",
		"name":       "张三",
		"salary":     "4567.89",
		"active":     "true",
		"created_at": tm.Format("2006-01-02 15:04:05"),
	}
	result, err := converter.Convert(row)
	if err != nil {
		t.Fatalf("转换失败: %v", err)
	}
	if result[0] != int64(123) {
		t.Errorf("id转换错误: %v", result[0])
	}
	if result[1] != "张三" {
		t.Errorf("name转换错误: %v", result[1])
	}
	if result[2] != 4567.89 {
		t.Errorf("salary转换错误: %v", result[2])
	}
	if result[3] != true {
		t.Errorf("active转换错误: %v", result[3])
	}
	if t1, ok := result[4].(time.Time); !ok || t1.Format("2006-01-02 15:04:05") != tm.Format("2006-01-02 15:04:05") {
		t.Errorf("created_at转换错误: %v", result[4])
	}
}

func TestConverter_GetColumnTypesAndNames(t *testing.T) {
	fields := []Field{
		{Name: "id", Type: TypeInt},
		{Name: "salary", Type: TypeFloat},
		{Name: "active", Type: TypeBoolean},
		{Name: "created_at", Type: TypeDate},
		{Name: "name", Type: TypeString},
	}
	converter := NewConverter(fields)
	types := converter.GetColumnTypes()
	names := converter.GetColumnNames()
	expectTypes := []string{"INT", "DOUBLE", "BOOLEAN", "DATETIME", "VARCHAR(255)"}
	expectNames := []string{"id", "salary", "active", "created_at", "name"}
	for i, typ := range expectTypes {
		if types[i] != typ {
			t.Errorf("列类型错误: 期望%s, 实际%s", typ, types[i])
		}
	}
	for i, name := range expectNames {
		if names[i] != name {
			t.Errorf("列名错误: 期望%s, 实际%s", name, names[i])
		}
	}
}
