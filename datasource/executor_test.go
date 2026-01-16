package datasource

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// 创建测试用的表配置
func createTestTableConfig() *TableConfig {
	wd, _ := os.Getwd()
	absPath := filepath.Join(wd, "..", "testdata", "test.json")
	return &TableConfig{
		Name:     "test_table",
		Type:     "json",
		FilePath: absPath,
		Fields: []Field{
			{Name: "id", Type: TypeInt},
			{Name: "name", Type: TypeString},
			{Name: "age", Type: TypeInt},
			{Name: "salary", Type: TypeFloat},
			{Name: "created_at", Type: TypeDate},
		},
		RowCount: 1000,
	}
}

// 创建 join_table 的表配置
func createJoinTableConfig() *TableConfig {
	wd, _ := os.Getwd()
	absPath := filepath.Join(wd, "..", "testdata", "join.json")
	return &TableConfig{
		Name:     "join_table",
		Type:     "json",
		FilePath: absPath,
		Fields: []Field{
			{Name: "main_id", Type: TypeInt},
			{Name: "address", Type: TypeString},
		},
		RowCount: 3,
	}
}

// 创建测试用的配置
func createTestConfig() *Config {
	return &Config{
		Tables: map[string]*TableConfig{
			"test_table": createTestTableConfig(),
		},
	}
}

// 创建包含 join_table 的测试配置
func createTestConfigWithJoin() *Config {
	return &Config{
		Tables: map[string]*TableConfig{
			"test_table": createTestTableConfig(),
			"join_table": createJoinTableConfig(),
		},
	}
}

// 创建测试数据
func createTestData() []Row {
	now := time.Now()
	return []Row{
		{
			"id":         int64(1),
			"name":       "张三",
			"age":        int64(25),
			"salary":     float64(10000),
			"created_at": now,
		},
		{
			"id":         int64(2),
			"name":       "李四",
			"age":        int64(30),
			"salary":     float64(15000),
			"created_at": now.Add(24 * time.Hour),
		},
		{
			"id":         int64(3),
			"name":       "王五",
			"age":        int64(35),
			"salary":     float64(20000),
			"created_at": now.Add(48 * time.Hour),
		},
	}
}

// 测试基本查询
func TestExecutor_BasicQuery(t *testing.T) {
	// 创建配置
	config := createTestConfig()

	// 创建执行器
	executor := NewExecutor(config)

	// 创建测试查询
	query := &Query{
		Type:   QueryTypeSelect,
		Table:  "test_table",
		Fields: []string{"id", "name", "age"},
		Where: []Condition{
			{
				Field:    "age",
				Operator: ">=",
				Value:    int64(0),
			},
		},
		OrderBy: []OrderBy{
			{Field: "age", Direction: "ASC"},
		},
	}

	// 执行查询
	results, err := executor.ExecuteQuery(context.Background(), query)
	if err != nil {
		t.Errorf("执行查询失败: %v", err)
	}

	// 验证结果数量
	if len(results) != 3 {
		t.Errorf("期望3条记录，实际得到%d条", len(results))
	}

	// 验证结果内容和顺序
	expectedResults := []struct {
		id   int64
		name string
		age  int64
	}{
		{1, "张三", 25},
		{2, "李四", 30},
		{3, "王五", 35},
	}

	for i, expected := range expectedResults {
		if i >= len(results) {
			t.Errorf("结果数量不足，期望至少%d条", i+1)
			break
		}
		if id, ok := results[i]["id"].(int64); !ok || id != expected.id {
			t.Errorf("第%d条记录ID不匹配，期望%d，实际%v", i+1, expected.id, results[i]["id"])
		}
		if name, ok := results[i]["name"].(string); !ok || name != expected.name {
			t.Errorf("第%d条记录姓名不匹配，期望%s，实际%v", i+1, expected.name, results[i]["name"])
		}
		if age, ok := results[i]["age"].(int64); !ok || age != expected.age {
			t.Errorf("第%d条记录年龄不匹配，期望%d，实际%v", i+1, expected.age, results[i]["age"])
		}
	}
}

// 测试分组查询
func TestExecutor_GroupByQuery(t *testing.T) {
	executor := NewExecutor(createTestConfig())

	query := &Query{
		Type:    QueryTypeSelect,
		Table:   "test_table",
		Fields:  []string{"age", "salary"},
		GroupBy: []string{"age"},
		Having: []Condition{
			{
				Field:    "salary",
				Operator: ">",
				Value:    float64(12000),
			},
		},
	}

	results, err := executor.ExecuteQuery(context.Background(), query)
	if err != nil {
		t.Errorf("执行分组查询失败: %v", err)
	}

	// 验证结果
	if len(results) != 2 {
		t.Errorf("期望2条记录，实际得到%d条", len(results))
	}

	// 验证分组结果
	for _, row := range results {
		salary, ok := row["salary"].(float64)
		if !ok {
			t.Errorf("薪资字段类型错误")
			continue
		}
		if salary <= 12000 {
			t.Errorf("薪资应该大于12000，实际为%v", salary)
		}
	}
}

// 测试内连接
func TestExecutor_InnerJoin(t *testing.T) {
	executor := NewExecutor(createTestConfigWithJoin())

	query := &Query{
		Type:   QueryTypeSelect,
		Table:  "test_table",
		Fields: []string{"id", "name", "address"},
		Joins: []Join{
			{
				Type:      JoinTypeInner,
				Table:     "join_table",
				Condition: "id = main_id",
			},
		},
	}

	results, err := executor.ExecuteQuery(context.Background(), query)
	if err != nil {
		t.Errorf("执行内连接查询失败: %v", err)
	}

	// 验证结果
	if len(results) != 2 {
		t.Errorf("期望2条记录，实际得到%d条", len(results))
	}

	// 验证连接结果
	expectedAddresses := map[int64]string{
		1: "北京",
		2: "上海",
	}

	for _, row := range results {
		idVal, ok := row["id"]
		if !ok || idVal == nil {
			t.Errorf("ID字段不存在或为nil")
			continue
		}
		id, ok := idVal.(int64)
		if !ok {
			t.Errorf("ID字段类型错误")
			continue
		}
		addressVal, exists := row["address"]
		var addressStr string
		if !exists || addressVal == nil {
			addressStr = ""
		} else {
			addressStr, ok = addressVal.(string)
			if !ok {
				t.Errorf("地址字段类型错误")
				continue
			}
		}
		if expectedAddr, exists := expectedAddresses[id]; !exists || expectedAddr != addressStr {
			t.Errorf("ID %d 的地址不匹配，期望%s，实际%s", id, expectedAddr, addressStr)
		}
	}
}

// 测试左连接
func TestExecutor_LeftJoin(t *testing.T) {
	executor := NewExecutor(createTestConfigWithJoin())

	query := &Query{
		Type:   QueryTypeSelect,
		Table:  "test_table",
		Fields: []string{"id", "name", "address"},
		Joins: []Join{
			{
				Type:      JoinTypeLeft,
				Table:     "join_table",
				Condition: "id = main_id",
			},
		},
	}

	results, err := executor.ExecuteQuery(context.Background(), query)
	if err != nil {
		t.Errorf("执行左连接查询失败: %v", err)
	}

	// 验证结果
	if len(results) != 3 {
		t.Errorf("期望3条记录，实际得到%d条", len(results))
	}

	// 验证连接结果
	expectedAddresses := map[int64]string{
		1: "北京",
		2: "上海",
		3: "", // 左连接中未匹配的记录地址为空
	}

	for _, row := range results {
		idVal, ok := row["id"]
		if !ok || idVal == nil {
			t.Errorf("ID字段不存在或为nil")
			continue
		}
		id, ok := idVal.(int64)
		if !ok {
			t.Errorf("ID字段类型错误")
			continue
		}
		addressVal, exists := row["address"]
		var addressStr string
		if !exists || addressVal == nil {
			addressStr = ""
		} else {
			addressStr, ok = addressVal.(string)
			if !ok {
				t.Errorf("地址字段类型错误")
				continue
			}
		}
		if expectedAddr, exists := expectedAddresses[id]; !exists || expectedAddr != addressStr {
			t.Errorf("ID %d 的地址不匹配，期望%s，实际%s", id, expectedAddr, addressStr)
		}
	}
}

// 测试右连接
func TestExecutor_RightJoin(t *testing.T) {
	executor := NewExecutor(createTestConfigWithJoin())

	query := &Query{
		Type:   QueryTypeSelect,
		Table:  "test_table",
		Fields: []string{"id", "name", "address"},
		Joins: []Join{
			{
				Type:      JoinTypeRight,
				Table:     "join_table",
				Condition: "id = main_id",
			},
		},
	}

	results, err := executor.ExecuteQuery(context.Background(), query)
	if err != nil {
		t.Errorf("执行右连接查询失败: %v", err)
	}

	// 验证结果
	if len(results) != 3 {
		t.Errorf("期望3条记录，实际得到%d条", len(results))
	}

	// 验证连接结果
	expectedAddresses := map[int64]string{
		1: "北京",
		2: "上海",
		4: "广州", // 右连接中包含未匹配的记录
	}

	for _, row := range results {
		idVal, ok := row["id"]
		if !ok || idVal == nil {
			// 右连接未匹配主表时，id 允许为 nil，跳过比对
			continue
		}
		id, ok := idVal.(int64)
		if !ok {
			t.Errorf("ID字段类型错误")
			continue
		}
		addressVal, exists := row["address"]
		var addressStr string
		if !exists || addressVal == nil {
			addressStr = ""
		} else {
			addressStr, ok = addressVal.(string)
			if !ok {
				t.Errorf("地址字段类型错误")
				continue
			}
		}
		if expectedAddr, exists := expectedAddresses[id]; !exists || expectedAddr != addressStr {
			t.Errorf("ID %d 的地址不匹配，期望%s，实际%s", id, expectedAddr, addressStr)
		}
	}
}
