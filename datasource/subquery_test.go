package datasource

import (
	"context"
	"testing"
	"time"
)

// 创建测试配置
func createSubqueryTestConfig() *Config {
	return &Config{
		Tables: map[string]*TableConfig{
			"test_table": {
				Name:     "test_table",
				Type:     "json",
				FilePath: "../testdata/test.json",
				Fields: []Field{
					{Name: "id", Type: TypeInt},
					{Name: "name", Type: TypeString},
					{Name: "age", Type: TypeInt},
				},
				RowCount: 1000,
			},
		},
	}
}

func TestSubqueryExecutor(t *testing.T) {
	// 创建配置
	config := createSubqueryTestConfig()

	// 创建执行器
	executor := NewExecutor(config)

	// 创建子查询执行器
	se := NewSubqueryExecutor(executor)

	// 测试基本查询
	t.Run("基本查询测试", func(t *testing.T) {
		query := &Query{
			Type:   QueryTypeSelect,
			Table:  "test_table",
			Fields: []string{"id", "name", "age"},
			Where: []Condition{
				{
					Field:    "age",
					Operator: ">",
					Value:    18,
				},
			},
		}
		params := map[string]interface{}{}

		ctx := context.Background()
		results, err := se.ExecuteSubquery(ctx, query, params)
		if err != nil {
			t.Errorf("执行查询失败: %v", err)
		}
		if results == nil {
			t.Error("查询结果为空")
		}
	})

	// 测试缓存功能
	t.Run("缓存测试", func(t *testing.T) {
		query := &Query{
			Type:   QueryTypeSelect,
			Table:  "test_table",
			Fields: []string{"id", "name", "age"},
			Where: []Condition{
				{
					Field:    "age",
					Operator: "<",
					Value:    100,
				},
			},
		}
		params := map[string]interface{}{}

		ctx := context.Background()

		// 第一次查询
		results1, err := se.ExecuteSubquery(ctx, query, params)
		if err != nil {
			t.Errorf("第一次查询失败: %v", err)
		}

		// 第二次查询应该从缓存获取
		results2, err := se.ExecuteSubquery(ctx, query, params)
		if err != nil {
			t.Errorf("第二次查询失败: %v", err)
		}

		// 验证两次结果是否相同
		if len(results1) != len(results2) {
			t.Error("缓存结果与原始结果不一致")
		}
	})

	// 测试并行查询
	t.Run("并行查询测试", func(t *testing.T) {
		queries := []*Query{
			{
				Type:   QueryTypeSelect,
				Table:  "test_table",
				Fields: []string{"id", "age"},
				Where: []Condition{
					{
						Field:    "age",
						Operator: ">",
						Value:    20,
					},
				},
			},
			{
				Type:   QueryTypeSelect,
				Table:  "test_table",
				Fields: []string{"id", "age"},
				Where: []Condition{
					{
						Field:    "age",
						Operator: "<",
						Value:    40,
					},
				},
			},
		}
		params := []map[string]interface{}{{}, {}}

		ctx := context.Background()
		results, err := se.ExecuteParallel(ctx, queries, params)
		if err != nil {
			t.Errorf("并行查询失败: %v", err)
		}
		if len(results) != len(queries) {
			t.Error("并行查询结果数量不匹配")
		}
	})

	// 测试超时设置
	t.Run("超时设置测试", func(t *testing.T) {
		se.SetTimeout(1 * time.Second)
		if se.timeout != 1*time.Second {
			t.Error("超时设置失败")
		}
	})

	// 测试最大并发数设置
	t.Run("最大并发数设置测试", func(t *testing.T) {
		se.SetMaxGoroutines(5)
		if se.maxGoroutines != 5 {
			t.Error("最大并发数设置失败")
		}
	})

	// 测试清除缓存
	t.Run("清除缓存测试", func(t *testing.T) {
		se.ClearCache()
		if len(se.cache.items) != 0 {
			t.Error("清除缓存失败")
		}
	})
}

// 测试子查询
func TestSubquery(t *testing.T) {
	// 创建配置
	config := createSubqueryTestConfig()

	// 创建执行器
	executor := NewExecutor(config)

	// 创建子查询
	subquery := &Query{
		Type:   QueryTypeSelect,
		Table:  "test_table",
		Fields: []string{"id"},
		Where: []Condition{
			{
				Field:    "age",
				Operator: ">",
				Value:    int64(30),
			},
		},
	}

	// 创建主查询
	query := &Query{
		Type:   QueryTypeSelect,
		Table:  "test_table",
		Fields: []string{"id", "name"},
		Where: []Condition{
			{
				Field:    "id",
				Operator: "IN",
				Subquery: subquery,
			},
		},
	}

	// 执行查询
	results, err := executor.ExecuteQuery(context.Background(), query)
	if err != nil {
		t.Errorf("执行查询失败: %v", err)
	}

	// 验证结果
	if len(results) != 1 {
		t.Errorf("期望1条记录，实际得到%d条", len(results))
	}

	// 验证结果内容
	expectedID := int64(3)
	expectedName := "王五"
	if id, ok := results[0]["id"].(int64); !ok || id != expectedID {
		t.Errorf("ID不匹配，期望%d，实际%v", expectedID, results[0]["id"])
	}
	if name, ok := results[0]["name"].(string); !ok || name != expectedName {
		t.Errorf("姓名不匹配，期望%s，实际%v", expectedName, results[0]["name"])
	}
}
