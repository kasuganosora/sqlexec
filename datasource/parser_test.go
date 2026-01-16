package datasource

import (
	"fmt"
	"testing"
)

func TestParser_Parse_SelectBasic(t *testing.T) {
	parser := NewParser(NewFunctionManager())
	query, err := parser.Parse("SELECT id, name FROM test_table")
	if err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	if query.Type != QueryTypeSelect {
		t.Errorf("期望QueryTypeSelect，实际%v", query.Type)
	}
	if query.Table != "test_table" {
		t.Errorf("期望表名test_table，实际%s", query.Table)
	}
	if len(query.Fields) != 2 || query.Fields[0] != "id" || query.Fields[1] != "name" {
		t.Errorf("字段解析错误: %#v", query.Fields)
	}
}

func TestParser_Parse_SelectWhere(t *testing.T) {
	parser := NewParser(NewFunctionManager())
	query, err := parser.Parse("SELECT id FROM test_table WHERE age > 18")
	if err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	if len(query.Where) != 1 {
		t.Errorf("期望1个where条件，实际%d", len(query.Where))
	}
	cond := query.Where[0]
	if cond.Field != "age" || cond.Operator != ">" || cond.Value != int64(18) {
		t.Errorf("where条件解析错误: %#v", cond)
	}
}

func TestParser_Parse_SelectGroupByHavingOrderByLimit(t *testing.T) {
	parser := NewParser(NewFunctionManager())
	sql := `SELECT id, name FROM test_table WHERE age > 18 GROUP BY name HAVING SUM(id) > 1 ORDER BY id DESC LIMIT 10 OFFSET 5`
	query, err := parser.Parse(sql)
	if err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	fmt.Printf("[DEBUG] 解析后的query: %+v\n", query)
	if len(query.GroupBy) != 1 || query.GroupBy[0] != "name" {
		t.Errorf("GroupBy解析错误: %#v", query.GroupBy)
	}
	if len(query.Having) != 1 {
		t.Errorf("Having解析错误: %#v", query.Having)
	} else {
		having := query.Having[0]
		fmt.Printf("[DEBUG] Having字段: Field=%s, Operator=%s, Value=%#v\n", having.Field, having.Operator, having.Value)
		if having.Field != "SUM(id)" {
			t.Errorf("Having字段名解析错误: %s", having.Field)
		}
		if having.Operator != ">" {
			t.Errorf("Having操作符解析错误: %s", having.Operator)
		}
		if having.Value != int64(1) {
			t.Errorf("Having值解析错误: %#v", having.Value)
		}
	}
	if len(query.OrderBy) != 1 {
		t.Errorf("期望1个ORDER BY条件，实际得到%d个", len(query.OrderBy))
	} else {
		expectedOrderBy := OrderBy{
			Field:     "id",
			Direction: "DESC",
		}
		if query.OrderBy[0] != expectedOrderBy {
			t.Errorf("ORDER BY不匹配，期望%v，实际%v", expectedOrderBy, query.OrderBy[0])
		}
	}
	if query.Limit != 10 || query.Offset != 5 {
		t.Errorf("Limit/Offset解析错误: limit=%d offset=%d", query.Limit, query.Offset)
	}
}

func TestParser_Parse_InvalidSQL(t *testing.T) {
	parser := NewParser(NewFunctionManager())
	_, err := parser.Parse("SELEC id FROM")
	if err == nil {
		t.Error("非法SQL应当解析失败")
	}
}
