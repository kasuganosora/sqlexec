package datasource

import (
	"testing"
	"time"
)

func TestPlanner_GeneratePlan(t *testing.T) {
	// 创建配置管理器
	configManager := NewConfigManager("./testdata")

	// 创建计划生成器
	planner := NewPlanner(configManager)

	// 测试用例1：简单的表扫描
	t.Run("简单表扫描", func(t *testing.T) {
		query := &Query{
			Table:  "users",
			Fields: []string{"id", "name", "email"},
		}

		plan, err := planner.GeneratePlan(query)
		if err != nil {
			t.Errorf("生成计划失败: %v", err)
		}

		if plan.Type != PlanTypeTableScan {
			t.Errorf("期望计划类型为 TableScan，实际为 %v", plan.Type)
		}

		if plan.Table != "users" {
			t.Errorf("期望表名为 users，实际为 %s", plan.Table)
		}
	})

	// 测试用例2：带WHERE条件的查询
	t.Run("带WHERE条件的查询", func(t *testing.T) {
		query := &Query{
			Table:  "users",
			Fields: []string{"id", "name"},
			Where: []Condition{
				{
					Field:    "age",
					Operator: ">",
					Value:    18,
				},
			},
		}

		plan, err := planner.GeneratePlan(query)
		if err != nil {
			t.Errorf("生成计划失败: %v", err)
		}

		if plan.Type != PlanTypeFilter {
			t.Errorf("期望计划类型为 Filter，实际为 %v", plan.Type)
		}

		if len(plan.Children) != 1 {
			t.Errorf("期望有1个子节点，实际有 %d 个", len(plan.Children))
		}

		if plan.Children[0].Type != PlanTypeTableScan {
			t.Errorf("期望子节点类型为 TableScan，实际为 %v", plan.Children[0].Type)
		}
	})

	// 测试用例3：带JOIN的查询
	t.Run("带JOIN的查询", func(t *testing.T) {
		query := &Query{
			Table:  "users",
			Fields: []string{"users.id", "orders.order_id"},
			Joins: []Join{
				{
					Type:      JoinTypeInner,
					Table:     "orders",
					Condition: "users.id = orders.user_id",
				},
			},
		}

		plan, err := planner.GeneratePlan(query)
		if err != nil {
			t.Errorf("生成计划失败: %v", err)
		}

		if plan.Type != PlanTypeNestedLoopJoin {
			t.Errorf("期望计划类型为 NestedLoopJoin，实际为 %v", plan.Type)
		}

		if len(plan.Children) != 1 {
			t.Errorf("期望有1个子节点，实际有 %d 个", len(plan.Children))
		}
	})
}

func TestPlanner_Stats(t *testing.T) {
	configManager := NewConfigManager("./testdata")
	planner := NewPlanner(configManager)

	// 测试统计信息
	t.Run("统计信息测试", func(t *testing.T) {
		stats := planner.GetStats()
		if stats == nil {
			t.Error("期望获取到统计信息，实际为 nil")
		}

		// 重置统计信息
		planner.ResetStats()
		stats = planner.GetStats()
		if stats.TotalTime != 0 {
			t.Errorf("期望总时间为0，实际为 %v", stats.TotalTime)
		}

		// 更新统计信息
		newStats := &QueryStats{
			TotalTime:     time.Second,
			RowsProcessed: 100,
		}
		planner.UpdateStats(newStats)
		stats = planner.GetStats()
		if stats.TotalTime != time.Second {
			t.Errorf("期望总时间为1秒，实际为 %v", stats.TotalTime)
		}
		if stats.RowsProcessed != 100 {
			t.Errorf("期望处理行数为100，实际为 %d", stats.RowsProcessed)
		}
	})
}
