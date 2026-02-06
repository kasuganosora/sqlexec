package optimizer

import (
	"sync"
	"testing"
	"time"
)

// TestNewHypotheticalIndexStore 测试创建存储
func TestNewHypotheticalIndexStore(t *testing.T) {
	store := NewHypotheticalIndexStore()
	if store == nil {
		t.Fatal("NewHypotheticalIndexStore returned nil")
	}
	if store.Count() != 0 {
		t.Errorf("Expected 0 indexes, got %d", store.Count())
	}
}

// TestCreateIndex 测试创建索引
func TestCreateIndex(t *testing.T) {
	store := NewHypotheticalIndexStore()

	// 创建第一个索引
	index, err := store.CreateIndex("users", []string{"id"}, true, false)
	if err != nil {
		t.Fatalf("CreateIndex failed: %v", err)
	}

	if index.ID == "" {
		t.Error("Expected non-empty index ID")
	}
	if index.TableName != "users" {
		t.Errorf("Expected table name 'users', got %s", index.TableName)
	}
	if len(index.Columns) != 1 || index.Columns[0] != "id" {
		t.Errorf("Expected columns [id], got %v", index.Columns)
	}
	if !index.IsUnique {
		t.Error("Expected IsUnique to be true")
	}
	if index.IsPrimary {
		t.Error("Expected IsPrimary to be false")
	}
	if store.Count() != 1 {
		t.Errorf("Expected 1 index, got %d", store.Count())
	}
}

// TestCreateIndexDuplicate 测试创建重复索引
func TestCreateIndexDuplicate(t *testing.T) {
	store := NewHypotheticalIndexStore()

	_, err := store.CreateIndex("users", []string{"id"}, true, false)
	if err != nil {
		t.Fatalf("First CreateIndex failed: %v", err)
	}

	// 尝试创建重复索引
	_, err = store.CreateIndex("users", []string{"id"}, true, false)
	if err == nil {
		t.Error("Expected error when creating duplicate index")
	}
}

// TestGetIndex 测试获取索引
func TestGetIndex(t *testing.T) {
	store := NewHypotheticalIndexStore()

	createdIndex, err := store.CreateIndex("users", []string{"id"}, true, false)
	if err != nil {
		t.Fatalf("CreateIndex failed: %v", err)
	}

	// 获取存在的索引
	index, ok := store.GetIndex(createdIndex.ID)
	if !ok {
		t.Error("Expected index to exist")
	}
	if index.ID != createdIndex.ID {
		t.Errorf("Expected ID %s, got %s", createdIndex.ID, index.ID)
	}

	// 获取不存在的索引
	_, ok = store.GetIndex("nonexistent")
	if ok {
		t.Error("Expected index to not exist")
	}
}

// TestGetTableIndexes 测试获取表的所有索引
func TestGetTableIndexes(t *testing.T) {
	store := NewHypotheticalIndexStore()

	_, _ = store.CreateIndex("users", []string{"id"}, true, false)
	_, _ = store.CreateIndex("users", []string{"email"}, false, false)
	_, _ = store.CreateIndex("orders", []string{"user_id"}, false, false)

	// 获取 users 表的索引
	userIndexes := store.GetTableIndexes("users")
	if len(userIndexes) != 2 {
		t.Errorf("Expected 2 indexes for 'users', got %d", len(userIndexes))
	}

	// 获取 orders 表的索引
	orderIndexes := store.GetTableIndexes("orders")
	if len(orderIndexes) != 1 {
		t.Errorf("Expected 1 index for 'orders', got %d", len(orderIndexes))
	}

	// 获取不存在表的索引
	nonexistent := store.GetTableIndexes("nonexistent")
	if len(nonexistent) != 0 {
		t.Errorf("Expected 0 indexes for 'nonexistent', got %d", len(nonexistent))
	}
}

// TestDeleteIndex 测试删除索引
func TestDeleteIndex(t *testing.T) {
	store := NewHypotheticalIndexStore()

	index, err := store.CreateIndex("users", []string{"id"}, true, false)
	if err != nil {
		t.Fatalf("CreateIndex failed: %v", err)
	}

	// 删除索引
	err = store.DeleteIndex(index.ID)
	if err != nil {
		t.Errorf("DeleteIndex failed: %v", err)
	}
	if store.Count() != 0 {
		t.Errorf("Expected 0 indexes after delete, got %d", store.Count())
	}

	// 删除不存在的索引
	err = store.DeleteIndex("nonexistent")
	if err == nil {
		t.Error("Expected error when deleting nonexistent index")
	}
}

// TestDeleteTableIndexes 测试删除表的所有索引
func TestDeleteTableIndexes(t *testing.T) {
	store := NewHypotheticalIndexStore()

	_, _ = store.CreateIndex("users", []string{"id"}, true, false)
	_, _ = store.CreateIndex("users", []string{"email"}, false, false)
	_, _ = store.CreateIndex("orders", []string{"user_id"}, false, false)

	// 删除 users 表的所有索引
	count := store.DeleteTableIndexes("users")
	if count != 2 {
		t.Errorf("Expected to delete 2 indexes, got %d", count)
	}
	if store.Count() != 1 {
		t.Errorf("Expected 1 index remaining, got %d", store.Count())
	}

	// 删除不存在表的索引
	count = store.DeleteTableIndexes("nonexistent")
	if count != 0 {
		t.Errorf("Expected to delete 0 indexes, got %d", count)
	}
}

// TestUpdateStats 测试更新统计信息
func TestUpdateStats(t *testing.T) {
	store := NewHypotheticalIndexStore()

	index, err := store.CreateIndex("users", []string{"id"}, true, false)
	if err != nil {
		t.Fatalf("CreateIndex failed: %v", err)
	}

	// 更新统计信息
	stats := &HypotheticalIndexStats{
		NDV:           1000,
		Selectivity:   0.01,
		EstimatedSize: 40960,
		NullFraction:  0.0,
		Correlation:   1.0,
	}
	err = store.UpdateStats(index.ID, stats)
	if err != nil {
		t.Errorf("UpdateStats failed: %v", err)
	}

	// 验证统计信息已更新
	updatedIndex, _ := store.GetIndex(index.ID)
	if updatedIndex.Stats == nil {
		t.Fatal("Expected stats to be set")
	}
	if updatedIndex.Stats.NDV != 1000 {
		t.Errorf("Expected NDV 1000, got %d", updatedIndex.Stats.NDV)
	}

	// 更新不存在的索引
	err = store.UpdateStats("nonexistent", stats)
	if err == nil {
		t.Error("Expected error when updating nonexistent index")
	}
}

// TestClear 测试清空存储
func TestClear(t *testing.T) {
	store := NewHypotheticalIndexStore()

	_, _ = store.CreateIndex("users", []string{"id"}, true, false)
	_, _ = store.CreateIndex("users", []string{"email"}, false, false)

	if store.Count() != 2 {
		t.Errorf("Expected 2 indexes, got %d", store.Count())
	}

	store.Clear()

	if store.Count() != 0 {
		t.Errorf("Expected 0 indexes after clear, got %d", store.Count())
	}
}

// TestFindIndexByColumns 测试根据列查找索引
func TestFindIndexByColumns(t *testing.T) {
	store := NewHypotheticalIndexStore()

	_, _ = store.CreateIndex("users", []string{"id"}, true, false)
	_, _ = store.CreateIndex("users", []string{"email"}, false, false)
	_, _ = store.CreateIndex("users", []string{"first_name", "last_name"}, false, false)

	// 查找存在的索引
	index, ok := store.FindIndexByColumns("users", []string{"email"})
	if !ok {
		t.Error("Expected to find index by columns [email]")
	}
	if len(index.Columns) != 1 || index.Columns[0] != "email" {
		t.Errorf("Expected columns [email], got %v", index.Columns)
	}

	// 查找多列索引
	index, ok = store.FindIndexByColumns("users", []string{"first_name", "last_name"})
	if !ok {
		t.Error("Expected to find index by columns [first_name, last_name]")
	}

	// 查找不存在的索引
	_, ok = store.FindIndexByColumns("users", []string{"nonexistent"})
	if ok {
		t.Error("Expected not to find index by columns [nonexistent]")
	}

	// 查找列顺序不同的索引（不应该匹配）
	_, ok = store.FindIndexByColumns("users", []string{"last_name", "first_name"})
	if ok {
		t.Error("Expected not to find index with different column order")
	}
}

// TestListAllIndexes 测试列出所有索引
func TestListAllIndexes(t *testing.T) {
	store := NewHypotheticalIndexStore()

	_, _ = store.CreateIndex("users", []string{"id"}, true, false)
	_, _ = store.CreateIndex("users", []string{"email"}, false, false)
	_, _ = store.CreateIndex("orders", []string{"user_id"}, false, false)

	allIndexes := store.ListAllIndexes()
	if len(allIndexes) != 3 {
		t.Errorf("Expected 3 indexes, got %d", len(allIndexes))
	}
}

// TestConcurrentAccess 测试并发访问
func TestConcurrentAccess(t *testing.T) {
	store := NewHypotheticalIndexStore()
	var wg sync.WaitGroup
	numGoroutines := 100

	// 并发创建索引
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			tableName := "users"
			columns := []string{"col_" + string(rune(i%26)+'a')}
			_, _ = store.CreateIndex(tableName, columns, false, false)
		}(i)
	}
	wg.Wait()

	// 并发读取索引
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			indexes := store.GetTableIndexes("users")
			_ = indexes
		}(i)
	}
	wg.Wait()

	// 并发删除索引
	for i := 0; i < numGoroutines/2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			allIndexes := store.ListAllIndexes()
			if len(allIndexes) > 0 {
				_ = store.DeleteIndex(allIndexes[0].ID)
			}
		}()
	}
	wg.Wait()

	// 验证没有死锁
	t.Log("Concurrent access test passed without deadlock")
}

// TestIndexCreationTime 测试索引创建时间
func TestIndexCreationTime(t *testing.T) {
	store := NewHypotheticalIndexStore()

	before := time.Now()
	index, _ := store.CreateIndex("users", []string{"id"}, true, false)
	after := time.Now()

	if index.CreatedAt.Before(before) || index.CreatedAt.After(after) {
		t.Error("Index CreatedAt is outside expected range")
	}
}

// TestMultiColumnIndex 测试多列索引
func TestMultiColumnIndex(t *testing.T) {
	store := NewHypotheticalIndexStore()

	columns := []string{"first_name", "last_name", "email"}
	index, err := store.CreateIndex("users", columns, false, false)
	if err != nil {
		t.Fatalf("CreateIndex failed: %v", err)
	}

	if len(index.Columns) != 3 {
		t.Errorf("Expected 3 columns, got %d", len(index.Columns))
	}
}

// TestPrimaryIndex 测试主键索引
func TestPrimaryIndex(t *testing.T) {
	store := NewHypotheticalIndexStore()

	index, err := store.CreateIndex("users", []string{"id"}, true, true)
	if err != nil {
		t.Fatalf("CreateIndex failed: %v", err)
	}

	if !index.IsPrimary {
		t.Error("Expected IsPrimary to be true")
	}
}
