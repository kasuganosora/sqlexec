package gorm

import (
	"context"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/api"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/resource/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

type Inventory struct {
	ID        int64  `gorm:"primaryKey;autoIncrement"`
	CharID    int64  `gorm:"column:char_id"`
	ItemID    int64  `gorm:"column:item_id"`
	Kind      string `gorm:"column:kind"`
	Equipped  bool   `gorm:"column:equipped"`
	SlotIndex int    `gorm:"column:slot_index"`
	Quantity  int    `gorm:"column:quantity"`
}

func (Inventory) TableName() string { return "inventories" }

func setupBoolTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	db, err := api.NewDB(&api.DBConfig{DebugMode: false})
	require.NoError(t, err)
	config := &domain.DataSourceConfig{Type: domain.DataSourceTypeMemory, Name: "test", Writable: true}
	memoryDS := memory.NewMVCCDataSource(config)
	require.NoError(t, memoryDS.Connect(context.Background()))
	db.RegisterDataSource("test", memoryDS)
	dialector := NewDialector(db.Session())
	gormDB, err := gorm.Open(dialector, &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, gormDB.AutoMigrate(&Inventory{}))
	t.Cleanup(func() {
		if sqlDB, _ := gormDB.DB(); sqlDB != nil {
			sqlDB.Close()
		}
		db.Close()
	})
	return gormDB
}

// TestBoolFalseWhereQuery verifies that WHERE equipped = false works.
func TestBoolFalseWhereQuery(t *testing.T) {
	gormDB := setupBoolTestDB(t)

	// Insert rows with equipped=true and equipped=false
	require.NoError(t, gormDB.Create(&Inventory{CharID: 1, ItemID: 100, Kind: "weapon", Equipped: true, SlotIndex: 1, Quantity: 1}).Error)
	require.NoError(t, gormDB.Create(&Inventory{CharID: 1, ItemID: 200, Kind: "consumable", Equipped: false, SlotIndex: 0, Quantity: 5}).Error)
	require.NoError(t, gormDB.Create(&Inventory{CharID: 1, ItemID: 300, Kind: "consumable", Equipped: false, SlotIndex: 0, Quantity: 3}).Error)

	// Query equipped = true (should work)
	var equippedItems []Inventory
	err := gormDB.Where("equipped = ?", true).Find(&equippedItems).Error
	require.NoError(t, err)
	assert.Equal(t, 1, len(equippedItems), "should find 1 equipped item")
	assert.Equal(t, int64(100), equippedItems[0].ItemID)

	// Query equipped = false (this was broken)
	var unequippedItems []Inventory
	err = gormDB.Where("equipped = ?", false).Find(&unequippedItems).Error
	require.NoError(t, err)
	assert.Equal(t, 2, len(unequippedItems), "should find 2 unequipped items")

	// Query equipped = ? with int 0 (also was broken)
	var unequippedItems2 []Inventory
	err = gormDB.Where("equipped = ?", 0).Find(&unequippedItems2).Error
	require.NoError(t, err)
	assert.Equal(t, 2, len(unequippedItems2), "should find 2 unequipped items with int 0")
}

// TestUpdateBoolToFalse verifies that updating a bool from true to false persists.
func TestUpdateBoolToFalse(t *testing.T) {
	gormDB := setupBoolTestDB(t)

	inv := Inventory{CharID: 1, ItemID: 100, Kind: "weapon", Equipped: true, SlotIndex: 1, Quantity: 1}
	require.NoError(t, gormDB.Create(&inv).Error)

	// Verify it was created as equipped=true
	var check Inventory
	require.NoError(t, gormDB.First(&check, inv.ID).Error)
	assert.True(t, check.Equipped, "should be equipped initially")

	// Update via map
	err := gormDB.Model(&Inventory{}).Where("id = ?", inv.ID).Updates(map[string]interface{}{
		"equipped":   false,
		"slot_index": 0,
	}).Error
	require.NoError(t, err)

	// Re-read
	var updated Inventory
	require.NoError(t, gormDB.First(&updated, inv.ID).Error)
	assert.False(t, updated.Equipped, "equipped should be false after update")

	// Update via Save
	updated.Equipped = true
	updated.SlotIndex = 2
	require.NoError(t, gormDB.Save(&updated).Error)

	var check2 Inventory
	require.NoError(t, gormDB.First(&check2, inv.ID).Error)
	assert.True(t, check2.Equipped, "equipped should be true after Save")

	check2.Equipped = false
	require.NoError(t, gormDB.Save(&check2).Error)

	var check3 Inventory
	require.NoError(t, gormDB.First(&check3, inv.ID).Error)
	assert.False(t, check3.Equipped, "equipped should be false after second Save")
}

// TestNegativeIntegerStorage verifies that negative integers are stored correctly.
func TestNegativeIntegerStorage(t *testing.T) {
	gormDB := setupBoolTestDB(t)

	inv := Inventory{CharID: 1, ItemID: 100, Kind: "weapon", Equipped: false, SlotIndex: -1, Quantity: 1}
	require.NoError(t, gormDB.Create(&inv).Error)

	var check Inventory
	require.NoError(t, gormDB.First(&check, inv.ID).Error)
	assert.Equal(t, -1, check.SlotIndex, "slot_index should be -1")

	// Update to another negative value
	err := gormDB.Model(&Inventory{}).Where("id = ?", inv.ID).Updates(map[string]interface{}{
		"slot_index": -5,
	}).Error
	require.NoError(t, err)

	var updated Inventory
	require.NoError(t, gormDB.First(&updated, inv.ID).Error)
	assert.Equal(t, -5, updated.SlotIndex, "slot_index should be -5 after update")
}

// TestExecNegativeAndBool verifies raw Exec works for bool false and negative ints.
func TestExecNegativeAndBool(t *testing.T) {
	gormDB := setupBoolTestDB(t)

	inv := Inventory{CharID: 1, ItemID: 100, Kind: "weapon", Equipped: true, SlotIndex: 5, Quantity: 1}
	require.NoError(t, gormDB.Create(&inv).Error)

	// Use raw Exec to set equipped=false and slot_index=-1
	err := gormDB.Exec("UPDATE inventories SET equipped = 0, slot_index = -1 WHERE id = ?", inv.ID).Error
	require.NoError(t, err)

	var check Inventory
	require.NoError(t, gormDB.First(&check, inv.ID).Error)
	assert.False(t, check.Equipped, "equipped should be false after Exec")
	assert.Equal(t, -1, check.SlotIndex, "slot_index should be -1 after Exec")
}

// TestWhereNegativeValue verifies WHERE with negative values.
func TestWhereNegativeValue(t *testing.T) {
	gormDB := setupBoolTestDB(t)

	require.NoError(t, gormDB.Create(&Inventory{CharID: 1, ItemID: 100, SlotIndex: -1, Quantity: 1}).Error)
	require.NoError(t, gormDB.Create(&Inventory{CharID: 1, ItemID: 200, SlotIndex: 0, Quantity: 1}).Error)
	require.NoError(t, gormDB.Create(&Inventory{CharID: 1, ItemID: 300, SlotIndex: 1, Quantity: 1}).Error)

	var items []Inventory
	err := gormDB.Where("slot_index = ?", -1).Find(&items).Error
	require.NoError(t, err)
	assert.Equal(t, 1, len(items), "should find 1 item with slot_index = -1")
	if len(items) > 0 {
		assert.Equal(t, int64(100), items[0].ItemID)
	}
}

// TestBoolScanIntoInt verifies that bool columns can be scanned into int fields via raw SQL.
func TestBoolScanIntoInt(t *testing.T) {
	gormDB := setupBoolTestDB(t)

	require.NoError(t, gormDB.Create(&Inventory{CharID: 1, ItemID: 100, Kind: "weapon", Equipped: true, SlotIndex: 1, Quantity: 1}).Error)
	require.NoError(t, gormDB.Create(&Inventory{CharID: 1, ItemID: 200, Kind: "weapon", Equipped: false, SlotIndex: 0, Quantity: 1}).Error)

	// Scan equipped (bool column) into int field
	type candidateRow struct {
		ID       int64
		Equipped int
	}
	var candidates []candidateRow
	err := gormDB.Raw("SELECT id, equipped FROM inventories ORDER BY id").Scan(&candidates).Error
	require.NoError(t, err, "scanning bool column into int field should not error")
	require.Len(t, candidates, 2)
	assert.Equal(t, 1, candidates[0].Equipped, "equipped=true should scan as 1")
	assert.Equal(t, 0, candidates[1].Equipped, "equipped=false should scan as 0")
}

// TestBoolFindMultipleRows verifies that Find() returns correct bool values for multiple rows.
func TestBoolFindMultipleRows(t *testing.T) {
	gormDB := setupBoolTestDB(t)

	// Create multiple items with mixed bool values
	require.NoError(t, gormDB.Create(&Inventory{CharID: 1, ItemID: 100, Equipped: true, SlotIndex: 1, Quantity: 1}).Error)
	require.NoError(t, gormDB.Create(&Inventory{CharID: 1, ItemID: 200, Equipped: false, SlotIndex: 0, Quantity: 1}).Error)
	require.NoError(t, gormDB.Create(&Inventory{CharID: 1, ItemID: 300, Equipped: true, SlotIndex: 2, Quantity: 1}).Error)

	// Find all items — verify multi-row results preserve bool values
	var items []Inventory
	err := gormDB.Where("char_id = ?", 1).Find(&items).Error
	require.NoError(t, err)
	require.Len(t, items, 3)
	assert.True(t, items[0].Equipped, "item 100 should be equipped=true")
	assert.False(t, items[1].Equipped, "item 200 should be equipped=false")
	assert.True(t, items[2].Equipped, "item 300 should be equipped=true")

	// First() then Find() in sequence — verify no state leakage
	var single Inventory
	require.NoError(t, gormDB.First(&single, items[1].ID).Error)
	assert.False(t, single.Equipped, "First() item 200 should be equipped=false")

	var others []Inventory
	err = gormDB.Where("char_id = ? AND id != ?", 1, items[1].ID).Find(&others).Error
	require.NoError(t, err)
	require.Len(t, others, 2)
	assert.True(t, others[0].Equipped, "Find() after First(): item 100 should be equipped=true")
	assert.True(t, others[1].Equipped, "Find() after First(): item 300 should be equipped=true")
}

// TestBoolConsistencyCreateVsExec verifies that Create(bool) and Exec(int) produce identical read behavior.
func TestBoolConsistencyCreateVsExec(t *testing.T) {
	gormDB := setupBoolTestDB(t)

	// Create via struct (Go bool)
	require.NoError(t, gormDB.Create(&Inventory{CharID: 1, ItemID: 100, Equipped: true, Quantity: 1}).Error)
	// Create via Exec (SQL int)
	require.NoError(t, gormDB.Exec("INSERT INTO inventories (char_id, item_id, equipped, quantity) VALUES (1, 200, 1, 1)").Error)

	var items []Inventory
	require.NoError(t, gormDB.Order("item_id").Find(&items).Error)
	require.Len(t, items, 2)

	// Both should read as equipped=true regardless of how they were written
	assert.True(t, items[0].Equipped, "Create(bool=true) should read as equipped=true")
	assert.True(t, items[1].Equipped, "Exec(int=1) should read as equipped=true")

	// Both should also be scannable into int fields
	type intRow struct {
		ItemID   int64
		Equipped int
	}
	var intRows []intRow
	err := gormDB.Raw("SELECT item_id, equipped FROM inventories ORDER BY item_id").Scan(&intRows).Error
	require.NoError(t, err)
	require.Len(t, intRows, 2)
	assert.Equal(t, 1, intRows[0].Equipped, "Create(bool=true) should scan as int 1")
	assert.Equal(t, 1, intRows[1].Equipped, "Exec(int=1) should scan as int 1")
}

// TestGormExprArithmetic verifies gorm.Expr("col - ?", val) works for arithmetic updates.
func TestGormExprArithmetic(t *testing.T) {
	gormDB := setupBoolTestDB(t)

	inv := Inventory{CharID: 1, ItemID: 100, Kind: "consumable", Equipped: false, SlotIndex: 0, Quantity: 10}
	require.NoError(t, gormDB.Create(&inv).Error)

	// Use gorm.Expr to subtract
	err := gormDB.Model(&Inventory{}).Where("id = ?", inv.ID).
		Update("quantity", gorm.Expr("quantity - ?", 3)).Error
	require.NoError(t, err)

	var check Inventory
	require.NoError(t, gormDB.First(&check, inv.ID).Error)
	assert.Equal(t, 7, check.Quantity, "quantity should be 10-3=7")

	// Use gorm.Expr to add
	err = gormDB.Model(&Inventory{}).Where("id = ?", inv.ID).
		Update("quantity", gorm.Expr("quantity + ?", 5)).Error
	require.NoError(t, err)

	require.NoError(t, gormDB.First(&check, inv.ID).Error)
	assert.Equal(t, 12, check.Quantity, "quantity should be 7+5=12")

	// Use gorm.Expr to multiply
	err = gormDB.Model(&Inventory{}).Where("id = ?", inv.ID).
		Update("quantity", gorm.Expr("quantity * ?", 2)).Error
	require.NoError(t, err)

	require.NoError(t, gormDB.First(&check, inv.ID).Error)
	assert.Equal(t, 24, check.Quantity, "quantity should be 12*2=24")

	// Use raw Exec with arithmetic
	err = gormDB.Exec("UPDATE inventories SET quantity = quantity - ? WHERE id = ?", 4, inv.ID).Error
	require.NoError(t, err)

	require.NoError(t, gormDB.First(&check, inv.ID).Error)
	assert.Equal(t, 20, check.Quantity, "quantity should be 24-4=20")
}
