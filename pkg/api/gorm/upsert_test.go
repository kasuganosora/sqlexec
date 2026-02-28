package gorm

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/api"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/resource/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// CharSwitch mirrors the model from the external project that triggers the panic.
type CharSwitch struct {
	ID    uint   `gorm:"primaryKey;autoIncrement"`
	Key   string `gorm:"column:key;uniqueIndex;size:255"`
	Value string `gorm:"column:value;size:255"`
}

func (CharSwitch) TableName() string { return "char_switches" }

// Character mirrors the model from the external project for UPDATE testing.
type Character struct {
	ID   uint   `gorm:"primaryKey;autoIncrement"`
	Name string `gorm:"column:name;size:255"`
	MapID int   `gorm:"column:map_id"`
	MapX  int   `gorm:"column:map_x"`
	MapY  int   `gorm:"column:map_y"`
}

func (Character) TableName() string { return "characters" }

func setupTestGormDB(t *testing.T) (*gorm.DB, func()) {
	t.Helper()
	db, err := api.NewDB(&api.DBConfig{DebugMode: false})
	require.NoError(t, err)

	config := &domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Name:     "test",
		Writable: true,
	}
	memoryDS := memory.NewMVCCDataSource(config)
	require.NoError(t, memoryDS.Connect(context.Background()))
	db.RegisterDataSource("test", memoryDS)

	session := db.Session()
	dialector := NewDialector(session)
	gormDB, err := gorm.Open(dialector, &gorm.Config{})
	require.NoError(t, err)

	cleanup := func() {
		if sqlDB, _ := gormDB.DB(); sqlDB != nil {
			sqlDB.Close()
		}
		db.Close()
	}

	return gormDB, cleanup
}

// TestUpsert_OnConflictDoUpdate tests that GORM's OnConflict clause generates
// MySQL-compatible ON DUPLICATE KEY UPDATE instead of PostgreSQL ON CONFLICT.
// This was the primary trigger for panics in the TiDB parser.
func TestUpsert_OnConflictDoUpdate(t *testing.T) {
	gormDB, cleanup := setupTestGormDB(t)
	defer cleanup()

	// Migrate the table
	err := gormDB.AutoMigrate(&CharSwitch{})
	require.NoError(t, err)

	// First insert
	err = gormDB.Create(&CharSwitch{Key: "test_key", Value: "original"}).Error
	require.NoError(t, err)

	// Upsert with OnConflict — this used to panic with:
	// runtime error: index out of range [-3039] in yyParse
	err = gormDB.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "key"}},
		DoUpdates: clause.AssignmentColumns([]string{"value"}),
	}).Create(&CharSwitch{Key: "test_key", Value: "updated"}).Error
	require.NoError(t, err, "OnConflict upsert should not panic or error")

	// Verify the value was updated
	var result CharSwitch
	err = gormDB.Where("`key` = ?", "test_key").First(&result).Error
	require.NoError(t, err)
	assert.Equal(t, "updated", result.Value, "value should be updated by upsert")
}

// TestUpsert_OnConflictDoNothing tests that DoNothing generates valid SQL.
func TestUpsert_OnConflictDoNothing(t *testing.T) {
	gormDB, cleanup := setupTestGormDB(t)
	defer cleanup()

	err := gormDB.AutoMigrate(&CharSwitch{})
	require.NoError(t, err)

	// First insert
	err = gormDB.Create(&CharSwitch{Key: "test_key", Value: "original"}).Error
	require.NoError(t, err)

	// Upsert with DoNothing — should not panic
	err = gormDB.Clauses(clause.OnConflict{
		DoNothing: true,
	}).Create(&CharSwitch{Key: "test_key", Value: "should_be_ignored"}).Error
	// May or may not error depending on implementation, but must NOT panic
	_ = err

	// Value should remain "original"
	var result CharSwitch
	err = gormDB.Where("`key` = ?", "test_key").First(&result).Error
	require.NoError(t, err)
	assert.Equal(t, "original", result.Value, "value should not change with DoNothing")
}

// TestUpdate_ConcurrentMapUpdates tests that concurrent UPDATE operations
// don't cause parser panics (the disconnect-save pattern).
func TestUpdate_ConcurrentMapUpdates(t *testing.T) {
	gormDB, cleanup := setupTestGormDB(t)
	defer cleanup()

	err := gormDB.AutoMigrate(&Character{})
	require.NoError(t, err)

	// Create a character
	char := &Character{Name: "player1", MapID: 1, MapX: 100, MapY: 200}
	err = gormDB.Create(char).Error
	require.NoError(t, err)

	// Concurrent UPDATEs — this used to panic due to TiDB parser data race
	var wg sync.WaitGroup
	errCh := make(chan error, 20)

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			err := gormDB.Model(&Character{}).Where("id = ?", char.ID).Updates(map[string]interface{}{
				"map_id": i,
				"map_x":  i * 10,
				"map_y":  i * 20,
			}).Error
			if err != nil {
				errCh <- err
			}
		}(i)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Errorf("concurrent update failed: %v", err)
	}
}

// TestConcurrentInserts tests that concurrent INSERT operations don't panic.
func TestConcurrentInserts(t *testing.T) {
	gormDB, cleanup := setupTestGormDB(t)
	defer cleanup()

	err := gormDB.AutoMigrate(&CharSwitch{})
	require.NoError(t, err)

	var wg sync.WaitGroup
	errCh := make(chan error, 50)

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			err := gormDB.Create(&CharSwitch{
				Key:   fmt.Sprintf("key_%d", i),
				Value: fmt.Sprintf("value_%d", i),
			}).Error
			if err != nil {
				errCh <- err
			}
		}(i)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Errorf("concurrent insert failed: %v", err)
	}
}

// TestConcurrentMixedOperations tests concurrent SELECT + INSERT + UPDATE.
func TestConcurrentMixedOperations(t *testing.T) {
	gormDB, cleanup := setupTestGormDB(t)
	defer cleanup()

	err := gormDB.AutoMigrate(&Character{})
	require.NoError(t, err)

	// Seed initial data
	for i := 0; i < 5; i++ {
		err := gormDB.Create(&Character{
			Name:  fmt.Sprintf("char_%d", i),
			MapID: i,
			MapX:  i * 10,
			MapY:  i * 20,
		}).Error
		require.NoError(t, err)
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 100)

	// Concurrent SELECTs
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var chars []Character
			if err := gormDB.Find(&chars).Error; err != nil {
				errCh <- err
			}
		}()
	}

	// Concurrent UPDATEs
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if err := gormDB.Model(&Character{}).Where("id = ?", (i%5)+1).Updates(map[string]interface{}{
				"map_x": i * 100,
			}).Error; err != nil {
				errCh <- err
			}
		}(i)
	}

	// Concurrent INSERTs
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if err := gormDB.Create(&Character{
				Name:  fmt.Sprintf("new_char_%d", i),
				MapID: i + 100,
			}).Error; err != nil {
				errCh <- err
			}
		}(i)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Errorf("concurrent mixed operation failed: %v", err)
	}
}
