package gorm

import (
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/api"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/resource/memory"
	"gorm.io/gorm"
)

func TestMigrator_HasTable(t *testing.T) {
	db, _ := api.NewDB(&api.DBConfig{DebugMode: false})
	defer db.Close()
	config := &domain.DataSourceConfig{Type: domain.DataSourceTypeMemory, Name: "test", Writable: true}
	memoryDS := memory.NewMVCCDataSource(config)
	db.RegisterDataSource("test", memoryDS)
	dialector := NewDialector(db.Session())
	gormDB, _ := gorm.Open(dialector, &gorm.Config{})
	defer func() {
		if sqlDB, _ := gormDB.DB(); sqlDB != nil {
			sqlDB.Close()
		}
	}()

	type TestModel struct {
		ID   uint `gorm:"primaryKey"`
		Name string
	}

	migrator := gormDB.Migrator()
	// HasTable should not panic even if table doesn't exist
	_ = migrator.HasTable(&TestModel{})
}

func TestMigrator_CreateTable(t *testing.T) {
	db, _ := api.NewDB(&api.DBConfig{DebugMode: false})
	defer db.Close()
	config := &domain.DataSourceConfig{Type: domain.DataSourceTypeMemory, Name: "test", Writable: true}
	memoryDS := memory.NewMVCCDataSource(config)
	db.RegisterDataSource("test", memoryDS)
	dialector := NewDialector(db.Session())
	gormDB, _ := gorm.Open(dialector, &gorm.Config{})
	defer func() {
		if sqlDB, _ := gormDB.DB(); sqlDB != nil {
			sqlDB.Close()
		}
	}()

	type TestModel struct {
		ID   uint `gorm:"primaryKey"`
		Name string
	}

	migrator := gormDB.Migrator()
	// CreateTable may fail with DDL not supported, but should not panic
	_ = migrator.CreateTable(&TestModel{})
}

func TestMigrator_DropTable(t *testing.T) {
	db, _ := api.NewDB(&api.DBConfig{DebugMode: false})
	defer db.Close()
	config := &domain.DataSourceConfig{Type: domain.DataSourceTypeMemory, Name: "test", Writable: true}
	memoryDS := memory.NewMVCCDataSource(config)
	db.RegisterDataSource("test", memoryDS)
	dialector := NewDialector(db.Session())
	gormDB, _ := gorm.Open(dialector, &gorm.Config{})
	defer func() {
		if sqlDB, _ := gormDB.DB(); sqlDB != nil {
			sqlDB.Close()
		}
	}()

	type TestModel struct {
		ID   uint `gorm:"primaryKey"`
		Name string
	}

	migrator := gormDB.Migrator()
	// DropTable may fail with DDL not supported, but should not panic
	_ = migrator.DropTable(&TestModel{})
}

func TestMigrator_RenameTable(t *testing.T) {
	db, _ := api.NewDB(&api.DBConfig{DebugMode: false})
	defer db.Close()
	config := &domain.DataSourceConfig{Type: domain.DataSourceTypeMemory, Name: "test", Writable: true}
	memoryDS := memory.NewMVCCDataSource(config)
	db.RegisterDataSource("test", memoryDS)
	dialector := NewDialector(db.Session())
	gormDB, _ := gorm.Open(dialector, &gorm.Config{})
	defer func() {
		if sqlDB, _ := gormDB.DB(); sqlDB != nil {
			sqlDB.Close()
		}
	}()

	migrator := gormDB.Migrator()
	// RenameTable may fail with DDL not supported, but should not panic
	_ = migrator.RenameTable("test_models", "new_test_models")
}

func TestMigrator_AddColumn(t *testing.T) {
	db, _ := api.NewDB(&api.DBConfig{DebugMode: false})
	defer db.Close()
	config := &domain.DataSourceConfig{Type: domain.DataSourceTypeMemory, Name: "test", Writable: true}
	memoryDS := memory.NewMVCCDataSource(config)
	db.RegisterDataSource("test", memoryDS)
	dialector := NewDialector(db.Session())
	gormDB, _ := gorm.Open(dialector, &gorm.Config{})
	defer func() {
		if sqlDB, _ := gormDB.DB(); sqlDB != nil {
			sqlDB.Close()
		}
	}()

	type TestModel struct {
		ID   uint `gorm:"primaryKey"`
		Name string
	}

	migrator := gormDB.Migrator()
	// AddColumn may fail with DDL not supported, but should not panic
	_ = migrator.AddColumn(&TestModel{}, "age")
}

func TestMigrator_DropColumn(t *testing.T) {
	db, _ := api.NewDB(&api.DBConfig{DebugMode: false})
	defer db.Close()
	config := &domain.DataSourceConfig{Type: domain.DataSourceTypeMemory, Name: "test", Writable: true}
	memoryDS := memory.NewMVCCDataSource(config)
	db.RegisterDataSource("test", memoryDS)
	dialector := NewDialector(db.Session())
	gormDB, _ := gorm.Open(dialector, &gorm.Config{})
	defer func() {
		if sqlDB, _ := gormDB.DB(); sqlDB != nil {
			sqlDB.Close()
		}
	}()

	type TestModel struct {
		ID   uint `gorm:"primaryKey"`
		Name string
	}

	migrator := gormDB.Migrator()
	// DropColumn may fail with DDL not supported, but should not panic
	_ = migrator.DropColumn(&TestModel{}, "name")
}

func TestMigrator_AlterColumn(t *testing.T) {
	db, _ := api.NewDB(&api.DBConfig{DebugMode: false})
	defer db.Close()
	config := &domain.DataSourceConfig{Type: domain.DataSourceTypeMemory, Name: "test", Writable: true}
	memoryDS := memory.NewMVCCDataSource(config)
	db.RegisterDataSource("test", memoryDS)
	dialector := NewDialector(db.Session())
	gormDB, _ := gorm.Open(dialector, &gorm.Config{})
	defer func() {
		if sqlDB, _ := gormDB.DB(); sqlDB != nil {
			sqlDB.Close()
		}
	}()

	type TestModel struct {
		ID   uint `gorm:"primaryKey"`
		Name string
	}

	migrator := gormDB.Migrator()
	// AlterColumn may fail with DDL not supported, but should not panic
	_ = migrator.AlterColumn(&TestModel{}, "name")
}

func TestMigrator_RenameColumn(t *testing.T) {
	db, _ := api.NewDB(&api.DBConfig{DebugMode: false})
	defer db.Close()
	config := &domain.DataSourceConfig{Type: domain.DataSourceTypeMemory, Name: "test", Writable: true}
	memoryDS := memory.NewMVCCDataSource(config)
	db.RegisterDataSource("test", memoryDS)
	dialector := NewDialector(db.Session())
	gormDB, _ := gorm.Open(dialector, &gorm.Config{})
	defer func() {
		if sqlDB, _ := gormDB.DB(); sqlDB != nil {
			sqlDB.Close()
		}
	}()

	type TestModel struct {
		ID   uint `gorm:"primaryKey"`
		Name string
	}

	migrator := gormDB.Migrator()
	// RenameColumn may fail with DDL not supported, but should not panic
	_ = migrator.RenameColumn(&TestModel{}, "name", "new_name")
}

func TestMigrator_ColumnTypes(t *testing.T) {
	db, _ := api.NewDB(&api.DBConfig{DebugMode: false})
	defer db.Close()
	config := &domain.DataSourceConfig{Type: domain.DataSourceTypeMemory, Name: "test", Writable: true}
	memoryDS := memory.NewMVCCDataSource(config)
	db.RegisterDataSource("test", memoryDS)
	dialector := NewDialector(db.Session())
	gormDB, _ := gorm.Open(dialector, &gorm.Config{})
	defer func() {
		if sqlDB, _ := gormDB.DB(); sqlDB != nil {
			sqlDB.Close()
		}
	}()

	type TestModel struct {
		ID   uint `gorm:"primaryKey"`
		Name string
	}

	migrator := gormDB.Migrator()
	migrator.CreateTable(&TestModel{})
	columnTypes, err := migrator.ColumnTypes(&TestModel{})
	if err != nil {
		t.Errorf("Failed to get column types: %v", err)
	}
	if columnTypes == nil {
		t.Error("ColumnTypes should not be nil")
	}
}

func TestMigrator_CreateIndex(t *testing.T) {
	db, _ := api.NewDB(&api.DBConfig{DebugMode: false})
	defer db.Close()
	config := &domain.DataSourceConfig{Type: domain.DataSourceTypeMemory, Name: "test", Writable: true}
	memoryDS := memory.NewMVCCDataSource(config)
	db.RegisterDataSource("test", memoryDS)
	dialector := NewDialector(db.Session())
	gormDB, _ := gorm.Open(dialector, &gorm.Config{})
	defer func() {
		if sqlDB, _ := gormDB.DB(); sqlDB != nil {
			sqlDB.Close()
		}
	}()

	type TestModel struct {
		ID   uint `gorm:"primaryKey"`
		Name string
	}

	migrator := gormDB.Migrator()
	// CreateIndex may fail with DDL not supported, but should not panic
	_ = migrator.CreateIndex(&TestModel{}, "idx_name")
}

func TestMigrator_DropIndex(t *testing.T) {
	db, _ := api.NewDB(&api.DBConfig{DebugMode: false})
	defer db.Close()
	config := &domain.DataSourceConfig{Type: domain.DataSourceTypeMemory, Name: "test", Writable: true}
	memoryDS := memory.NewMVCCDataSource(config)
	db.RegisterDataSource("test", memoryDS)
	dialector := NewDialector(db.Session())
	gormDB, _ := gorm.Open(dialector, &gorm.Config{})
	defer func() {
		if sqlDB, _ := gormDB.DB(); sqlDB != nil {
			sqlDB.Close()
		}
	}()

	type TestModel struct {
		ID   uint `gorm:"primaryKey"`
		Name string
	}

	migrator := gormDB.Migrator()
	// DropIndex may fail with DDL not supported, but should not panic
	_ = migrator.DropIndex(&TestModel{}, "idx_name")
}

func TestMigrator_HasIndex(t *testing.T) {
	db, _ := api.NewDB(&api.DBConfig{DebugMode: false})
	defer db.Close()
	config := &domain.DataSourceConfig{Type: domain.DataSourceTypeMemory, Name: "test", Writable: true}
	memoryDS := memory.NewMVCCDataSource(config)
	db.RegisterDataSource("test", memoryDS)
	dialector := NewDialector(db.Session())
	gormDB, _ := gorm.Open(dialector, &gorm.Config{})
	defer func() {
		if sqlDB, _ := gormDB.DB(); sqlDB != nil {
			sqlDB.Close()
		}
	}()

	type TestModel struct {
		ID   uint `gorm:"primaryKey"`
		Name string
	}

	migrator := gormDB.Migrator()
	migrator.CreateTable(&TestModel{})
	_ = migrator.HasIndex(&TestModel{}, "idx_name")
}

func TestMigrator_RenameIndex(t *testing.T) {
	db, _ := api.NewDB(&api.DBConfig{DebugMode: false})
	defer db.Close()
	config := &domain.DataSourceConfig{Type: domain.DataSourceTypeMemory, Name: "test", Writable: true}
	memoryDS := memory.NewMVCCDataSource(config)
	db.RegisterDataSource("test", memoryDS)
	dialector := NewDialector(db.Session())
	gormDB, _ := gorm.Open(dialector, &gorm.Config{})
	defer func() {
		if sqlDB, _ := gormDB.DB(); sqlDB != nil {
			sqlDB.Close()
		}
	}()

	type TestModel struct {
		ID   uint   `gorm:"primaryKey"`
		Name string `gorm:"index:idx_old"`
	}

	migrator := gormDB.Migrator()
	// Create the table (which includes the Name column)
	err := migrator.CreateTable(&TestModel{})
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Create the idx_old index first so it exists before renaming
	err = migrator.CreateIndex(&TestModel{}, "idx_old")
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	// Rename idx_old -> idx_new
	err = migrator.RenameIndex(&TestModel{}, "idx_old", "idx_new")
	if err != nil {
		t.Errorf("Failed to rename index: %v", err)
	}
}

func TestMigrator_AutoMigrate(t *testing.T) {
	db, _ := api.NewDB(&api.DBConfig{DebugMode: false})
	defer db.Close()
	config := &domain.DataSourceConfig{Type: domain.DataSourceTypeMemory, Name: "test", Writable: true}
	memoryDS := memory.NewMVCCDataSource(config)
	db.RegisterDataSource("test", memoryDS)
	dialector := NewDialector(db.Session())
	gormDB, _ := gorm.Open(dialector, &gorm.Config{})
	defer func() {
		if sqlDB, _ := gormDB.DB(); sqlDB != nil {
			sqlDB.Close()
		}
	}()

	type TestModel struct {
		ID   uint `gorm:"primaryKey"`
		Name string
	}

	migrator := gormDB.Migrator()
	err := migrator.AutoMigrate(&TestModel{})
	if err != nil {
		t.Errorf("Failed to auto migrate: %v", err)
	}
}

// TestMigrator_AutoMigrate_EmbeddedStruct tests that embedded structs
// with empty DBName are handled correctly (fallback to field name or skip)
func TestMigrator_AutoMigrate_EmbeddedStruct(t *testing.T) {
	db, _ := api.NewDB(&api.DBConfig{DebugMode: false})
	defer db.Close()
	config := &domain.DataSourceConfig{Type: domain.DataSourceTypeMemory, Name: "test", Writable: true}
	memoryDS := memory.NewMVCCDataSource(config)
	db.RegisterDataSource("test", memoryDS)
	dialector := NewDialector(db.Session())
	gormDB, _ := gorm.Open(dialector, &gorm.Config{})
	defer func() {
		if sqlDB, _ := gormDB.DB(); sqlDB != nil {
			sqlDB.Close()
		}
	}()

	// Model with embedded struct that might cause empty DBName
	type BaseModel struct {
		ID        uint `gorm:"primaryKey"`
		CreatedAt int64
	}

	type Order struct {
		BaseModel
		Symbol   string `gorm:"column:symbol"`
		Quantity int    `gorm:"column:quantity"`
	}

	migrator := gormDB.Migrator()
	// This should not panic or generate SQL with empty column names
	err := migrator.AutoMigrate(&Order{})
	if err != nil {
		t.Errorf("Failed to auto migrate with embedded struct: %v", err)
	}
}

func TestMigrator_HasColumn(t *testing.T) {
	db, _ := api.NewDB(&api.DBConfig{DebugMode: false})
	defer db.Close()
	config := &domain.DataSourceConfig{Type: domain.DataSourceTypeMemory, Name: "test", Writable: true}
	memoryDS := memory.NewMVCCDataSource(config)
	db.RegisterDataSource("test", memoryDS)
	dialector := NewDialector(db.Session())
	gormDB, _ := gorm.Open(dialector, &gorm.Config{})
	defer func() {
		if sqlDB, _ := gormDB.DB(); sqlDB != nil {
			sqlDB.Close()
		}
	}()

	type TestModel struct {
		ID   uint `gorm:"primaryKey"`
		Name string
	}

	migrator := gormDB.Migrator()
	migrator.CreateTable(&TestModel{})
	_ = migrator.HasColumn(&TestModel{}, "name")
}

func TestMigrator_GetTables(t *testing.T) {
	db, _ := api.NewDB(&api.DBConfig{DebugMode: false})
	defer db.Close()
	config := &domain.DataSourceConfig{Type: domain.DataSourceTypeMemory, Name: "test", Writable: true}
	memoryDS := memory.NewMVCCDataSource(config)
	db.RegisterDataSource("test", memoryDS)
	dialector := NewDialector(db.Session())
	gormDB, _ := gorm.Open(dialector, &gorm.Config{})
	defer func() {
		if sqlDB, _ := gormDB.DB(); sqlDB != nil {
			sqlDB.Close()
		}
	}()

	type TestModel struct {
		ID   uint `gorm:"primaryKey"`
		Name string
	}

	migrator := gormDB.Migrator()
	migrator.CreateTable(&TestModel{})
	tables, err := migrator.GetTables()
	if err != nil {
		t.Errorf("Failed to get tables: %v", err)
	}
	if tables == nil {
		t.Error("Tables should not be nil")
	}
}
