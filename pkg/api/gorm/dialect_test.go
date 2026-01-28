package gorm

import (
    "testing"
    "github.com/kasuganosora/sqlexec/pkg/api"
    "github.com/kasuganosora/sqlexec/pkg/resource/domain"
    "github.com/kasuganosora/sqlexec/pkg/resource/memory"
    "gorm.io/gorm"
)

func TestNewDialector(t *testing.T) {
    db, _ := api.NewDB(&api.DBConfig{DebugMode: false})
    defer db.Close()
    config := &domain.DataSourceConfig{Type: domain.DataSourceTypeMemory, Name: "test", Writable: true}
    memoryDS := memory.NewMVCCDataSource(config)
    db.RegisterDataSource("test", memoryDS)
    dialector := NewDialector(db.Session())
    if dialector == nil {
        t.Fatal("Dialector should not be nil")
    }
    gormDB, _ := gorm.Open(dialector, &gorm.Config{})
    defer func() { if sqlDB, _ := gormDB.DB(); sqlDB != nil { sqlDB.Close() } }()
}

func TestDialector_Name(t *testing.T) {
    db, _ := api.NewDB(&api.DBConfig{DebugMode: false})
    defer db.Close()
    config := &domain.DataSourceConfig{Type: domain.DataSourceTypeMemory, Name: "test", Writable: true}
    memoryDS := memory.NewMVCCDataSource(config)
    db.RegisterDataSource("test", memoryDS)
    dialector := NewDialector(db.Session())
    if dialector.Name() == "" {
        t.Error("Dialector.Name should not be empty")
    }
}

func TestDialector_Migrator(t *testing.T) {
    db, _ := api.NewDB(&api.DBConfig{DebugMode: false})
    defer db.Close()
    config := &domain.DataSourceConfig{Type: domain.DataSourceTypeMemory, Name: "test", Writable: true}
    memoryDS := memory.NewMVCCDataSource(config)
    db.RegisterDataSource("test", memoryDS)
    dialector := NewDialector(db.Session())
    gormDB, _ := gorm.Open(dialector, &gorm.Config{})
    defer func() { if sqlDB, _ := gormDB.DB(); sqlDB != nil { sqlDB.Close() } }()
    migrator := dialector.Migrator(gormDB)
    if migrator == nil {
        t.Fatal("Migrator should not be nil")
    }
}
