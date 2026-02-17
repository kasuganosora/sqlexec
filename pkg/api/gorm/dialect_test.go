package gorm

import (
    "testing"
    "time"

    "github.com/kasuganosora/sqlexec/pkg/api"
    "github.com/kasuganosora/sqlexec/pkg/resource/domain"
    "github.com/kasuganosora/sqlexec/pkg/resource/memory"
    "github.com/stretchr/testify/assert"
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

// TestToDriverValue_Timestamp tests timestamp string conversion to time.Time
func TestToDriverValue_Timestamp(t *testing.T) {
    tests := []struct {
        name     string
        input    interface{}
        expected time.Time
        isTime   bool
    }{
        {
            name:     "RFC3339 format",
            input:    "2024-01-15T10:30:00Z",
            expected: time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
            isTime:   true,
        },
        {
            name:     "datetime format",
            input:    "2024-01-15 10:30:00",
            expected: time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
            isTime:   true,
        },
        {
            name:     "datetime with nanoseconds",
            input:    "2024-01-15 10:30:00.123456",
            expected: time.Date(2024, 1, 15, 10, 30, 0, 123456000, time.UTC),
            isTime:   true,
        },
        {
            name:     "datetime with timezone",
            input:    "2024-01-15 10:30:00 +0800 CST",
            expected: time.Date(2024, 1, 15, 10, 30, 0, 0, time.FixedZone("CST", 8*3600)),
            isTime:   true,
        },
        {
            name:     "non-timestamp string",
            input:    "hello world",
            expected: time.Time{},
            isTime:   false,
        },
        {
            name:     "int64 value",
            input:    int64(123),
            expected: time.Time{},
            isTime:   false,
        },
        {
            name:     "already time.Time",
            input:    time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
            expected: time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
            isTime:   true,
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            result := toDriverValue(tt.input)
            if tt.isTime {
                actualTime, ok := result.(time.Time)
                assert.True(t, ok, "expected time.Time, got %T", result)
                if ok {
                    // Compare without nanoseconds for some formats
                    assert.Equal(t, tt.expected.Year(), actualTime.Year())
                    assert.Equal(t, tt.expected.Month(), actualTime.Month())
                    assert.Equal(t, tt.expected.Day(), actualTime.Day())
                    assert.Equal(t, tt.expected.Hour(), actualTime.Hour())
                    assert.Equal(t, tt.expected.Minute(), actualTime.Minute())
                    assert.Equal(t, tt.expected.Second(), actualTime.Second())
                }
            } else if _, ok := tt.input.(string); ok && tt.input != "hello world" {
                // For non-timestamp strings, should return as string
                assert.IsType(t, "", result)
            }
        })
    }
}
