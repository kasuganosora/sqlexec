package gorm

import (
	"sync"
	"testing"

	"gorm.io/gorm/schema"
)

type TestModelWithDefault struct {
	ID       string `gorm:"column:id;primaryKey"`
	Name     string `gorm:"column:name;default:'unknown'"`
	Count    int    `gorm:"column:count;default:0"`
	EmptyStr string `gorm:"column:empty_str;default:''"`
}

func TestDefaultValues(t *testing.T) {
	namer := schema.NamingStrategy{}
	s, err := schema.Parse(&TestModelWithDefault{}, &sync.Map{}, namer)
	if err != nil {
		t.Fatalf("failed to parse schema: %v", err)
	}
	for _, f := range s.Fields {
		t.Logf("Field: %s, DBName: %s, DefaultValue: [%s], DefaultValueInterface: %v", f.Name, f.DBName, f.DefaultValue, f.DefaultValueInterface)
	}
}
