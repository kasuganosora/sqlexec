package gorm

import (
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"gorm.io/gorm/schema"
)

type TestModelWithIgnore struct {
	ID          string   `gorm:"column:id;primaryKey"`
	Name        string   `gorm:"column:name"`
	Ignored     string   `gorm:"-"`
	AlsoIgnored []string `gorm:"-"`
}

func TestGormIgnoreTag(t *testing.T) {
	namer := schema.NamingStrategy{}
	s, err := schema.Parse(&TestModelWithIgnore{}, &sync.Map{}, namer)
	if err != nil {
		t.Fatalf("failed to parse schema: %v", err)
	}

	t.Logf("Table: %s", s.Table)
	t.Logf("Fields count: %d", len(s.Fields))
	for _, f := range s.Fields {
		t.Logf("  - Name: %s, DBName: %s", f.Name, f.DBName)
	}

	// GORM schema includes all fields, but gorm:"-" fields have empty DBName
	// Check that ignored fields have empty DBName
	for _, f := range s.Fields {
		if f.Name == "Ignored" || f.Name == "AlsoIgnored" {
			assert.Empty(t, f.DBName, "Field %s should have empty DBName (ignored)", f.Name)
		}
	}
}

func TestMigrator_IgnoreFields(t *testing.T) {
	namer := schema.NamingStrategy{}
	s, err := schema.Parse(&TestModelWithIgnore{}, &sync.Map{}, namer)
	if err != nil {
		t.Fatalf("failed to parse schema: %v", err)
	}

	// Create a test migrator
	d := &Dialector{}
	m := &Migrator{Dialector: d}

	// Generate CREATE TABLE SQL
	sql := m.generateCreateTableSQLFromSchema(s)
	t.Logf("Generated SQL: %s", sql)

	// Verify that ignored fields are NOT in the SQL
	assert.NotContains(t, strings.ToLower(sql), "ignored", "Ignored field should not be in CREATE TABLE SQL")
	assert.NotContains(t, strings.ToLower(sql), "also_ignored", "AlsoIgnored field should not be in CREATE TABLE SQL")

	// Verify that non-ignored fields ARE in the SQL
	assert.Contains(t, strings.ToLower(sql), "`id`", "ID field should be in CREATE TABLE SQL")
	assert.Contains(t, strings.ToLower(sql), "`name`", "Name field should be in CREATE TABLE SQL")
}
