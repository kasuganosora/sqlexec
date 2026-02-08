package plan

import (
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/types"
)

func TestTableScanConfig(t *testing.T) {
	tests := []struct {
		name            string
		tableName       string
		columns         []types.ColumnInfo
		filters         []domain.Filter
		limitInfo       *types.LimitInfo
		enableParallel  bool
		minParallelRows int64
	}{
		{
			name:            "Simple scan",
			tableName:       "users",
			columns:         []types.ColumnInfo{},
			filters:         []domain.Filter{},
			limitInfo:       nil,
			enableParallel:  false,
			minParallelRows: 0,
		},
		{
			name:      "Scan with columns",
			tableName: "products",
			columns: []types.ColumnInfo{
				{Name: "id", Type: "int"},
				{Name: "name", Type: "varchar"},
				{Name: "price", Type: "decimal"},
			},
			filters:         []domain.Filter{},
			limitInfo:       nil,
			enableParallel:  false,
			minParallelRows: 0,
		},
		{
			name:      "Scan with filters",
			tableName: "orders",
			columns: []types.ColumnInfo{
				{Name: "id", Type: "int"},
				{Name: "status", Type: "varchar"},
			},
			filters: []domain.Filter{
				{Field: "status", Operator: "=", Value: "pending"},
			},
			limitInfo:       nil,
			enableParallel:  true,
			minParallelRows: 1000,
		},
		{
			name:      "Scan with limit",
			tableName: "logs",
			columns:   []types.ColumnInfo{},
			filters:   []domain.Filter{},
			limitInfo: &types.LimitInfo{
				Limit:  100,
				Offset: 0,
			},
			enableParallel:  false,
			minParallelRows: 0,
		},
		{
			name:            "Parallel scan enabled",
			tableName:       "large_table",
			columns:         []types.ColumnInfo{},
			filters:         []domain.Filter{},
			limitInfo:       nil,
			enableParallel:  true,
			minParallelRows: 50000,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := &TableScanConfig{
				TableName:       tt.tableName,
				Columns:         tt.columns,
				Filters:         tt.filters,
				LimitInfo:       tt.limitInfo,
				EnableParallel:  tt.enableParallel,
				MinParallelRows: tt.minParallelRows,
			}

			if config.TableName != tt.tableName {
				t.Errorf("TableName = %v, want %v", config.TableName, tt.tableName)
			}

			if len(config.Columns) != len(tt.columns) {
				t.Errorf("Columns length = %v, want %v", len(config.Columns), len(tt.columns))
			}

			if len(config.Filters) != len(tt.filters) {
				t.Errorf("Filters length = %v, want %v", len(config.Filters), len(tt.filters))
			}

			if tt.limitInfo != nil {
				if config.LimitInfo == nil {
					t.Error("LimitInfo is nil, want non-nil")
				} else if config.LimitInfo.Limit != tt.limitInfo.Limit {
					t.Errorf("LimitInfo.Limit = %v, want %v", config.LimitInfo.Limit, tt.limitInfo.Limit)
				}
			}

			if config.EnableParallel != tt.enableParallel {
				t.Errorf("EnableParallel = %v, want %v", config.EnableParallel, tt.enableParallel)
			}

			if config.MinParallelRows != tt.minParallelRows {
				t.Errorf("MinParallelRows = %v, want %v", config.MinParallelRows, tt.minParallelRows)
			}
		})
	}
}

func TestTableScanConfigWithPlan(t *testing.T) {
	columns := []types.ColumnInfo{
		{Name: "id", Type: "int"},
		{Name: "username", Type: "varchar"},
	}
	filters := []domain.Filter{
		{Field: "status", Operator: "=", Value: "active"},
	}

	scanConfig := &TableScanConfig{
		TableName:       "users",
		Columns:         columns,
		Filters:         filters,
		LimitInfo:       nil,
		EnableParallel:  true,
		MinParallelRows: 10000,
	}

	plan := &Plan{
		ID:     "scan_001",
		Type:   TypeTableScan,
		Config: scanConfig,
	}

	if plan.Type != TypeTableScan {
		t.Errorf("Plan.Type = %v, want %v", plan.Type, TypeTableScan)
	}

	retrievedConfig, ok := plan.Config.(*TableScanConfig)
	if !ok {
		t.Fatal("Failed to retrieve TableScanConfig from Plan")
	}

	if retrievedConfig.TableName != "users" {
		t.Errorf("TableName = %v, want users", retrievedConfig.TableName)
	}

	if len(retrievedConfig.Columns) != 2 {
		t.Errorf("Columns length = %v, want 2", len(retrievedConfig.Columns))
	}

	if len(retrievedConfig.Filters) != 1 {
		t.Errorf("Filters length = %v, want 1", len(retrievedConfig.Filters))
	}

	if !retrievedConfig.EnableParallel {
		t.Error("EnableParallel should be true")
	}

	if retrievedConfig.MinParallelRows != 10000 {
		t.Errorf("MinParallelRows = %v, want 10000", retrievedConfig.MinParallelRows)
	}

	if plan.Explain() != "TableScan[scan_001]" {
		t.Errorf("Plan.Explain() = %v, want TableScan[scan_001]", plan.Explain())
	}
}

func TestTableScanConfigNilFields(t *testing.T) {
	config := &TableScanConfig{
		TableName:       "test",
		Columns:         nil,
		Filters:         nil,
		LimitInfo:       nil,
		EnableParallel:  false,
		MinParallelRows: 0,
	}

	if config.Columns != nil {
		t.Errorf("Expected Columns to be nil, got %v", config.Columns)
	}

	if config.Filters != nil {
		t.Errorf("Expected Filters to be nil, got %v", config.Filters)
	}

	if config.LimitInfo != nil {
		t.Errorf("Expected LimitInfo to be nil, got %v", config.LimitInfo)
	}
}

func TestTableScanConfigParallelThresholds(t *testing.T) {
	tests := []struct {
		name            string
		enableParallel  bool
		minParallelRows int64
	}{
		{"Disabled", false, 0},
		{"Enabled with small threshold", true, 100},
		{"Enabled with medium threshold", true, 10000},
		{"Enabled with large threshold", true, 1000000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := &TableScanConfig{
				TableName:       "test_table",
				Columns:         []types.ColumnInfo{},
				Filters:         []domain.Filter{},
				LimitInfo:       nil,
				EnableParallel:  tt.enableParallel,
				MinParallelRows: tt.minParallelRows,
			}

			if config.EnableParallel != tt.enableParallel {
				t.Errorf("EnableParallel = %v, want %v", config.EnableParallel, tt.enableParallel)
			}

			if config.MinParallelRows != tt.minParallelRows {
				t.Errorf("MinParallelRows = %v, want %v", config.MinParallelRows, tt.minParallelRows)
			}
		})
	}
}
