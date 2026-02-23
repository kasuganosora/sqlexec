package plan

import (
	"testing"
)

func TestLimitConfig(t *testing.T) {
	tests := []struct {
		name   string
		limit  int64
		offset int64
	}{
		{
			name:   "Simple limit",
			limit:  10,
			offset: 0,
		},
		{
			name:   "Limit with offset",
			limit:  5,
			offset: 20,
		},
		{
			name:   "Large limit",
			limit:  1000000,
			offset: 0,
		},
		{
			name:   "Large offset",
			limit:  10,
			offset: 999999,
		},
		{
			name:   "Zero limit",
			limit:  0,
			offset: 0,
		},
		{
			name:   "Negative offset",
			limit:  10,
			offset: -1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := &LimitConfig{
				Limit:  tt.limit,
				Offset: tt.offset,
			}

			if config.Limit != tt.limit {
				t.Errorf("Limit = %v, want %v", config.Limit, tt.limit)
			}

			if config.Offset != tt.offset {
				t.Errorf("Offset = %v, want %v", config.Offset, tt.offset)
			}
		})
	}
}

func TestLimitConfigWithPlan(t *testing.T) {
	limitConfig := &LimitConfig{
		Limit:  25,
		Offset: 75,
	}

	plan := &Plan{
		ID:     "limit_001",
		Type:   TypeLimit,
		Config: limitConfig,
	}

	if plan.Type != TypeLimit {
		t.Errorf("Plan.Type = %v, want %v", plan.Type, TypeLimit)
	}

	retrievedConfig, ok := plan.Config.(*LimitConfig)
	if !ok {
		t.Fatal("Failed to retrieve LimitConfig from Plan")
	}

	if retrievedConfig.Limit != 25 {
		t.Errorf("Limit = %v, want 25", retrievedConfig.Limit)
	}

	if retrievedConfig.Offset != 75 {
		t.Errorf("Offset = %v, want 75", retrievedConfig.Offset)
	}
}

func TestLimitConfigPagination(t *testing.T) {
	// Test common pagination patterns
	paginationTests := []struct {
		name       string
		page       int
		perPage    int
		wantLimit  int64
		wantOffset int64
	}{
		{
			name:       "Page 1, 10 per page",
			page:       1,
			perPage:    10,
			wantLimit:  10,
			wantOffset: 0,
		},
		{
			name:       "Page 2, 10 per page",
			page:       2,
			perPage:    10,
			wantLimit:  10,
			wantOffset: 10,
		},
		{
			name:       "Page 3, 20 per page",
			page:       3,
			perPage:    20,
			wantLimit:  20,
			wantOffset: 40,
		},
	}

	for _, tt := range paginationTests {
		t.Run(tt.name, func(t *testing.T) {
			config := &LimitConfig{
				Limit:  int64(tt.perPage),
				Offset: int64((tt.page - 1) * tt.perPage),
			}

			if config.Limit != tt.wantLimit {
				t.Errorf("Limit = %v, want %v", config.Limit, tt.wantLimit)
			}

			if config.Offset != tt.wantOffset {
				t.Errorf("Offset = %v, want %v", config.Offset, tt.wantOffset)
			}
		})
	}
}

func TestLimitConfigZeroValues(t *testing.T) {
	config := &LimitConfig{
		Limit:  0,
		Offset: 0,
	}

	if config.Limit != 0 {
		t.Errorf("Limit = %v, want 0", config.Limit)
	}

	if config.Offset != 0 {
		t.Errorf("Offset = %v, want 0", config.Offset)
	}

	// Test with negative values (edge case)
	config.Limit = -1
	config.Offset = -1

	if config.Limit != -1 {
		t.Errorf("Negative Limit = %v, want -1", config.Limit)
	}

	if config.Offset != -1 {
		t.Errorf("Negative Offset = %v, want -1", config.Offset)
	}
}
