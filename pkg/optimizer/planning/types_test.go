package planning

import (
	"testing"
)

func TestAggregationTypeString(t *testing.T) {
	tests := []struct {
		aggType AggregationType
		want    string
	}{
		{Count, "COUNT"},
		{Sum, "SUM"},
		{Avg, "AVG"},
		{Max, "MAX"},
		{Min, "MIN"},
		{AggregationType(99), "UNKNOWN"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			if got := tt.aggType.String(); got != tt.want {
				t.Errorf("AggregationType.String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAggregationItem(t *testing.T) {
	item := &AggregationItem{
		Type:     Count,
		Expr:     "column1",
		Alias:    "count_alias",
		Distinct: true,
	}

	if item.Type != Count {
		t.Errorf("Type = %v, want Count", item.Type)
	}

	if item.Expr != "column1" {
		t.Errorf("Expr = %v, want column1", item.Expr)
	}

	if item.Alias != "count_alias" {
		t.Errorf("Alias = %v, want count_alias", item.Alias)
	}

	if !item.Distinct {
		t.Error("Distinct should be true")
	}
}

func TestLimitInfo(t *testing.T) {
	tests := []struct {
		name   string
		limit  int64
		offset int64
	}{
		{"Simple limit", 10, 0},
		{"Limit with offset", 5, 20},
		{"Large limit", 1000, 100},
		{"Zero limit", 0, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			info := &LimitInfo{
				Limit:  tt.limit,
				Offset: tt.offset,
			}

			if info.Limit != tt.limit {
				t.Errorf("Limit = %v, want %v", info.Limit, tt.limit)
			}

			if info.Offset != tt.offset {
				t.Errorf("Offset = %v, want %v", info.Offset, tt.offset)
			}
		})
	}
}
