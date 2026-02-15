package session

import "testing"

func TestExtractTraceID(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		wantID    string
		wantClean string
	}{
		{
			name:      "no trace_id",
			sql:       "SELECT 1",
			wantID:    "",
			wantClean: "SELECT 1",
		},
		{
			name:      "simple trace_id",
			sql:       "/*trace_id=abc123*/ SELECT 1",
			wantID:    "abc123",
			wantClean: " SELECT 1",
		},
		{
			name:      "trace_id with spaces",
			sql:       "/* trace_id = req-456 */ SELECT * FROM users",
			wantID:    "req-456",
			wantClean: " SELECT * FROM users",
		},
		{
			name:      "trace_id with uuid",
			sql:       "/*trace_id=550e8400-e29b-41d4-a716-446655440000*/ SELECT 1",
			wantID:    "550e8400-e29b-41d4-a716-446655440000",
			wantClean: " SELECT 1",
		},
		{
			name:      "other comment not extracted",
			sql:       "/* hint: use index */ SELECT 1",
			wantID:    "",
			wantClean: "/* hint: use index */ SELECT 1",
		},
		{
			name:      "trace_id at end",
			sql:       "SELECT 1 /*trace_id=end123*/",
			wantID:    "end123",
			wantClean: "SELECT 1 ",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotID, gotClean := ExtractTraceID(tt.sql)
			if gotID != tt.wantID {
				t.Errorf("ExtractTraceID() traceID = %q, want %q", gotID, tt.wantID)
			}
			if gotClean != tt.wantClean {
				t.Errorf("ExtractTraceID() cleanSQL = %q, want %q", gotClean, tt.wantClean)
			}
		})
	}
}
