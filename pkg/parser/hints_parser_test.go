package parser

import (
	"testing"
	"time"
)

func TestHintsParser_ParseFromComment_Empty(t *testing.T) {
	parser := NewHintsParser()
	hints, err := parser.ParseFromComment("")
	if err != nil {
		t.Fatalf("ParseFromComment failed: %v", err)
	}

	if hints == nil {
		t.Fatal("hints should not be nil")
	}

	// All fields should be empty/false
	if hints.HashAgg {
		t.Error("HashAgg should be false")
	}
	if hints.StreamAgg {
		t.Error("StreamAgg should be false")
	}
	if hints.StraightJoin {
		t.Error("StraightJoin should be false")
	}
	if hints.SemiJoinRewrite {
		t.Error("SemiJoinRewrite should be false")
	}
}

func TestHintsParser_ParseFromComment_BooleanHints(t *testing.T) {
	parser := NewHintsParser()

	tests := []struct {
		name      string
		comment   string
		checkHint func(*ParsedHints) bool
	}{
		{
			name:    "HASH_AGG",
			comment: "HASH_AGG()",
			checkHint: func(h *ParsedHints) bool {
				return h.HashAgg
			},
		},
		{
			name:    "STREAM_AGG",
			comment: "STREAM_AGG()",
			checkHint: func(h *ParsedHints) bool {
				return h.StreamAgg
			},
		},
		{
			name:    "MPP_1PHASE_AGG",
			comment: "MPP_1PHASE_AGG()",
			checkHint: func(h *ParsedHints) bool {
				return h.MPP1PhaseAgg
			},
		},
		{
			name:    "MPP_2PHASE_AGG",
			comment: "MPP_2PHASE_AGG()",
			checkHint: func(h *ParsedHints) bool {
				return h.MPP2PhaseAgg
			},
		},
		{
			name:    "STRAIGHT_JOIN",
			comment: "STRAIGHT_JOIN()",
			checkHint: func(h *ParsedHints) bool {
				return h.StraightJoin
			},
		},
		{
			name:    "SEMI_JOIN_REWRITE",
			comment: "SEMI_JOIN_REWRITE()",
			checkHint: func(h *ParsedHints) bool {
				return h.SemiJoinRewrite
			},
		},
		{
			name:    "NO_DECORRELATE",
			comment: "NO_DECORRELATE()",
			checkHint: func(h *ParsedHints) bool {
				return h.NoDecorrelate
			},
		},
		{
			name:    "USE_TOJA",
			comment: "USE_TOJA()",
			checkHint: func(h *ParsedHints) bool {
				return h.UseTOJA
			},
		},
		{
			name:    "READ_CONSISTENT_REPLICA",
			comment: "READ_CONSISTENT_REPLICA()",
			checkHint: func(h *ParsedHints) bool {
				return h.ReadConsistentReplica
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hints, err := parser.ParseFromComment(tt.comment)
			if err != nil {
				t.Fatalf("ParseFromComment failed: %v", err)
			}

			if !tt.checkHint(hints) {
				t.Errorf("%s hint should be true", tt.name)
			}
		})
	}
}

func TestHintsParser_ParseFromComment_JoinHints(t *testing.T) {
	parser := NewHintsParser()

	// Test HASH_JOIN
	hints, err := parser.ParseFromComment("HASH_JOIN(t1, t2, t3)")
	if err != nil {
		t.Fatalf("ParseFromComment failed: %v", err)
	}

	expectedTables := []string{"t1", "t2", "t3"}
	if len(hints.HashJoinTables) != len(expectedTables) {
		t.Fatalf("Expected %d tables, got %d", len(expectedTables), len(hints.HashJoinTables))
	}

	for i, table := range expectedTables {
		if hints.HashJoinTables[i] != table {
			t.Errorf("Expected table %s at index %d, got %s", table, i, hints.HashJoinTables[i])
		}
	}

	// Test MERGE_JOIN
	hints, err = parser.ParseFromComment("MERGE_JOIN(t1)")
	if err != nil {
		t.Fatalf("ParseFromComment failed: %v", err)
	}

	if len(hints.MergeJoinTables) != 1 || hints.MergeJoinTables[0] != "t1" {
		t.Errorf("MERGE_JOIN(t1) failed: %v", hints.MergeJoinTables)
	}

	// Test LEADING
	hints, err = parser.ParseFromComment("LEADING(t1, t2)")
	if err != nil {
		t.Fatalf("ParseFromComment failed: %v", err)
	}

	expectedLeading := []string{"t1", "t2"}
	if len(hints.LeadingOrder) != len(expectedLeading) {
		t.Fatalf("Expected %d tables in leading order, got %d", len(expectedLeading), len(hints.LeadingOrder))
	}

	for i, table := range expectedLeading {
		if hints.LeadingOrder[i] != table {
			t.Errorf("Expected table %s at index %d, got %s", table, i, hints.LeadingOrder[i])
		}
	}

	// Test NO_* hints
	hints, err = parser.ParseFromComment("NO_HASH_JOIN(t1) NO_MERGE_JOIN(t2) NO_INDEX_JOIN(t3)")
	if err != nil {
		t.Fatalf("ParseFromComment failed: %v", err)
	}

	if len(hints.NoHashJoinTables) != 1 || hints.NoHashJoinTables[0] != "t1" {
		t.Errorf("NO_HASH_JOIN failed: %v", hints.NoHashJoinTables)
	}
	if len(hints.NoMergeJoinTables) != 1 || hints.NoMergeJoinTables[0] != "t2" {
		t.Errorf("NO_MERGE_JOIN failed: %v", hints.NoMergeJoinTables)
	}
	if len(hints.NoIndexJoinTables) != 1 || hints.NoIndexJoinTables[0] != "t3" {
		t.Errorf("NO_INDEX_JOIN failed: %v", hints.NoIndexJoinTables)
	}
}

func TestHintsParser_ParseFromComment_IndexHints(t *testing.T) {
	parser := NewHintsParser()

	// Test USE_INDEX without specific index
	hints, err := parser.ParseFromComment("USE_INDEX(t)")
	if err != nil {
		t.Fatalf("ParseFromComment failed: %v", err)
	}

	if len(hints.UseIndex) != 1 {
		t.Fatalf("Expected 1 table in UseIndex, got %d", len(hints.UseIndex))
	}

	if _, ok := hints.UseIndex["t"]; !ok {
		t.Error("Expected table 't' in UseIndex")
	}

	// Test USE_INDEX with specific index
	hints, err = parser.ParseFromComment("USE_INDEX(t@idx1)")
	if err != nil {
		t.Fatalf("ParseFromComment failed: %v", err)
	}

	indexes, ok := hints.UseIndex["t"]
	if !ok {
		t.Fatal("Expected table 't' in UseIndex")
	}

	if len(indexes) != 1 || indexes[0] != "idx1" {
		t.Errorf("Expected index 'idx1', got %v", indexes)
	}

	// Test FORCE_INDEX
	hints, err = parser.ParseFromComment("FORCE_INDEX(t@idx1)")
	if err != nil {
		t.Fatalf("ParseFromComment failed: %v", err)
	}

	indexes, ok = hints.ForceIndex["t"]
	if !ok {
		t.Fatal("Expected table 't' in ForceIndex")
	}

	if len(indexes) != 1 || indexes[0] != "idx1" {
		t.Errorf("Expected index 'idx1', got %v", indexes)
	}

	// Test IGNORE_INDEX
	hints, err = parser.ParseFromComment("IGNORE_INDEX(t@idx1)")
	if err != nil {
		t.Fatalf("ParseFromComment failed: %v", err)
	}

	indexes, ok = hints.IgnoreIndex["t"]
	if !ok {
		t.Fatal("Expected table 't' in IgnoreIndex")
	}

	if len(indexes) != 1 || indexes[0] != "idx1" {
		t.Errorf("Expected index 'idx1', got %v", indexes)
	}

	// Test ORDER_INDEX
	hints, err = parser.ParseFromComment("ORDER_INDEX(t@idx1)")
	if err != nil {
		t.Fatalf("ParseFromComment failed: %v", err)
	}

	indexName, ok := hints.OrderIndex["t"]
	if !ok {
		t.Fatal("Expected table 't' in OrderIndex")
	}

	if indexName != "idx1" {
		t.Errorf("Expected index 'idx1', got %s", indexName)
	}

	// Test NO_ORDER_INDEX
	hints, err = parser.ParseFromComment("NO_ORDER_INDEX(t@idx1)")
	if err != nil {
		t.Fatalf("ParseFromComment failed: %v", err)
	}

	indexName, ok = hints.NoOrderIndex["t"]
	if !ok {
		t.Fatal("Expected table 't' in NoOrderIndex")
	}

	if indexName != "idx1" {
		t.Errorf("Expected index 'idx1', got %s", indexName)
	}
}

func TestHintsParser_ParseFromComment_MultipleHints(t *testing.T) {
	parser := NewHintsParser()

	hints, err := parser.ParseFromComment("HASH_JOIN(t1, t2) USE_INDEX(t@idx1) HASH_AGG()")
	if err != nil {
		t.Fatalf("ParseFromComment failed: %v", err)
	}

	if !hints.HashAgg {
		t.Error("HashAgg should be true")
	}

	if len(hints.HashJoinTables) != 2 {
		t.Errorf("Expected 2 tables in HashJoinTables, got %d", len(hints.HashJoinTables))
	}

	indexes, ok := hints.UseIndex["t"]
	if !ok {
		t.Fatal("Expected table 't' in UseIndex")
	}

	if len(indexes) != 1 || indexes[0] != "idx1" {
		t.Errorf("Expected index 'idx1', got %v", indexes)
	}
}

func TestHintsParser_ParseFromComment_GlobalHints(t *testing.T) {
	parser := NewHintsParser()

	// Test MAX_EXECUTION_TIME
	hints, err := parser.ParseFromComment("MAX_EXECUTION_TIME(1000ms)")
	if err != nil {
		t.Fatalf("ParseFromComment failed: %v", err)
	}

	expectedDuration := 1000 * time.Millisecond
	if hints.MaxExecutionTime != expectedDuration {
		t.Errorf("Expected duration %v, got %v", expectedDuration, hints.MaxExecutionTime)
	}

	// Test MEMORY_QUOTA
	hints, err = parser.ParseFromComment("MEMORY_QUOTA(1073741824)")
	if err != nil {
		t.Fatalf("ParseFromComment failed: %v", err)
	}

	if hints.MemoryQuota != 1073741824 {
		t.Errorf("Expected memory quota 1073741824, got %d", hints.MemoryQuota)
	}

	// Test RESOURCE_GROUP
	hints, err = parser.ParseFromComment("RESOURCE_GROUP(rg1)")
	if err != nil {
		t.Fatalf("ParseFromComment failed: %v", err)
	}

	if hints.ResourceGroup != "rg1" {
		t.Errorf("Expected resource group 'rg1', got %s", hints.ResourceGroup)
	}

	// Test QB_NAME
	hints, err = parser.ParseFromComment("QB_NAME(qb1)")
	if err != nil {
		t.Fatalf("ParseFromComment failed: %v", err)
	}

	if hints.QBName != "qb1" {
		t.Errorf("Expected QB name 'qb1', got %s", hints.QBName)
	}
}

func TestHintsParser_ParseDuration(t *testing.T) {
	parser := NewHintsParser()

	tests := []struct {
		input       string
		expected    time.Duration
		expectError bool
	}{
		{"1000ms", 1000 * time.Millisecond, false},
		{"5s", 5 * time.Second, false},
		{"2m", 2 * time.Minute, false},
		{"1h", 1 * time.Hour, false},
		{"invalid", 0, true},
		{"10x", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			duration, err := parser.parseDuration(tt.input)

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error for input %s, got nil", tt.input)
				}
				return
			}

			if err != nil {
				t.Fatalf("parseDuration failed: %v", err)
			}

			if duration != tt.expected {
				t.Errorf("Expected duration %v, got %v", tt.expected, duration)
			}
		})
	}
}

func TestHintsParser_ExtractHintsFromSQL(t *testing.T) {
	parser := NewHintsParser()

	tests := []struct {
		name          string
		inputSQL      string
		expectedHints string
		expectedSQL   string
		expectNoHints bool
	}{
		{
			name:          "Single hint",
			inputSQL:      "SELECT /*+ HASH_AGG() */ * FROM t",
			expectedHints: "HASH_AGG",
			expectedSQL:   "SELECT  * FROM t",
		},
		{
			name:          "Multiple hints",
			inputSQL:      "SELECT /*+ HASH_JOIN(t1, t2) USE_INDEX(t@idx1) */ * FROM t",
			expectedHints: "HASH_JOIN(t1,t2) USE_INDEX(t@idx1)",
			expectedSQL:   "SELECT  * FROM t",
		},
		{
			name:          "No hints",
			inputSQL:      "SELECT * FROM t",
			expectNoHints: true,
			expectedSQL:   "SELECT * FROM t",
		},
		{
			name:          "Empty comment",
			inputSQL:      "SELECT /**/ * FROM t",
			expectNoHints: true,
			expectedSQL:   "SELECT /**/ * FROM t",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hints, cleanSQL, err := parser.ExtractHintsFromSQL(tt.inputSQL)
			if err != nil {
				t.Fatalf("ExtractHintsFromSQL failed: %v", err)
			}

			if tt.expectNoHints {
				hintString := hints.String()
				if hintString != "" {
					t.Errorf("Expected no hints, got some: %s", hintString)
				}
			} else {
				if hints == nil {
					t.Fatal("Expected hints, got nil")
				}

				hintString := hints.String()
				if hintString != tt.expectedHints {
					t.Errorf("Expected hints %s, got %s", tt.expectedHints, hintString)
				}
			}

			if cleanSQL != tt.expectedSQL {
				t.Errorf("Expected clean SQL '%s', got '%s'", tt.expectedSQL, cleanSQL)
			}
		})
	}
}

func TestParsedHints_String(t *testing.T) {
	hints := &ParsedHints{
		HashAgg:          true,
		HashJoinTables:   []string{"t1", "t2"},
		UseIndex:         map[string][]string{"t": []string{"idx1"}},
		MaxExecutionTime: 1000 * time.Millisecond,
	}

	hintString := hints.String()
	if hintString == "" {
		t.Error("String() should not return empty string")
	}

	// Check if all hints are present
	if !contains(hintString, "HASH_AGG") {
		t.Error("String() should contain HASH_AGG")
	}
	if !contains(hintString, "HASH_JOIN") {
		t.Error("String() should contain HASH_JOIN")
	}
	if !contains(hintString, "USE_INDEX") {
		t.Error("String() should contain USE_INDEX")
	}
	if !contains(hintString, "MAX_EXECUTION_TIME") {
		t.Error("String() should contain MAX_EXECUTION_TIME")
	}
}

func TestHintsParser_ParseFromComment_INLHints(t *testing.T) {
	parser := NewHintsParser()

	// Test INL_JOIN
	hints, err := parser.ParseFromComment("INL_JOIN(t1, t2)")
	if err != nil {
		t.Fatalf("ParseFromComment failed: %v", err)
	}

	expectedTables := []string{"t1", "t2"}
	if len(hints.INLJoinTables) != len(expectedTables) {
		t.Fatalf("Expected %d tables, got %d", len(expectedTables), len(hints.INLJoinTables))
	}

	// Test INL_HASH_JOIN
	hints, err = parser.ParseFromComment("INL_HASH_JOIN(t1)")
	if err != nil {
		t.Fatalf("ParseFromComment failed: %v", err)
	}

	if len(hints.INLHashJoinTables) != 1 || hints.INLHashJoinTables[0] != "t1" {
		t.Errorf("INL_HASH_JOIN failed: %v", hints.INLHashJoinTables)
	}

	// Test INL_MERGE_JOIN
	hints, err = parser.ParseFromComment("INL_MERGE_JOIN(t1)")
	if err != nil {
		t.Fatalf("ParseFromComment failed: %v", err)
	}

	if len(hints.INLMergeJoinTables) != 1 || hints.INLMergeJoinTables[0] != "t1" {
		t.Errorf("INL_MERGE_JOIN failed: %v", hints.INLMergeJoinTables)
	}
}

func TestHintsParser_CaseInsensitive(t *testing.T) {
	parser := NewHintsParser()

	// Test lowercase hint name (converted to uppercase by parser)
	hints, err := parser.ParseFromComment("HASH_AGG()")
	if err != nil {
		t.Fatalf("ParseFromComment failed: %v", err)
	}

	if !hints.HashAgg {
		t.Error("HashAgg should be true (case insensitive)")
	}

	// Test mixed case
	hints, err = parser.ParseFromComment("HASH_AGG()")
	if err != nil {
		t.Fatalf("ParseFromComment failed: %v", err)
	}

	if !hints.HashAgg {
		t.Error("HashAgg should be true (case insensitive)")
	}
}

func TestHintsParser_WhitespaceHandling(t *testing.T) {
	parser := NewHintsParser()

	// Test with extra whitespace
	hints, err := parser.ParseFromComment(" HASH_JOIN ( t1 , t2 ) ")
	if err != nil {
		t.Fatalf("ParseFromComment failed: %v", err)
	}

	expectedTables := []string{"t1", "t2"}
	if len(hints.HashJoinTables) != len(expectedTables) {
		t.Fatalf("Expected %d tables, got %d", len(expectedTables), len(hints.HashJoinTables))
	}

	for i, table := range expectedTables {
		if hints.HashJoinTables[i] != table {
			t.Errorf("Expected table %s at index %d, got %s", table, i, hints.HashJoinTables[i])
		}
	}
}

func TestHintsParser_CommaHandling(t *testing.T) {
	parser := NewHintsParser()

	// Test trailing comma
	hints, err := parser.ParseFromComment("HASH_JOIN(t1, t2,)")
	if err != nil {
		t.Fatalf("ParseFromComment failed: %v", err)
	}

	if len(hints.HashJoinTables) != 2 {
		t.Errorf("Expected 2 tables, got %d", len(hints.HashJoinTables))
	}
}

func TestHintsParser_UnknownHint(t *testing.T) {
	parser := NewHintsParser()

	// Unknown hint should not cause error, just log warning
	_, err := parser.ParseFromComment("UNKNOWN_HINT()")
	if err != nil {
		t.Errorf("ParseFromComment should not fail on unknown hint, got: %v", err)
	}
}

func TestHintsParser_NewParsedHints(t *testing.T) {
	parser := NewHintsParser()

	hints := parser.NewParsedHints()
	if hints == nil {
		t.Fatal("NewParsedHints should not return nil")
	}

	if hints.UseIndex == nil {
		t.Error("UseIndex map should be initialized")
	}
	if hints.ForceIndex == nil {
		t.Error("ForceIndex map should be initialized")
	}
	if hints.IgnoreIndex == nil {
		t.Error("IgnoreIndex map should be initialized")
	}
	if hints.OrderIndex == nil {
		t.Error("OrderIndex map should be initialized")
	}
	if hints.NoOrderIndex == nil {
		t.Error("NoOrderIndex map should be initialized")
	}
}

// Helper function
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) && indexOf(s, substr) >= 0)
}

func indexOf(s, substr string) int {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}
