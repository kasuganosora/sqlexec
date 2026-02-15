package utils

import (
	"bytes"
	"sync"
	"testing"
)

func TestCollationEngine_BinaryComparison(t *testing.T) {
	e := NewCollationEngine()

	tests := []struct {
		a, b string
		want int
	}{
		{"abc", "abc", 0},
		{"abc", "abd", -1},
		{"abd", "abc", 1},
		{"ABC", "abc", -1}, // binary: 'A' (65) < 'a' (97)
		{"", "", 0},
		{"a", "", 1},
		{"", "a", -1},
	}

	for _, tt := range tests {
		result, err := e.Compare(tt.a, tt.b, "utf8mb4_bin")
		if err != nil {
			t.Fatalf("Compare(%q, %q, binary) error: %v", tt.a, tt.b, err)
		}
		if result != tt.want {
			t.Errorf("Compare(%q, %q, binary) = %d, want %d", tt.a, tt.b, result, tt.want)
		}
	}
}

func TestCollationEngine_CaseInsensitive(t *testing.T) {
	e := NewCollationEngine()

	ciCollations := []string{
		"utf8mb4_general_ci",
		"utf8mb4_unicode_ci",
	}

	for _, coll := range ciCollations {
		result, err := e.Compare("abc", "ABC", coll)
		if err != nil {
			t.Fatalf("Compare(abc, ABC, %s) error: %v", coll, err)
		}
		if result != 0 {
			t.Errorf("Compare(abc, ABC, %s) = %d, want 0 (case-insensitive)", coll, result)
		}

		result, err = e.Compare("Hello", "hello", coll)
		if err != nil {
			t.Fatalf("Compare(Hello, hello, %s) error: %v", coll, err)
		}
		if result != 0 {
			t.Errorf("Compare(Hello, hello, %s) = %d, want 0", coll, result)
		}
	}
}

func TestCollationEngine_AccentInsensitive(t *testing.T) {
	e := NewCollationEngine()

	// utf8mb4_0900_ai_ci should treat accented chars as equal to base
	result, err := e.Compare("cafe", "café", "utf8mb4_0900_ai_ci")
	if err != nil {
		t.Fatalf("Compare error: %v", err)
	}
	if result != 0 {
		t.Errorf("Compare(cafe, café, 0900_ai_ci) = %d, want 0 (accent-insensitive)", result)
	}

	// Also case insensitive
	result, err = e.Compare("CAFE", "café", "utf8mb4_0900_ai_ci")
	if err != nil {
		t.Fatalf("Compare error: %v", err)
	}
	if result != 0 {
		t.Errorf("Compare(CAFE, café, 0900_ai_ci) = %d, want 0", result)
	}
}

func TestCollationEngine_TurkishLocale(t *testing.T) {
	e := NewCollationEngine()

	// In Turkish, 'I' (capital) lowercases to 'ı' (dotless i), not 'i'
	// And 'İ' (capital dotted i) lowercases to 'i'
	// Under Turkish CI collation, 'I' and 'ı' should be equal
	result, err := e.Compare("I", "ı", "utf8mb4_turkish_ci")
	if err != nil {
		t.Fatalf("Compare error: %v", err)
	}
	if result != 0 {
		t.Errorf("Compare(I, ı, turkish_ci) = %d, want 0", result)
	}

	// 'İ' and 'i' should be equal under Turkish CI
	result, err = e.Compare("İ", "i", "utf8mb4_turkish_ci")
	if err != nil {
		t.Fatalf("Compare error: %v", err)
	}
	if result != 0 {
		t.Errorf("Compare(İ, i, turkish_ci) = %d, want 0", result)
	}
}

func TestCollationEngine_GermanPhonebook(t *testing.T) {
	e := NewCollationEngine()

	// Under German phonebook ordering, "ä" should sort near "ae"
	// Verify that ä and a are not considered equal (they sort differently)
	result, err := e.Compare("ä", "a", "utf8mb4_german2_ci")
	if err != nil {
		t.Fatalf("Compare error: %v", err)
	}
	// In phonebook ordering, ä sorts after a
	if result == 0 {
		t.Errorf("Compare(ä, a, german2_ci) = 0, expected non-zero")
	}

	// Case insensitivity should still work
	result, err = e.Compare("Ä", "ä", "utf8mb4_german2_ci")
	if err != nil {
		t.Fatalf("Compare error: %v", err)
	}
	if result != 0 {
		t.Errorf("Compare(Ä, ä, german2_ci) = %d, want 0 (case-insensitive)", result)
	}
}

func TestCollationEngine_SortKey(t *testing.T) {
	e := NewCollationEngine()

	// Sort keys for "abc" and "abd" under unicode_ci: abc < abd
	keyA, err := e.SortKey("abc", "utf8mb4_unicode_ci")
	if err != nil {
		t.Fatalf("SortKey error: %v", err)
	}
	keyB, err := e.SortKey("abd", "utf8mb4_unicode_ci")
	if err != nil {
		t.Fatalf("SortKey error: %v", err)
	}

	if bytes.Compare(keyA, keyB) >= 0 {
		t.Errorf("SortKey(abc) should be < SortKey(abd)")
	}

	// Under CI collation, sort keys for "ABC" and "abc" should be equal
	keyUpper, err := e.SortKey("ABC", "utf8mb4_unicode_ci")
	if err != nil {
		t.Fatalf("SortKey error: %v", err)
	}
	keyLower, err := e.SortKey("abc", "utf8mb4_unicode_ci")
	if err != nil {
		t.Fatalf("SortKey error: %v", err)
	}

	if bytes.Compare(keyUpper, keyLower) != 0 {
		t.Errorf("SortKey(ABC, ci) should equal SortKey(abc, ci)")
	}

	// Binary sort keys should differ for different cases
	keyBinUpper, _ := e.SortKey("ABC", "utf8mb4_bin")
	keyBinLower, _ := e.SortKey("abc", "utf8mb4_bin")

	if bytes.Compare(keyBinUpper, keyBinLower) == 0 {
		t.Errorf("SortKey(ABC, bin) should differ from SortKey(abc, bin)")
	}
}

func TestCollationEngine_NewCollator(t *testing.T) {
	e := NewCollationEngine()

	info, ok := e.GetCollationInfo("utf8mb4_unicode_ci")
	if !ok {
		t.Fatal("GetCollationInfo returned false for unicode_ci")
	}

	c := e.newCollator(info)
	if c == nil {
		t.Fatal("newCollator returned nil for non-binary collation")
	}

	// Verify it works
	result := c.CompareString("abc", "ABC")
	if result != 0 {
		t.Errorf("CompareString(abc, ABC) with IgnoreCase = %d, want 0", result)
	}
}

func TestCollationEngine_UnknownCollation(t *testing.T) {
	e := NewCollationEngine()

	// Unknown collation should resolve to binary
	resolved := e.ResolveCollation("nonexistent_collation")
	if resolved != "utf8mb4_bin" {
		t.Errorf("ResolveCollation(nonexistent) = %q, want %q", resolved, "utf8mb4_bin")
	}

	// Should still compare correctly (binary: 'a'=97 > 'A'=65)
	result, err := e.Compare("abc", "ABC", "nonexistent_collation")
	if err != nil {
		t.Fatalf("Compare error: %v", err)
	}
	if result <= 0 {
		t.Errorf("Compare(abc, ABC, binary fallback) should be > 0 (binary: 'a' > 'A'), got %d", result)
	}
}

func TestCollationEngine_EmptyCollation(t *testing.T) {
	e := NewCollationEngine()

	resolved := e.ResolveCollation("")
	if resolved != "utf8mb4_bin" {
		t.Errorf("ResolveCollation('') = %q, want %q", resolved, "utf8mb4_bin")
	}
}

func TestCollationEngine_Aliases(t *testing.T) {
	e := NewCollationEngine()

	tests := []struct {
		alias string
		want  string
	}{
		{"utf8mb4", "utf8mb4_general_ci"},
		{"utf8", "utf8_general_ci"},
		{"default", "utf8mb4_0900_ai_ci"},
	}

	for _, tt := range tests {
		got := e.ResolveCollation(tt.alias)
		if got != tt.want {
			t.Errorf("ResolveCollation(%q) = %q, want %q", tt.alias, got, tt.want)
		}
	}
}

func TestCollationEngine_ConcurrentAccess(t *testing.T) {
	e := NewCollationEngine()

	var wg sync.WaitGroup
	collations := []string{
		"utf8mb4_unicode_ci",
		"utf8mb4_turkish_ci",
		"utf8mb4_general_ci",
		"utf8mb4_bin",
		"utf8mb4_german2_ci",
	}

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			coll := collations[idx%len(collations)]
			_, _ = e.Compare("hello", "world", coll)
			_, _ = e.SortKey("test", coll)
			_ = e.ResolveCollation(coll)
			_, _ = e.GetCollationInfo(coll)
		}(i)
	}

	wg.Wait()
}

func TestCollationEngine_ListCollations(t *testing.T) {
	e := NewCollationEngine()
	list := e.ListCollations()

	if len(list) < 25 {
		t.Errorf("ListCollations() returned %d collations, expected at least 25", len(list))
	}

	// Check that key collations are present
	foundNames := make(map[string]bool)
	for _, info := range list {
		foundNames[info.Name] = true
	}

	required := []string{
		"utf8mb4_bin", "utf8mb4_general_ci", "utf8mb4_unicode_ci",
		"utf8mb4_0900_ai_ci", "utf8mb4_turkish_ci",
	}
	for _, name := range required {
		if !foundNames[name] {
			t.Errorf("ListCollations() missing required collation %q", name)
		}
	}
}

func TestCollationEngine_IsCaseInsensitive(t *testing.T) {
	e := NewCollationEngine()

	if e.IsCaseInsensitive("utf8mb4_bin") {
		t.Errorf("utf8mb4_bin should not be case-insensitive")
	}
	if !e.IsCaseInsensitive("utf8mb4_unicode_ci") {
		t.Errorf("utf8mb4_unicode_ci should be case-insensitive")
	}
	if !e.IsCaseInsensitive("utf8mb4_0900_ai_ci") {
		t.Errorf("utf8mb4_0900_ai_ci should be case-insensitive")
	}
}

func TestCollationEngine_IsAccentInsensitive(t *testing.T) {
	e := NewCollationEngine()

	if e.IsAccentInsensitive("utf8mb4_unicode_ci") {
		t.Errorf("utf8mb4_unicode_ci should not be accent-insensitive")
	}
	if !e.IsAccentInsensitive("utf8mb4_0900_ai_ci") {
		t.Errorf("utf8mb4_0900_ai_ci should be accent-insensitive")
	}
}

func TestCollationEngine_GetCollationInfo(t *testing.T) {
	e := NewCollationEngine()

	info, ok := e.GetCollationInfo("utf8mb4_turkish_ci")
	if !ok {
		t.Fatal("GetCollationInfo(turkish) returned false")
	}
	if info.Name != "utf8mb4_turkish_ci" {
		t.Errorf("info.Name = %q, want %q", info.Name, "utf8mb4_turkish_ci")
	}
	if !info.CaseInsensitive {
		t.Error("Turkish CI should be case-insensitive")
	}
	if info.IsBinary {
		t.Error("Turkish CI should not be binary")
	}

	// Unknown
	_, ok = e.GetCollationInfo("unknown_collation")
	if ok {
		// It resolves to binary, so it should still return info
		info, _ := e.GetCollationInfo("unknown_collation")
		if !info.IsBinary {
			t.Error("Unknown collation should resolve to binary")
		}
	}
}

func TestCollationEngine_BinaryCollatorNil(t *testing.T) {
	e := NewCollationEngine()

	info, ok := e.GetCollationInfo("utf8mb4_bin")
	if !ok {
		t.Fatal("GetCollationInfo returned false for bin")
	}
	c := e.newCollator(info)
	if c != nil {
		t.Error("newCollator(binary) should return nil")
	}
}

func TestGlobalCollationEngine(t *testing.T) {
	e1 := GetGlobalCollationEngine()
	e2 := GetGlobalCollationEngine()

	if e1 != e2 {
		t.Error("GetGlobalCollationEngine should return the same instance")
	}

	if e1 == nil {
		t.Fatal("GetGlobalCollationEngine returned nil")
	}
}
