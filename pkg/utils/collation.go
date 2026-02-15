package utils

import (
	"strings"
	"sync"

	"golang.org/x/text/collate"
	"golang.org/x/text/language"
)

// CollationInfo describes a collation's properties
type CollationInfo struct {
	Name              string
	ID                uint8
	Charset           string
	Tag               language.Tag
	CaseInsensitive   bool
	AccentInsensitive bool
	IsBinary          bool
	options           []collate.Option // pre-computed collate options
}

// CollationEngine provides locale-aware string comparison and sort key generation.
// It maps MySQL collation names to golang.org/x/text/collate configurations.
// Collator instances are created per-call because they are NOT goroutine-safe.
type CollationEngine struct {
	registry map[string]*CollationInfo
	aliases  map[string]string // alias -> canonical name
}

// Global singleton
var (
	globalEngine *CollationEngine
	engineOnce   sync.Once
)

// GetGlobalCollationEngine returns the global CollationEngine singleton.
func GetGlobalCollationEngine() *CollationEngine {
	engineOnce.Do(func() {
		globalEngine = NewCollationEngine()
	})
	return globalEngine
}

// NewCollationEngine creates a new CollationEngine with the supported collation registry.
func NewCollationEngine() *CollationEngine {
	e := &CollationEngine{
		registry: make(map[string]*CollationInfo),
		aliases:  make(map[string]string),
	}
	e.initRegistry()
	return e
}

// initRegistry populates the collation registry with supported MySQL collations.
func (e *CollationEngine) initRegistry() {
	// Binary collations
	e.registerCollation(&CollationInfo{
		Name: "utf8mb4_bin", ID: CharsetUtf8mb4Bin,
		Charset: "utf8mb4", IsBinary: true,
	})
	e.registerCollation(&CollationInfo{
		Name: "utf8_bin", ID: 83,
		Charset: "utf8", IsBinary: true,
	})
	e.registerCollation(&CollationInfo{
		Name: "binary", ID: 63,
		Charset: "binary", IsBinary: true,
	})

	// General CI collations (CLDR root, case-insensitive)
	e.registerCollation(&CollationInfo{
		Name: "utf8mb4_general_ci", ID: CharsetUtf8mb4GeneralCI,
		Charset: "utf8mb4", Tag: language.Und, CaseInsensitive: true,
		options: []collate.Option{collate.IgnoreCase},
	})
	e.registerCollation(&CollationInfo{
		Name: "utf8_general_ci", ID: CharsetUtf8GeneralCI,
		Charset: "utf8", Tag: language.Und, CaseInsensitive: true,
		options: []collate.Option{collate.IgnoreCase},
	})

	// Unicode CI collations (CLDR root, case-insensitive)
	e.registerCollation(&CollationInfo{
		Name: "utf8mb4_unicode_ci", ID: CharsetUtf8mb4UnicodeCI,
		Charset: "utf8mb4", Tag: language.Und, CaseInsensitive: true,
		options: []collate.Option{collate.IgnoreCase},
	})
	e.registerCollation(&CollationInfo{
		Name: "utf8mb4_unicode_520_ci", ID: CharsetUtf8mb4Unicode520CI,
		Charset: "utf8mb4", Tag: language.Und, CaseInsensitive: true,
		options: []collate.Option{collate.IgnoreCase},
	})

	// MySQL 8.0 default: accent-insensitive + case-insensitive
	e.registerCollation(&CollationInfo{
		Name: "utf8mb4_0900_ai_ci", ID: CharsetUtf8mb40900AICi,
		Charset: "utf8mb4", Tag: language.Und,
		CaseInsensitive: true, AccentInsensitive: true,
		options: []collate.Option{collate.IgnoreCase, collate.Loose},
	})

	// Locale-specific CI collations
	type localeCI struct {
		name    string
		id      uint8
		langTag string
	}

	localeCIs := []localeCI{
		{"utf8mb4_turkish_ci", CharsetUtf8mb4TurkishCI, "tr"},
		{"utf8mb4_german2_ci", CharsetUtf8mb4German2CI, "de-u-co-phonebk"},
		{"utf8mb4_spanish_ci", CharsetUtf8mb4SpanishCI, "es"},
		{"utf8mb4_swedish_ci", CharsetUtf8mb4SwedishCI, "sv"},
		{"utf8mb4_danish_ci", CharsetUtf8mb4DanishCI, "da"},
		{"utf8mb4_polish_ci", CharsetUtf8mb4PolishCI, "pl"},
		{"utf8mb4_czech_ci", CharsetUtf8mb4CzechCI, "cs"},
		{"utf8mb4_icelandic_ci", CharsetUtf8mb4IcelandicCI, "is"},
		{"utf8mb4_romanian_ci", CharsetUtf8mb4RomanianCI, "ro"},
		{"utf8mb4_hungarian_ci", CharsetUtf8mb4HungarianCI, "hu"},
		{"utf8mb4_croatian_ci", CharsetUtf8mb4CroatianCI, "hr"},
		{"utf8mb4_slovenian_ci", CharsetUtf8mb4SlovenianCI, "sl"},
		{"utf8mb4_estonian_ci", CharsetUtf8mb4EstonianCI, "et"},
		{"utf8mb4_latvian_ci", CharsetUtf8mb4LatvianCI, "lv"},
		{"utf8mb4_lithuanian_ci", CharsetUtf8mb4LithuanianCI, "lt"},
		{"utf8mb4_persian_ci", CharsetUtf8mb4PersianCI, "fa"},
		{"utf8mb4_vietnamese_ci", CharsetUtf8mb4VietnameseCI, "vi"},
		{"utf8mb4_slovak_ci", CharsetUtf8mb4SlovakCI, "sk"},
		{"utf8mb4_sinhala_ci", CharsetUtf8mb4SinhalaCI, "si"},
		{"utf8mb4_spanish2_ci", CharsetUtf8mb4Spanish2CI, "es-u-co-trad"},
		{"utf8mb4_roman_ci", CharsetUtf8mb4RomanCI, "la"},
		{"utf8mb4_esperanto_ci", CharsetUtf8mb4EsperantoCI, "eo"},
	}

	for _, lc := range localeCIs {
		e.registerCollation(&CollationInfo{
			Name:            lc.name,
			ID:              lc.id,
			Charset:         "utf8mb4",
			Tag:             language.MustParse(lc.langTag),
			CaseInsensitive: true,
			options:         []collate.Option{collate.IgnoreCase},
		})
	}

	// Common aliases
	e.aliases["utf8mb4"] = "utf8mb4_general_ci"
	e.aliases["utf8"] = "utf8_general_ci"
	e.aliases["default"] = "utf8mb4_0900_ai_ci"
}

// registerCollation adds a collation to the registry.
func (e *CollationEngine) registerCollation(info *CollationInfo) {
	e.registry[info.Name] = info
}

// ResolveCollation normalizes a collation name, resolving aliases and case differences.
// Returns the canonical collation name.
func (e *CollationEngine) ResolveCollation(name string) string {
	lower := strings.ToLower(strings.TrimSpace(name))
	if lower == "" {
		return "utf8mb4_bin"
	}

	// Check aliases first
	if canonical, ok := e.aliases[lower]; ok {
		return canonical
	}

	// Check direct match
	if _, ok := e.registry[lower]; ok {
		return lower
	}

	// Unknown collation: fall back to binary
	return "utf8mb4_bin"
}

// GetCollationInfo returns metadata for a collation, or (nil, false) if unknown.
func (e *CollationEngine) GetCollationInfo(name string) (*CollationInfo, bool) {
	resolved := e.ResolveCollation(name)
	info, ok := e.registry[resolved]
	return info, ok
}

// ListCollations returns all registered collations.
func (e *CollationEngine) ListCollations() []*CollationInfo {
	result := make([]*CollationInfo, 0, len(e.registry))
	for _, info := range e.registry {
		result = append(result, info)
	}
	return result
}

// newCollator creates a new collator for the given collation info.
// Collators are NOT goroutine-safe and must not be shared.
func (e *CollationEngine) newCollator(info *CollationInfo) *collate.Collator {
	if info.IsBinary {
		return nil
	}
	return collate.New(info.Tag, info.options...)
}

// Compare compares two strings using the specified collation.
// Returns -1, 0, or 1.
func (e *CollationEngine) Compare(a, b string, collationName string) (int, error) {
	resolved := e.ResolveCollation(collationName)
	info := e.registry[resolved]

	if info == nil || info.IsBinary {
		return binaryCompare(a, b), nil
	}

	c := e.newCollator(info)
	return c.CompareString(a, b), nil
}

// SortKey generates a binary sort key for the given string and collation.
// Sort keys can be compared with bytes.Compare for correct collation ordering.
func (e *CollationEngine) SortKey(s string, collationName string) ([]byte, error) {
	resolved := e.ResolveCollation(collationName)
	info := e.registry[resolved]

	if info == nil || info.IsBinary {
		return []byte(s), nil
	}

	c := e.newCollator(info)
	buf := &collate.Buffer{}
	return c.KeyFromString(buf, s), nil
}

// IsCaseInsensitive returns true if the named collation is case-insensitive.
func (e *CollationEngine) IsCaseInsensitive(collationName string) bool {
	info, ok := e.GetCollationInfo(collationName)
	if !ok {
		return false
	}
	return info.CaseInsensitive
}

// IsAccentInsensitive returns true if the named collation is accent-insensitive.
func (e *CollationEngine) IsAccentInsensitive(collationName string) bool {
	info, ok := e.GetCollationInfo(collationName)
	if !ok {
		return false
	}
	return info.AccentInsensitive
}

// binaryCompare performs byte-level string comparison.
func binaryCompare(a, b string) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}
