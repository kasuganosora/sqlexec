package builtin

import (
	"encoding/hex"
	"fmt"
	"strings"
	"unicode"

	"github.com/kasuganosora/sqlexec/pkg/utils"
	"golang.org/x/text/unicode/norm"
)

func init() {
	icuFunctions := []*FunctionInfo{
		{
			Name: "icu_sort_key",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "icu_sort_key", ReturnType: "string", ParamTypes: []string{"string", "string"}, Variadic: false},
			},
			Handler:     icuSortKey,
			Description: "Generate a hexadecimal sort key for a string using the specified collation",
			Example:     "ICU_SORT_KEY('hello', 'utf8mb4_unicode_ci')",
			Category:    "icu",
		},
		{
			Name: "collation",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "collation", ReturnType: "string", ParamTypes: []string{}, Variadic: false},
			},
			Handler:     icuCollation,
			Description: "Return the default collation name",
			Example:     "COLLATION()",
			Category:    "icu",
		},
		{
			Name: "unicode_normalize",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "unicode_normalize", ReturnType: "string", ParamTypes: []string{"string"}, Variadic: true},
			},
			Handler:     icuUnicodeNormalize,
			Description: "Normalize a string using the specified Unicode normalization form (NFC, NFD, NFKC, NFKD). Default is NFC.",
			Example:     "UNICODE_NORMALIZE('café', 'NFC')",
			Category:    "icu",
		},
		{
			Name: "strip_accents",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "strip_accents", ReturnType: "string", ParamTypes: []string{"string"}, Variadic: false},
			},
			Handler:     icuStripAccents,
			Description: "Remove accent/diacritical marks from a string",
			Example:     "STRIP_ACCENTS('café')",
			Category:    "icu",
		},
		{
			Name: "icu_compare",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "icu_compare", ReturnType: "integer", ParamTypes: []string{"string", "string"}, Variadic: true},
			},
			Handler:     icuCompare,
			Description: "Compare two strings using ICU collation. Returns -1, 0, or 1. Default collation is utf8mb4_unicode_ci.",
			Example:     "ICU_COMPARE('a', 'b', 'utf8mb4_unicode_ci')",
			Category:    "icu",
		},
		{
			Name: "transliterate",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "transliterate", ReturnType: "string", ParamTypes: []string{"string", "string"}, Variadic: false},
			},
			Handler:     icuTransliterate,
			Description: "Transliterate a string using the specified rule (Latin-ASCII, Upper, Lower)",
			Example:     "TRANSLITERATE('café', 'Latin-ASCII')",
			Category:    "icu",
		},
		{
			Name: "nfc",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "nfc", ReturnType: "string", ParamTypes: []string{"string"}, Variadic: false},
			},
			Handler:     icuNFC,
			Description: "Normalize a string to NFC form",
			Example:     "NFC('café')",
			Category:    "icu",
		},
		{
			Name: "nfd",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "nfd", ReturnType: "string", ParamTypes: []string{"string"}, Variadic: false},
			},
			Handler:     icuNFD,
			Description: "Normalize a string to NFD form",
			Example:     "NFD('café')",
			Category:    "icu",
		},
		{
			Name: "nfkc",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "nfkc", ReturnType: "string", ParamTypes: []string{"string"}, Variadic: false},
			},
			Handler:     icuNFKC,
			Description: "Normalize a string to NFKC form",
			Example:     "NFKC('ﬁ')",
			Category:    "icu",
		},
		{
			Name: "nfkd",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "nfkd", ReturnType: "string", ParamTypes: []string{"string"}, Variadic: false},
			},
			Handler:     icuNFKD,
			Description: "Normalize a string to NFKD form",
			Example:     "NFKD('ﬁ')",
			Category:    "icu",
		},
	}

	for _, fn := range icuFunctions {
		RegisterGlobal(fn)
	}
}

// icuSortKey generates a hex-encoded sort key for the given string and collation.
func icuSortKey(args []interface{}) (interface{}, error) {
	if len(args) < 2 {
		return nil, fmt.Errorf("icu_sort_key() requires exactly 2 arguments")
	}
	if args[0] == nil {
		return nil, nil
	}
	if args[1] == nil {
		return nil, fmt.Errorf("icu_sort_key(): collation must not be nil")
	}

	s := toString(args[0])
	collation := toString(args[1])

	engine := utils.GetGlobalCollationEngine()
	key, err := engine.SortKey(s, collation)
	if err != nil {
		return nil, fmt.Errorf("icu_sort_key(): %v", err)
	}
	return hex.EncodeToString(key), nil
}

// icuCollation returns the default collation name.
func icuCollation(args []interface{}) (interface{}, error) {
	return "utf8mb4_unicode_ci", nil
}

// icuUnicodeNormalize normalizes a string using the specified form.
func icuUnicodeNormalize(args []interface{}) (interface{}, error) {
	if len(args) < 1 {
		return nil, fmt.Errorf("unicode_normalize() requires at least 1 argument")
	}
	if args[0] == nil {
		return nil, nil
	}

	s := toString(args[0])
	formName := "NFC"
	if len(args) >= 2 && args[1] != nil {
		formName = strings.ToUpper(toString(args[1]))
	}

	var f norm.Form
	switch formName {
	case "NFC":
		f = norm.NFC
	case "NFD":
		f = norm.NFD
	case "NFKC":
		f = norm.NFKC
	case "NFKD":
		f = norm.NFKD
	default:
		return nil, fmt.Errorf("unicode_normalize(): unsupported form %q, use NFC, NFD, NFKC, or NFKD", formName)
	}

	return f.String(s), nil
}

// stripAccentsString removes accent/diacritical marks from a string.
// It decomposes to NFD, removes combining marks (unicode.Mn), then recomposes to NFC.
func stripAccentsString(s string) string {
	decomposed := norm.NFD.String(s)

	var b strings.Builder
	b.Grow(len(decomposed))
	for _, r := range decomposed {
		if !unicode.Is(unicode.Mn, r) {
			b.WriteRune(r)
		}
	}

	return norm.NFC.String(b.String())
}

// icuStripAccents removes accent marks from a string.
func icuStripAccents(args []interface{}) (interface{}, error) {
	if len(args) < 1 {
		return nil, fmt.Errorf("strip_accents() requires exactly 1 argument")
	}
	if args[0] == nil {
		return nil, nil
	}

	s := toString(args[0])
	return stripAccentsString(s), nil
}

// icuCompare compares two strings using the specified collation.
func icuCompare(args []interface{}) (interface{}, error) {
	if len(args) < 2 {
		return nil, fmt.Errorf("icu_compare() requires at least 2 arguments")
	}
	if args[0] == nil || args[1] == nil {
		return nil, nil
	}

	a := toString(args[0])
	b := toString(args[1])

	collation := "utf8mb4_unicode_ci"
	if len(args) >= 3 && args[2] != nil {
		collation = toString(args[2])
	}

	engine := utils.GetGlobalCollationEngine()
	result, err := engine.Compare(a, b, collation)
	if err != nil {
		return nil, fmt.Errorf("icu_compare(): %v", err)
	}
	return int64(result), nil
}

// icuTransliterate applies a transliteration rule to a string.
func icuTransliterate(args []interface{}) (interface{}, error) {
	if len(args) < 2 {
		return nil, fmt.Errorf("transliterate() requires exactly 2 arguments")
	}
	if args[0] == nil {
		return nil, nil
	}
	if args[1] == nil {
		return nil, fmt.Errorf("transliterate(): rule must not be nil")
	}

	s := toString(args[0])
	rule := toString(args[1])

	switch rule {
	case "Latin-ASCII":
		return stripAccentsString(s), nil
	case "Upper":
		return strings.ToUpper(s), nil
	case "Lower":
		return strings.ToLower(s), nil
	default:
		return nil, fmt.Errorf("transliterate(): unsupported rule %q, use 'Latin-ASCII', 'Upper', or 'Lower'", rule)
	}
}

// icuNFC normalizes a string to NFC form.
func icuNFC(args []interface{}) (interface{}, error) {
	if len(args) < 1 {
		return nil, fmt.Errorf("nfc() requires exactly 1 argument")
	}
	if args[0] == nil {
		return nil, nil
	}
	return norm.NFC.String(toString(args[0])), nil
}

// icuNFD normalizes a string to NFD form.
func icuNFD(args []interface{}) (interface{}, error) {
	if len(args) < 1 {
		return nil, fmt.Errorf("nfd() requires exactly 1 argument")
	}
	if args[0] == nil {
		return nil, nil
	}
	return norm.NFD.String(toString(args[0])), nil
}

// icuNFKC normalizes a string to NFKC form.
func icuNFKC(args []interface{}) (interface{}, error) {
	if len(args) < 1 {
		return nil, fmt.Errorf("nfkc() requires exactly 1 argument")
	}
	if args[0] == nil {
		return nil, nil
	}
	return norm.NFKC.String(toString(args[0])), nil
}

// icuNFKD normalizes a string to NFKD form.
func icuNFKD(args []interface{}) (interface{}, error) {
	if len(args) < 1 {
		return nil, fmt.Errorf("nfkd() requires exactly 1 argument")
	}
	if args[0] == nil {
		return nil, nil
	}
	return norm.NFKD.String(toString(args[0])), nil
}
