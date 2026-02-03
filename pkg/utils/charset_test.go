package utils

import (
	"fmt"
	"testing"
)

func TestGetCharsetName(t *testing.T) {
	tests := []struct {
		name       string
		charsetID  uint8
		expected   string
	}{
		// 常用字符集
		{"UTF8", CharsetUtf8, "utf8_general_ci"},
		{"UTF8MB4", CharsetUtf8mb4, "utf8mb4_general_ci"},
		{"UTF8MB4_DEFAULT", CharsetUtf8mb40900AICi, "utf8mb4_0900_ai_ci"},
		{"Latin1", CharsetLatin1, "latin1_swedish_ci"},
		{"ASCII", CharsetAscii, "ascii_general_ci"},
		{"GBK", CharsetGBK, "gbk_chinese_ci"},
		{"GB2312", CharsetGb2312, "gb2312_chinese_ci"},
		{"BIG5", CharsetBIG5, "big5_chinese_ci"},

		// 日文字符集
		{"SJIS", CharsetSjis, "sjis_japanese_ci"},
		{"UJIS", CharsetUjis, "ujis_japanese_ci"},

		// 韩文字符集
		{"EUCKR", CharsetEuckr, "euckr_korean_ci"},

		// 各种latin字符集
		{"Latin2", CharsetLatin2GeneralCI, "latin2_general_ci"},
		{"Latin5", CharsetLatin5TurkishCI, "latin5_turkish_ci"},
		{"Latin7", CharsetLatin7GeneralCI, "latin7_general_ci"},

		// 各种CP字符集
		{"CP850", CharsetCp850GeneralCI, "cp850_general_ci"},
		{"CP852", CharsetCp852GeneralCI, "cp852_general_ci"},
		{"CP866", CharsetCp866GeneralCI, "cp866_general_ci"},
		{"CP1250", CharsetCp1250GeneralCI, "cp1250_general_ci"},
		{"CP1251", CharsetCp1251BulgarianCI, "cp1251_bulgarian_ci"},
		{"CP1256", CharsetCp1256GeneralCI, "cp1256_general_ci"},
		{"CP1257", CharsetCp1257LithuanianCI, "cp1257_lithuanian_ci"},

		// KOI8字符集
		{"KOI8R", CharsetKoi8rGeneralCI, "koi8r_general_ci"},
		{"KOI8U", CharsetKoi8uGeneralCI, "koi8u_general_ci"},

		// Mac字符集
		{"MacCE", CharsetMacceGeneralCI, "macce_general_ci"},
		{"MacRoman", CharsetMacromanGeneralCI, "macroman_general_ci"},

		// UTF8MB4变体
		{"UTF8MB4_Bin", CharsetUtf8mb4Bin, "utf8mb4_bin"},
		{"UTF8MB4_Unicode", CharsetUtf8mb4UnicodeCI, "utf8mb4_unicode_ci"},
		{"UTF8MB4_520", CharsetUtf8mb4Unicode520CI, "utf8mb4_unicode_520_ci"},

		// UTF8MB4地区变体
		{"UTF8MB4_Icelandic", CharsetUtf8mb4IcelandicCI, "utf8mb4_icelandic_ci"},
		{"UTF8MB4_Latvian", CharsetUtf8mb4LatvianCI, "utf8mb4_latvian_ci"},
		{"UTF8MB4_Romanian", CharsetUtf8mb4RomanianCI, "utf8mb4_romanian_ci"},
		{"UTF8MB4_Slovenian", CharsetUtf8mb4SlovenianCI, "utf8mb4_slovenian_ci"},
		{"UTF8MB4_Polish", CharsetUtf8mb4PolishCI, "utf8mb4_polish_ci"},
		{"UTF8MB4_Estonian", CharsetUtf8mb4EstonianCI, "utf8mb4_estonian_ci"},
		{"UTF8MB4_Spanish", CharsetUtf8mb4SpanishCI, "utf8mb4_spanish_ci"},
		{"UTF8MB4_Swedish", CharsetUtf8mb4SwedishCI, "utf8mb4_swedish_ci"},
		{"UTF8MB4_Turkish", CharsetUtf8mb4TurkishCI, "utf8mb4_turkish_ci"},
		{"UTF8MB4_Czech", CharsetUtf8mb4CzechCI, "utf8mb4_czech_ci"},
		{"UTF8MB4_Danish", CharsetUtf8mb4DanishCI, "utf8mb4_danish_ci"},
		{"UTF8MB4_Lithuanian", CharsetUtf8mb4LithuanianCI, "utf8mb4_lithuanian_ci"},
		{"UTF8MB4_Slovak", CharsetUtf8mb4SlovakCI, "utf8mb4_slovak_ci"},
		{"UTF8MB4_Spanish2", CharsetUtf8mb4Spanish2CI, "utf8mb4_spanish2_ci"},
		{"UTF8MB4_Roman", CharsetUtf8mb4RomanCI, "utf8mb4_roman_ci"},
		{"UTF8MB4_Persian", CharsetUtf8mb4PersianCI, "utf8mb4_persian_ci"},
		{"UTF8MB4_Esperanto", CharsetUtf8mb4EsperantoCI, "utf8mb4_esperanto_ci"},
		{"UTF8MB4_Hungarian", CharsetUtf8mb4HungarianCI, "utf8mb4_hungarian_ci"},
		{"UTF8MB4_Sinhala", CharsetUtf8mb4SinhalaCI, "utf8mb4_sinhala_ci"},
		{"UTF8MB4_German2", CharsetUtf8mb4German2CI, "utf8mb4_german2_ci"},
		{"UTF8MB4_Croatian", CharsetUtf8mb4CroatianCI, "utf8mb4_croatian_ci"},
		{"UTF8MB4_Vietnamese", CharsetUtf8mb4VietnameseCI, "utf8mb4_vietnamese_ci"},

		// 其他字符集
		{"Dec8", CharsetDec8SwedishCI, "dec8_swedish_ci"},
		{"Hp8", CharsetHp8EnglishCI, "hp8_english_ci"},
		{"Swe7", CharsetSwe7SwedishCI, "swe7_swedish_ci"},
		{"Hebrew", CharsetHebrewGeneralCI, "hebrew_general_ci"},
		{"Tis620", CharsetTis620ThaiCI, "tis620_thai_ci"},
		{"Greek", CharsetGreekGeneralCI, "greek_general_ci"},
		{"Keybcs2", CharsetKeybcs2GeneralCI, "keybcs2_general_ci"},

		// 边界情况
		{"未知的ID", 0, "unknown"},
		{"未知的ID2", 100, "unknown"},
		{"未知的ID3", 255, "unknown"},
		{"最大uint8", 255, "utf8mb4_0900_ai_ci"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GetCharsetName(tt.charsetID)
			if result != tt.expected {
				t.Errorf("GetCharsetName(%d) = %q, want %q", tt.charsetID, result, tt.expected)
			}
		})
	}
}

func TestGetCharsetID(t *testing.T) {
	tests := []struct {
		name       string
		charsetName string
		expected   uint8
	}{
		// 常用字符集
		{"UTF8", "utf8_general_ci", CharsetUtf8},
		{"UTF8", "utf8", CharsetUtf8},
		{"UTF8MB4", "utf8mb4_general_ci", CharsetUtf8mb4},
		{"UTF8MB4", "utf8mb4", CharsetUtf8mb4},
		{"UTF8MB4_DEFAULT", "utf8mb4_0900_ai_ci", CharsetUtf8mb40900AICi},
		{"Latin1", "latin1_swedish_ci", CharsetLatin1},
		{"ASCII", "ascii_general_ci", CharsetAscii},
		{"GBK", "gbk_chinese_ci", CharsetGBK},
		{"GB2312", "gb2312_chinese_ci", CharsetGb2312},
		{"BIG5", "big5_chinese_ci", CharsetBig5ChineseCI},

		// 日文字符集
		{"SJIS", "sjis_japanese_ci", CharsetSjisJapaneseCI},
		{"UJIS", "ujis_japanese_ci", CharsetUjisJapaneseCI},

		// 韩文字符集
		{"EUCKR", "euckr_korean_ci", CharsetEuckrKoreanCI},

		// 各种latin字符集
		{"Latin2", "latin2_general_ci", CharsetLatin2GeneralCI},
		{"Latin5", "latin5_turkish_ci", CharsetLatin5TurkishCI},
		{"Latin7", "latin7_general_ci", CharsetLatin7GeneralCI},

		// 各种CP字符集
		{"CP850", "cp850_general_ci", CharsetCp850GeneralCI},
		{"CP852", "cp852_general_ci", CharsetCp852GeneralCI},
		{"CP866", "cp866_general_ci", CharsetCp866GeneralCI},
		{"CP1250", "cp1250_general_ci", CharsetCp1250GeneralCI},
		{"CP1251", "cp1251_bulgarian_ci", CharsetCp1251BulgarianCI},
		{"CP1256", "cp1256_general_ci", CharsetCp1256GeneralCI},
		{"CP1257", "cp1257_lithuanian_ci", CharsetCp1257LithuanianCI},

		// KOI8字符集
		{"KOI8R", "koi8r_general_ci", CharsetKoi8rGeneralCI},
		{"KOI8U", "koi8u_general_ci", CharsetKoi8uGeneralCI},

		// Mac字符集
		{"MacCE", "macce_general_ci", CharsetMacceGeneralCI},
		{"MacRoman", "macroman_general_ci", CharsetMacromanGeneralCI},

		// UTF8MB4变体
		{"UTF8MB4_Bin", "utf8mb4_bin", CharsetUtf8mb4Bin},
		{"UTF8MB4_Unicode", "utf8mb4_unicode_ci", CharsetUtf8mb4UnicodeCI},
		{"UTF8MB4_520", "utf8mb4_unicode_520_ci", CharsetUtf8mb4Unicode520CI},

		// UTF8MB4地区变体
		{"UTF8MB4_Icelandic", "utf8mb4_icelandic_ci", CharsetUtf8mb4IcelandicCI},
		{"UTF8MB4_Latvian", "utf8mb4_latvian_ci", CharsetUtf8mb4LatvianCI},
		{"UTF8MB4_Romanian", "utf8mb4_romanian_ci", CharsetUtf8mb4RomanianCI},
		{"UTF8MB4_Slovenian", "utf8mb4_slovenian_ci", CharsetUtf8mb4SlovenianCI},
		{"UTF8MB4_Polish", "utf8mb4_polish_ci", CharsetUtf8mb4PolishCI},
		{"UTF8MB4_Estonian", "utf8mb4_estonian_ci", CharsetUtf8mb4EstonianCI},
		{"UTF8MB4_Spanish", "utf8mb4_spanish_ci", CharsetUtf8mb4SpanishCI},
		{"UTF8MB4_Swedish", "utf8mb4_swedish_ci", CharsetUtf8mb4SwedishCI},
		{"UTF8MB4_Turkish", "utf8mb4_turkish_ci", CharsetUtf8mb4TurkishCI},
		{"UTF8MB4_Czech", "utf8mb4_czech_ci", CharsetUtf8mb4CzechCI},
		{"UTF8MB4_Danish", "utf8mb4_danish_ci", CharsetUtf8mb4DanishCI},
		{"UTF8MB4_Lithuanian", "utf8mb4_lithuanian_ci", CharsetUtf8mb4LithuanianCI},
		{"UTF8MB4_Slovak", "utf8mb4_slovak_ci", CharsetUtf8mb4SlovakCI},
		{"UTF8MB4_Spanish2", "utf8mb4_spanish2_ci", CharsetUtf8mb4Spanish2CI},
		{"UTF8MB4_Roman", "utf8mb4_roman_ci", CharsetUtf8mb4RomanCI},
		{"UTF8MB4_Persian", "utf8mb4_persian_ci", CharsetUtf8mb4PersianCI},
		{"UTF8MB4_Esperanto", "utf8mb4_esperanto_ci", CharsetUtf8mb4EsperantoCI},
		{"UTF8MB4_Hungarian", "utf8mb4_hungarian_ci", CharsetUtf8mb4HungarianCI},
		{"UTF8MB4_Sinhala", "utf8mb4_sinhala_ci", CharsetUtf8mb4SinhalaCI},
		{"UTF8MB4_German2", "utf8mb4_german2_ci", CharsetUtf8mb4German2CI},
		{"UTF8MB4_Croatian", "utf8mb4_croatian_ci", CharsetUtf8mb4CroatianCI},
		{"UTF8MB4_Vietnamese", "utf8mb4_vietnamese_ci", CharsetUtf8mb4VietnameseCI},

		// 其他字符集
		{"Dec8", "dec8_swedish_ci", CharsetDec8SwedishCI},
		{"Hp8", "hp8_english_ci", CharsetHp8EnglishCI},
		{"Swe7", "swe7_swedish_ci", CharsetSwe7SwedishCI},
		{"Hebrew", "hebrew_general_ci", CharsetHebrewGeneralCI},
		{"Tis620", "tis620_thai_ci", CharsetTis620ThaiCI},
		{"Greek", "greek_general_ci", CharsetGreekGeneralCI},
		{"Keybcs2", "keybcs2_general_ci", CharsetKeybcs2GeneralCI},

		// 边界情况
		{"空字符串", "", CharsetUtf8mb40900AICi},
		{"未知的字符集", "unknown_charset", CharsetUtf8mb40900AICi},
		{"大小写敏感", "UTF8_GENERAL_CI", CharsetUtf8mb40900AICi},
		{"部分匹配", "utf8", CharsetUtf8},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GetCharsetID(tt.charsetName)
			if result != tt.expected {
				t.Errorf("GetCharsetID(%q) = %d, want %d", tt.charsetName, result, tt.expected)
			}
		})
	}
}

func TestCharsetRoundTrip(t *testing.T) {
	// 测试双向转换
	charsetIDs := []uint8{
		CharsetUtf8,
		CharsetUtf8mb4,
		CharsetUtf8mb40900AICi,
		CharsetLatin1,
		CharsetAscii,
		CharsetGBK,
		CharsetGb2312,
		CharsetBIG5,
		CharsetSjis,
		CharsetUjis,
		CharsetEuckr,
		CharsetUtf8mb4Bin,
		CharsetUtf8mb4UnicodeCI,
		CharsetUtf8mb4VietnameseCI,
	}

	for _, id := range charsetIDs {
		t.Run(fmt.Sprintf("ID_%d", id), func(t *testing.T) {
			name := GetCharsetName(id)
			if name == "unknown" {
				t.Errorf("GetCharsetName(%d) returned unknown", id)
				return
			}
			resultID := GetCharsetID(name)
			if resultID != id {
				t.Errorf("Round trip failed: %d -> %q -> %d", id, name, resultID)
			}
		})
	}
}

func TestCharsetConstants(t *testing.T) {
	// 测试常量值
	tests := []struct {
		name     string
		actual   uint8
		expected uint8
	}{
		{"CharsetUtf8", CharsetUtf8, 33},
		{"CharsetUtf8mb4", CharsetUtf8mb4, 45},
		{"CharsetUtf8mb40900AICi", CharsetUtf8mb40900AICi, 255},
		{"CharsetLatin1", CharsetLatin1, 8},
		{"CharsetAscii", CharsetAscii, 11},
		{"CharsetGBK", CharsetGBK, 28},
		{"CharsetGb2312", CharsetGb2312, 24},
		{"CharsetBIG5", CharsetBIG5, 1},
		{"CharsetDefault", CharsetDefault, 255},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.actual != tt.expected {
				t.Errorf("%s = %d, want %d", tt.name, tt.actual, tt.expected)
			}
		})
	}
}

func TestCharsetIDBoundary(t *testing.T) {
	// 测试ID边界值
	tests := []struct {
		name      string
		charsetID uint8
		expected  string
	}{
		{"最小有效ID", 1, "big5_chinese_ci"},
		{"最大有效ID", 255, "utf8mb4_0900_ai_ci"},
		{"无效ID0", 0, "unknown"},
		{"无效ID128", 128, "unknown"},
		{"无效ID200", 200, "unknown"},
		{"无效ID254", 254, "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GetCharsetName(tt.charsetID)
			if result != tt.expected {
				t.Errorf("GetCharsetName(%d) = %q, want %q", tt.charsetID, result, tt.expected)
			}
		})
	}
}

func TestCharsetIDAllValues(t *testing.T) {
	// 测试所有已定义的字符集ID
	knownIDs := []uint8{
		1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 18, 19, 20, 21,
		22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 45, 46, 47, 57, 58, 59, 60,
		61, 62, 63, 64, 65, 66, 224, 225, 226, 227, 228, 229, 230, 231, 232, 233, 234,
		235, 236, 237, 238, 239, 240, 241, 242, 243, 244, 245, 246, 247, 255,
	}

	for _, id := range knownIDs {
		t.Run(fmt.Sprintf("ID_%d", id), func(t *testing.T) {
			name := GetCharsetName(id)
			if name == "unknown" {
				t.Errorf("GetCharsetName(%d) returned unknown for known ID", id)
			}
		})
	}
}

func TestCharsetNameAllValues(t *testing.T) {
	// 测试所有已知的字符集名称
	knownNames := []string{
		"big5_chinese_ci", "latin2_czech_cs", "dec8_swedish_ci", "cp850_general_ci",
		"latin1_german1_ci", "hp8_english_ci", "koi8r_general_ci", "latin1_swedish_ci",
		"latin2_general_ci", "swe7_swedish_ci", "ascii_general_ci", "ujis_japanese_ci",
		"sjis_japanese_ci", "cp1251_bulgarian_ci", "latin1_danish_ci", "hebrew_general_ci",
		"tis620_thai_ci", "euckr_korean_ci", "latin7_estonian_cs", "latin2_hungarian_ci",
		"koi8u_general_ci", "cp1251_ukrainian_ci", "gb2312_chinese_ci", "greek_general_ci",
		"cp1250_general_ci", "latin2_croatian_ci", "gbk_chinese_ci", "cp1257_lithuanian_ci",
		"latin5_turkish_ci", "latin1_german2_ci", "armscii8_general_ci", "utf8_general_ci",
		"utf8mb4_general_ci", "utf8mb4_bin", "latin1_spanish_ci", "cp1256_general_ci",
		"cp866_general_ci", "keybcs2_general_ci", "macce_general_ci", "macroman_general_ci",
		"cp852_general_ci", "latin7_general_ci", "latin7_general_cs", "macce_bin",
		"cp1250_croatian_ci", "utf8mb4_unicode_ci", "utf8mb4_icelandic_ci", "utf8mb4_latvian_ci",
		"utf8mb4_romanian_ci", "utf8mb4_slovenian_ci", "utf8mb4_polish_ci", "utf8mb4_estonian_ci",
		"utf8mb4_spanish_ci", "utf8mb4_swedish_ci", "utf8mb4_turkish_ci", "utf8mb4_czech_ci",
		"utf8mb4_danish_ci", "utf8mb4_lithuanian_ci", "utf8mb4_slovak_ci", "utf8mb4_spanish2_ci",
		"utf8mb4_roman_ci", "utf8mb4_persian_ci", "utf8mb4_esperanto_ci", "utf8mb4_hungarian_ci",
		"utf8mb4_sinhala_ci", "utf8mb4_german2_ci", "utf8mb4_croatian_ci", "utf8mb4_unicode_520_ci",
		"utf8mb4_vietnamese_ci", "utf8mb4_0900_ai_ci",
	}

	for _, name := range knownNames {
		t.Run(name, func(t *testing.T) {
			id := GetCharsetID(name)
			resultName := GetCharsetName(id)
			if resultName != name {
				t.Errorf("GetCharsetID(%q) = %d, GetCharsetName(%d) = %q, want %q",
					name, id, id, resultName, name)
			}
		})
	}
}

func TestCharsetAliases(t *testing.T) {
	// 测试字符集别名
	tests := []struct {
		name     string
		alias    string
		expected uint8
	}{
		{"UTF8", "utf8", CharsetUtf8},
		{"UTF8MB4", "utf8mb4", CharsetUtf8mb4},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GetCharsetID(tt.alias)
			if result != tt.expected {
				t.Errorf("GetCharsetID(%q) = %d, want %d", tt.alias, result, tt.expected)
			}
		})
	}
}

func BenchmarkGetCharsetName(b *testing.B) {
	for i := 0; i < b.N; i++ {
		GetCharsetName(CharsetUtf8mb4)
	}
}

func BenchmarkGetCharsetID(b *testing.B) {
	for i := 0; i < b.N; i++ {
		GetCharsetID("utf8mb4_general_ci")
	}
}

func BenchmarkCharsetRoundTrip(b *testing.B) {
	for i := 0; i < b.N; i++ {
		name := GetCharsetName(CharsetUtf8mb4)
		GetCharsetID(name)
	}
}

func ExampleGetCharsetName() {
	name := GetCharsetName(CharsetUtf8mb4)
	fmt.Println(name)

	name2 := GetCharsetName(CharsetUtf8)
	fmt.Println(name2)

	name3 := GetCharsetName(0)
	fmt.Println(name3)

	// Output:
	// utf8mb4_general_ci
	// utf8_general_ci
	// unknown
}

func ExampleGetCharsetID() {
	id := GetCharsetID("utf8mb4_general_ci")
	fmt.Println(id)

	id2 := GetCharsetID("utf8")
	fmt.Println(id2)

	id3 := GetCharsetID("")
	fmt.Println(id3)

	// Output:
	// 45
	// 33
	// 255
}

