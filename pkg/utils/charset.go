package utils

// MySQL字符集常量定义
// 参考: https://dev.mysql.com/doc/refman/8.0/en/charset-charsets.html

const (
	// 常用字符集编号
	CharsetBig5ChineseCI        = 1   // big5_chinese_ci
	CharsetLatin2CzechCS        = 2   // latin2_czech_cs
	CharsetDec8SwedishCI        = 3   // dec8_swedish_ci
	CharsetCp850GeneralCI       = 4   // cp850_general_ci
	CharsetLatin1German1CI      = 5   // latin1_german1_ci
	CharsetHp8EnglishCI         = 6   // hp8_english_ci
	CharsetKoi8rGeneralCI       = 7   // koi8r_general_ci
	CharsetLatin1SwedishCI      = 8   // latin1_swedish_ci
	CharsetLatin2GeneralCI      = 9   // latin2_general_ci
	CharsetSwe7SwedishCI        = 10  // swe7_swedish_ci
	CharsetAsciiGeneralCI       = 11  // ascii_general_ci
	CharsetUjisJapaneseCI       = 12  // ujis_japanese_ci
	CharsetSjisJapaneseCI       = 13  // sjis_japanese_ci
	CharsetCp1251BulgarianCI    = 14  // cp1251_bulgarian_ci
	CharsetLatin1DanishCI       = 15  // latin1_danish_ci
	CharsetHebrewGeneralCI      = 16  // hebrew_general_ci
	CharsetTis620ThaiCI         = 18  // tis620_thai_ci
	CharsetEuckrKoreanCI        = 19  // euckr_korean_ci
	CharsetLatin7EstonianCS     = 20  // latin7_estonian_cs
	CharsetLatin2HungarianCI    = 21  // latin2_hungarian_ci
	CharsetKoi8uGeneralCI       = 22  // koi8u_general_ci
	CharsetCp1251UkrainianCI    = 23  // cp1251_ukrainian_ci
	CharsetGb2312ChineseCI      = 24  // gb2312_chinese_ci
	CharsetGreekGeneralCI       = 25  // greek_general_ci
	CharsetCp1250GeneralCI      = 26  // cp1250_general_ci
	CharsetLatin2CroatianCI     = 27  // latin2_croatian_ci
	CharsetGbkChineseCI         = 28  // gbk_chinese_ci
	CharsetCp1257LithuanianCI   = 29  // cp1257_lithuanian_ci
	CharsetLatin5TurkishCI      = 30  // latin5_turkish_ci
	CharsetLatin1German2CI      = 31  // latin1_german2_ci
	CharsetArmscii8GeneralCI    = 32  // armscii8_general_ci
	CharsetUtf8GeneralCI        = 33  // utf8_general_ci (utf8)
	CharsetUtf8mb4GeneralCI     = 45  // utf8mb4_general_ci (utf8mb4)
	CharsetUtf8mb4Bin            = 46  // utf8mb4_bin
	CharsetLatin1SpanishCI      = 47  // latin1_spanish_ci
	CharsetCp1256GeneralCI      = 57  // cp1256_general_ci
	CharsetCp866GeneralCI       = 58  // cp866_general_ci
	CharsetKeybcs2GeneralCI     = 59  // keybcs2_general_ci
	CharsetMacceGeneralCI       = 60  // macce_general_ci
	CharsetMacromanGeneralCI    = 61  // macroman_general_ci
	CharsetCp852GeneralCI       = 62  // cp852_general_ci
	CharsetLatin7GeneralCI      = 63  // latin7_general_ci
	CharsetLatin7GeneralCS      = 64  // latin7_general_cs
	CharsetMacceBin              = 65  // macce_bin
	CharsetCp1250CroatianCI     = 66  // cp1250_croatian_ci
	CharsetUtf8mb4UnicodeCI     = 224 // utf8mb4_unicode_ci
	CharsetUtf8mb4IcelandicCI   = 225 // utf8mb4_icelandic_ci
	CharsetUtf8mb4LatvianCI     = 226 // utf8mb4_latvian_ci
	CharsetUtf8mb4RomanianCI    = 227 // utf8mb4_romanian_ci
	CharsetUtf8mb4SlovenianCI   = 228 // utf8mb4_slovenian_ci
	CharsetUtf8mb4PolishCI      = 229 // utf8mb4_polish_ci
	CharsetUtf8mb4EstonianCI    = 230 // utf8mb4_estonian_ci
	CharsetUtf8mb4SpanishCI     = 231 // utf8mb4_spanish_ci
	CharsetUtf8mb4SwedishCI     = 232 // utf8mb4_swedish_ci
	CharsetUtf8mb4TurkishCI     = 233 // utf8mb4_turkish_ci
	CharsetUtf8mb4CzechCI       = 234 // utf8mb4_czech_ci
	CharsetUtf8mb4DanishCI      = 235 // utf8mb4_danish_ci
	CharsetUtf8mb4LithuanianCI  = 236 // utf8mb4_lithuanian_ci
	CharsetUtf8mb4SlovakCI      = 237 // utf8mb4_slovak_ci
	CharsetUtf8mb4Spanish2CI    = 238 // utf8mb4_spanish2_ci
	CharsetUtf8mb4RomanCI       = 239 // utf8mb4_roman_ci
	CharsetUtf8mb4PersianCI     = 240 // utf8mb4_persian_ci
	CharsetUtf8mb4EsperantoCI   = 241 // utf8mb4_esperanto_ci
	CharsetUtf8mb4HungarianCI   = 242 // utf8mb4_hungarian_ci
	CharsetUtf8mb4SinhalaCI     = 243 // utf8mb4_sinhala_ci
	CharsetUtf8mb4German2CI     = 244 // utf8mb4_german2_ci
	CharsetUtf8mb4CroatianCI    = 245 // utf8mb4_croatian_ci
	CharsetUtf8mb4Unicode520CI = 246 // utf8mb4_unicode_520_ci
	CharsetUtf8mb4VietnameseCI  = 247 // utf8mb4_vietnamese_ci
	CharsetUtf8mb40900AICi     = 255 // utf8mb4_0900_ai_ci (MySQL 8.0默认)
)

// 常用字符集别名
const (
	// UTF-8相关
	CharsetUtf8    = CharsetUtf8GeneralCI    // utf8 (3字节)
	CharsetUtf8mb4 = CharsetUtf8mb4GeneralCI // utf8mb4 (4字节，推荐)

	// 默认字符集
	CharsetDefault = CharsetUtf8mb40900AICi // MySQL 8.0默认字符集

	// 中文相关
	CharsetGBK    = CharsetGbkChineseCI    // gbk
	CharsetGb2312 = CharsetGb2312ChineseCI // gb2312
	CharsetBIG5   = CharsetBig5ChineseCI   // big5

	// 日文相关
	CharsetSjis = CharsetSjisJapaneseCI // sjis
	CharsetUjis = CharsetUjisJapaneseCI // ujis

	// 韩文相关
	CharsetEuckr = CharsetEuckrKoreanCI // euckr

	// 其他常用
	CharsetLatin1 = CharsetLatin1SwedishCI // latin1
	CharsetAscii  = CharsetAsciiGeneralCI  // ascii
)

// GetCharsetName 根据字符集编号获取字符集名称
func GetCharsetName(charsetID uint8) string {
	switch charsetID {
	case CharsetBig5ChineseCI:
		return "big5_chinese_ci"
	case CharsetLatin2CzechCS:
		return "latin2_czech_cs"
	case CharsetDec8SwedishCI:
		return "dec8_swedish_ci"
	case CharsetCp850GeneralCI:
		return "cp850_general_ci"
	case CharsetLatin1German1CI:
		return "latin1_german1_ci"
	case CharsetHp8EnglishCI:
		return "hp8_english_ci"
	case CharsetKoi8rGeneralCI:
		return "koi8r_general_ci"
	case CharsetLatin1SwedishCI:
		return "latin1_swedish_ci"
	case CharsetLatin2GeneralCI:
		return "latin2_general_ci"
	case CharsetSwe7SwedishCI:
		return "swe7_swedish_ci"
	case CharsetAsciiGeneralCI:
		return "ascii_general_ci"
	case CharsetUjisJapaneseCI:
		return "ujis_japanese_ci"
	case CharsetSjisJapaneseCI:
		return "sjis_japanese_ci"
	case CharsetCp1251BulgarianCI:
		return "cp1251_bulgarian_ci"
	case CharsetLatin1DanishCI:
		return "latin1_danish_ci"
	case CharsetHebrewGeneralCI:
		return "hebrew_general_ci"
	case CharsetTis620ThaiCI:
		return "tis620_thai_ci"
	case CharsetEuckrKoreanCI:
		return "euckr_korean_ci"
	case CharsetLatin7EstonianCS:
		return "latin7_estonian_cs"
	case CharsetLatin2HungarianCI:
		return "latin2_hungarian_ci"
	case CharsetKoi8uGeneralCI:
		return "koi8u_general_ci"
	case CharsetCp1251UkrainianCI:
		return "cp1251_ukrainian_ci"
	case CharsetGb2312ChineseCI:
		return "gb2312_chinese_ci"
	case CharsetGreekGeneralCI:
		return "greek_general_ci"
	case CharsetCp1250GeneralCI:
		return "cp1250_general_ci"
	case CharsetLatin2CroatianCI:
		return "latin2_croatian_ci"
	case CharsetGbkChineseCI:
		return "gbk_chinese_ci"
	case CharsetCp1257LithuanianCI:
		return "cp1257_lithuanian_ci"
	case CharsetLatin5TurkishCI:
		return "latin5_turkish_ci"
	case CharsetLatin1German2CI:
		return "latin1_german2_ci"
	case CharsetArmscii8GeneralCI:
		return "armscii8_general_ci"
	case CharsetUtf8GeneralCI:
		return "utf8_general_ci"
	case CharsetUtf8mb4GeneralCI:
		return "utf8mb4_general_ci"
	case CharsetUtf8mb4Bin:
		return "utf8mb4_bin"
	case CharsetLatin1SpanishCI:
		return "latin1_spanish_ci"
	case CharsetCp1256GeneralCI:
		return "cp1256_general_ci"
	case CharsetCp866GeneralCI:
		return "cp866_general_ci"
	case CharsetKeybcs2GeneralCI:
		return "keybcs2_general_ci"
	case CharsetMacceGeneralCI:
		return "macce_general_ci"
	case CharsetMacromanGeneralCI:
		return "macroman_general_ci"
	case CharsetCp852GeneralCI:
		return "cp852_general_ci"
	case CharsetLatin7GeneralCI:
		return "latin7_general_ci"
	case CharsetLatin7GeneralCS:
		return "latin7_general_cs"
	case CharsetMacceBin:
		return "macce_bin"
	case CharsetCp1250CroatianCI:
		return "cp1250_croatian_ci"
	case CharsetUtf8mb4UnicodeCI:
		return "utf8mb4_unicode_ci"
	case CharsetUtf8mb4IcelandicCI:
		return "utf8mb4_icelandic_ci"
	case CharsetUtf8mb4LatvianCI:
		return "utf8mb4_latvian_ci"
	case CharsetUtf8mb4RomanianCI:
		return "utf8mb4_romanian_ci"
	case CharsetUtf8mb4SlovenianCI:
		return "utf8mb4_slovenian_ci"
	case CharsetUtf8mb4PolishCI:
		return "utf8mb4_polish_ci"
	case CharsetUtf8mb4EstonianCI:
		return "utf8mb4_estonian_ci"
	case CharsetUtf8mb4SpanishCI:
		return "utf8mb4_spanish_ci"
	case CharsetUtf8mb4SwedishCI:
		return "utf8mb4_swedish_ci"
	case CharsetUtf8mb4TurkishCI:
		return "utf8mb4_turkish_ci"
	case CharsetUtf8mb4CzechCI:
		return "utf8mb4_czech_ci"
	case CharsetUtf8mb4DanishCI:
		return "utf8mb4_danish_ci"
	case CharsetUtf8mb4LithuanianCI:
		return "utf8mb4_lithuanian_ci"
	case CharsetUtf8mb4SlovakCI:
		return "utf8mb4_slovak_ci"
	case CharsetUtf8mb4Spanish2CI:
		return "utf8mb4_spanish2_ci"
	case CharsetUtf8mb4RomanCI:
		return "utf8mb4_roman_ci"
	case CharsetUtf8mb4PersianCI:
		return "utf8mb4_persian_ci"
	case CharsetUtf8mb4EsperantoCI:
		return "utf8mb4_esperanto_ci"
	case CharsetUtf8mb4HungarianCI:
		return "utf8mb4_hungarian_ci"
	case CharsetUtf8mb4SinhalaCI:
		return "utf8mb4_sinhala_ci"
	case CharsetUtf8mb4German2CI:
		return "utf8mb4_german2_ci"
	case CharsetUtf8mb4CroatianCI:
		return "utf8mb4_croatian_ci"
	case CharsetUtf8mb4Unicode520CI:
		return "utf8mb4_unicode_520_ci"
	case CharsetUtf8mb4VietnameseCI:
		return "utf8mb4_vietnamese_ci"
	case CharsetUtf8mb40900AICi:
		return "utf8mb4_0900_ai_ci"
	default:
		return "unknown"
	}
}

// GetCharsetID 根据字符集名称获取字符集编号
func GetCharsetID(charsetName string) uint8 {
	switch charsetName {
	case "big5_chinese_ci":
		return CharsetBig5ChineseCI
	case "latin2_czech_cs":
		return CharsetLatin2CzechCS
	case "dec8_swedish_ci":
		return CharsetDec8SwedishCI
	case "cp850_general_ci":
		return CharsetCp850GeneralCI
	case "latin1_german1_ci":
		return CharsetLatin1German1CI
	case "hp8_english_ci":
		return CharsetHp8EnglishCI
	case "koi8r_general_ci":
		return CharsetKoi8rGeneralCI
	case "latin1_swedish_ci":
		return CharsetLatin1SwedishCI
	case "latin2_general_ci":
		return CharsetLatin2GeneralCI
	case "swe7_swedish_ci":
		return CharsetSwe7SwedishCI
	case "ascii_general_ci":
		return CharsetAsciiGeneralCI
	case "ujis_japanese_ci":
		return CharsetUjisJapaneseCI
	case "sjis_japanese_ci":
		return CharsetSjisJapaneseCI
	case "cp1251_bulgarian_ci":
		return CharsetCp1251BulgarianCI
	case "latin1_danish_ci":
		return CharsetLatin1DanishCI
	case "hebrew_general_ci":
		return CharsetHebrewGeneralCI
	case "tis620_thai_ci":
		return CharsetTis620ThaiCI
	case "euckr_korean_ci":
		return CharsetEuckrKoreanCI
	case "latin7_estonian_cs":
		return CharsetLatin7EstonianCS
	case "latin2_hungarian_ci":
		return CharsetLatin2HungarianCI
	case "koi8u_general_ci":
		return CharsetKoi8uGeneralCI
	case "cp1251_ukrainian_ci":
		return CharsetCp1251UkrainianCI
	case "gb2312_chinese_ci":
		return CharsetGb2312ChineseCI
	case "greek_general_ci":
		return CharsetGreekGeneralCI
	case "cp1250_general_ci":
		return CharsetCp1250GeneralCI
	case "latin2_croatian_ci":
		return CharsetLatin2CroatianCI
	case "gbk_chinese_ci":
		return CharsetGbkChineseCI
	case "cp1257_lithuanian_ci":
		return CharsetCp1257LithuanianCI
	case "latin5_turkish_ci":
		return CharsetLatin5TurkishCI
	case "latin1_german2_ci":
		return CharsetLatin1German2CI
	case "armscii8_general_ci":
		return CharsetArmscii8GeneralCI
	case "utf8_general_ci", "utf8":
		return CharsetUtf8GeneralCI
	case "utf8mb4_general_ci", "utf8mb4":
		return CharsetUtf8mb4GeneralCI
	case "utf8mb4_bin":
		return CharsetUtf8mb4Bin
	case "latin1_spanish_ci":
		return CharsetLatin1SpanishCI
	case "cp1256_general_ci":
		return CharsetCp1256GeneralCI
	case "cp866_general_ci":
		return CharsetCp866GeneralCI
	case "keybcs2_general_ci":
		return CharsetKeybcs2GeneralCI
	case "macce_general_ci":
		return CharsetMacceGeneralCI
	case "macroman_general_ci":
		return CharsetMacromanGeneralCI
	case "cp852_general_ci":
		return CharsetCp852GeneralCI
	case "latin7_general_ci":
		return CharsetLatin7GeneralCI
	case "latin7_general_cs":
		return CharsetLatin7GeneralCS
	case "macce_bin":
		return CharsetMacceBin
	case "cp1250_croatian_ci":
		return CharsetCp1250CroatianCI
	case "utf8mb4_unicode_ci":
		return CharsetUtf8mb4UnicodeCI
	case "utf8mb4_icelandic_ci":
		return CharsetUtf8mb4IcelandicCI
	case "utf8mb4_latvian_ci":
		return CharsetUtf8mb4LatvianCI
	case "utf8mb4_romanian_ci":
		return CharsetUtf8mb4RomanianCI
	case "utf8mb4_slovenian_ci":
		return CharsetUtf8mb4SlovenianCI
	case "utf8mb4_polish_ci":
		return CharsetUtf8mb4PolishCI
	case "utf8mb4_estonian_ci":
		return CharsetUtf8mb4EstonianCI
	case "utf8mb4_spanish_ci":
		return CharsetUtf8mb4SpanishCI
	case "utf8mb4_swedish_ci":
		return CharsetUtf8mb4SwedishCI
	case "utf8mb4_turkish_ci":
		return CharsetUtf8mb4TurkishCI
	case "utf8mb4_czech_ci":
		return CharsetUtf8mb4CzechCI
	case "utf8mb4_danish_ci":
		return CharsetUtf8mb4DanishCI
	case "utf8mb4_lithuanian_ci":
		return CharsetUtf8mb4LithuanianCI
	case "utf8mb4_slovak_ci":
		return CharsetUtf8mb4SlovakCI
	case "utf8mb4_spanish2_ci":
		return CharsetUtf8mb4Spanish2CI
	case "utf8mb4_roman_ci":
		return CharsetUtf8mb4RomanCI
	case "utf8mb4_persian_ci":
		return CharsetUtf8mb4PersianCI
	case "utf8mb4_esperanto_ci":
		return CharsetUtf8mb4EsperantoCI
	case "utf8mb4_hungarian_ci":
		return CharsetUtf8mb4HungarianCI
	case "utf8mb4_sinhala_ci":
		return CharsetUtf8mb4SinhalaCI
	case "utf8mb4_german2_ci":
		return CharsetUtf8mb4German2CI
	case "utf8mb4_croatian_ci":
		return CharsetUtf8mb4CroatianCI
	case "utf8mb4_unicode_520_ci":
		return CharsetUtf8mb4Unicode520CI
	case "utf8mb4_vietnamese_ci":
		return CharsetUtf8mb4VietnameseCI
	case "utf8mb4_0900_ai_ci":
		return CharsetUtf8mb40900AICi
	default:
		return CharsetUtf8mb40900AICi // 默认返回MySQL 8.0默认字符集
	}
}
