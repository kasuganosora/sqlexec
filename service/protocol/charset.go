package protocol

// MySQL Character Set and Collation
// Reference: https://dev.mysql.com/doc/refman/8.0/en/charset-charsets.html

const (
	// Character Set Constants
	CHARSET_BIG5_CHINESE_CI      = 1  // big5_chinese_ci
	CHARSET_LATIN2_CZECH_CS      = 2  // latin2_czech_cs
	CHARSET_DEC8_SWEDISH_CI      = 3  // dec8_swedish_ci
	CHARSET_CP850_GENERAL_CI     = 4  // cp850_general_ci
	CHARSET_LATIN1_GERMAN1_CI    = 5  // latin1_german1_ci
	CHARSET_HP8_ENGLISH_CI       = 6  // hp8_english_ci
	CHARSET_KOI8R_GENERAL_CI     = 7  // koi8r_general_ci
	CHARSET_LATIN1_SWEDISH_CI    = 8  // latin1_swedish_ci
	CHARSET_LATIN2_GENERAL_CI    = 9  // latin2_general_ci
	CHARSET_SWE7_SWEDISH_CI      = 10 // swe7_swedish_ci
	CHARSET_ASCII_GENERAL_CI     = 11 // ascii_general_ci
	CHARSET_UJIS_JAPANESE_CI     = 12 // ujis_japanese_ci
	CHARSET_SJIS_JAPANESE_CI     = 13 // sjis_japanese_ci
	CHARSET_CP1251_BULGARIAN_CI  = 14 // cp1251_bulgarian_ci
	CHARSET_LATIN1_DANISH_CI     = 15 // latin1_danish_ci
	CHARSET_HEBREW_GENERAL_CI    = 16 // hebrew_general_ci
	CHARSET_TIS620_THAI_CI       = 18 // tis620_thai_ci
	CHARSET_EUCKR_KOREAN_CI      = 19 // euckr_korean_ci
	CHARSET_LATIN7_ESTONIAN_CS   = 20 // latin7_estonian_cs
	CHARSET_LATIN2_HUNGARIAN_CI  = 21 // latin2_hungarian_ci
	CHARSET_KOI8U_GENERAL_CI     = 22 // koi8u_general_ci
	CHARSET_CP1251_UKRAINIAN_CI  = 23 // cp1251_ukrainian_ci
	CHARSET_GB2312_CHINESE_CI    = 24 // gb2312_chinese_ci
	CHARSET_GREEK_GENERAL_CI     = 25 // greek_general_ci
	CHARSET_LATIN2_CROATIAN_CI   = 27 // latin2_croatian_ci
	CHARSET_GBK_CHINESE_CI       = 28 // gbk_chinese_ci
	CHARSET_CP1257_LITHUANIAN_CI = 29 // cp1257_lithuanian_ci
	CHARSET_LATIN5_TURKISH_CI    = 30 // latin5_turkish_ci
	CHARSET_LATIN1_GERMAN2_CI    = 31 // latin1_german2_ci
	CHARSET_ARMSCII8_GENERAL_CI  = 32 // armscii8_general_ci
	CHARSET_UTF8_GENERAL_CI      = 33 // utf8_general_ci (utf8)
	CHARSET_UTF8MB4_GENERAL_CI   = 45 // utf8mb4_general_ci (utf8mb4)
	CHARSET_UTF8MB4_BIN          = 46 // utf8mb4_bin
	CHARSET_LATIN1_SPANISH_CI    = 47 // latin1_spanish_ci
	CHARSET_LATIN1_BIN_OLD       = 94 // latin1_bin (old)
	CHARSET_LATIN1_GENERAL_CI    = 95 // latin1_general_ci (old)
	CHARSET_CP1256_GENERAL_CI    = 57 // cp1256_general_ci
	CHARSET_CP866_GENERAL_CI     = 58 // cp866_general_ci
	CHARSET_KEYBCS2_GENERAL_CI   = 59 // keybcs2_general_ci
	CHARSET_MACCE_GENERAL_CI     = 60 // macce_general_ci
	CHARSET_MACROMAN_GENERAL_CI  = 61 // macroman_general_ci
	CHARSET_CP852_GENERAL_CI     = 62 // cp852_general_ci
	CHARSET_LATIN7_GENERAL_CI    = 63 // latin7_general_ci
	CHARSET_LATIN7_GENERAL_CS    = 64 // latin7_general_cs
	CHARSET_MACCE_BIN            = 77 // macce_bin
	CHARSET_LATIN2_BIN           = 78 // latin2_bin
	CHARSET_UTF8MB3_GENERAL_CI   = 83 // utf8mb3_general_ci
	CHARSET_UTF8MB3_BIN          = 84 // utf8mb3_bin
	CHARSET_CP850_BIN            = 80 // cp850_bin
)

// GetCharsetString returns the charset name for the given charset ID
func GetCharsetString(charsetID uint8) string {
	switch charsetID {
	case CHARSET_UTF8MB4_GENERAL_CI:
		return "utf8mb4"
	case CHARSET_UTF8MB4_BIN:
		return "utf8mb4"
	case CHARSET_UTF8_GENERAL_CI:
		return "utf8"
	case CHARSET_LATIN1_GENERAL_CI:
		return "latin1"
	case CHARSET_LATIN1_SWEDISH_CI:
		return "latin1"
	case CHARSET_GBK_CHINESE_CI:
		return "gbk"
	case CHARSET_GB2312_CHINESE_CI:
		return "gb2312"
	case CHARSET_BIG5_CHINESE_CI:
		return "big5"
	case CHARSET_SJIS_JAPANESE_CI:
		return "sjis"
	case CHARSET_UJIS_JAPANESE_CI:
		return "ujis"
	case CHARSET_EUCKR_KOREAN_CI:
		return "euckr"
	default:
		return "utf8mb4"
	}
}

// DefaultCharset is the default charset for MySQL connections
const DefaultCharset = CHARSET_UTF8MB4_GENERAL_CI
