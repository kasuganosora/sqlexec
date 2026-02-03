package protocol

import (
	"github.com/kasuganosora/sqlexec/pkg/utils"
)

// MySQL字符集常量定义
// 参考: https://dev.mysql.com/doc/refman/8.0/en/charset-charsets.html

const (
	// 常用字符集编号
	CHARSET_BIG5_CHINESE_CI        = 1   // big5_chinese_ci
	CHARSET_LATIN2_CZECH_CS        = 2   // latin2_czech_cs
	CHARSET_DEC8_SWEDISH_CI        = 3   // dec8_swedish_ci
	CHARSET_CP850_GENERAL_CI       = 4   // cp850_general_ci
	CHARSET_LATIN1_GERMAN1_CI      = 5   // latin1_german1_ci
	CHARSET_HP8_ENGLISH_CI         = 6   // hp8_english_ci
	CHARSET_KOI8R_GENERAL_CI       = 7   // koi8r_general_ci
	CHARSET_LATIN1_SWEDISH_CI      = 8   // latin1_swedish_ci
	CHARSET_LATIN2_GENERAL_CI      = 9   // latin2_general_ci
	CHARSET_SWE7_SWEDISH_CI        = 10  // swe7_swedish_ci
	CHARSET_ASCII_GENERAL_CI       = 11  // ascii_general_ci
	CHARSET_UJIS_JAPANESE_CI       = 12  // ujis_japanese_ci
	CHARSET_SJIS_JAPANESE_CI       = 13  // sjis_japanese_ci
	CHARSET_CP1251_BULGARIAN_CI    = 14  // cp1251_bulgarian_ci
	CHARSET_LATIN1_DANISH_CI       = 15  // latin1_danish_ci
	CHARSET_HEBREW_GENERAL_CI      = 16  // hebrew_general_ci
	CHARSET_TIS620_THAI_CI         = 18  // tis620_thai_ci
	CHARSET_EUCKR_KOREAN_CI        = 19  // euckr_korean_ci
	CHARSET_LATIN7_ESTONIAN_CS     = 20  // latin7_estonian_cs
	CHARSET_LATIN2_HUNGARIAN_CI    = 21  // latin2_hungarian_ci
	CHARSET_KOI8U_GENERAL_CI       = 22  // koi8u_general_ci
	CHARSET_CP1251_UKRAINIAN_CI    = 23  // cp1251_ukrainian_ci
	CHARSET_GB2312_CHINESE_CI      = 24  // gb2312_chinese_ci
	CHARSET_GREEK_GENERAL_CI       = 25  // greek_general_ci
	CHARSET_CP1250_GENERAL_CI      = 26  // cp1250_general_ci
	CHARSET_LATIN2_CROATIAN_CI     = 27  // latin2_croatian_ci
	CHARSET_GBK_CHINESE_CI         = 28  // gbk_chinese_ci
	CHARSET_CP1257_LITHUANIAN_CI   = 29  // cp1257_lithuanian_ci
	CHARSET_LATIN5_TURKISH_CI      = 30  // latin5_turkish_ci
	CHARSET_LATIN1_GERMAN2_CI      = 31  // latin1_german2_ci
	CHARSET_ARMSCII8_GENERAL_CI    = 32  // armscii8_general_ci
	CHARSET_UTF8_GENERAL_CI        = 33  // utf8_general_ci (utf8)
	CHARSET_UTF8MB4_GENERAL_CI     = 45  // utf8mb4_general_ci (utf8mb4)
	CHARSET_UTF8MB4_BIN            = 46  // utf8mb4_bin
	CHARSET_LATIN1_SPANISH_CI      = 47  // latin1_spanish_ci
	CHARSET_CP1256_GENERAL_CI      = 57  // cp1256_general_ci
	CHARSET_CP866_GENERAL_CI       = 58  // cp866_general_ci
	CHARSET_KEYBCS2_GENERAL_CI     = 59  // keybcs2_general_ci
	CHARSET_MACCE_GENERAL_CI       = 60  // macce_general_ci
	CHARSET_MACROMAN_GENERAL_CI    = 61  // macroman_general_ci
	CHARSET_CP852_GENERAL_CI       = 62  // cp852_general_ci
	CHARSET_LATIN7_GENERAL_CI      = 63  // latin7_general_ci
	CHARSET_LATIN7_GENERAL_CS      = 64  // latin7_general_cs
	CHARSET_MACCE_BIN              = 65  // macce_bin
	CHARSET_CP1250_CROATIAN_CI     = 66  // cp1250_croatian_ci
	CHARSET_UTF8MB4_UNICODE_CI     = 224 // utf8mb4_unicode_ci
	CHARSET_UTF8MB4_ICELANDIC_CI   = 225 // utf8mb4_icelandic_ci
	CHARSET_UTF8MB4_LATVIAN_CI     = 226 // utf8mb4_latvian_ci
	CHARSET_UTF8MB4_ROMANIAN_CI    = 227 // utf8mb4_romanian_ci
	CHARSET_UTF8MB4_SLOVENIAN_CI   = 228 // utf8mb4_slovenian_ci
	CHARSET_UTF8MB4_POLISH_CI      = 229 // utf8mb4_polish_ci
	CHARSET_UTF8MB4_ESTONIAN_CI    = 230 // utf8mb4_estonian_ci
	CHARSET_UTF8MB4_SPANISH_CI     = 231 // utf8mb4_spanish_ci
	CHARSET_UTF8MB4_SWEDISH_CI     = 232 // utf8mb4_swedish_ci
	CHARSET_UTF8MB4_TURKISH_CI     = 233 // utf8mb4_turkish_ci
	CHARSET_UTF8MB4_CZECH_CI       = 234 // utf8mb4_czech_ci
	CHARSET_UTF8MB4_DANISH_CI      = 235 // utf8mb4_danish_ci
	CHARSET_UTF8MB4_LITHUANIAN_CI  = 236 // utf8mb4_lithuanian_ci
	CHARSET_UTF8MB4_SLOVAK_CI      = 237 // utf8mb4_slovak_ci
	CHARSET_UTF8MB4_SPANISH2_CI    = 238 // utf8mb4_spanish2_ci
	CHARSET_UTF8MB4_ROMAN_CI       = 239 // utf8mb4_roman_ci
	CHARSET_UTF8MB4_PERSIAN_CI     = 240 // utf8mb4_persian_ci
	CHARSET_UTF8MB4_ESPERANTO_CI   = 241 // utf8mb4_esperanto_ci
	CHARSET_UTF8MB4_HUNGARIAN_CI   = 242 // utf8mb4_hungarian_ci
	CHARSET_UTF8MB4_SINHALA_CI     = 243 // utf8mb4_sinhala_ci
	CHARSET_UTF8MB4_GERMAN2_CI     = 244 // utf8mb4_german2_ci
	CHARSET_UTF8MB4_CROATIAN_CI    = 245 // utf8mb4_croatian_ci
	CHARSET_UTF8MB4_UNICODE_520_CI = 246 // utf8mb4_unicode_520_ci
	CHARSET_UTF8MB4_VIETNAMESE_CI  = 247 // utf8mb4_vietnamese_ci
	CHARSET_UTF8MB4_0900_AI_CI     = 255 // utf8mb4_0900_ai_ci (MySQL 8.0默认)
)

// 常用字符集别名
const (
	// UTF-8相关
	CHARSET_UTF8    = CHARSET_UTF8_GENERAL_CI    // utf8 (3字节)
	CHARSET_UTF8MB4 = CHARSET_UTF8MB4_GENERAL_CI // utf8mb4 (4字节，推荐)

	// 默认字符集
	CHARSET_DEFAULT = CHARSET_UTF8MB4_0900_AI_CI // MySQL 8.0默认字符集

	// 中文相关
	CHARSET_GBK    = CHARSET_GBK_CHINESE_CI    // gbk
	CHARSET_GB2312 = CHARSET_GB2312_CHINESE_CI // gb2312
	CHARSET_BIG5   = CHARSET_BIG5_CHINESE_CI   // big5

	// 日文相关
	CHARSET_SJIS = CHARSET_SJIS_JAPANESE_CI // sjis
	CHARSET_UJIS = CHARSET_UJIS_JAPANESE_CI // ujis

	// 韩文相关
	CHARSET_EUCKR = CHARSET_EUCKR_KOREAN_CI // euckr

	// 其他常用
	CHARSET_LATIN1 = CHARSET_LATIN1_SWEDISH_CI // latin1
	CHARSET_ASCII  = CHARSET_ASCII_GENERAL_CI  // ascii
)

// GetCharsetName 根据字符集编号获取字符集名称
func GetCharsetName(charsetID uint8) string {
	return utils.GetCharsetName(charsetID)
}

// GetCharsetID 根据字符集名称获取字符集编号
func GetCharsetID(charsetName string) uint8 {
	return utils.GetCharsetID(charsetName)
}
