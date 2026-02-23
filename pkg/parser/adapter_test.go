package parser

import (
	"testing"

	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
	"github.com/stretchr/testify/assert"
)

// TestSimplifyTypeName 测试简化类型名
func TestSimplifyTypeName(t *testing.T) {
	testCases := []struct {
		input    string
		expected string
	}{
		{"DECIMAL(10,2)", "DECIMAL"},
		{"VARCHAR(255)", "VARCHAR"},
		{"INT", "INT"},
		{"DOUBLE", "DOUBLE"},
		{"FLOAT(10,2)", "FLOAT"},
		{"CHAR(10)", "CHAR"},
	}

	for _, tc := range testCases {
		result := simplifyTypeName(tc.input)
		assert.Equal(t, tc.expected, result)
	}
}

// TestConvertTiDBValue 测试TiDB值转换
func TestConvertTiDBValue(t *testing.T) {
	t.Run("integer types should convert to int64", func(t *testing.T) {
		vals := []interface{}{
			int(42),
			int8(8),
			int16(16),
			int32(32),
			int64(64),
		}

		for _, v := range vals {
			converted, err := convertTiDBValue(v)
			assert.NoError(t, err)
			var expected int64
			switch val := v.(type) {
			case int:
				expected = int64(val)
			case int8:
				expected = int64(val)
			case int16:
				expected = int64(val)
			case int32:
				expected = int64(val)
			case int64:
				expected = val
			}
			assert.Equal(t, expected, converted)
		}
	})

	t.Run("unsigned integer types", func(t *testing.T) {
		vals := []interface{}{
			uint(42),
			uint8(8),
			uint16(16),
			uint32(32),
			uint64(64),
		}

		for _, v := range vals {
			converted, err := convertTiDBValue(v)
			assert.NoError(t, err)
			var expected int64
			switch val := v.(type) {
			case uint:
				expected = int64(val)
			case uint8:
				expected = int64(val)
			case uint16:
				expected = int64(val)
			case uint32:
				expected = int64(val)
			case uint64:
				expected = int64(val)
			}
			assert.Equal(t, expected, converted)
		}
	})

	t.Run("float types", func(t *testing.T) {
		vals := []struct {
			input    interface{}
			expected interface{}
		}{
			{float32(3.14), float64(3.140000104904175)},
			{float64(2.718), float64(2.718)},
		}

		for _, tc := range vals {
			converted, err := convertTiDBValue(tc.input)
			assert.NoError(t, err)
			assert.Equal(t, tc.expected, converted)
		}
	})

	t.Run("string types", func(t *testing.T) {
		vals := []interface{}{
			"123.45",
			"100.00",
			"0.0",
		}

		for _, v := range vals {
			converted, err := convertTiDBValue(v)
			assert.NoError(t, err)
			assert.IsType(t, float64(0), converted)
		}
	})

	t.Run("nil value", func(t *testing.T) {
		converted, err := convertTiDBValue(nil)
		assert.NoError(t, err)
		assert.Nil(t, converted)
	})

	t.Run("unknown type with String() method", func(t *testing.T) {
		// 模拟TiDB的MyDecimal类型
		type MyDecimal struct {
			Value string
		}

		md := MyDecimal{Value: "123.45"}

		converted, err := convertTiDBValue(md)
		assert.NoError(t, err)
		// 对于未知类型，返回原始值或尝试解析
		assert.NotNil(t, converted)
	})
}

// TestParseDecimalString 测试DECIMAL字符串解析
func TestParseDecimalString(t *testing.T) {
	testCases := []struct {
		input    string
		expected float64
	}{
		{"123.45", 123.45},
		{"100.00", 100.00},
		{"0.0", 0.0},
		{"-50.25", -50.25},
	}

	for _, tc := range testCases {
		result, err := parseDecimalString(tc.input)
		assert.NoError(t, err)
		assert.Equal(t, tc.expected, result)
	}
}
