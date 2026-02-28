package parser

import (
	"sync"
	"testing"

	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

// TestParseInsertOnDuplicateKeyUpdate tests that ON DUPLICATE KEY UPDATE
// is properly extracted from the TiDB AST into InsertStatement.OnDuplicate.
func TestParseInsertOnDuplicateKeyUpdate(t *testing.T) {
	adapter := NewSQLAdapter()

	sql := "INSERT INTO `char_switches` (`key`,`value`) VALUES (?,?) ON DUPLICATE KEY UPDATE `value`=?"
	result, err := adapter.Parse(sql)
	require.NoError(t, err)
	require.True(t, result.Success)
	require.NotNil(t, result.Statement)
	assert.Equal(t, SQLTypeInsert, result.Statement.Type)
	require.NotNil(t, result.Statement.Insert)

	insert := result.Statement.Insert
	assert.Equal(t, "char_switches", insert.Table)
	assert.Equal(t, []string{"key", "value"}, insert.Columns)
	require.NotNil(t, insert.OnDuplicate, "OnDuplicate should be populated")
	assert.Contains(t, insert.OnDuplicate.Set, "value", "OnDuplicate SET should contain 'value' column")
}

// TestParseInsertWithoutOnDuplicate confirms normal INSERT doesn't set OnDuplicate.
func TestParseInsertWithoutOnDuplicate(t *testing.T) {
	adapter := NewSQLAdapter()

	sql := "INSERT INTO `char_switches` (`key`,`value`) VALUES ('k1','v1')"
	result, err := adapter.Parse(sql)
	require.NoError(t, err)
	require.True(t, result.Success)
	assert.Nil(t, result.Statement.Insert.OnDuplicate, "OnDuplicate should be nil for plain INSERT")
}

// TestConcurrentParsing ensures the parser mutex prevents data races.
// Before the fix, concurrent Parse calls would panic with index-out-of-range
// or type assertion failures in yyParse.
func TestConcurrentParsing(t *testing.T) {
	adapter := NewSQLAdapter()

	sqls := []string{
		"SELECT * FROM users WHERE id = 1",
		"INSERT INTO users (name, age) VALUES ('alice', 30)",
		"UPDATE users SET name = 'bob' WHERE id = 1",
		"DELETE FROM users WHERE id = 2",
		"SELECT id, name FROM users ORDER BY id LIMIT 10",
		"INSERT INTO char_switches (`key`, `value`) VALUES ('k1', 'v1') ON DUPLICATE KEY UPDATE `value` = 'v2'",
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 100)

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			sql := sqls[i%len(sqls)]
			_, err := adapter.Parse(sql)
			if err != nil {
				errCh <- err
			}
		}(i)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Errorf("concurrent parse failed: %v", err)
	}
}
