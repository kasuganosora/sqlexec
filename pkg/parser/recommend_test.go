package parser

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParseRecommendIndexRun tests parsing RECOMMEND INDEX RUN
func TestParseRecommendIndexRun(t *testing.T) {
	parser := NewRecommendIndexParser()

	// Test with FOR clause
	sql := "RECOMMEND INDEX RUN FOR \"SELECT * FROM t1 WHERE a = 1\""
	stmt, err := parser.Parse(sql)

	require.NoError(t, err)
	assert.Equal(t, "RUN", stmt.Action)
	assert.True(t, stmt.ForQuery)
	// TiDB parser normalizes identifiers to uppercase
	assert.Equal(t, "SELECT * FROM T1 WHERE A = 1", stmt.Query)
}

// TestParseRecommendIndexRunWorkload 测试解析工作负载模式
func TestParseRecommendIndexRunWorkload(t *testing.T) {
	parser := NewRecommendIndexParser()

	// 测试不带 FOR 子句（工作负载模式）
	sql := "RECOMMEND INDEX RUN"
	stmt, err := parser.Parse(sql)

	require.NoError(t, err)
	assert.Equal(t, "RUN", stmt.Action)
	assert.False(t, stmt.ForQuery)
	assert.Empty(t, stmt.Query)
}

// TestParseRecommendIndexShow 测试解析 RECOMMEND INDEX SHOW
func TestParseRecommendIndexShow(t *testing.T) {
	parser := NewRecommendIndexParser()

	// 测试 SHOW OPTION
	sql := "RECOMMEND INDEX SHOW OPTION"
	stmt, err := parser.Parse(sql)

	require.NoError(t, err)
	assert.Equal(t, "SHOW", stmt.Action)
}

// TestParseRecommendIndexSet tests parsing RECOMMEND INDEX SET
func TestParseRecommendIndexSet(t *testing.T) {
	parser := NewRecommendIndexParser()

	// Test SET integer parameter
	sql1 := "RECOMMEND INDEX SET max_num_index = 10"
	stmt1, err := parser.Parse(sql1)

	require.NoError(t, err)
	assert.Equal(t, "SET", stmt1.Action)
	// TiDB parser normalizes identifiers to uppercase
	assert.Equal(t, "MAX_NUM_INDEX", stmt1.OptionName)
	assert.Equal(t, "10", stmt1.OptionValue)

	// Test SET string parameter (with quotes)
	sql2 := "RECOMMEND INDEX SET timeout = '60'"
	stmt2, err := parser.Parse(sql2)

	require.NoError(t, err)
	assert.Equal(t, "SET", stmt2.Action)
	// TiDB parser normalizes identifiers to uppercase
	assert.Equal(t, "TIMEOUT", stmt2.OptionName)
	assert.Equal(t, "60", stmt2.OptionValue)
}

// TestExtractQuotedString 测试提取引号字符串
func TestExtractQuotedString(t *testing.T) {
	parser := NewRecommendIndexParser()

	// 测试单引号
	str1, err := parser.extractQuotedString("'hello world'")
	require.NoError(t, err)
	assert.Equal(t, "hello world", str1)

	// 测试双引号
	str2, err := parser.extractQuotedString("\"SELECT * FROM t\"")
	require.NoError(t, err)
	assert.Equal(t, "SELECT * FROM t", str2)

	// 测试空字符串
	_, err = parser.extractQuotedString("")
	assert.Error(t, err)

	// 测试未闭合引号
	_, err = parser.extractQuotedString("'unclosed")
	assert.Error(t, err)
}

// TestExtractAction 测试提取动作
func TestExtractAction(t *testing.T) {
	parser := NewRecommendIndexParser()

	action1, remaining1, err := parser.extractAction("RUN FOR \"SELECT * FROM t\"")
	require.NoError(t, err)
	assert.Equal(t, "RUN", action1)
	assert.Equal(t, "FOR \"SELECT * FROM t\"", remaining1)

	action2, remaining2, err := parser.extractAction("SET option = value")
	require.NoError(t, err)
	assert.Equal(t, "SET", action2)
	assert.Equal(t, "option = value", remaining2)
}

// TestParseInvalidStatements 测试解析无效语句
func TestParseInvalidStatements(t *testing.T) {
	parser := NewRecommendIndexParser()

	// 非法语句
	_, err := parser.Parse("SELECT * FROM t")
	assert.Error(t, err)

	// 未知动作
	_, err = parser.Parse("RECOMMEND INDEX UNKNOWN")
	assert.Error(t, err)

	// 缺少引号
	_, err = parser.Parse("RECOMMEND INDEX RUN FOR SELECT * FROM t")
	assert.Error(t, err)

	// SET 语法错误
	_, err = parser.Parse("RECOMMEND INDEX SET option")
	assert.Error(t, err)
}

// TestDefaultRecommendIndexConfig 测试默认配置
func TestDefaultRecommendIndexConfig(t *testing.T) {
	config := DefaultRecommendIndexConfig()

	assert.Equal(t, 5, config.MaxNumIndexes)
	assert.Equal(t, 3, config.MaxIndexColumns)
	assert.Equal(t, 1000, config.MaxNumQuery)
	assert.Equal(t, 30, config.Timeout)
}

// TestApplyConfig 测试应用配置
func TestApplyConfig(t *testing.T) {
	config := DefaultRecommendIndexConfig()

	// 测试设置 max_num_index
	err := config.ApplyConfig("max_num_index", "10")
	require.NoError(t, err)
	assert.Equal(t, 10, config.MaxNumIndexes)

	// 测试设置 max_index_columns
	err = config.ApplyConfig("max_index_columns", "5")
	require.NoError(t, err)
	assert.Equal(t, 5, config.MaxIndexColumns)

	// 测试设置 max_num_query
	err = config.ApplyConfig("max_num_query", "500")
	require.NoError(t, err)
	assert.Equal(t, 500, config.MaxNumQuery)

	// 测试设置 timeout
	err = config.ApplyConfig("timeout", "60")
	require.NoError(t, err)
	assert.Equal(t, 60, config.Timeout)
}

// TestApplyConfigInvalid 测试应用无效配置
func TestApplyConfigInvalid(t *testing.T) {
	config := DefaultRecommendIndexConfig()

	// 测试无效的值
	err := config.ApplyConfig("max_num_index", "invalid")
	assert.Error(t, err)

	// 测试未知的选项
	err = config.ApplyConfig("unknown_option", "value")
	assert.Error(t, err)
}

// TestGetConfigString 测试获取配置字符串
func TestGetConfigString(t *testing.T) {
	config := DefaultRecommendIndexConfig()

	configStr := config.GetConfigString()

	assert.Contains(t, configStr, "max_num_index: 5")
	assert.Contains(t, configStr, "max_index_columns: 3")
	assert.Contains(t, configStr, "max_num_query: 1000")
	assert.Contains(t, configStr, "timeout: 30")
}

// TestIsRecommendIndexStatement 测试识别 RECOMMEND INDEX 语句
func TestIsRecommendIndexStatement(t *testing.T) {
	// 正确的语句
	assert.True(t, IsRecommendIndexStatement("RECOMMEND INDEX RUN"))
	assert.True(t, IsRecommendIndexStatement("  RECOMMEND INDEX SHOW OPTION  "))
	assert.True(t, IsRecommendIndexStatement("recommend index run")) // 大小写不敏感

	// 不正确的语句
	assert.False(t, IsRecommendIndexStatement("SELECT * FROM t"))
	assert.False(t, IsRecommendIndexStatement("CREATE INDEX idx ON t(a)"))
	assert.False(t, IsRecommendIndexStatement(""))
}

// TestComplexQueries 测试复杂查询解析
func TestComplexQueries(t *testing.T) {
	parser := NewRecommendIndexParser()

	// 复杂查询
	sql := "RECOMMEND INDEX RUN FOR \"SELECT a, b, c FROM t1 JOIN t2 ON t1.id = t2.id WHERE t1.status = 'active' GROUP BY t1.category ORDER BY t1.count DESC LIMIT 10\""
	stmt, err := parser.Parse(sql)

	require.NoError(t, err)
	assert.Equal(t, "RUN", stmt.Action)
	assert.True(t, stmt.ForQuery)
	assert.Contains(t, stmt.Query, "SELECT")
	assert.Contains(t, stmt.Query, "JOIN")
	assert.Contains(t, stmt.Query, "WHERE")
	assert.Contains(t, stmt.Query, "GROUP BY")
	assert.Contains(t, stmt.Query, "ORDER BY")
	assert.Contains(t, stmt.Query, "LIMIT")
}

// TestMultipleOptions 测试多个配置选项
func TestMultipleOptions(t *testing.T) {
	config := DefaultRecommendIndexConfig()

	options := []struct {
		name  string
		value string
	}{
		{"max_num_index", "10"},
		{"max_index_columns", "4"},
		{"max_num_query", "2000"},
		{"timeout", "120"},
	}

	for _, opt := range options {
		err := config.ApplyConfig(opt.name, opt.value)
		require.NoError(t, err, "Failed to apply %s", opt.name)
	}

	// 验证所有选项都已设置
	assert.Equal(t, 10, config.MaxNumIndexes)
	assert.Equal(t, 4, config.MaxIndexColumns)
	assert.Equal(t, 2000, config.MaxNumQuery)
	assert.Equal(t, 120, config.Timeout)
}

// BenchmarkParseRecommendIndex 基准测试
func BenchmarkParseRecommendIndex(b *testing.B) {
	parser := NewRecommendIndexParser()
	sql := "RECOMMEND INDEX RUN FOR \"SELECT * FROM t1 WHERE a = 1 AND b = 2\""

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = parser.Parse(sql)
	}
}
