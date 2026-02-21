package parser

import (
	"fmt"
	"strings"
)

// RecommendIndexStatement RECOMMEND INDEX 语句
type RecommendIndexStatement struct {
	Action string // "RUN", "SHOW", "SET"

	// RUN 参数
	Query    string // SQL 查询字符串
	ForQuery bool   // 是否指定了 FOR 子句

	// SET 参数
	OptionName  string // 选项名称
	OptionValue string // 选项值
}

// RecommendIndexParser RECOMMEND INDEX 语句解析器
type RecommendIndexParser struct{}

// NewRecommendIndexParser 创建 RECOMMEND INDEX 解析器
func NewRecommendIndexParser() *RecommendIndexParser {
	return &RecommendIndexParser{}
}

// Parse 解析 RECOMMEND INDEX 语句
func (p *RecommendIndexParser) Parse(sql string) (*RecommendIndexStatement, error) {
	// 标准化 SQL - only uppercase for prefix matching, preserve original for content
	sql = strings.TrimSpace(sql)
	upperSQL := strings.ToUpper(sql)

	// 检查是否为 RECOMMEND INDEX 语句
	if !strings.HasPrefix(upperSQL, "RECOMMEND INDEX") {
		return nil, fmt.Errorf("not a RECOMMEND INDEX statement")
	}

	// 移除 "RECOMMEND INDEX" (use length of prefix to preserve original case of remainder)
	sql = strings.TrimSpace(sql[len("RECOMMEND INDEX"):])
	upperSQL = strings.ToUpper(sql)

	// 提取动作
	action, remaining, err := p.extractAction(sql)
	if err != nil {
		return nil, err
	}

	stmt := &RecommendIndexStatement{
		Action: action,
	}

	// 根据动作解析参数
	switch action {
	case "RUN":
		return p.parseRunAction(stmt, remaining)
	case "SHOW":
		return p.parseShowAction(stmt, remaining)
	case "SET":
		return p.parseSetAction(stmt, remaining)
	default:
		return nil, fmt.Errorf("unknown action: %s", action)
	}
}

// extractAction 提取动作
func (p *RecommendIndexParser) extractAction(sql string) (string, string, error) {
	parts := strings.Fields(sql)
	if len(parts) == 0 {
		return "", "", fmt.Errorf("no action specified")
	}

	// Uppercase only the action keyword, preserve rest
	action := strings.ToUpper(parts[0])
	// Reconstruct remaining from original sql to preserve case
	remaining := strings.TrimSpace(sql[len(parts[0]):])

	return action, remaining, nil
}

// parseRunAction 解析 RUN 动作
func (p *RecommendIndexParser) parseRunAction(stmt *RecommendIndexStatement, sql string) (*RecommendIndexStatement, error) {
	sql = strings.TrimSpace(sql)

	// 检查是否有 FOR 子句 (case-insensitive)
	if strings.HasPrefix(strings.ToUpper(sql), "FOR") {
		stmt.ForQuery = true
		sql = sql[len("FOR"):]
		sql = strings.TrimSpace(sql)

		// 提取 SQL（用引号包裹）
		query, err := p.extractQuotedString(sql)
		if err != nil {
			return nil, fmt.Errorf("invalid query string: %w", err)
		}

		stmt.Query = query
	} else {
		// 工作负载模式，没有 FOR 子句
		stmt.ForQuery = false
	}

	return stmt, nil
}

// parseShowAction 解析 SHOW 动作
func (p *RecommendIndexParser) parseShowAction(stmt *RecommendIndexStatement, sql string) (*RecommendIndexStatement, error) {
	sql = strings.TrimSpace(sql)

	// SHOW OPTION - 显示配置
	if strings.ToUpper(sql) == "OPTION" {
		return stmt, nil
	}

	return nil, fmt.Errorf("unknown SHOW action: %s", sql)
}

// parseSetAction 解析 SET 动作
func (p *RecommendIndexParser) parseSetAction(stmt *RecommendIndexStatement, sql string) (*RecommendIndexStatement, error) {
	sql = strings.TrimSpace(sql)

	// SET option = value
	// 查找等号
	eqIndex := strings.Index(sql, "=")
	if eqIndex == -1 {
		return nil, fmt.Errorf("missing '=' in SET statement")
	}

	optionName := strings.TrimSpace(sql[:eqIndex])
	optionValue := strings.TrimSpace(sql[eqIndex+1:])

	// 去除值的引号
	if strings.HasPrefix(optionValue, "'") || strings.HasPrefix(optionValue, "\"") {
		optionValue = optionValue[1:]
	}
	if strings.HasSuffix(optionValue, "'") || strings.HasSuffix(optionValue, "\"") {
		optionValue = optionValue[:len(optionValue)-1]
	}

	stmt.OptionName = optionName
	stmt.OptionValue = optionValue

	return stmt, nil
}

// extractQuotedString 提取引号包裹的字符串
func (p *RecommendIndexParser) extractQuotedString(sql string) (string, error) {
	sql = strings.TrimSpace(sql)

	if len(sql) == 0 {
		return "", fmt.Errorf("empty string")
	}

	quote := sql[0]
	if quote != '\'' && quote != '"' {
		return "", fmt.Errorf("string must be quoted")
	}

	// 查找结束引号
	endIndex := strings.Index(sql[1:], string(quote))
	if endIndex == -1 {
		return "", fmt.Errorf("unclosed quote")
	}

	// 提取内容
	content := sql[1 : endIndex+1]

	return content, nil
}

// RecommendIndexConfig 索引推荐配置
type RecommendIndexConfig struct {
	MaxNumIndexes   int
	MaxIndexColumns int
	MaxNumQuery     int
	Timeout         int // 秒
}

// DefaultRecommendIndexConfig 默认配置
func DefaultRecommendIndexConfig() *RecommendIndexConfig {
	return &RecommendIndexConfig{
		MaxNumIndexes:   5,
		MaxIndexColumns: 3,
		MaxNumQuery:     1000,
		Timeout:         30,
	}
}

// ApplyConfig 应用配置更改
func (cfg *RecommendIndexConfig) ApplyConfig(optionName, optionValue string) error {
	switch optionName {
	case "max_num_index":
		var value int
		_, err := fmt.Sscanf(optionValue, "%d", &value)
		if err != nil {
			return fmt.Errorf("invalid value for max_num_index: %s", optionValue)
		}
		cfg.MaxNumIndexes = value

	case "max_index_columns":
		var value int
		_, err := fmt.Sscanf(optionValue, "%d", &value)
		if err != nil {
			return fmt.Errorf("invalid value for max_index_columns: %s", optionValue)
		}
		cfg.MaxIndexColumns = value

	case "max_num_query":
		var value int
		_, err := fmt.Sscanf(optionValue, "%d", &value)
		if err != nil {
			return fmt.Errorf("invalid value for max_num_query: %s", optionValue)
		}
		cfg.MaxNumQuery = value

	case "timeout":
		var value int
		_, err := fmt.Sscanf(optionValue, "%d", &value)
		if err != nil {
			return fmt.Errorf("invalid value for timeout: %s", optionValue)
		}
		cfg.Timeout = value

	default:
		return fmt.Errorf("unknown option: %s", optionName)
	}

	return nil
}

// GetConfigString 获取配置字符串
func (cfg *RecommendIndexConfig) GetConfigString() string {
	return fmt.Sprintf(
		"max_num_index: %d, max_index_columns: %d, max_num_query: %d, timeout: %d",
		cfg.MaxNumIndexes,
		cfg.MaxIndexColumns,
		cfg.MaxNumQuery,
		cfg.Timeout,
	)
}

// IsRecommendIndexStatement 检查是否为 RECOMMEND INDEX 语句
func IsRecommendIndexStatement(sql string) bool {
	sql = strings.TrimSpace(sql)
	sql = strings.ToUpper(sql)
	return strings.HasPrefix(sql, "RECOMMEND INDEX")
}
