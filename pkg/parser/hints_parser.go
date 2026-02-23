package parser

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// HintsParser hints 解析器
type HintsParser struct {
	// Hint patterns
	hintCommentPattern *regexp.Regexp
	hintPattern        *regexp.Regexp
	leadingPattern     *regexp.Regexp
	tableListPattern   *regexp.Regexp
	indexListPattern   *regexp.Regexp
	durationPattern    *regexp.Regexp
	qbNamePattern      *regexp.Regexp
	optionPattern      *regexp.Regexp
}

// NewHintsParser 创建 hints 解析器
func NewHintsParser() *HintsParser {
	return &HintsParser{
		hintCommentPattern: regexp.MustCompile(`/\*+(.*?)\*/`),
		hintPattern:        regexp.MustCompile(`([A-Z_0-9]+)\s*\(\s*(.*?)\s*\)`),
		leadingPattern:     regexp.MustCompile(`LEADING\s*\((.*?)\)`),
		tableListPattern:   regexp.MustCompile(`([^,]+)(?:,|$)`),
		indexListPattern:   regexp.MustCompile(`([^@]+)(?:@([^,]+))?(?:,|$)`),
		durationPattern:    regexp.MustCompile(`(\d+)\s*(ms|s|m|h)`),
		qbNamePattern:      regexp.MustCompile(`QB_NAME\s*\(([^)]+)\)`),
		optionPattern:      regexp.MustCompile(`(\w+)\s*=\s*(\w+)`),
	}
}

// ParseFromComment 从注释中解析 hints
func (hp *HintsParser) ParseFromComment(comment string) (*ParsedHints, error) {
	hints := &ParsedHints{
		UseIndex:     make(map[string][]string),
		ForceIndex:   make(map[string][]string),
		IgnoreIndex:  make(map[string][]string),
		OrderIndex:   make(map[string]string),
		NoOrderIndex: make(map[string]string),
	}

	if comment == "" || strings.TrimSpace(comment) == "" {
		return hints, nil
	}

	// 查找所有 hint 定义
	matches := hp.hintPattern.FindAllStringSubmatch(comment, -1)

	for _, match := range matches {
		if len(match) < 3 {
			continue
		}

		hintName := strings.ToUpper(strings.TrimSpace(match[1]))
		hintArgs := strings.TrimSpace(match[2])

		if hintArgs == "" {
			// Boolean hints (e.g., HASH_AGG, STREAM_AGG)
			switch hintName {
			case "HASH_AGG":
				hints.HashAgg = true
			case "STREAM_AGG":
				hints.StreamAgg = true
			case "MPP_1PHASE_AGG", "MPP1PHASEAGG":
				hints.MPP1PhaseAgg = true
			case "MPP_2PHASE_AGG", "MPP2PHASEAGG":
				hints.MPP2PhaseAgg = true
			case "STRAIGHT_JOIN":
				hints.StraightJoin = true
			case "SEMI_JOIN_REWRITE":
				hints.SemiJoinRewrite = true
			case "NO_DECORRELATE":
				hints.NoDecorrelate = true
			case "USE_TOJA":
				hints.UseTOJA = true
			case "READ_CONSISTENT_REPLICA":
				hints.ReadConsistentReplica = true
			}
			continue
		}

		// Hints with arguments
		switch hintName {
		case "HASH_JOIN":
			hints.HashJoinTables = hp.parseTableList(hintArgs)
		case "MERGE_JOIN":
			hints.MergeJoinTables = hp.parseTableList(hintArgs)
		case "INL_JOIN":
			hints.INLJoinTables = hp.parseTableList(hintArgs)
		case "INL_HASH_JOIN":
			hints.INLHashJoinTables = hp.parseTableList(hintArgs)
		case "INL_MERGE_JOIN":
			hints.INLMergeJoinTables = hp.parseTableList(hintArgs)
		case "NO_HASH_JOIN":
			hints.NoHashJoinTables = hp.parseTableList(hintArgs)
		case "NO_MERGE_JOIN":
			hints.NoMergeJoinTables = hp.parseTableList(hintArgs)
		case "NO_INDEX_JOIN":
			hints.NoIndexJoinTables = hp.parseTableList(hintArgs)
		case "LEADING":
			hints.LeadingOrder = hp.parseTableList(hintArgs)
		case "USE_INDEX":
			hp.parseIndexHint(hintArgs, hints.UseIndex)
		case "FORCE_INDEX":
			hp.parseIndexHint(hintArgs, hints.ForceIndex)
		case "IGNORE_INDEX":
			hp.parseIndexHint(hintArgs, hints.IgnoreIndex)
		case "ORDER_INDEX":
			table, index := hp.parseTableIndexPair(hintArgs)
			if table != "" && index != "" {
				hints.OrderIndex[table] = index
			}
		case "NO_ORDER_INDEX":
			table, index := hp.parseTableIndexPair(hintArgs)
			if table != "" && index != "" {
				hints.NoOrderIndex[table] = index
			}
		case "MAX_EXECUTION_TIME":
			duration, err := hp.parseDuration(hintArgs)
			if err == nil {
				hints.MaxExecutionTime = duration
			}
		case "MEMORY_QUOTA":
			quota, err := strconv.ParseInt(hintArgs, 10, 64)
			if err == nil {
				hints.MemoryQuota = quota
			}
		case "RESOURCE_GROUP":
			hints.ResourceGroup = hintArgs
		case "QB_NAME":
			hints.QBName = hintArgs
		default:
			// Unknown hint - log warning but don't fail
			fmt.Printf("[WARN] Unknown hint: %s\n", hintName)
		}
	}

	return hints, nil
}

// ExtractHintsFromSQL 从 SQL 中提取 hints
func (hp *HintsParser) ExtractHintsFromSQL(sql string) (*ParsedHints, string, error) {
	// 查找 hint 注释
	matches := hp.hintCommentPattern.FindAllStringSubmatch(sql, -1)
	if len(matches) == 0 {
		return hp.NewParsedHints(), sql, nil
	}

	// 解析所有匹配的 hint 注释
	var combinedComment strings.Builder
	for _, match := range matches {
		if len(match) >= 2 {
			if combinedComment.Len() > 0 {
				combinedComment.WriteString(" ")
			}
			combinedComment.WriteString(match[1])
		}
	}

	// 解析 hints
	hints, err := hp.ParseFromComment(combinedComment.String())
	if err != nil {
		return nil, sql, fmt.Errorf("parse hints failed: %w", err)
	}

	// 从 SQL 中移除 hint 注释
	cleanSQL := sql
	for _, match := range matches {
		cleanSQL = strings.Replace(cleanSQL, match[0], "", 1)
	}

	// Check if hints are empty (no actual hints found)
	if hints.String() == "" {
		return hp.NewParsedHints(), sql, nil
	}

	return hints, cleanSQL, nil
}

// parseTableList 解析表名列表
func (hp *HintsParser) parseTableList(args string) []string {
	args = strings.TrimSpace(args)
	if args == "" {
		return nil
	}

	// Split by comma and clean each table name
	result := make([]string, 0)
	start := 0
	for i, ch := range args {
		if ch == ',' {
			table := strings.TrimSpace(args[start:i])
			if table != "" {
				result = append(result, table)
			}
			start = i + 1
		}
	}
	// Don't forget the last element
	if start < len(args) {
		table := strings.TrimSpace(args[start:])
		if table != "" {
			result = append(result, table)
		}
	}
	return result
}

// parseIndexHint 解析索引 hint
func (hp *HintsParser) parseIndexHint(args string, target map[string][]string) {
	// Split by comma and process each table-index pair
	start := 0
	for i := 0; i <= len(args); i++ {
		if i == len(args) || args[i] == ',' {
			item := strings.TrimSpace(args[start:i])
			if item != "" {
				// Check if index is specified with @ notation
				if atIndex := strings.Index(item, "@"); atIndex >= 0 {
					tableName := strings.TrimSpace(item[:atIndex])
					indexName := strings.TrimSpace(item[atIndex+1:])
					if tableName != "" && indexName != "" {
						target[tableName] = []string{indexName}
					}
				} else {
					// Table without specific index
					target[item] = []string{}
				}
			}
			start = i + 1
		}
	}
}

// parseTableIndexPair 解析表-索引对
func (hp *HintsParser) parseTableIndexPair(args string) (string, string) {
	parts := strings.Split(args, "@")
	if len(parts) == 2 {
		table := strings.TrimSpace(parts[0])
		index := strings.TrimSpace(parts[1])
		return table, index
	}
	return "", ""
}

// parseDuration 解析持续时间
func (hp *HintsParser) parseDuration(args string) (time.Duration, error) {
	args = strings.TrimSpace(args)

	// Try pattern with unit suffix first (e.g., "500ms", "10s")
	match := hp.durationPattern.FindStringSubmatch(args)
	if len(match) == 3 {
		value, err := strconv.Atoi(match[1])
		if err != nil {
			return 0, err
		}

		unit := match[2]
		switch unit {
		case "ms":
			return time.Duration(value) * time.Millisecond, nil
		case "s":
			return time.Duration(value) * time.Second, nil
		case "m":
			return time.Duration(value) * time.Minute, nil
		case "h":
			return time.Duration(value) * time.Hour, nil
		default:
			return 0, fmt.Errorf("unknown duration unit: %s", unit)
		}
	}

	// Plain integer: treat as milliseconds (common TiDB MAX_EXECUTION_TIME format)
	value, err := strconv.Atoi(args)
	if err == nil {
		return time.Duration(value) * time.Millisecond, nil
	}

	return 0, fmt.Errorf("invalid duration format: %s", args)
}

// ParsedHints 解析后的 hints
type ParsedHints struct {
	// JOIN hints
	HashJoinTables     []string
	MergeJoinTables    []string
	INLJoinTables      []string
	INLHashJoinTables  []string
	INLMergeJoinTables []string
	NoHashJoinTables   []string
	NoMergeJoinTables  []string
	NoIndexJoinTables  []string
	LeadingOrder       []string
	StraightJoin       bool

	// INDEX hints
	UseIndex     map[string][]string
	ForceIndex   map[string][]string
	IgnoreIndex  map[string][]string
	OrderIndex   map[string]string
	NoOrderIndex map[string]string

	// AGG hints
	HashAgg      bool
	StreamAgg    bool
	MPP1PhaseAgg bool
	MPP2PhaseAgg bool

	// Subquery hints
	SemiJoinRewrite bool
	NoDecorrelate   bool
	UseTOJA         bool

	// Global hints
	QBName                string
	MaxExecutionTime      time.Duration
	MemoryQuota           int64
	ReadConsistentReplica bool
	ResourceGroup         string
}

// NewParsedHints 创建空的 ParsedHints
func (hp *HintsParser) NewParsedHints() *ParsedHints {
	return &ParsedHints{
		UseIndex:     make(map[string][]string),
		ForceIndex:   make(map[string][]string),
		IgnoreIndex:  make(map[string][]string),
		OrderIndex:   make(map[string]string),
		NoOrderIndex: make(map[string]string),
	}
}

// String 返回 hints 的字符串表示
func (h *ParsedHints) String() string {
	var parts []string

	if len(h.HashJoinTables) > 0 {
		parts = append(parts, fmt.Sprintf("HASH_JOIN(%s)", strings.Join(h.HashJoinTables, ",")))
	}
	if len(h.MergeJoinTables) > 0 {
		parts = append(parts, fmt.Sprintf("MERGE_JOIN(%s)", strings.Join(h.MergeJoinTables, ",")))
	}
	if len(h.INLJoinTables) > 0 {
		parts = append(parts, fmt.Sprintf("INL_JOIN(%s)", strings.Join(h.INLJoinTables, ",")))
	}
	if len(h.INLHashJoinTables) > 0 {
		parts = append(parts, fmt.Sprintf("INL_HASH_JOIN(%s)", strings.Join(h.INLHashJoinTables, ",")))
	}
	if len(h.INLMergeJoinTables) > 0 {
		parts = append(parts, fmt.Sprintf("INL_MERGE_JOIN(%s)", strings.Join(h.INLMergeJoinTables, ",")))
	}
	if len(h.NoHashJoinTables) > 0 {
		parts = append(parts, fmt.Sprintf("NO_HASH_JOIN(%s)", strings.Join(h.NoHashJoinTables, ",")))
	}
	if len(h.NoMergeJoinTables) > 0 {
		parts = append(parts, fmt.Sprintf("NO_MERGE_JOIN(%s)", strings.Join(h.NoMergeJoinTables, ",")))
	}
	if len(h.NoIndexJoinTables) > 0 {
		parts = append(parts, fmt.Sprintf("NO_INDEX_JOIN(%s)", strings.Join(h.NoIndexJoinTables, ",")))
	}
	if len(h.LeadingOrder) > 0 {
		parts = append(parts, fmt.Sprintf("LEADING(%s)", strings.Join(h.LeadingOrder, ",")))
	}
	if h.StraightJoin {
		parts = append(parts, "STRAIGHT_JOIN")
	}
	if h.HashAgg {
		parts = append(parts, "HASH_AGG")
	}
	if h.StreamAgg {
		parts = append(parts, "STREAM_AGG")
	}
	if h.MPP1PhaseAgg {
		parts = append(parts, "MPP_1PHASE_AGG")
	}
	if h.MPP2PhaseAgg {
		parts = append(parts, "MPP_2PHASE_AGG")
	}
	if h.SemiJoinRewrite {
		parts = append(parts, "SEMI_JOIN_REWRITE")
	}
	if h.NoDecorrelate {
		parts = append(parts, "NO_DECORRELATE")
	}
	if h.UseTOJA {
		parts = append(parts, "USE_TOJA")
	}
	if h.MaxExecutionTime > 0 {
		parts = append(parts, fmt.Sprintf("MAX_EXECUTION_TIME(%dms)", h.MaxExecutionTime.Milliseconds()))
	}
	if h.MemoryQuota > 0 {
		parts = append(parts, fmt.Sprintf("MEMORY_QUOTA(%d)", h.MemoryQuota))
	}
	if h.ReadConsistentReplica {
		parts = append(parts, "READ_CONSISTENT_REPLICA")
	}
	if h.ResourceGroup != "" {
		parts = append(parts, fmt.Sprintf("RESOURCE_GROUP(%s)", h.ResourceGroup))
	}
	if h.QBName != "" {
		parts = append(parts, fmt.Sprintf("QB_NAME(%s)", h.QBName))
	}

	for table, indexes := range h.UseIndex {
		if len(indexes) == 0 {
			parts = append(parts, fmt.Sprintf("USE_INDEX(%s)", table))
		} else {
			parts = append(parts, fmt.Sprintf("USE_INDEX(%s@%s)", table, strings.Join(indexes, "@")))
		}
	}
	for table, indexes := range h.ForceIndex {
		if len(indexes) == 0 {
			parts = append(parts, fmt.Sprintf("FORCE_INDEX(%s)", table))
		} else {
			parts = append(parts, fmt.Sprintf("FORCE_INDEX(%s@%s)", table, strings.Join(indexes, "@")))
		}
	}
	for table, indexes := range h.IgnoreIndex {
		if len(indexes) == 0 {
			parts = append(parts, fmt.Sprintf("IGNORE_INDEX(%s)", table))
		} else {
			parts = append(parts, fmt.Sprintf("IGNORE_INDEX(%s@%s)", table, strings.Join(indexes, "@")))
		}
	}
	for table, index := range h.OrderIndex {
		parts = append(parts, fmt.Sprintf("ORDER_INDEX(%s@%s)", table, index))
	}
	for table, index := range h.NoOrderIndex {
		parts = append(parts, fmt.Sprintf("NO_ORDER_INDEX(%s@%s)", table, index))
	}

	if len(parts) == 0 {
		return ""
	}
	return strings.Join(parts, " ")
}
