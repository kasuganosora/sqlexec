package json

import (
	"fmt"
	"strconv"
	"strings"
)

// PathLeg represents a single leg in a JSON path expression
type PathLeg interface {
	Apply(bj BinaryJSON) ([]BinaryJSON, error)
	String() string
}

// Path represents a JSON path expression like $.key[0].name
type Path struct {
	Legs []PathLeg
}

// ParsePath parses a JSON path expression string
// Supports: $.key, $[index], $.*, $[*], $.key[index]
func ParsePath(pathExpr string) (*Path, error) {
	if pathExpr == "" {
		return nil, NewPathError("", "empty path expression")
	}

	pathExpr = strings.TrimSpace(pathExpr)

	if !strings.HasPrefix(pathExpr, "$") {
		return nil, NewPathError(pathExpr, "path must start with '$'")
	}

	pathExpr = pathExpr[1:]

	path := &Path{Legs: make([]PathLeg, 0)}

	for pathExpr != "" {
		// Skip leading dot if present
		if strings.HasPrefix(pathExpr, ".") {
			pathExpr = pathExpr[1:]
		}

		if pathExpr == "" {
			break
		}

		// Check for array indexing [...]
		if pathExpr[0] == '[' {
			leg, remaining, err := parseArrayLeg(pathExpr)
			if err != nil {
				return nil, err
			}
			path.Legs = append(path.Legs, leg)
			pathExpr = remaining
			continue
		}

		// Parse key leg (key or *)
		leg, remaining, err := parseKeyLeg(pathExpr)
		if err != nil {
			return nil, err
		}
		path.Legs = append(path.Legs, leg)
		pathExpr = remaining
	}

	return path, nil
}

// parseArrayLeg parses [index], [*], [last], [0 to 2], etc.
func parseArrayLeg(pathExpr string) (PathLeg, string, error) {
	if len(pathExpr) == 0 || pathExpr[0] != '[' {
		return nil, "", fmt.Errorf("expected '['")
	}

	closeIdx := strings.IndexByte(pathExpr, ']')
	if closeIdx == -1 {
		return nil, "", fmt.Errorf("missing closing ']'")
	}

	content := pathExpr[1:closeIdx]
	remaining := pathExpr[closeIdx+1:]

	// Check for wildcard *
	if content == "*" {
		return &ArrayLeg{Index: -2, Wildcard: true}, remaining, nil
	}

	// Check for "last"
	if content == "last" {
		return &ArrayLeg{Index: -1, Last: true}, remaining, nil
	}

	// Check for range "start to end"
	if strings.Contains(content, " to ") {
		parts := strings.Split(content, " to ")
		if len(parts) != 2 {
			return nil, "", fmt.Errorf("invalid range format")
		}
		startStr := strings.TrimSpace(parts[0])
		endStr := strings.TrimSpace(parts[1])

		var start, end int
		var startLast, endLast bool

		if startStr == "last" {
			startLast = true
		} else {
			start, _ = strconv.Atoi(startStr)
		}

		if endStr == "last" {
			endLast = true
		} else {
			end, _ = strconv.Atoi(endStr)
		}

		return &RangeLeg{
			Start:     start,
			StartLast: startLast,
			End:       end,
			EndLast:   endLast,
		}, remaining, nil
	}

	// Parse as integer index
	var idx int
	var lastOffset int

	// Check for negative index
	if strings.HasPrefix(content, "-") {
		if offset, err := strconv.Atoi(content[1:]); err == nil {
			lastOffset = -offset
			return &ArrayLeg{Index: -1, LastOffset: lastOffset}, remaining, nil
		}
	}

	idx, err := strconv.Atoi(content)
	if err != nil {
		return nil, "", err
	}

	return &ArrayLeg{Index: idx}, remaining, nil
}

// parseKeyLeg parses "key" or "*"
func parseKeyLeg(pathExpr string) (PathLeg, string, error) {
	end := len(pathExpr)

	// Check for next array index or dot
	nextIdx := strings.IndexAny(pathExpr, "[]")
	if nextIdx != -1 {
		end = nextIdx
	}
	nextDot := strings.IndexByte(pathExpr, '.')
	if nextDot != -1 && (nextDot < end || nextIdx == -1) {
		end = nextDot
	}

	if end == 0 {
		return nil, "", fmt.Errorf("empty key")
	}

	key := pathExpr[:end]
	remaining := pathExpr[end:]

	if key == "*" {
		return &KeyLeg{Key: key, Wildcard: true}, remaining, nil
	}

	return &KeyLeg{Key: key}, remaining, nil
}

// KeyLeg represents object key access
type KeyLeg struct {
	Key      string
	Wildcard bool
}

// Apply applies the key leg to a JSON value
func (k *KeyLeg) Apply(bj BinaryJSON) ([]BinaryJSON, error) {
	if !bj.IsObject() {
		return nil, &JSONError{Code: ErrTypeMismatch, Message: "cannot apply key leg to non-object"}
	}

	obj, _ := bj.GetObject()

	if k.Wildcard {
		// Wildcard - return all values
		result := make([]BinaryJSON, 0, len(obj))
		for _, key := range sortedKeys(obj) {
			if val, ok := obj[key]; ok {
				parsed, _ := NewBinaryJSON(val)
				result = append(result, parsed)
			}
		}
		return result, nil
	}

	// Regular key access
	if val, ok := obj[k.Key]; ok {
		parsed, _ := NewBinaryJSON(val)
		return []BinaryJSON{parsed}, nil
	}

	return nil, NewNotFoundError(k.Key)
}

// String returns string representation
func (k *KeyLeg) String() string {
	if k.Wildcard {
		return "*"
	}
	return k.Key
}

// ArrayLeg represents array index access
type ArrayLeg struct {
	Index      int    // -1 for last, -2 for wildcard
	Last       bool
	LastOffset int
	Wildcard  bool
}

// Apply applies the array leg to a JSON value
func (a *ArrayLeg) Apply(bj BinaryJSON) ([]BinaryJSON, error) {
	if !bj.IsArray() {
		return nil, &JSONError{Code: ErrTypeMismatch, Message: "cannot apply array leg to non-array"}
	}

	arr, _ := bj.GetArray()
	arrLen := len(arr)

	// Handle wildcard
	if a.Wildcard {
		result := make([]BinaryJSON, 0, arrLen)
		for _, val := range arr {
			parsed, _ := NewBinaryJSON(val)
			result = append(result, parsed)
		}
		return result, nil
	}

	// Calculate actual index
	idx := a.Index
	if a.Last {
		idx = arrLen - 1
	} else if a.LastOffset != 0 {
		idx = arrLen + a.LastOffset
	}

	// Validate index
	if idx < 0 || idx >= arrLen {
		return nil, NewIndexError(idx)
	}

	parsed, _ := NewBinaryJSON(arr[idx])
	return []BinaryJSON{parsed}, nil
}

// String returns string representation
func (a *ArrayLeg) String() string {
	if a.Wildcard {
		return "[*]"
	}
	if a.Last {
		if a.LastOffset != 0 {
			return fmt.Sprintf("[last%d]", a.LastOffset)
		}
		return "[last]"
	}
	return fmt.Sprintf("[%d]", a.Index)
}

// RangeLeg represents array range access
type RangeLeg struct {
	Start     int
	StartLast bool
	End       int
	EndLast   bool
}

// Apply applies the range leg to a JSON value
func (r *RangeLeg) Apply(bj BinaryJSON) ([]BinaryJSON, error) {
	if !bj.IsArray() {
		return nil, &JSONError{Code: ErrTypeMismatch, Message: "cannot apply range leg to non-array"}
	}

	arr, _ := bj.GetArray()
	arrLen := len(arr)

	start := r.Start
	if r.StartLast {
		start = arrLen - 1
	}

	end := r.End
	if r.EndLast {
		end = arrLen - 1
	}

	// Validate range
	if start < 0 || start >= arrLen || end < 0 || end >= arrLen || start > end {
		return nil, NewIndexError(start)
	}

	result := make([]BinaryJSON, 0, end-start+1)
	for i := start; i <= end; i++ {
		parsed, _ := NewBinaryJSON(arr[i])
		result = append(result, parsed)
	}

	return result, nil
}


// String returns string representation
func (r *RangeLeg) String() string {
	startStr := fmt.Sprintf("%d", r.Start)
	if r.StartLast {
		startStr = "last"
	}

	endStr := fmt.Sprintf("%d", r.End)
	if r.EndLast {
		endStr = "last"
	}

	return fmt.Sprintf("[%s to %s]", startStr, endStr)
}

// sortedKeys returns sorted keys from a map
func sortedKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	// Simple sort
	for i := 0; i < len(keys); i++ {
		for j := i + 1; j < len(keys); j++ {
			if keys[i] > keys[j] {
				keys[i], keys[j] = keys[j], keys[i]
			}
		}
	}
	return keys
}

// Evaluate evaluates the path against a JSON value
func (p *Path) Evaluate(bj BinaryJSON) ([]BinaryJSON, error) {
	results := []BinaryJSON{bj}

	for _, leg := range p.Legs {
		newResults := make([]BinaryJSON, 0)
		for _, result := range results {
			matches, err := leg.Apply(result)
			if err != nil {
				return nil, err
			}
			newResults = append(newResults, matches...)
		}
		results = newResults
		if len(results) == 0 {
			return nil, NewNotFoundError(p.String())
		}
	}

	return results, nil
}

// Extract extracts a single value from the JSON using the path
func (p *Path) Extract(bj BinaryJSON) (BinaryJSON, error) {
	results, err := p.Evaluate(bj)
	if err != nil {
		return BinaryJSON{}, err
	}

	if len(results) == 0 {
		return BinaryJSON{}, NewNotFoundError(p.String())
	}

	// If multiple results (from wildcard), wrap them in an array
	if len(results) > 1 {
		arrayResult := make([]interface{}, len(results))
		for i, result := range results {
			arrayResult[i] = result.GetInterface()
		}
		wrapped, _ := NewBinaryJSON(arrayResult)
		return wrapped, nil
	}

	return results[0], nil
}

// String returns the path expression as a string
func (p *Path) String() string {
	if len(p.Legs) == 0 {
		return "$"
	}

	var builder strings.Builder
	builder.WriteString("$")

	for _, leg := range p.Legs {
		legStr := leg.String()
		switch leg.(type) {
		case *KeyLeg:
			builder.WriteString(".")
			builder.WriteString(legStr)
		case *ArrayLeg, *RangeLeg:
			builder.WriteString(legStr)
		}
	}

	return builder.String()
}

// CheckExists checks if the path exists in the JSON value
func (p *Path) CheckExists(bj BinaryJSON) bool {
	_, err := p.Evaluate(bj)
	return err == nil
}
