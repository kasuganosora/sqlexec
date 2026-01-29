package json

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/kasuganosora/sqlexec/pkg/builtin"
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
// Supports: $.key, $[index], $.*, $[*], $.key[index], $.key[*], **, last
func ParsePath(pathExpr string) (*Path, error) {
	if pathExpr == "" {
		return nil, NewPathError("", "empty path expression")
	}

	// Normalize path
	pathExpr = strings.TrimSpace(pathExpr)
	
	// Must start with '$'
	if !strings.HasPrefix(pathExpr, "$") {
		return nil, NewPathError(pathExpr, "path must start with '$'")
	}
	
	// Remove '$' prefix
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
		
		// Check for ** (wildcard)
		if strings.HasPrefix(pathExpr, "**") {
			if len(pathExpr) > 2 && pathExpr[2] != '.' && pathExpr[2] != '[' {
				return nil, NewPathError(pathExpr, "invalid '**' usage")
			}
			// Treat ** as .* (recursive wildcard)
			path.Legs = append(path.Legs, &KeyLeg{Key: "*", Recursive: true})
			pathExpr = pathExpr[2:]
			if pathExpr != "" && pathExpr[0] == '.' {
				pathExpr = pathExpr[1:]
			}
			continue
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

// parseKeyLeg parses a key leg like "key" or "*"
func parseKeyLeg(pathExpr string) (PathLeg, string, error) {
	// Find end of key
	end := len(pathExpr)
	
	// Check for array indexing after key
	if idx := strings.IndexByte(pathExpr, '['); idx != -1 {
		end = idx
	}
	
	// Check for next dot
	if idx := strings.IndexByte(pathExpr, '.'); idx != -1 && idx < end {
		end = idx
	}
	
	if end == 0 {
		return nil, "", NewPathError(pathExpr, "empty key")
	}
	
	key := pathExpr[:end]
	remaining := pathExpr[end:]
	
	// Check for wildcard
	if key == "*" {
		return &KeyLeg{Key: "*", Wildcard: true}, remaining, nil
	}
	
	return &KeyLeg{Key: key}, remaining, nil
}

// parseArrayLeg parses an array leg like [0], [*], [last], [0 to 2], [last-1 to last]
func parseArrayLeg(pathExpr string) (PathLeg, string, error) {
	if len(pathExpr) == 0 || pathExpr[0] != '[' {
		return nil, "", NewPathError(pathExpr, "expected '['")
	}
	
	// Find closing bracket
	closeIdx := strings.IndexByte(pathExpr, ']')
	if closeIdx == -1 {
		return nil, "", NewPathError(pathExpr, "missing closing ']'")
	}
	
	content := pathExpr[1:closeIdx]
	remaining := pathExpr[closeIdx+1:]
	
	// Check for wildcard
	if content == "*" {
		return &ArrayLeg{Index: -2, Wildcard: true}, remaining, nil
	}
	
	// Check for "last"
	if content == "last" {
		return &ArrayLeg{Index: -1, Last: true}, remaining, nil
	}
	
	// Check for range: "start to end"
	if strings.Contains(content, " to ") {
		return parseRangeLeg(content, remaining, pathExpr)
	}
	
	// Check for "last - N" or "last + N"
	if strings.HasPrefix(content, "last ") || strings.HasPrefix(content, "last+") {
		offsetStr := content[5:]
		offset, err := strconv.Atoi(offsetStr)
		if err != nil {
			return nil, "", NewPathError(pathExpr, fmt.Sprintf("invalid last offset: %s", offsetStr))
		}
		return &ArrayLeg{Index: -1, LastOffset: offset}, remaining, nil
	}
	
	// Parse as integer index
	index, err := strconv.Atoi(content)
	if err != nil {
		return nil, "", NewPathError(pathExpr, fmt.Sprintf("invalid array index: %s", content))
	}
	
	return &ArrayLeg{Index: index}, remaining, nil
}

// parseRangeLeg parses a range leg like "0 to 2", "last-1 to last"
func parseRangeLeg(content, remaining, fullExpr string) (PathLeg, string, error) {
	parts := strings.Split(content, " to ")
	if len(parts) != 2 {
		return nil, "", NewPathError(fullExpr, fmt.Sprintf("invalid range: %s", content))
	}
	
	startStr := strings.TrimSpace(parts[0])
	endStr := strings.TrimSpace(parts[1])
	
	// Handle start
	var start int
	var startLast bool
	if startStr == "last" {
		startLast = true
	} else {
		start, _ = strconv.Atoi(startStr)
	}
	
	// Handle end
	var end int
	var endLast bool
	if endStr == "last" {
		endLast = true
	} else {
		end, _ = strconv.Atoi(endStr)
	}
	
	return &RangeLeg{
		Start:      start,
		StartLast:  startLast,
		End:        end,
		EndLast:    endLast,
	}, remaining, nil
}

// KeyLeg represents a path leg accessing an object key
type KeyLeg struct {
	Key       string
	Wildcard  bool // true for *
	Recursive bool // true for **
}

// Apply applies the key leg to a JSON value
func (k *KeyLeg) Apply(bj BinaryJSON) ([]BinaryJSON, error) {
	// Handle wildcard
	if k.Wildcard {
		if !bj.IsObject() {
			return nil, &JSONError{Code: ErrTypeMismatch, Message: "cannot apply key leg to non-object"}
		}
		
		obj, _ := bj.GetObject()
		result := make([]BinaryJSON, 0, len(obj))
		for _, key := range sortedKeys(obj) {
			if val, ok := obj[key]; ok {
				parsed, err := NewBinaryJSON(val)
				if err != nil {
					return nil, err
				}
				result = append(result, parsed)
			}
		}
		return result, nil
	}
	
	// Handle regular key access
	if !bj.IsObject() {
		return nil, &JSONError{Code: ErrTypeMismatch, Message: "cannot apply key leg to non-object"}
	}
	
	obj, _ := bj.GetObject()
	if val, ok := obj[k.Key]; ok {
		parsed, err := NewBinaryJSON(val)
		if err != nil {
			return nil, err
		}
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

// ArrayLeg represents a path leg accessing an array index
type ArrayLeg struct {
	Index      int   // -1 for last, -2 for wildcard
	Last       bool  // true if using last keyword
	LastOffset int   // offset from last (e.g., last-1 => -1)
	Wildcard  bool  // true for *
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
			parsed, err := NewBinaryJSON(val)
			if err != nil {
				return nil, err
			}
			result = append(result, parsed)
		}
		return result, nil
	}
	
	// Calculate actual index
	index := a.Index
	if a.Last {
		index = arrLen - 1
	} else if a.LastOffset != 0 {
		index = arrLen + a.LastOffset
	}
	
	// Validate index
	if index < 0 || index >= arrLen {
		return nil, NewIndexError(index)
	}
	
	parsed, err := NewBinaryJSON(arr[index])
	if err != nil {
		return nil, err
	}
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

// RangeLeg represents a path leg accessing a range of array indices
type RangeLeg struct {
	Start      int
	StartLast  bool
	End        int
	EndLast    bool
}

// Apply applies the range leg to a JSON value
func (r *RangeLeg) Apply(bj BinaryJSON) ([]BinaryJSON, error) {
	if !bj.IsArray() {
		return nil, &JSONError{Code: ErrTypeMismatch, Message: "cannot apply range leg to non-array"}
	}
	
	arr, _ := bj.GetArray()
	arrLen := len(arr)
	
	// Calculate start index
	start := r.Start
	if r.StartLast {
		start = arrLen - 1
	}
	
	// Calculate end index
	end := r.End
	if r.EndLast {
		end = arrLen - 1
	}
	
	// Validate range
	if start < 0 || start >= arrLen || end < 0 || end >= arrLen {
		return nil, NewIndexError(start)
	}
	
	// Ensure start <= end
	if start > end {
		start, end = end, start
	}
	
	result := make([]BinaryJSON, 0, end-start+1)
	for i := start; i <= end; i++ {
		parsed, err := NewBinaryJSON(arr[i])
		if err != nil {
			return nil, err
		}
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
		
		if len(newResults) == 0 {
			return nil, NewNotFoundError(p.String())
		}
		
		results = newResults
	}
	
	return results, nil
}

// Extract extracts a single value from the JSON using the path
// Returns the first match if multiple matches exist
func (p *Path) Extract(bj BinaryJSON) (BinaryJSON, error) {
	results, err := p.Evaluate(bj)
	if err != nil {
		return BinaryJSON{}, err
	}
	
	if len(results) == 0 {
		return BinaryJSON{}, NewNotFoundError(p.String())
	}
	
	// Return first match
	return results[0], nil
}

// ExtractPath returns the path as a string
func (p *Path) ExtractPath(bj BinaryJSON) (string, error) {
	results, err := p.Evaluate(bj)
	if err != nil {
		return "", err
	}
	
	if len(results) == 0 {
		return "", NewNotFoundError(p.String())
	}
	
	// For single result, return path directly
	if len(results) == 1 {
		return p.String(), nil
	}
	
	// Multiple paths matched, return first one
	return p.String(), nil
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
		// Check if we need to add a dot before this leg
		switch leg.(type) {
		case *KeyLeg:
			// Dot is part of leg string for non-first keys
			if !strings.HasPrefix(legStr, "[") {
				builder.WriteString(".")
			}
		case *ArrayLeg, *RangeLeg:
			// Bracket is part of leg string
		}
		builder.WriteString(legStr)
	}
	
	return builder.String()
}

// CheckExists checks if the path exists in the JSON value
func (p *Path) CheckExists(bj BinaryJSON) bool {
	_, err := p.Evaluate(bj)
	return err == nil
}
