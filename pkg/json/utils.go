package json

import (
	"fmt"
	"strings"

	"github.com/kasuganosora/sqlexec/pkg/builtin"
)

// Length returns the length of JSON value
// For arrays: returns element count
// For objects: returns key count
// For strings: returns character count
// For others: returns 1
func Length(bj BinaryJSON) (int, error) {
	if bj.IsNull() {
		return 0, nil
	}
	
	switch bj.TypeCode {
	case TypeArray:
		arr, _ := bj.GetArray()
		return len(arr), nil
		
	case TypeObject:
		obj, _ := bj.GetObject()
		return len(obj), nil
		
	case TypeString:
		str, _ := bj.GetString()
		return len(str), nil
		
	default:
		return 1, nil
	}
}

// Depth returns the maximum depth of JSON value
// Scalar values have depth 1
func Depth(bj BinaryJSON) error {
	if bj.IsNull() {
		return 0, nil
	}
	
	return calculateDepth(bj.Value, 1)
}

// calculateDepth recursively calculates the maximum depth
func calculateDepth(value interface{}, currentDepth int) error {
	if value == nil {
		return currentDepth, nil
	}
	
	switch v := value.(type) {
	case map[string]interface{}:
		maxDepth := currentDepth
		for _, val := range v {
			d, err := calculateDepth(val, currentDepth+1)
			if err != nil {
				return err
			}
			if d > maxDepth {
				maxDepth = d
			}
		}
		return maxDepth, nil
		
	case []interface{}:
		if len(v) == 0 {
			return currentDepth, nil
		}
		maxDepth := currentDepth
		for _, val := range v {
			d, err := calculateDepth(val, currentDepth+1)
			if err != nil {
				return err
			}
			if d > maxDepth {
				maxDepth = d
			}
		}
		return maxDepth, nil
		
	default:
		return currentDepth, nil
	}
}

// Keys returns the keys of a JSON object
// Returns error if value is not an object
func Keys(bj BinaryJSON) (BinaryJSON, error) {
	if !bj.IsObject() {
		return BinaryJSON{}, &JSONError{Code: ErrTypeMismatch, Message: "JSON_KEYS() requires a JSON object"}
	}
	
	obj, _ := bj.GetObject()
	keyArray := make([]interface{}, 0, len(obj))
	
	for k := range obj {
		keyArray = append(keyArray, k)
	}
	
	return NewBinaryJSON(keyArray), nil
}

// Contains checks if target JSON is contained in source JSON
// For objects: checks if all key-value pairs of target exist in source
// For arrays: checks if target element exists in source array
// For scalars: performs value equality
func Contains(source, target interface{}) (bool, error) {
	parsedSource, err := NewBinaryJSON(source)
	if err != nil {
		return false, err
	}
	
	parsedTarget, err := NewBinaryJSON(target)
	if err != nil {
		return false, err
	}
	
	return containsValue(parsedSource, parsedTarget)
}

// containsValue recursively checks if target is contained in source
func containsValue(source, target BinaryJSON) bool {
	if source.IsNull() {
		return false
	}
	
	// Array: check if any element equals target
	if source.IsArray() {
		arr, _ := source.GetArray()
		for _, elem := range arr {
			elemJSON, _ := NewBinaryJSON(elem)
			if containsValue(elemJSON, target) {
				return true
			}
		}
		return false
	}
	
	// Object: check if target is a subset of source
	if source.IsObject() {
		return containsObject(source, target)
	}
	
	// Primitive: direct equality
	return Equal(source, target)
}

// containsObject checks if target object is contained in source object
func containsObject(source, target BinaryJSON) bool {
	if !target.IsObject() {
		return false
	}
	
	targetObj, _ := target.GetObject()
	sourceObj, _ := source.GetObject()
	
	// Check if all target key-value pairs exist in source
	for tk, tv := range targetObj {
		sv, ok := sourceObj[tk]
		if !ok {
			return false
		}
		
		svJSON, _ := NewBinaryJSON(sv)
		tvJSON, _ := NewBinaryJSON(tv)
		
		if !Equal(svJSON, tvJSON) {
			return false
		}
	}
	
	return true
}

// ContainsPath checks if any of the specified paths exist in JSON
// oneOrAll: 'one' returns true if at least one path exists, 'all' requires all paths
func ContainsPath(bj BinaryJSON, oneOrAll string, paths ...string) (bool, error) {
	if len(paths) == 0 {
		return false, &JSONError{Code: ErrInvalidParam, Message: "no paths specified"}
	}
	
	pathsLower := strings.ToLower(oneOrAll)
	requireAll := pathsLower == "all"
	
	if requireAll {
		// All paths must exist
		for _, pathStr := range paths {
			path, err := ParsePath(pathStr)
			if err != nil {
				return false, err
			}
			
			if !path.CheckExists(bj) {
				return false, nil
			}
		}
		return true, nil
	}
	
	// At least one path must exist
	for _, pathStr := range paths {
		path, err := ParsePath(pathStr)
		if err != nil {
			return false, err
		}
		
		if path.CheckExists(bj) {
			return true, nil
		}
	}
	
	return false, nil
}

// Search searches for a string in JSON values
// Returns a JSON array of paths to matching strings
// oneOrAll: 'one' returns first match, 'all' returns all matches
func Search(bj BinaryJSON, searchStr, oneOrAll string, pathStr ...string) (BinaryJSON, error) {
	// Parse search string
	var searchPattern interface{}
	if err := parseSearchString(searchStr, &searchPattern); err != nil {
		return BinaryJSON{}, err
	}
	
	// Get search paths
	var searchPaths []*Path
	if len(pathStr) == 0 {
		// No path specified, search entire document
		searchPaths = append(searchPaths, &Path{Legs: []PathLeg{&KeyLeg{Key: "*", Wildcard: true}}})
	} else {
		// Parse search paths
		for _, p := range pathStr {
			path, err := ParsePath(p)
			if err != nil {
				return BinaryJSON{}, err
			}
			searchPaths = append(searchPaths, path)
		}
	}
	
	// Perform search
	matches := performSearch(bj, searchPattern, searchPaths, oneOrAll == "all")
	
	// Convert matches to JSON paths
	resultPaths := make([]interface{}, 0, len(matches))
	for _, match := range matches {
		resultPaths = append(resultPaths, match.Path)
	}
	
	return NewBinaryJSON(resultPaths), nil
}

// parseSearchString parses the search string pattern
func parseSearchString(searchStr string, pattern *interface{}) error {
	searchStr = strings.TrimSpace(searchStr)
	
	// Handle literal string
	if len(searchStr) > 1 && searchStr[0] == '"' && searchStr[len(searchStr)-1] == '"' {
		*pattern = strings.Trim(searchStr, "\"")
		return nil
	}
	
	// Handle % pattern
	if strings.Contains(searchStr, "%") {
		*pattern = strings.ReplaceAll(searchStr, "%", "*")
		return nil
	}
	
	// Default: treat as literal
	*pattern = searchStr
	return nil
}

// searchMatch represents a search result
type searchMatch struct {
	Path  string
	Value string
}

// performSearch performs the actual search
func performSearch(bj BinaryJSON, pattern interface{}, paths []*Path, findAll bool) []searchMatch {
	var matches []searchMatch
	
	for _, path := range paths {
		match := searchInPath(bj, pattern, path)
		if match.Value != "" {
			matches = append(matches, match)
			if !findAll {
				return matches
			}
		}
	}
	
	return matches
}

// searchInPath searches for pattern in a specific path
func searchInPath(bj BinaryJSON, pattern interface{}, path *Path) searchMatch {
	results, err := path.Evaluate(bj)
	if err != nil {
		return searchMatch{}
	}
	
	for _, result := range results {
		value, err := result.GetString()
		if err != nil {
			// Not a string, skip
			continue
		}
		
		if matchesPattern(value, pattern) {
			return searchMatch{
				Path:  path.String(),
				Value: value,
			}
		}
	}
	
	return searchMatch{}
}

// matchesPattern checks if a value matches the search pattern
func matchesPattern(value string, pattern interface{}) bool {
	switch p := pattern.(type) {
	case string:
		// Exact match
		return value == p
		
	case []interface{}:
		// % wildcard pattern
		if len(p) != 1 {
			return false
		}
		wildcard := p[0].(string)
		return strings.Contains(value, wildcard)
		
	default:
		// Try exact match
		strPattern, _ := builtin.ToString(pattern)
		return value == strPattern
	}
}

// StorageSize returns the storage size of JSON value in bytes
// This is an estimate for the MySQL-compatible storage format
func StorageSize(bj BinaryJSON) (int, error) {
	data, err := bj.MarshalJSON()
	if err != nil {
		return 0, err
	}
	
	return len(data), nil
}

// Pretty returns a formatted JSON string with indentation
func Pretty(bj BinaryJSON) (string, error) {
	data, err := bj.MarshalJSON()
	if err != nil {
		return "", err
	}
	
	// Pretty-print with 2-space indentation
	return prettyPrint(data, 0), nil
}

// prettyPrint formats JSON with indentation
func prettyPrint(data []byte, indent int) string {
	var builder strings.Builder
	currentIndent := indent
	
	for i := 0; i < len(data); i++ {
		ch := data[i]
		
		switch ch {
		case '{':
			builder.WriteString("{")
			currentIndent += 2
			writeIndent(&builder, currentIndent)
			builder.WriteString("\n")
			
		case '}':
			currentIndent -= 2
			builder.WriteString("\n")
			writeIndent(&builder, currentIndent)
			builder.WriteString("}")
			
		case '[':
			builder.WriteString("[")
			currentIndent += 2
			builder.WriteString("\n")
			writeIndent(&builder, currentIndent)
			
		case ']':
			currentIndent -= 2
			builder.WriteString("\n")
			writeIndent(&builder, currentIndent)
			builder.WriteString("]")
			
		case ',':
			builder.WriteString(",")
			builder.WriteString("\n")
			writeIndent(&builder, currentIndent)
			
		case '"':
			builder.WriteString("\"")
			// Read string literal
			j := i + 1
			for j < len(data) && data[j] != '"' {
				if data[j] == '\\' {
					// Escaped character
					if j+1 < len(data) {
						builder.WriteRune(rune(data[j]))
						builder.WriteRune(rune(data[j+1]))
						j++
					} else {
						builder.WriteRune(rune(data[j]))
					}
				} else {
					builder.WriteRune(rune(data[j]))
				}
				j++
			}
			if j < len(data) {
				builder.WriteString("\"")
			}
			i = j - 1
			
		case ':':
			builder.WriteString(": ")
			
		default:
			if !isWhitespace(ch) {
				builder.WriteByte(ch)
			}
		}
	}
	
	return builder.String()
}

// writeIndent writes indentation spaces to builder
func writeIndent(builder *strings.Builder, count int) {
	for i := 0; i < count; i++ {
		builder.WriteByte(' ')
	}
}

// isWhitespace checks if a character is whitespace
func isWhitespace(ch byte) bool {
	return ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r'
}

// Type returns the MySQL-compatible type name of a value
func Type(value interface{}) string {
	if value == nil {
		return "NULL"
	}
	
	bj, err := NewBinaryJSON(value)
	if err != nil {
		return "UNKNOWN"
	}
	
	return bj.Type()
}

// Valid checks if a value is valid JSON
func Valid(value interface{}) bool {
	_, err := NewBinaryJSON(value)
	return err == nil
}

// Quote adds JSON escaping to a string
func Quote(str string) string {
	// Use JSON marshaling to add proper escaping
	data, _ := NewBinaryJSON(str).MarshalJSON()
	return string(data)
}

// Unquote removes JSON escaping from a string
func Unquote(str string) (string, error) {
	// Try to parse as JSON
	bj, err := ParseJSON(str)
	if err != nil {
		return "", err
	}
	
	// Return the unquoted string
	return bj.GetString()
}
