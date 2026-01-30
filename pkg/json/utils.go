package json

import (
	"strings"
)

// parseJSONValue parses a value that can be either a JSON string or a Go interface{}
// This unifies parameter handling for JSON functions
func parseJSONValue(value interface{}) (BinaryJSON, error) {
	if str, ok := value.(string); ok {
		return ParseJSON(str)
	}
	return NewBinaryJSON(value)
}

// Length returns the length of JSON value
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
func Depth(bj BinaryJSON) (int, error) {
	if bj.IsNull() {
		return 1, nil
	}
	return calculateDepth(bj.Value, 1)
}

// calculateDepth recursively calculates the maximum depth
func calculateDepth(value interface{}, currentDepth int) (int, error) {
	if value == nil {
		return currentDepth, nil
	}

	switch v := value.(type) {
	case map[string]interface{}:
		maxDepth := currentDepth
		for _, val := range v {
			depth, err := calculateDepth(val, currentDepth+1)
			if err != nil {
				return 0, err
			}
			if depth > maxDepth {
				maxDepth = depth
			}
		}
		return maxDepth, nil
	case []interface{}:
		if len(v) == 0 {
			return currentDepth, nil
		}
		maxDepth := currentDepth
		for _, val := range v {
			depth, err := calculateDepth(val, currentDepth+1)
			if err != nil {
				return 0, err
			}
			if depth > maxDepth {
				maxDepth = depth
			}
		}
		return maxDepth, nil
	default:
		return currentDepth, nil
	}
}

// Keys returns the keys of a JSON object
func Keys(bj BinaryJSON) (BinaryJSON, error) {
	if !bj.IsObject() {
		return BinaryJSON{}, &JSONError{Code: ErrTypeMismatch, Message: "JSON_KEYS() requires a JSON object"}
	}

	obj, _ := bj.GetObject()
	keyArray := make([]interface{}, 0, len(obj))

	for k := range obj {
		keyArray = append(keyArray, k)
	}

	return NewBinaryJSON(keyArray)
}

// Extract extracts a value using a path
func Extract(bj BinaryJSON, path string) (BinaryJSON, error) {
	return bj.Extract(path)
}

// Contains checks if target JSON is contained in source JSON
func Contains(source, target interface{}) (bool, error) {
	parsedSource, err := parseJSONValue(source)
	if err != nil {
		return false, err
	}

	parsedTarget, err := parseJSONValue(target)
	if err != nil {
		return false, err
	}

	return containsValue(parsedSource, parsedTarget), nil
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
			if elemJSON.Equals(target) {
				return true
			}
		}
		return false
	}

	// Object: check if target is a subset of source OR target is a key or value
	if source.IsObject() {
		// First, check if target is a subset (for JSON_CONTAINS semantics)
		if target.IsObject() {
			if containsObject(source, target) {
				return true
			}
		}

		// Then check if target is a key or value (for JSON_MEMBER_OF semantics)
		obj, _ := source.GetObject()
		for key, value := range obj {
			// Check if target equals a key (for string targets)
			if target.IsString() {
				keyJSON, _ := NewBinaryJSON(key)
				if keyJSON.Equals(target) {
					return true
				}
			}
			// Check if target equals a value
			valueJSON, _ := NewBinaryJSON(value)
			if valueJSON.Equals(target) {
				return true
			}
		}
		return false
	}

	// Primitive: direct equality
	return source.Equals(target)
}

// containsObject checks if target object is contained in source object
func containsObject(source, target BinaryJSON) bool {
	if !target.IsObject() {
		return false
	}

	targetObj, _ := target.GetObject()
	sourceObj, _ := source.GetObject()

	for tk, tv := range targetObj {
		sv, ok := sourceObj[tk]
		if !ok {
			return false
		}
		svJSON, _ := NewBinaryJSON(sv)
		tvJSON, _ := NewBinaryJSON(tv)
		if !svJSON.Equals(tvJSON) {
			return false
		}
	}

	return true
}

// ContainsPath checks if any of the specified paths exist in JSON
func ContainsPath(bj BinaryJSON, oneOrAll string, paths ...string) (bool, error) {
	if len(paths) == 0 {
		return false, &JSONError{Code: ErrInvalidParam, Message: "no paths specified"}
	}

	requireAll := strings.ToLower(oneOrAll) == "all"

	if requireAll {
		// All paths must exist
		for _, pathStr := range paths {
			parsed, err := ParsePath(pathStr)
			if err != nil {
				return false, err
			}
			if !parsed.CheckExists(bj) {
				return false, nil
			}
		}
		return true, nil
	}

	// At least one path must exist
	for _, pathStr := range paths {
		parsed, err := ParsePath(pathStr)
		if err != nil {
			return false, err
		}
		if parsed.CheckExists(bj) {
			return true, nil
		}
	}

	return false, nil
}

// StorageSize returns the storage size of JSON value in bytes
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
	return prettyPrintJSON(data), nil
}

// prettyPrintJSON formats JSON with 2-space indentation
func prettyPrintJSON(data []byte) string {
	var builder strings.Builder
	indent := 0
	inString := false

	for i := 0; i < len(data); i++ {
		ch := data[i]

		if ch == '"' {
			if inString {
				builder.WriteByte('\\')
			}
			builder.WriteByte(ch)
			inString = true
			continue
		}

		if ch == '\\' && i+1 < len(data) {
			builder.WriteByte(ch)
			next := data[i+1]
			if next == 'n' || next == 'r' || next == 't' || next == 'b' || next == 'f' || next == '"' || next == '\\' {
				builder.WriteByte(next)
				i++
			} else {
				builder.WriteByte('\n')
				for j := 0; j < indent; j++ {
					builder.WriteByte(' ')
				}
				builder.WriteByte(ch)
			}
			continue
		}

		inString = false
		builder.WriteByte(ch)

		switch ch {
		case '{', '[':
			indent += 2
		case '}', ']':
			indent -= 2
		case '\n':
			for j := 0; j < indent; j++ {
				builder.WriteByte(' ')
			}
		}
	}

	return builder.String()
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
	data, _ := NewBinaryJSON(str)
	jsonBytes, _ := data.MarshalJSON()
	return string(jsonBytes)
}

// Unquote removes JSON escaping from a string
func Unquote(str string) (string, error) {
	bj, err := ParseJSON(str)
	if err != nil {
		return "", err
	}
	return bj.GetString()
}

// MemberOf checks if a value is a member of an array or object
func MemberOf(target, container interface{}) (bool, error) {
	parsedTarget, err := parseJSONValue(target)
	if err != nil {
		return false, err
	}

	parsedContainer, err := parseJSONValue(container)
	if err != nil {
		return false, err
	}

	return containsValue(parsedContainer, parsedTarget), nil
}

// Overlaps checks if two JSON arrays have overlapping elements
func Overlaps(a, b interface{}) (bool, error) {
	parsedA, err := parseJSONValue(a)
	if err != nil {
		return false, err
	}

	parsedB, err := parseJSONValue(b)
	if err != nil {
		return false, err
	}

	if !parsedA.IsArray() || !parsedB.IsArray() {
		return false, nil
	}

	return checkArrayOverlap(parsedA, parsedB), nil
}

// checkArrayOverlap checks if two arrays have common elements
func checkArrayOverlap(a, b BinaryJSON) bool {
	arrA, _ := a.GetArray()
	arrB, _ := b.GetArray()

	// Create a set for elements in A
	setA := make(map[interface{}]bool)
	for _, elem := range arrA {
		setA[elem] = true
	}

	// Check if any element in B exists in A
	for _, elem := range arrB {
		if setA[elem] {
			return true
		}
	}

	return false
}
