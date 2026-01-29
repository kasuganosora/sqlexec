package json

import (
	"fmt"
	"strings"

	"github.com/kasuganosora/sqlexec/pkg/builtin"
)

// Set replaces or inserts a value at the specified path
// Multiple (path, value) pairs can be specified
// Returns the modified JSON
func Set(bj BinaryJSON, args ...interface{}) (BinaryJSON, error) {
	if len(args) == 0 || len(args)%2 != 0 {
		return BinaryJSON{}, &JSONError{Code: ErrInvalidParam, Message: "JSON_SET requires even number of arguments"}
	}
	
	current := bj
	
	// Process each (path, value) pair
	for i := 0; i < len(args); i += 2 {
		pathStr := builtin.ToString(args[i])
		value := args[i+1]
		
		// Apply operation
		var err error
		current, err = current.Set(pathStr, value)
		if err != nil {
			return BinaryJSON{}, err
		}
	}
	
	return current, nil
}

// Insert inserts a value at the specified path
// Only creates new paths, doesn't modify existing ones
// Multiple (path, value) pairs can be specified
// Returns the modified JSON
func Insert(bj BinaryJSON, args ...interface{}) (BinaryJSON, error) {
	if len(args) == 0 || len(args)%2 != 0 {
		return BinaryJSON{}, &JSONError{Code: ErrInvalidParam, Message: "JSON_INSERT requires even number of arguments"}
	}
	
	current := bj
	
	// Process each (path, value) pair
	for i := 0; i < len(args); i += 2 {
		pathStr := builtin.ToString(args[i])
		value := args[i+1]
		
		// Apply operation
		var err error
		current, err = current.Insert(pathStr, value)
		if err != nil {
			return BinaryJSON{}, err
		}
	}
	
	return current, nil
}

// Replace replaces a value at the specified path
// Only modifies existing paths
// Multiple (path, value) pairs can be specified
// Returns the modified JSON
func Replace(bj BinaryJSON, args ...interface{}) (BinaryJSON, error) {
	if len(args) == 0 || len(args)%2 != 0 {
		return BinaryJSON{}, &JSONError{Code: ErrInvalidParam, Message: "JSON_REPLACE requires even number of arguments"}
	}
	
	current := bj
	
	// Process each (path, value) pair
	for i := 0; i < len(args); i += 2 {
		pathStr := builtin.ToString(args[i])
		value := args[i+1]
		
		// Apply operation
		var err error
		current, err = current.Replace(pathStr, value)
		if err != nil {
			return BinaryJSON{}, err
		}
	}
	
	return current, nil
}

// Remove removes values at the specified paths
// Multiple paths can be specified
// Returns the modified JSON
func Remove(bj BinaryJSON, args ...interface{}) (BinaryJSON, error) {
	if len(args) == 0 {
		return BinaryJSON{}, &JSONError{Code: ErrInvalidParam, Message: "JSON_REMOVE requires at least one argument"}
	}
	
	current := bj
	
	// Process each path
	for _, arg := range args {
		pathStr := builtin.ToString(arg)
		
		// Apply operation
		var err error
		current, err = current.Remove(pathStr)
		if err != nil {
			return BinaryJSON{}, err
		}
	}
	
	return current, nil
}

// MergePreserve merges multiple JSON values (JSON_MERGE_PRESERVE)
// For objects: merges all key-value pairs
// For arrays: concatenates arrays
// Returns the merged JSON
func MergePreserve(args ...interface{}) (BinaryJSON, error) {
	if len(args) == 0 {
		return BinaryJSON{TypeCode: TypeLiteral, Value: LiteralNull}, nil
	}
	
	// Parse all arguments
	parsedArgs := make([]BinaryJSON, 0, len(args))
	for i, arg := range args {
		parsed, err := NewBinaryJSON(arg)
		if err != nil {
			return BinaryJSON{}, err
		}
		parsedArgs[i] = parsed
	}
	
	// Start with first argument
	result := parsedArgs[0]
	
	// Merge each subsequent argument
	for i := 1; i < len(parsedArgs); i++ {
		var err error
		result, err = result.Merge(parsedArgs[i])
		if err != nil {
			return BinaryJSON{}, err
		}
	}
	
	return result, nil
}

// MergePatch merges JSON values using RFC 7396 (JSON_MERGE_PATCH)
// For objects: replaces recursively, null removes key
// For arrays: replaces completely
// Returns the patched JSON
func MergePatch(args ...interface{}) (BinaryJSON, error) {
	if len(args) == 0 {
		return BinaryJSON{TypeCode: TypeLiteral, Value: LiteralNull}, nil
	}
	
	// Parse all arguments
	parsedArgs := make([]BinaryJSON, 0, len(args))
	for i, arg := range args {
		parsed, err := NewBinaryJSON(arg)
		if err != nil {
			return BinaryJSON{}, err
		}
		parsedArgs[i] = parsed
	}
	
	// Start with first argument
	result := parsedArgs[0]
	
	// Patch each subsequent argument
	for i := 1; i < len(parsedArgs); i++ {
		var err error
		result, err = result.MergePatchWithOne(parsedArgs[i])
		if err != nil {
			return BinaryJSON{}, err
		}
	}
	
	return result, nil
}

// MergePatchWithOne patches the JSON with one other value
func (bj BinaryJSON) MergePatchWithOne(other BinaryJSON) (BinaryJSON, error) {
	// If other is null, result is null
	if other.IsNull() {
		return bj, nil
	}
	
	// If bj is not an object, just replace with other
	if !bj.IsObject() {
		return other, nil
	}
	
	// If other is not an object, bj remains unchanged
	if !other.IsObject() {
		return bj, nil
	}
	
	// Both are objects, perform RFC 7396 merge
	return bj.MergePatchObject(other)
}

// MergePatchObject performs RFC 7396 merge on two objects
func (bj BinaryJSON) MergePatchObject(other BinaryJSON) (BinaryJSON, error) {
	obj, _ := bj.GetObject()
	otherObj, _ := other.GetObject()
	
	// Create new merged object
	merged := make(map[string]interface{})
	
	// Copy all keys from bj
	for k, v := range obj {
		merged[k] = v
	}
	
	// Merge keys from other, overriding bj's values
	for k, v := range otherObj {
		if v.IsNull() {
			// null removes the key
			delete(merged, k)
		} else if _, exists := merged[k]; !exists {
			// New key, just add it
			merged[k] = v
		} else {
			// Existing key, replace with other's value
			// Check if we need to recursively merge
			if shouldMergeValues(merged[k], v) {
				// Recursively merge
				bjValue, _ := NewBinaryJSON(merged[k])
				otherValue, _ := NewBinaryJSON(v)
				mergedValue, err := bjValue.MergePatchWithOne(otherValue)
				if err != nil {
					return BinaryJSON{}, err
				}
				merged[k] = mergedValue.GetInterface()
			} else {
				// Replace with other's value
				merged[k] = v
			}
		}
	}
	
	return NewBinaryJSON(merged), nil
}

// shouldMergeValues determines if two values should be recursively merged
func shouldMergeValues(a, b interface{}) bool {
	// Both objects: merge
	objA, okA := a.(map[string]interface{})
	objB, okB := b.(map[string]interface{})
	if okA && okB {
		return true
	}
	
	// Both arrays: replace (not merge in patch mode)
	arrA, okA := a.([]interface{})
	arrB, okB := b.([]interface{})
	if okA && okB {
		return false
	}
	
	// Otherwise: don't recursively merge
	return false
}

// MemberOf checks if a value is a member of an array or object
// Returns 1 or 0 (for boolean context)
func MemberOf(target, container interface{}) (bool, error) {
	parsedTarget, err := NewBinaryJSON(target)
	if err != nil {
		return false, err
	}
	
	parsedContainer, err := NewBinaryJSON(container)
	if err != nil {
		return false, err
	}
	
	return containsValue(parsedContainer, parsedTarget)
}

// Overlaps checks if two JSON arrays have overlapping elements
// Returns 1 or 0 (for boolean context)
func Overlaps(a, b interface{}) (bool, error) {
	parsedA, err := NewBinaryJSON(a)
	if err != nil {
		return false, err
	}
	
	parsedB, err := NewBinaryJSON(b)
	if err != nil {
		return false, err
	}
	
	// Both must be arrays
	if !parsedA.IsArray() || !parsedB.IsArray() {
		return false, nil
	}
	
	return checkArrayOverlap(parsedA, parsedB)
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
