package json

import (
	"fmt"

	"github.com/kasuganosora/sqlexec/pkg/builtin"
)

// Extract extracts a value from JSON using a path expression
// Returns the first matching value
func (bj BinaryJSON) Extract(pathStr string) (BinaryJSON, error) {
	if pathStr == "" {
		return bj, nil
	}
	
	// Parse the path
	path, err := ParsePath(pathStr)
	if err != nil {
		return BinaryJSON{}, err
	}
	
	// Evaluate the path
	return path.Extract(bj)
}

// Get extracts a value from JSON using a path
// Shorthand for Extract
func (bj BinaryJSON) Get(pathStr string) (interface{}, error) {
	result, err := bj.Extract(pathStr)
	if err != nil {
		return nil, err
	}
	
	return result.GetInterface(), nil
}

// Set sets a value at the specified path
// Creates the path if it doesn't exist
func (bj BinaryJSON) Set(pathStr string, value interface{}) (BinaryJSON, error) {
	path, err := ParsePath(pathStr)
	if err != nil {
		return BinaryJSON{}, err
	}
	
	// Convert value to BinaryJSON
	parsedValue, err := NewBinaryJSON(value)
	if err != nil {
		return BinaryJSON{}, err
	}
	
	return SetPath(bj, path, parsedValue)
}

// SetPath sets a value at the specified path using parsed path
func SetPath(bj BinaryJSON, path *Path, value BinaryJSON) (BinaryJSON, error) {
	if len(path.Legs) == 0 {
		// No path, just replace entire value
		return value, nil
	}
	
	// Set by navigating through the path
	return setPathRecursive(bj, path, value, 0)
}

// setPathRecursive recursively sets a value at a path
func setPathRecursive(bj BinaryJSON, path *Path, value BinaryJSON, depth int) (BinaryJSON, error) {
	if depth >= len(path.Legs) {
		// Last leg reached, this is the leaf where we set the value
		return value, nil
	}
	
	leg := path.Legs[depth]
	results, err := leg.Apply(bj)
	if err != nil {
		return BinaryJSON{}, err
	}
	
	if len(results) == 0 {
		return BinaryJSON{}, NewNotFoundError(path.String())
	}
	
	// Multiple matches - need to set all of them
	newResults := make([]interface{}, 0, len(results))
	for i, result := range results {
		remainingPath := &Path{Legs: path.Legs[depth+1:]}
		newValue, err := setPathRecursive(result, remainingPath, value, depth+1)
		if err != nil {
			return BinaryJSON{}, err
		}
		newResults[i] = newValue.GetInterface()
	}
	
	// Reconstruct the parent based on results
	var parent interface{}
	switch bj.TypeCode {
	case TypeObject:
		obj, _ := bj.GetObject()
		parent = reconstructObject(obj, results, leg)
	case TypeArray:
		arr, _ := bj.GetArray()
		parent = reconstructArray(arr, results, leg)
	default:
		return BinaryJSON{}, &JSONError{Code: ErrTypeMismatch, Message: "can only set path in objects or arrays"}
	}
	
	return NewBinaryJSON(parent)
}

// reconstructObject reconstructs an object after setting a path leg
func reconstructObject(obj map[string]interface{}, results []BinaryJSON, leg PathLeg) map[string]interface{} {
	newObj := make(map[string]interface{})
	
	// Copy non-modified keys
	for k, v := range obj {
		newObj[k] = v
	}
	
	// Apply modifications based on leg type
	switch l := leg.(type) {
	case *KeyLeg:
		keyLeg := l.(*KeyLeg)
		if keyLeg.Wildcard {
			// Wildcard - set all matching keys
			for k := range obj {
				if val, ok := obj[k]; ok {
					newObj[k] = val
				}
			}
		} else {
			// Set specific key
			if len(results) > 0 {
				newObj[keyLeg.Key] = results[0].GetInterface()
			} else {
				delete(newObj, keyLeg.Key)
			}
		}
	case *ArrayLeg:
		// This shouldn't happen for object
		return obj
	case *RangeLeg:
		// This shouldn't happen for object
		return obj
	}
	
	return newObj
}

// reconstructArray reconstructs an array after setting a path leg
func reconstructArray(arr []interface{}, results []BinaryJSON, leg PathLeg) []interface{} {
	newArr := make([]interface{}, len(arr))
	copy(newArr, arr)
	
	switch l := leg.(type) {
	case *ArrayLeg:
		arrayLeg := l.(*ArrayLeg)
		if arrayLeg.Wildcard {
			// Wildcard - set all elements
			for i := range arr {
				if len(results) > 0 {
					newArr[i] = results[0].GetInterface()
				}
			}
		} else {
			// Set specific index
			idx := arrayLeg.Index
			if arrayLeg.Last {
				idx = len(arr) - 1
			}
			
			if idx >= 0 && idx < len(arr) {
				if len(results) > 0 {
					newArr[idx] = results[0].GetInterface()
				}
			}
		}
	case *RangeLeg:
		rangeLeg := l.(*RangeLeg)
		start, end := getRangeIndices(rangeLeg, len(arr))
		for i := start; i <= end; i++ {
			if i >= 0 && i < len(arr) && len(results) > 0 {
				newArr[i] = results[0].GetInterface()
			}
		}
	case *KeyLeg:
		// This shouldn't happen for array
		return arr
	}
	
	return newArr
}

// getRangeIndices calculates actual indices from a RangeLeg
func getRangeIndices(r *RangeLeg, arrLen int) (int, int) {
	start := r.Start
	if r.StartLast {
		start = arrLen - 1
	}
	
	end := r.End
	if r.EndLast {
		end = arrLen - 1
	}
	
	// Validate
	if start < 0 || start >= arrLen || end < 0 || end >= arrLen {
		return 0, 0
	}
	
	if start > end {
		start, end = end, start
	}
	
	return start, end
}

// Insert inserts a value at the specified path
// Only creates new paths, doesn't modify existing ones
func (bj BinaryJSON) Insert(pathStr string, value interface{}) (BinaryJSON, error) {
	path, err := ParsePath(pathStr)
	if err != nil {
		return BinaryJSON{}, err
	}
	
	parsedValue, err := NewBinaryJSON(value)
	if err != nil {
		return BinaryJSON{}, err
	}
	
	return InsertPath(bj, path, parsedValue)
}

// InsertPath inserts a value at the specified path
func InsertPath(bj BinaryJSON, path *Path, value BinaryJSON) (BinaryJSON, error) {
	if len(path.Legs) == 0 {
		return bj, nil // No path to insert into
	}
	
	// Check if path already exists
	_, err := path.Extract(bj)
	if err == nil {
		// Path exists, insertion should fail
		return bj, nil
	}
	
	// Create the new path
	return setPathRecursive(bj, path, value, 0)
}

// Replace replaces a value at the specified path
// Only modifies existing paths
func (bj BinaryJSON) Replace(pathStr string, value interface{}) (BinaryJSON, error) {
	path, err := ParsePath(pathStr)
	if err != nil {
		return BinaryJSON{}, err
	}
	
	parsedValue, err := NewBinaryJSON(value)
	if err != nil {
		return BinaryJSON{}, err
	}
	
	return ReplacePath(bj, path, parsedValue)
}

// ReplacePath replaces a value at the specified path
func ReplacePath(bj BinaryJSON, path *Path, value BinaryJSON) (BinaryJSON, error) {
	if len(path.Legs) == 0 {
		// No path, just replace entire value
		return value, nil
	}
	
	// Check if path exists
	_, err := path.Extract(bj)
	if err != nil {
		// Path doesn't exist, replacement should fail
		return bj, nil
	}
	
	// Replace the value
	return setPathRecursive(bj, path, value, 0)
}

// Remove removes a value at the specified path
func (bj BinaryJSON) Remove(pathStr string) (BinaryJSON, error) {
	path, err := ParsePath(pathStr)
	if err != nil {
		return BinaryJSON{}, err
	}
	
	return RemovePath(bj, path)
}

// RemovePath removes values at the specified path
func RemovePath(bj BinaryJSON, path *Path) (BinaryJSON, error) {
	if len(path.Legs) == 0 {
		return BinaryJSON{}, &JSONError{Code: ErrInvalidPath, Message: "cannot remove entire JSON"}
	}
	
	// Get the parent of the path
	parentPath := &Path{Legs: path.Legs[:len(path.Legs)-1]}
	lastLeg := path.Legs[len(path.Legs)-1]
	
	parentResults, err := parentPath.Evaluate(bj)
	if err != nil {
		return BinaryJSON{}, err
	}
	
	if len(parentResults) == 0 {
		return BinaryJSON{}, NewNotFoundError(parentPath.String())
	}
	
	// Remove from each matching parent
	var newBj BinaryJSON
	var removalErr error
	
	for _, parent := range parentResults {
		newBj, removalErr = removePathFromParent(parent, lastLeg)
		if removalErr != nil {
			return BinaryJSON{}, removalErr
		}
	}
	
	return newBj, nil
}

// removePathFromParent removes a value from a parent JSON value
func removePathFromParent(parent BinaryJSON, leg PathLeg) (BinaryJSON, error) {
	if parent.IsArray() {
		return removeFromArray(parent, leg)
	}
	if parent.IsObject() {
		return removeFromObject(parent, leg)
	}
	
	return BinaryJSON{}, &JSONError{Code: ErrTypeMismatch, Message: "can only remove from objects or arrays"}
}

// removeFromArray removes elements from an array
func removeFromArray(arr BinaryJSON, leg PathLeg) (BinaryJSON, error) {
	arrayData, _ := arr.GetArray()
	
	switch l := leg.(type) {
	case *ArrayLeg:
		arrayLeg := l.(*ArrayLeg)
		if arrayLeg.Wildcard {
			// Remove all elements
			return NewBinaryJSON([]interface{}{}), nil
		}
		
		// Remove specific index
		idx := arrayLeg.Index
		if arrayLeg.Last {
			idx = len(arrayData) - 1
		}
		
		if idx < 0 || idx >= len(arrayData) {
			return BinaryJSON{}, NewIndexError(idx)
		}
		
		newArr := make([]interface{}, 0, len(arrayData)-1)
		copy(newArr, arrayData[:idx])
		copy(newArr[idx:], arrayData[idx+1:])
		return NewBinaryJSON(newArr), nil
		
	case *RangeLeg:
		rangeLeg := l.(*RangeLeg)
		start, end := getRangeIndices(rangeLeg, len(arrayData))
		if start > end {
			start, end = end, start
		}
		
		if start < 0 || start >= len(arrayData) || end >= len(arrayData) {
			return BinaryJSON{}, NewIndexError(start)
		}
		
		removedCount := end - start + 1
		if removedCount >= len(arrayData) {
			return NewBinaryJSON([]interface{}{}), nil
		}
		
		newArr := make([]interface{}, len(arrayData)-removedCount)
		copy(newArr, arrayData[:start])
		copy(newArr[start:], arrayData[end+1:])
		return NewBinaryJSON(newArr), nil
		
	default:
		return BinaryJSON{}, &JSONError{Code: ErrInvalidPath, Message: "invalid leg type for array"}
	}
}

// removeFromObject removes a key from an object
func removeFromObject(obj BinaryJSON, leg PathLeg) (BinaryJSON, error) {
	objectData, _ := obj.GetObject()
	
	keyLeg, ok := leg.(*KeyLeg)
	if !ok {
		return BinaryJSON{}, &JSONError{Code: ErrInvalidPath, Message: "invalid leg type for object"}
	}
	
	if keyLeg.Wildcard {
		// Wildcard - remove all keys
		return NewBinaryJSON(map[string]interface{}{}), nil
	}
	
	// Remove specific key
	if _, exists := objectData[keyLeg.Key]; !exists {
		return BinaryJSON{}, NewKeyError(keyLeg.Key)
	}
	
	newObj := make(map[string]interface{})
	for k, v := range objectData {
		if k != keyLeg.Key {
			newObj[k] = v
		}
	}
	
	return NewBinaryJSON(newObj), nil
}

// Merge merges two JSON values (JSON_MERGE_PRESERVE)
func (bj BinaryJSON) Merge(other interface{}) (BinaryJSON, error) {
	parsedOther, err := NewBinaryJSON(other)
	if err != nil {
		return BinaryJSON{}, err
	}
	
	return mergeValues(bj, parsedOther, false)
}

// MergePatch merges two JSON values using RFC 7396 (JSON_MERGE_PATCH)
func (bj BinaryJSON) MergePatch(other interface{}) (BinaryJSON, error) {
	parsedOther, err := NewBinaryJSON(other)
	if err != nil {
		return BinaryJSON{}, err
	}
	
	return mergeValues(bj, parsedOther, true)
}

// mergeValues merges two JSON values
func mergeValues(a, b BinaryJSON, isPatch bool) (BinaryJSON, error) {
	// Handle null values
	if a.IsNull() {
		return b, nil
	}
	if b.IsNull() {
		return a, nil
	}
	
	// Handle primitive types
	if !a.IsObject() && !a.IsArray() {
		if isPatch {
			return a, nil // Patch doesn't replace non-objects
		}
		return b, nil // Preserve takes b when a is not an object
	}
	
	if !b.IsObject() && !b.IsArray() {
		if isPatch {
			return a, nil // Patch doesn't replace non-objects
		}
		return a, nil // Preserve takes a when b is not an object
	}
	
	objA, _ := a.GetObject()
	objB, _ := b.GetObject()
	
	// Merge objects
	merged := make(map[string]interface{})
	
	// Copy all keys from A
	for k, v := range objA {
		merged[k] = v
	}
	
	// Merge keys from B
	for k, v := range objB {
		if existing, ok := merged[k]; ok {
			// Key exists in both
			if isPatch {
				// Patch mode: replace with B's value
				merged[k] = v
			} else {
				// Preserve mode: merge arrays/objects
				if shouldMerge(existing, v) {
					merged[k] = mergeValues(
						BinaryJSON{Value: existing},
						BinaryJSON{Value: v},
						false,
					).Value
				} else {
					// Replace primitives with B's value
					merged[k] = v
				}
			}
		} else {
			// New key from B
			merged[k] = v
		}
	}
	
	return NewBinaryJSON(merged), nil
}

// shouldMerge determines if two values should be merged
func shouldMerge(a, b interface{}) bool {
	if a == nil {
		return true
	}
	if b == nil {
		return true
	}
	
	// Both are objects: merge
	objA, okA := a.(map[string]interface{})
	objB, okB := b.(map[string]interface{})
	if okA && okB {
		return true
	}
	
	// Both are arrays: merge
	arrA, okA := a.([]interface{})
	arrB, okB := b.([]interface{})
	if okA && okB {
		return true
	}
	
	// Otherwise: don't merge, just replace
	return false
}

// Equal checks if two JSON values are equal
func (bj BinaryJSON) Equal(other interface{}) bool {
	parsedOther, err := NewBinaryJSON(other)
	if err != nil {
		return false
	}
	
	return equalValues(bj.Value, parsedOther.Value)
}

// equalValues recursively compares two JSON values
func equalValues(a, b interface{}) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	
	switch av := a.(type) {
	case bool:
		bv, ok := b.(bool)
		return ok && av == bv
	case float64:
		bv, ok := b.(float64)
		return ok && av == bv
	case int64:
		bv, ok := b.(int64)
		return ok && av == bv
	case string:
		bv, ok := b.(string)
		return ok && av == bv
	case map[string]interface{}:
		bv, ok := b.(map[string]interface{})
		if !ok {
			return false
		}
		if len(av) != len(bv) {
			return false
		}
		for k, v := range av {
			if !equalValues(v, bv[k]) {
				return false
			}
		}
		return true
	case []interface{}:
		bv, ok := b.([]interface{})
		if !ok {
			return false
		}
		if len(av) != len(bv) {
			return false
		}
		for i := range av {
			if !equalValues(av[i], bv[i]) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

// Copy creates a deep copy of the JSON value
func (bj BinaryJSON) Copy() BinaryJSON {
	if bj.IsNull() {
		return bj, nil
	}
	
	// Create a copy by marshaling and unmarshaling
	data, err := bj.MarshalJSON()
	if err != nil {
		return bj, err
	}
	
	var copy BinaryJSON
	err = copy.UnmarshalJSON(data)
	if err != nil {
		return bj, err
	}
	
	return copy, nil
}

// ToSQLValue converts BinaryJSON to a format suitable for SQL results
func (bj BinaryJSON) ToSQLValue() interface{} {
	if bj.IsNull() {
		return nil
	}
	
	// For arrays and objects, return the underlying interface
	if bj.IsArray() || bj.IsObject() {
		return bj.Value
	}
	
	// For primitives, return the value directly
	return bj.Value
}

// Compare compares two JSON values
// Returns: 0 if equal, -1 if a < b, 1 if a > b
func Compare(a, b interface{}) int {
	// Normalize to BinaryJSON
	bjA, errA := NewBinaryJSON(a)
	if errA != nil {
		// Use direct comparison for error cases
		bjB, errB := NewBinaryJSON(b)
		if errB != nil {
			return 0
		}
		return compareNative(a, bjB.Value)
	}
	
	bjB, errB := NewBinaryJSON(b)
	if errB != nil {
		return compareNative(bjA.Value, b)
	}
	
	return compareNative(bjA.Value, bjB.Value)
}

// compareNative compares two native Go values
func compareNative(a, b interface{}) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1 // null < any value
	}
	if b == nil {
		return 1 // any value > null
	}
	
	// Get type order
	typeOrder := getTypeOrder(a) - getTypeOrder(b)
	if typeOrder != 0 {
		return typeOrder
	}
	
	// Same type, compare values
	return compareSameType(a, b)
}

// getTypeOrder returns the type ordering for comparison
func getTypeOrder(v interface{}) int {
	switch v.(type) {
	case bool:
		return 4
	case []interface{}:
		return 3
	case map[string]interface{}:
		return 2
	case string:
		return 1
	case float64, int64:
		return 0
	default:
		return 0
	}
}

// compareSameType compares two values of the same type
func compareSameType(a, b interface{}) int {
	switch av := a.(type) {
	case float64:
		bv, _ := b.(float64)
		if av == bv {
			return 0
		}
		if av < bv {
			return -1
		}
		return 1
	case int64:
		bv, _ := b.(int64)
		if av == bv {
			return 0
		}
		if av < bv {
			return -1
		}
		return 1
	case string:
		bv, _ := b.(string)
		if av == bv {
			return 0
		}
		if av < bv {
			return -1
		}
		return 1
	case bool:
		bv, _ := b.(bool)
		if av == bv {
			return 0
		}
		if !av && bv {
			return -1
		}
		if av && !bv {
			return 1
		}
		return 0
	case []interface{}:
		bv, _ := b.([]interface{})
		return compareArrays(av, bv)
	case map[string]interface{}:
		bv, _ := b.(map[string]interface{})
		return compareObjects(av, bv)
	}
	return 0
}

// compareArrays compares two JSON arrays
func compareArrays(a, b []interface{}) int {
	minLen := len(a)
	if len(b) < minLen {
		minLen = len(b)
	}
	
	for i := 0; i < minLen; i++ {
		cmp := compareNative(a[i], b[i])
		if cmp != 0 {
			return cmp
		}
	}
	
	return len(a) - len(b)
}

// compareObjects compares two JSON objects
func compareObjects(a, b map[string]interface{}) int {
	// Objects are equal if they have same key-value pairs
	if len(a) != len(b) {
		return len(a) - len(b)
	}
	
	// Compare keys in sorted order
	keys := sortedKeys(a)
	for _, k := range keys {
		valA, okA := a[k]
		valB, okB := b[k]
		
		if !okA || !okB {
			return len(a) - len(b)
		}
		
		cmp := compareNative(valA, valB)
		if cmp != 0 {
			return cmp
		}
	}
	
	return 0
}
