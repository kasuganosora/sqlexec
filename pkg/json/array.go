package json

import (
	"fmt"

	"github.com/kasuganosora/sqlexec/pkg/builtin"
)

// ArrayAppend appends a value to the end of an array at the specified path
// Multiple (path, value) pairs can be specified
func ArrayAppend(bj BinaryJSON, args ...interface{}) (BinaryJSON, error) {
	if len(args) == 0 || len(args)%2 != 0 {
		return BinaryJSON{}, &JSONError{Code: ErrInvalidParam, Message: "ARRAY_APPEND requires even number of arguments"}
	}
	
	current := bj
	
	// Process each (path, value) pair
	for i := 0; i < len(args); i += 2 {
		pathStr := builtin.ToString(args[i])
		value := args[i+1]
		
		// Parse the path
		path, err := ParsePath(pathStr)
		if err != nil {
			return BinaryJSON{}, err
		}
		
		// Get the array to append to
		if len(path.Legs) == 0 {
			// No path, treat entire bj as array
			if !current.IsArray() {
				return BinaryJSON{}, &JSONError{Code: ErrTypeMismatch, Message: "value is not an array"}
			}
		} else {
			// Get array at path
			target, err := current.Extract(pathStr)
			if err != nil {
				return BinaryJSON{}, err
			}
			if !target.IsArray() {
				return BinaryJSON{}, &JSONError{Code: ErrTypeMismatch, Message: "target is not an array"}
			}
			current = target
		}
		
		// Parse the value to append
		parsedValue, err := NewBinaryJSON(value)
		if err != nil {
			return BinaryJSON{}, err
		}
		
		// Append to array
		if !current.IsArray() {
			return BinaryJSON{}, &JSONError{Code: ErrTypeMismatch, Message: "cannot append to non-array"}
		}
		
		arr, _ := current.GetArray()
		newArr := append(arr, parsedValue.Value)
		
		// Create new BinaryJSON with appended array
		current, _ = NewBinaryJSON(newArr)
	}
	
	return current, nil
}

// ArrayInsert inserts a value into an array at the specified path and position
// Format: JSON_ARRAY_INSERT(json_doc, path, pos, value)
func ArrayInsert(bj BinaryJSON, args ...interface{}) (BinaryJSON, error) {
	if len(args) < 2 || len(args) > 3 {
		return BinaryJSON{}, &JSONError{Code: ErrInvalidParam, Message: "ARRAY_INSERT requires 2 or 3 arguments"}
	}
	
	pathStr := builtin.ToString(args[0])
	var position int
	
	// Parse position if provided
	if len(args) == 3 {
		posArg := args[1]
		switch v := posArg.(type) {
		case float64:
			position = int(v)
		case int64:
			position = int(v)
		case int:
			position = v
		default:
			return BinaryJSON{}, &JSONError{Code: ErrInvalidParam, Message: "position must be a number"}
		}
		valueArg := args[2]
		
		// Get target array
		path, err := ParsePath(pathStr)
		if err != nil {
			return BinaryJSON{}, err
		}
		
		target, err := bj.Extract(pathStr)
		if err != nil {
			return BinaryJSON{}, err
		}
		
		if !target.IsArray() {
			return BinaryJSON{}, &JSONError{Code: ErrTypeMismatch, Message: "target is not an array"}
		}
		
		arr, _ := target.GetArray()
		arrLen := len(arr)
		
		// Handle negative position (from end)
		if position < 0 {
			position = arrLen + position
		}
		
		// Validate position
		if position < 0 || position > arrLen {
			return BinaryJSON{}, NewIndexError(position)
		}
		
		// Parse value to insert
		parsedValue, err := NewBinaryJSON(valueArg)
		if err != nil {
			return BinaryJSON{}, err
		}
		
		// Insert value
		newArr := make([]interface{}, 0, len(arr)+1)
		copy(newArr[:position], arr[:position])
		newArr[position] = parsedValue.Value
		copy(newArr[position+1:], arr[position:])
		
		// Create new BinaryJSON
		return NewBinaryJSON(newArr), nil
	}
}

// ArrayGet gets an element from an array at the specified path and index
// Format: JSON_ARRAY_GET(json_doc, path, index)
func ArrayGet(bj BinaryJSON, pathStr string, index int) (BinaryJSON, error) {
	path, err := ParsePath(pathStr)
	if err != nil {
		return BinaryJSON{}, err
	}
	
	// Get array at path
	target, err := bj.Extract(pathStr)
	if err != nil {
		return BinaryJSON{}, err
	}
	
	if !target.IsArray() {
		return BinaryJSON{}, &JSONError{Code: ErrTypeMismatch, Message: "target is not an array"}
	}
	
	arr, _ := target.GetArray()
	arrLen := len(arr)
	
	// Handle negative index (from end)
	if index < 0 {
		index = arrLen + index
	}
	
	// Validate index
	if index < 0 || index >= arrLen {
		return BinaryJSON{}, NewIndexError(index)
	}
	
	return NewBinaryJSON(arr[index]), nil
}
