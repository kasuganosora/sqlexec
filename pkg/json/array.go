package json

import (
	"github.com/kasuganosora/sqlexec/pkg/utils"
)

// ArrayAppend appends values to an array at the specified path
// Usage: ArrayAppend(bj, path, value1, value2, ...)
func ArrayAppend(bj BinaryJSON, args ...interface{}) (BinaryJSON, error) {
	if len(args) < 2 {
		return BinaryJSON{}, &JSONError{Code: ErrInvalidParam, Message: "ARRAY_APPEND requires at least 2 arguments (path and at least one value)"}
	}

	pathStr := toString(args[0])
	values := args[1:]

	// Parse path
	parsed, err := ParsePath(pathStr)
	if err != nil {
		return BinaryJSON{}, err
	}

	// Get the array to append to
	var arr []interface{}
	var parentBj BinaryJSON
	var lastLeg PathLeg

	if len(parsed.Legs) == 0 {
		// No path, treat entire bj as array
		if !bj.IsArray() {
			return BinaryJSON{}, &JSONError{Code: ErrTypeMismatch, Message: "value is not an array"}
		}
		arr, _ = bj.GetArray()
		parentBj = bj
	} else {
		// Get array at path
		// Extract parent path and last leg
		parentPath := &Path{Legs: parsed.Legs[:len(parsed.Legs)-1]}
		lastLeg = parsed.Legs[len(parsed.Legs)-1]

		parentBj, err = parentPath.Extract(bj)
		if err != nil {
			return BinaryJSON{}, err
		}

		// Get the array
		arrBj, err := lastLeg.Apply(parentBj)
		if err != nil {
			return BinaryJSON{}, err
		}
		if len(arrBj) != 1 || !arrBj[0].IsArray() {
			return BinaryJSON{}, &JSONError{Code: ErrTypeMismatch, Message: "target is not an array"}
		}
		arr, _ = arrBj[0].GetArray()
	}

	// Append all values
	newArr := make([]interface{}, len(arr))
	copy(newArr, arr)
	for _, value := range values {
		parsedValue, err := NewBinaryJSON(value)
		if err != nil {
			return BinaryJSON{}, err
		}
		newArr = append(newArr, parsedValue.Value)
	}

	// Create new array BinaryJSON
	newArrBj, err := NewBinaryJSON(newArr)
	if err != nil {
		return BinaryJSON{}, err
	}

	// If no path or path is just "$", return the array directly
	if len(parsed.Legs) == 0 {
		return newArrBj, nil
	}

	// Otherwise, reconstruct the object with the updated array
	return setRecursive(bj, parsed.Legs, newArrBj, 0)
}

// ArrayInsert inserts a value into an array at the specified path and position
func ArrayInsert(bj BinaryJSON, args ...interface{}) (BinaryJSON, error) {
	if len(args) < 2 || len(args) > 3 {
		return BinaryJSON{}, &JSONError{Code: ErrInvalidParam, Message: "ARRAY_INSERT requires 2 or 3 arguments"}
	}

	pathStr := toString(args[0])
	var position int

	// Parse arguments
	var valueArg interface{}
	if len(args) == 3 {
		// Format: (path, position, value)
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
		valueArg = args[2]
	} else {
		// Format: (path, value) - insert at end
		valueArg = args[1]
	}

	// Get target array
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
	if len(args) == 3 {
		if position < 0 {
			position = arrLen + position
		}
	} else {
		// Insert at end
		position = arrLen
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
	newArr := make([]interface{}, len(arr)+1)
	copy(newArr[:position], arr[:position])
	newArr[position] = parsedValue.Value
	copy(newArr[position+1:], arr[position:])

	// Create new BinaryJSON
	return NewBinaryJSON(newArr)
}

// ArrayGet gets an element from an array at the specified path and index
func ArrayGet(bj BinaryJSON, pathStr string, index int) (BinaryJSON, error) {
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

	return NewBinaryJSON(arr[index])
}

// toString converts any value to string (using utils package)
func toString(v interface{}) string {
	return utils.ToString(v)
}
