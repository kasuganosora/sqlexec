package json

// ArrayAppend appends values to an array at the specified path
func ArrayAppend(bj BinaryJSON, args ...interface{}) (BinaryJSON, error) {
	if len(args) == 0 || len(args)%2 != 0 {
		return BinaryJSON{}, &JSONError{Code: ErrInvalidParam, Message: "ARRAY_APPEND requires even number of arguments"}
	}

	current := bj

	// Process each (path, value) pair
	for i := 0; i < len(args); i += 2 {
		pathStr := toString(args[i])
		value := args[i+1]

		parsed, err := ParsePath(pathStr)
		if err != nil {
			return BinaryJSON{}, err
		}

		// Get array to append to
		if len(parsed.Legs) == 0 {
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

		// Parse value to append
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
func ArrayInsert(bj BinaryJSON, args ...interface{}) (BinaryJSON, error) {
	if len(args) < 2 || len(args) > 3 {
		return BinaryJSON{}, &JSONError{Code: ErrInvalidParam, Message: "ARRAY_INSERT requires 2 or 3 arguments"}
	}

	pathStr := toString(args[0])
	var position int

	// Parse arguments
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
		valueArg := args[2]
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
	newArr := make([]interface{}, 0, len(arr)+1)
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

// toString converts any value to string
func toString(v interface{}) string {
	switch val := v.(type) {
	case string:
		return val
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", val)
	case float32, float64:
		return fmt.Sprintf("%f", val)
	case bool:
		return fmt.Sprintf("%t", val)
	default:
		return fmt.Sprintf("%v", val)
	}
}
