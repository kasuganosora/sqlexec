package json

// Set sets a value at the specified path
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

	// Use simplified recursive logic
	return setRecursive(bj, path.Legs, parsedValue, 0)
}

// setRecursive is a recursive helper that takes legs array directly
func setRecursive(bj BinaryJSON, legs []PathLeg, value BinaryJSON, depth int) (BinaryJSON, error) {
	if depth >= len(legs) {
		return value, nil
	}

	leg := legs[depth]
	isLast := (depth == len(legs)-1)

	if isLast {
		// Last leg - set the value
		if bj.IsObject() {
			obj, _ := bj.GetObject()
			if keyLeg, ok := leg.(*KeyLeg); ok && !keyLeg.Wildcard {
				obj[keyLeg.Key] = value.GetInterface()
				return NewBinaryJSON(obj)
			}
		} else if bj.IsArray() {
			arr, _ := bj.GetArray()
			if arrayLeg, ok := leg.(*ArrayLeg); ok && !arrayLeg.Wildcard {
				idx := arrayLeg.Index
				if arrayLeg.Last {
					idx = len(arr) - 1
				}
				if idx >= 0 && idx < len(arr) {
					arr[idx] = value.GetInterface()
					return NewBinaryJSON(arr)
				}
			}
		}
		return BinaryJSON{}, &JSONError{Code: ErrTypeMismatch, Message: "cannot set value at path"}
	}

	// Not last - navigate or create
	results, err := leg.Apply(bj)
	if err != nil {
		if jsonErr, ok := err.(*JSONError); ok && jsonErr.Code == ErrPathNotFound {
			// Path doesn't exist, create it
			if bj.IsObject() {
				obj, _ := bj.GetObject()
				if keyLeg, ok := leg.(*KeyLeg); ok && !keyLeg.Wildcard {
					// Create nested object
					newObj := make(map[string]interface{})
					obj[keyLeg.Key] = newObj
					newBj, _ := NewBinaryJSON(newObj)
					// Recurse into the new object with remaining legs
					return setRecursive(newBj, legs, value, depth+1)
				}
			}
		}
		return BinaryJSON{}, err
	}

	if len(results) == 0 {
		return BinaryJSON{}, NewNotFoundError("path not found")
	}

	// Path exists, recurse into the first result
	newResult, err := setRecursive(results[0], legs, value, depth+1)
	if err != nil {
		return BinaryJSON{}, err
	}

	// Reconstruct the parent with the modified child
	if bj.IsObject() {
		obj, _ := bj.GetObject()
		if keyLeg, ok := leg.(*KeyLeg); ok && !keyLeg.Wildcard {
			obj[keyLeg.Key] = newResult.GetInterface()
		}
		return NewBinaryJSON(obj)
	} else if bj.IsArray() {
		arr, _ := bj.GetArray()
		if arrayLeg, ok := leg.(*ArrayLeg); ok && !arrayLeg.Wildcard {
			idx := arrayLeg.Index
			if arrayLeg.Last {
				idx = len(arr) - 1
			}
			if idx >= 0 && idx < len(arr) {
				arr[idx] = newResult.GetInterface()
			}
		}
		return NewBinaryJSON(arr)
	}

	return bj, nil
}

// Insert inserts a value at the specified path (only if path doesn't exist)
func (bj BinaryJSON) Insert(pathStr string, value interface{}) (BinaryJSON, error) {
	_, err := ParsePath(pathStr)
	if err != nil {
		return BinaryJSON{}, err
	}

	// Check if path exists
	_, err = bj.Extract(pathStr)
	if err == nil {
		return BinaryJSON{}, &JSONError{Code: ErrPathExists, Message: "path already exists"}
	}

	// Use Set for insertion
	parsedValue, err := NewBinaryJSON(value)
	if err != nil {
		return BinaryJSON{}, err
	}

	return bj.Set(pathStr, parsedValue.Value)
}

// Replace replaces a value at the specified path (only if path exists)
func (bj BinaryJSON) Replace(pathStr string, value interface{}) (BinaryJSON, error) {
	// Check if path exists
	_, err := bj.Extract(pathStr)
	if err != nil {
		return BinaryJSON{}, &JSONError{Code: ErrPathNotFound, Message: "path does not exist"}
	}

	// Use Set for replacement
	return bj.Set(pathStr, value)
}
