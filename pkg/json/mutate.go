package json

// Set sets a value at specified path
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
		// Last leg - set value using helper functions
		if bj.IsObject() {
			obj, _ := bj.GetObject()
			newObj := reconstructObject(obj, value.GetInterface(), leg)
			return NewBinaryJSON(newObj)
		} else if bj.IsArray() {
			arr, _ := bj.GetArray()
			newArr := reconstructArray(arr, value.GetInterface(), leg)
			return NewBinaryJSON(newArr)
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
					// Create empty nested object and recurse into it
					emptyObj := make(map[string]interface{})
					emptyBj, _ := NewBinaryJSON(emptyObj)
					result, err := setRecursive(emptyBj, legs, value, depth+1)
					if err != nil {
						return BinaryJSON{}, err
					}
					// Reconstruct parent with the nested result
					parentObj := make(map[string]interface{})
					for k, v := range obj {
						parentObj[k] = v
					}
					parentObj[keyLeg.Key] = result.GetInterface()
					return NewBinaryJSON(parentObj)
				}
			}
		}
		return BinaryJSON{}, err
	}

	if len(results) == 0 {
		return BinaryJSON{}, NewNotFoundError("path not found")
	}

	// Path exists, recurse into first result
	newResult, err := setRecursive(results[0], legs, value, depth+1)
	if err != nil {
		return BinaryJSON{}, err
	}

	// Reconstruct parent with modified child using helper functions
	if bj.IsObject() {
		obj, _ := bj.GetObject()
		newObj := reconstructObject(obj, newResult.GetInterface(), leg)
		return NewBinaryJSON(newObj)
	} else if bj.IsArray() {
		arr, _ := bj.GetArray()
		newArr := reconstructArray(arr, newResult.GetInterface(), leg)
		return NewBinaryJSON(newArr)
	}

	return bj, nil
}

// Insert inserts a value at specified path (only if path doesn't exist)
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

// Replace replaces a value at specified path (only if path exists)
func (bj BinaryJSON) Replace(pathStr string, value interface{}) (BinaryJSON, error) {
	// Check if path exists
	_, err := bj.Extract(pathStr)
	if err != nil {
		return BinaryJSON{}, &JSONError{Code: ErrPathNotFound, Message: "path does not exist"}
	}

	// Use Set for replacement
	return bj.Set(pathStr, value)
}

// This file contains the reconstruct helper functions used by mutate operations.
// These functions are kept separate from the main operation files to avoid circular
// dependencies and to keep the codebase organized by functionality.

// reconstructObject reconstructs an object after applying a path leg
func reconstructObject(obj map[string]interface{}, value interface{}, leg PathLeg) map[string]interface{} {
	newObj := make(map[string]interface{})

	// Copy all non-modified keys
	for k, v := range obj {
		newObj[k] = v
	}

	// Apply modifications
	switch l := leg.(type) {
	case *KeyLeg:
		keyLeg := l
		if keyLeg.Wildcard {
			// Wildcard - apply to all keys
			if value != nil {
				for k := range obj {
					newObj[k] = value
				}
			}
		} else {
			// Set specific key
			if value != nil {
				newObj[keyLeg.Key] = value
			} else {
				delete(newObj, keyLeg.Key)
			}
		}
	default:
		// For other leg types, just return original
		return obj
	}

	return newObj
}

// reconstructArray reconstructs an array after applying a path leg
func reconstructArray(arr []interface{}, value interface{}, leg PathLeg) []interface{} {
	newArr := make([]interface{}, len(arr))
	copy(newArr, arr)

	// Apply modifications
	switch l := leg.(type) {
	case *ArrayLeg:
		arrayLeg := l
		if arrayLeg.Wildcard {
			// Wildcard - apply to all elements
			if value != nil {
				for i := range arr {
					newArr[i] = value
				}
			}
		} else {
			// Set specific index
			idx := arrayLeg.Index
			if arrayLeg.Last {
				idx = len(arr) - 1
			}

			if idx >= 0 && idx < len(arr) {
				if value != nil {
					newArr[idx] = value
				}
			}
		}
	case *RangeLeg:
		rangeLeg := l
		start, end := getRangeIndices(rangeLeg, len(arr))
		for i := start; i <= end; i++ {
			if i >= 0 && i < len(arr) && value != nil {
				newArr[i] = value
			}
		}
	default:
		// For other leg types, just return original
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

	return start, end
}
