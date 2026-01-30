package json

// Extract extracts a value from JSON using a path expression
func (bj BinaryJSON) Extract(pathStr string) (BinaryJSON, error) {
	if pathStr == "" {
		return bj, nil
	}

	// Parse path
	path, err := ParsePath(pathStr)
	if err != nil {
		return BinaryJSON{}, err
	}

	// Evaluate path
	return path.Extract(bj)
}

// Get extracts a value from JSON using a path
func (bj BinaryJSON) Get(pathStr string) (interface{}, error) {
	result, err := bj.Extract(pathStr)
	if err != nil {
		return nil, err
	}

	return result.GetInterface(), nil
}

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

// ensurePathExists ensures all intermediate paths in the path exist, creating them if necessary
func ensurePathExists(bj BinaryJSON, path *Path) (BinaryJSON, error) {
	result := bj
	current := bj

	for i := 0; i < len(path.Legs)-1; i++ {
		leg := path.Legs[i]
		results, err := leg.Apply(current)

		if err != nil {
			if jsonErr, ok := err.(*JSONError); ok && jsonErr.Code == ErrPathNotFound {
				// Path doesn't exist, create it
				if current.IsObject() {
					obj, _ := current.GetObject()
					if keyLeg, ok := leg.(*KeyLeg); ok && !keyLeg.Wildcard {
						// Create empty object
						newObj := make(map[string]interface{})
						obj[keyLeg.Key] = newObj
						current, _ = NewBinaryJSON(newObj)
						result, _ = NewBinaryJSON(obj)
					} else {
						return bj, err
					}
				} else {
					return bj, err
				}
			} else {
				return bj, err
			}
		} else if len(results) > 0 {
			// Path exists, move to the next level
			current = results[0]
		}
	}

	return result, nil
}

// applyPath is a legacy function kept for compatibility, but not used by Set
func applyPath(bj BinaryJSON, path *Path, value BinaryJSON, depth int) (BinaryJSON, error) {
	return bj, nil
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

// Remove removes values at the specified paths
func (bj BinaryJSON) Remove(paths ...string) (BinaryJSON, error) {
	if len(paths) == 0 {
		return bj, nil
	}

	result := bj
	for _, pathStr := range paths {
		path, err := ParsePath(pathStr)
		if err != nil {
			return BinaryJSON{}, err
		}
		result, err = removePath(result, path, 0)
		if err != nil {
			return BinaryJSON{}, err
		}
	}
	return result, nil
}

// deepCopy creates a deep copy of a value
func deepCopy(value interface{}) interface{} {
	switch v := value.(type) {
	case map[string]interface{}:
		newMap := make(map[string]interface{})
		for k, val := range v {
			newMap[k] = deepCopy(val)
		}
		return newMap
	case []interface{}:
		newArr := make([]interface{}, len(v))
		for i, val := range v {
			newArr[i] = deepCopy(val)
		}
		return newArr
	default:
		return v
	}
}

// removePath removes a path recursively - simplified version
func removePath(bj BinaryJSON, path *Path, depth int) (BinaryJSON, error) {
	if depth >= len(path.Legs) {
		return bj, nil
	}

	currentLeg := path.Legs[depth]
	isLastLeg := (depth == len(path.Legs)-1)

	// Handle object
	if bj.IsObject() {
		obj, _ := bj.GetObject()
		keyLeg, ok := currentLeg.(*KeyLeg)
		if !ok || keyLeg.Wildcard {
			return bj, nil
		}

		// Check if key exists
		if _, exists := obj[keyLeg.Key]; !exists {
			return bj, nil
		}

		if isLastLeg {
			// Delete the key
			newObj := make(map[string]interface{})
			for k, v := range obj {
				if k != keyLeg.Key {
					newObj[k] = deepCopy(v)
				}
			}
			return NewBinaryJSON(newObj)
		}

		// Not last leg - recurse into the value
		value := obj[keyLeg.Key]
		valueBj, _ := NewBinaryJSON(value)
		newValueBj, err := removePath(valueBj, path, depth+1)
		if err != nil {
			return BinaryJSON{}, err
		}

		// Rebuild the object with the new value
		newObj := make(map[string]interface{})
		for k, v := range obj {
			newObj[k] = deepCopy(v)
		}
		newObj[keyLeg.Key] = newValueBj.GetInterface()
		return NewBinaryJSON(newObj)
	}

	// Handle array
	if bj.IsArray() {
		arr, _ := bj.GetArray()
		arrayLeg, ok := currentLeg.(*ArrayLeg)
		if !ok || arrayLeg.Wildcard {
			return bj, nil
		}

		idx := arrayLeg.Index
		if arrayLeg.Last {
			idx = len(arr) - 1
		}

		if idx < 0 || idx >= len(arr) {
			return bj, nil
		}

		if isLastLeg {
			// Remove the element
			newArr := make([]interface{}, 0, len(arr)-1)
			newArr = append(newArr, arr[:idx]...)
			newArr = append(newArr, arr[idx+1:]...)
			return NewBinaryJSON(newArr)
		}

		// Not last leg - recurse into the element
		value := arr[idx]
		valueBj, _ := NewBinaryJSON(value)
		newValueBj, err := removePath(valueBj, path, depth+1)
		if err != nil {
			return BinaryJSON{}, err
		}

		// Rebuild the array with the new value
		newArr := make([]interface{}, len(arr))
		for i, v := range arr {
			newArr[i] = deepCopy(v)
		}
		newArr[idx] = newValueBj.GetInterface()
		return NewBinaryJSON(newArr)
	}

	return bj, nil
}

// Merge merges another JSON value (JSON_MERGE_PRESERVE)
func (bj BinaryJSON) Merge(value interface{}) (BinaryJSON, error) {
	// If value is a string, parse it as JSON
	if str, ok := value.(string); ok {
		parsed, err := ParseJSON(str)
		if err != nil {
			return BinaryJSON{}, err
		}
		return bj.Merge(parsed.GetInterface())
	}
	
	other, err := NewBinaryJSON(value)
	if err != nil {
		return BinaryJSON{}, err
	}

	// If bj is null, return other
	if bj.IsNull() {
		return other, nil
	}

	// If other is null, return bj
	if other.IsNull() {
		return bj, nil
	}

	// If both are objects, merge them (preserve values from first object)
	if bj.IsObject() && other.IsObject() {
		obj1, _ := bj.GetObject()
		obj2, _ := other.GetObject()
		merged := make(map[string]interface{})
		// Copy obj2 first
		for k, v := range obj2 {
			merged[k] = v
		}
		// Then copy obj1 to preserve its values (overwriting obj2's values)
		for k, v := range obj1 {
			merged[k] = v
		}
		return NewBinaryJSON(merged)
	}

	// If bj is array, append other to it
	if bj.IsArray() {
		arr, _ := bj.GetArray()
		if other.IsArray() {
			arr2, _ := other.GetArray()
			merged := make([]interface{}, 0, len(arr)+len(arr2))
			merged = append(merged, arr...)
			merged = append(merged, arr2...)
			return NewBinaryJSON(merged)
		}
		merged := make([]interface{}, 0, len(arr)+1)
		merged = append(merged, arr...)
		merged = append(merged, other.GetInterface())
		return NewBinaryJSON(merged)
	}

	// If other is array, wrap bj in array and append
	if other.IsArray() {
		merged := make([]interface{}, 0, 1+len(other.GetInterface().([]interface{})))
		merged = append(merged, bj.GetInterface())
		merged = append(merged, other.GetInterface().([]interface{})...)
		return NewBinaryJSON(merged)
	}

	// Otherwise, wrap both in array
	return NewBinaryJSON([]interface{}{bj.GetInterface(), other.GetInterface()})
}

// Patch patches with another JSON value (RFC 7396 JSON_MERGE_PATCH)
func (bj BinaryJSON) Patch(value interface{}) (BinaryJSON, error) {
	// If value is a string, parse it as JSON first
	if str, ok := value.(string); ok {
		parsed, err := ParseJSON(str)
		if err != nil {
			return BinaryJSON{}, err
		}
		return bj.Patch(parsed.GetInterface())
	}
	
	other, err := NewBinaryJSON(value)
	if err != nil {
		return BinaryJSON{}, err
	}

	// If other is null, delete bj (return null)
	if other.IsNull() {
		return BinaryJSON{TypeCode: TypeLiteral, Value: nil}, nil
	}

	// If bj is not an object, replace with other
	if !bj.IsObject() {
		return other, nil
	}

	// If other is not an object, replace bj with other
	if !other.IsObject() {
		return other, nil
	}

	// Both are objects - recursively patch
	obj1, _ := bj.GetObject()
	obj2, _ := other.GetObject()
	patched := make(map[string]interface{})

	// Copy all keys from bj first
	for k, v := range obj1 {
		patched[k] = v
	}

	// Apply patches from obj2
	for k, v := range obj2 {
		if v == nil {
			// Null value means delete the key
			delete(patched, k)
		} else {
			// Recursive patch for nested objects
			if existing, ok := patched[k]; ok {
				existingBJ, _ := NewBinaryJSON(existing)
				patchBJ, _ := NewBinaryJSON(v)
				merged, err := existingBJ.Patch(patchBJ.GetInterface())
				if err != nil {
					return BinaryJSON{}, err
				}
				patched[k] = merged.GetInterface()
			} else {
				patched[k] = v
			}
		}
	}

	return NewBinaryJSON(patched)
}
