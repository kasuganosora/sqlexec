package json

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
