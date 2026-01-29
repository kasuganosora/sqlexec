package json

// Set replaces or inserts a value at the specified path
func Set(bj BinaryJSON, path string, value interface{}) (BinaryJSON, error) {
	return bj.Set(path, value)
}

// Insert inserts a value at the specified path
// Only creates new paths, doesn't modify existing ones
func Insert(bj BinaryJSON, path string, value interface{}) (BinaryJSON, error) {
	parsed, err := ParsePath(path)
	if err != nil {
		return BinaryJSON{}, err
	}

	// Check if path exists
	results, err := parsed.Evaluate(bj)
	if err != nil {
		return BinaryJSON{}, err
	}

	// If path exists, don't insert (per JSON_INSERT semantics)
	if len(results) > 0 {
		return bj, nil
	}

	// Path doesn't exist, insert it
	return bj.Set(path, value)
}

// Replace replaces a value at the specified path
// Only modifies existing paths
func Replace(bj BinaryJSON, path string, value interface{}) (BinaryJSON, error) {
	parsed, err := ParsePath(path)
	if err != nil {
		return BinaryJSON{}, err
	}

	// Check if path exists
	results, err := parsed.Evaluate(bj)
	if err != nil {
		return BinaryJSON{}, err
	}

	// If path doesn't exist, don't replace (per JSON_REPLACE semantics)
	if len(results) == 0 {
		return bj, nil
	}

	// Path exists, replace it
	parsedValue, err := NewBinaryJSON(value)
	if err != nil {
		return BinaryJSON{}, err
	}
	return applyPath(bj, parsed, parsedValue, 0)
}

// Remove removes values at the specified paths
func Remove(bj BinaryJSON, paths ...string) (BinaryJSON, error) {
	current := bj

	for _, path := range paths {
		current, err = applyRemovePath(current, path)
		if err != nil {
			return BinaryJSON{}, err
		}
	}

	return current, nil
}

// applyRemovePath removes a single path
func applyRemovePath(bj BinaryJSON, pathStr string) (BinaryJSON, error) {
	parsed, err := ParsePath(pathStr)
	if err != nil {
		return BinaryJSON{}, err
	}

	// Get parent path
	// Remove the last leg to get parent
	if len(parsed.Legs) == 0 {
		return bj, nil
	}

	parentPath := &Path{Legs: parsed.Legs[:len(parsed.Legs)-1]}
	results, err := parentPath.Evaluate(bj)
	if err != nil {
		return BinaryJSON{}, err
	}

	if len(results) == 0 {
		return BinaryJSON{}, nil
	}

	// Remove the key/index at parent level
	lastLeg := parsed.Legs[len(parsed.Legs)-1]

	parent := results[0]
	var newParent interface{}

	switch parent.TypeCode {
	case TypeObject:
		obj, _ := parent.GetObject()
		if leg, ok := lastLeg.(*KeyLeg); ok {
			newObj := make(map[string]interface{})
			for k, v := range obj {
				if k != leg.Key {
					newObj[k] = v
				}
			}
			newParent = newObj
		}
	case TypeArray:
		arr, _ := parent.GetArray()
		if leg, ok := lastLeg.(*ArrayLeg); ok {
			idx := leg.Index
			if leg.Last {
				idx = len(arr) - 1
			}
			newArr := make([]interface{}, 0)
			for i, v := range arr {
				if i != idx {
					newArr = append(newArr, v)
				}
			}
			newParent = newArr
		} else if leg, ok := lastLeg.(*RangeLeg); ok {
			// Remove range
			_ = leg
			newParent = []interface{}{}  // Empty array
		}
	}

	result, _ := NewBinaryJSON(newParent)
	return result, nil
}

// MergePreserve merges multiple JSON values (JSON_MERGE_PRESERVE)
func MergePreserve(values ...interface{}) (BinaryJSON, error) {
	if len(values) == 0 {
		return BinaryJSON{}, &JSONError{Code: ErrInvalidParam, Message: "MERGE_PRESERVE requires at least one argument"}
	}

	// Start with first value
	result, err := NewBinaryJSON(values[0])
	if err != nil {
		return BinaryJSON{}, err
	}

	// Merge each subsequent value
	for i := 1; i < len(values); i++ {
		val, err := NewBinaryJSON(values[i])
		if err != nil {
			return BinaryJSON{}, err
		}
		result, err = mergeTwo(result, val)
		if err != nil {
			return BinaryJSON{}, err
		}
	}

	return result, nil
}

// MergePatch merges JSON values using RFC 7396 (JSON_MERGE_PATCH)
func MergePatch(values ...interface{}) (BinaryJSON, error) {
	if len(values) == 0 {
		return BinaryJSON{}, &JSONError{Code: ErrInvalidParam, Message: "MERGE_PATCH requires at least one argument"}
	}

	// Start with first value
	result, err := NewBinaryJSON(values[0])
	if err != nil {
		return BinaryJSON{}, err
	}

	// Patch each subsequent value
	for i := 1; i < len(values); i++ {
		patch, err := NewBinaryJSON(values[i])
		if err != nil {
			return BinaryJSON{}, err
		}
		result, err = patchOne(result, patch)
		if err != nil {
			return BinaryJSON{}, err
		}
	}

	return result, nil
}

// mergeTwo merges two JSON values (for MERGE_PRESERVE)
func mergeTwo(a, b BinaryJSON) (BinaryJSON, error) {
	if a.IsNull() {
		return b, nil
	}
	if b.IsNull() {
		return a, nil
	}

	// Both objects: merge all key-value pairs
	if a.IsObject() && b.IsObject() {
		objA, _ := a.GetObject()
		objB, _ := b.GetObject()
		merged := make(map[string]interface{})
		for k, v := range objA {
			merged[k] = v
		}
		for k, v := range objB {
			merged[k] = v
		}
		return NewBinaryJSON(merged)
	}

	// Both arrays: concatenate
	if a.IsArray() && b.IsArray() {
		arrA, _ := a.GetArray()
		arrB, _ := b.GetArray()
		merged := append(arrA, arrB...)
		return NewBinaryJSON(merged)
	}

	// Different types or scalars: b replaces a
	return b, nil
}

// patchOne patches JSON with another value (RFC 7396)
func patchOne(target, patch BinaryJSON) (BinaryJSON, error) {
	// If patch is null, result is null
	if patch.IsNull() {
		return BinaryJSON{}, nil
	}

	// If target is not an object, replace completely
	if !target.IsObject() {
		return patch, nil
	}

	// If patch is not an object, target remains unchanged
	if !patch.IsObject() {
		return target, nil
	}

	// Both are objects - perform RFC 7396 merge
	return patchObject(target, patch)
}

// patchObject patches an object with another object
func patchObject(target, patch BinaryJSON) (BinaryJSON, error) {
	targetObj, _ := target.GetObject()
	patchObj, _ := patch.GetObject()

	merged := make(map[string]interface{})
	for k, v := range targetObj {
		merged[k] = v
	}

	for k, v := range patchObj {
		if v.IsNull() {
			// null removes the key
			delete(merged, k)
		} else {
			merged[k] = v
		}
	}

	result, _ := NewBinaryJSON(merged)
	return result, nil
}
