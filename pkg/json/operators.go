package json

// Set replaces or inserts a value at the specified path
func Set(bj BinaryJSON, path string, value interface{}) (BinaryJSON, error) {
	return bj.Set(path, value)
}

// Insert inserts a value at the specified path
// Only creates new paths, doesn't modify existing ones
func Insert(bj BinaryJSON, path string, value interface{}) (BinaryJSON, error) {
	return bj.Insert(path, value)
}

// Replace replaces a value at the specified path
// Only modifies existing paths
func Replace(bj BinaryJSON, path string, value interface{}) (BinaryJSON, error) {
	return bj.Replace(path, value)
}

// Remove removes values at the specified paths
func Remove(bj BinaryJSON, paths ...string) (BinaryJSON, error) {
	return applyRemove(bj, paths...)
}

// applyRemovePath applies a single path removal
func applyRemovePath(bj BinaryJSON, pathStr string) (BinaryJSON, error) {
	parsed, parseErr := ParsePath(pathStr)
	if parseErr != nil {
		return BinaryJSON{}, parseErr
	}

	// Get parent path
	// Remove last leg to get parent
	if len(parsed.Legs) == 0 {
		return bj, nil
	}

	parentPath := &Path{Legs: parsed.Legs[:len(parsed.Legs)-1]}
	results, evalErr := parentPath.Evaluate(bj)
	if evalErr != nil {
		return BinaryJSON{}, evalErr
	}

	if len(results) == 0 {
		return BinaryJSON{}, nil
	}

	// Remove key/index at parent level
	lastLeg := parsed.Legs[len(parsed.Legs)-1]

	var newParent interface{}
	if bj.IsObject() {
		obj, _ := bj.GetObject()
		if leg, ok := lastLeg.(*KeyLeg); ok {
			newObj := make(map[string]interface{})
			for k, v := range obj {
				if k != leg.Key {
					newObj[k] = v
				}
			}
			newParent = newObj
		}
	} else if bj.IsArray() {
		arr, _ := bj.GetArray()
		if leg, ok := lastLeg.(*ArrayLeg); ok {
			idx := leg.Index
			if leg.Last {
				idx = len(arr) - 1
			}
			newArr := make([]interface{}, 0, len(arr))
			for i, v := range arr {
				if i != idx {
					newArr = append(newArr, v)
				}
			}
			newParent = newArr
		} else if leg, ok := lastLeg.(*RangeLeg); ok {
			// Remove range - set to empty
			newParent = []interface{}{}
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
		val, parseErr := NewBinaryJSON(values[i])
		if parseErr != nil {
			return BinaryJSON{}, parseErr
		}
		result, mergeErr := mergeTwo(result, val)
		if mergeErr != nil {
			return BinaryJSON{}, mergeErr
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
		patch, parseErr := NewBinaryJSON(values[i])
		if parseErr != nil {
			return BinaryJSON{}, parseErr
		}
		result, mergeErr := patchOne(result, patch)
		if mergeErr != nil {
			return BinaryJSON{}, mergeErr
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
		result, _ := NewBinaryJSON(merged)
		return result, nil
	}

	// Both arrays: concatenate
	if a.IsArray() && b.IsArray() {
		arrA, _ := a.GetArray()
		arrB, _ := b.GetArray()
		merged := append(arrA, arrB...)
		result, _ := NewBinaryJSON(merged)
		return result, nil
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
