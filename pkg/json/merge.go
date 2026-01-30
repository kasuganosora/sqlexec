package json

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
