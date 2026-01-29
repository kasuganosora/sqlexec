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
	return bj.Remove(paths...)
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
		result, err = result.Merge(values[i])
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
		result, err = result.Patch(values[i])
		if err != nil {
			return BinaryJSON{}, err
		}
	}

	return result, nil
}
