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
