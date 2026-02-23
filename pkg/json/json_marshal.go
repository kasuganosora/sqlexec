package json

import (
	"encoding/json"
)

// unmarshalJSON wraps encoding/json.Unmarshal
func unmarshalJSON(data []byte, value *interface{}) error {
	return json.Unmarshal(data, value)
}

// marshalJSON wraps encoding/json.Marshal
func marshalJSON(value interface{}) ([]byte, error) {
	return json.Marshal(value)
}
