package json

import "fmt"

// LiteralNull represents JSON null value (use nil literal in code)
// const LiteralNull = nil // nil cannot be used as a constant in Go

// TypeCode represents JSON type constants
type TypeCode byte

const (
	TypeLiteral   TypeCode = iota // Literal values: null, true, false
	TypeObject                 // JSON object
	TypeArray                  // JSON array
	TypeString                 // JSON string
	TypeInteger                // JSON integer
	TypeDouble                 // JSON number (double)
	TypeOpaque                 // Opaque/binary type
)

// BinaryJSON represents a JSON value in binary format
type BinaryJSON struct {
	TypeCode TypeCode
	Value    interface{}
}

// NewBinaryJSON creates a new BinaryJSON from a value
func NewBinaryJSON(value interface{}) (BinaryJSON, error) {
	if value == nil {
		return BinaryJSON{TypeCode: TypeLiteral, Value: nil}, nil
	}

	switch v := value.(type) {
	case string:
		return BinaryJSON{TypeCode: TypeString, Value: v}, nil
	case int:
		return BinaryJSON{TypeCode: TypeInteger, Value: int64(v)}, nil
	case int8:
		return BinaryJSON{TypeCode: TypeInteger, Value: int64(v)}, nil
	case int16:
		return BinaryJSON{TypeCode: TypeInteger, Value: int64(v)}, nil
	case int32:
		return BinaryJSON{TypeCode: TypeInteger, Value: int64(v)}, nil
	case int64:
		return BinaryJSON{TypeCode: TypeInteger, Value: v}, nil
	case uint:
		return BinaryJSON{TypeCode: TypeInteger, Value: int64(v)}, nil
	case uint8:
		return BinaryJSON{TypeCode: TypeInteger, Value: int64(v)}, nil
	case uint16:
		return BinaryJSON{TypeCode: TypeInteger, Value: int64(v)}, nil
	case uint32:
		return BinaryJSON{TypeCode: TypeInteger, Value: int64(v)}, nil
	case uint64:
		return BinaryJSON{TypeCode: TypeInteger, Value: int64(v)}, nil
	case float32:
		return BinaryJSON{TypeCode: TypeDouble, Value: float64(v)}, nil
	case float64:
		return BinaryJSON{TypeCode: TypeDouble, Value: v}, nil
	case bool:
		return BinaryJSON{TypeCode: TypeLiteral, Value: v}, nil
	case map[string]interface{}:
		return BinaryJSON{TypeCode: TypeObject, Value: v}, nil
	case []interface{}:
		return BinaryJSON{TypeCode: TypeArray, Value: v}, nil
	default:
		return BinaryJSON{TypeCode: TypeOpaque, Value: v}, nil
	}
}

// ParseJSON parses a JSON string into BinaryJSON
func ParseJSON(jsonStr string) (BinaryJSON, error) {
	if jsonStr == "" {
		return BinaryJSON{}, &JSONError{Code: ErrInvalidJSON, Message: "empty JSON string"}
	}

	// Use encoding/json to parse
	var value interface{}
	err := unmarshalJSON([]byte(jsonStr), &value)
	if err != nil {
		return BinaryJSON{}, err
	}

	return NewBinaryJSON(value)
}

// MarshalJSON converts BinaryJSON to JSON bytes
func (bj BinaryJSON) MarshalJSON() ([]byte, error) {
	return marshalJSON(bj.Value)
}

// String returns JSON string representation
func (bj BinaryJSON) String() string {
	if bj.IsNull() {
		return "null"
	}

	if bj.IsBoolean() {
		return fmt.Sprintf("%v", bj.Value)
	}

	data, err := bj.MarshalJSON()
	if err != nil {
		return ""
	}
	return string(data)
}

// IsNull checks if value is null
func (bj BinaryJSON) IsNull() bool {
	return bj.TypeCode == TypeLiteral && bj.Value == nil
}

// IsBoolean checks if value is boolean
func (bj BinaryJSON) IsBoolean() bool {
	if bj.TypeCode != TypeLiteral {
		return false
	}
	_, ok := bj.Value.(bool)
	return ok
}

// IsString checks if value is string
func (bj BinaryJSON) IsString() bool {
	return bj.TypeCode == TypeString
}

// IsNumber checks if value is a number
func (bj BinaryJSON) IsNumber() bool {
	return bj.TypeCode == TypeInteger || bj.TypeCode == TypeDouble
}

// IsObject checks if value is an object
func (bj BinaryJSON) IsObject() bool {
	return bj.TypeCode == TypeObject
}

// IsArray checks if value is an array
func (bj BinaryJSON) IsArray() bool {
	return bj.TypeCode == TypeArray
}

// Type returns MySQL-compatible type name
func (bj BinaryJSON) Type() string {
	switch bj.TypeCode {
	case TypeLiteral:
		if bj.IsNull() {
			return "NULL"
		}
		if bj.IsBoolean() {
			return "BOOLEAN"
		}
		return "OPAQUE"
	case TypeObject:
		return "OBJECT"
	case TypeArray:
		return "ARRAY"
	case TypeString:
		return "STRING"
	case TypeInteger:
		return "INTEGER"
	case TypeDouble:
		return "DOUBLE"
	default:
		return "UNKNOWN"
	}
}

// GetString returns string value
func (bj BinaryJSON) GetString() (string, error) {
	if !bj.IsString() {
		return "", &JSONError{Code: ErrTypeMismatch, Message: "value is not a string"}
	}
	s, ok := bj.Value.(string)
	if !ok {
		return "", &JSONError{Code: ErrInvalidType, Message: "invalid string value"}
	}
	return s, nil
}

// GetFloat64 returns float64 value
func (bj BinaryJSON) GetFloat64() (float64, error) {
	if !bj.IsNumber() {
		return 0, &JSONError{Code: ErrTypeMismatch, Message: "value is not a number"}
	}
	switch v := bj.Value.(type) {
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	case int:
		return float64(v), nil
	case int8:
		return float64(v), nil
	case int16:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case uint:
		return float64(v), nil
	case uint8:
		return float64(v), nil
	case uint16:
		return float64(v), nil
	case uint32:
		return float64(v), nil
	case uint64:
		return float64(v), nil
	default:
		return 0, &JSONError{Code: ErrInvalidType, Message: "invalid number value"}
	}
}

// GetInt64 returns int64 value
func (bj BinaryJSON) GetInt64() (int64, error) {
	if !bj.IsNumber() {
		return 0, &JSONError{Code: ErrTypeMismatch, Message: "value is not a number"}
	}
	switch v := bj.Value.(type) {
	case int:
		return int64(v), nil
	case int8:
		return int64(v), nil
	case int16:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case int64:
		return v, nil
	case uint:
		return int64(v), nil
	case uint8:
		return int64(v), nil
	case uint16:
		return int64(v), nil
	case uint32:
		return int64(v), nil
	case uint64:
		return int64(v), nil
	case float64:
		return int64(v), nil
	case float32:
		return int64(v), nil
	default:
		return 0, &JSONError{Code: ErrInvalidType, Message: "invalid number value"}
	}
}

// GetObject returns object value
func (bj BinaryJSON) GetObject() (map[string]interface{}, error) {
	if !bj.IsObject() {
		return nil, &JSONError{Code: ErrTypeMismatch, Message: "value is not an object"}
	}
	obj, ok := bj.Value.(map[string]interface{})
	if !ok {
		return nil, &JSONError{Code: ErrInvalidType, Message: "invalid object value"}
	}
	return obj, nil
}

// GetArray returns array value
func (bj BinaryJSON) GetArray() ([]interface{}, error) {
	if !bj.IsArray() {
		return nil, &JSONError{Code: ErrTypeMismatch, Message: "value is not an array"}
	}
	arr, ok := bj.Value.([]interface{})
	if !ok {
		return nil, &JSONError{Code: ErrInvalidType, Message: "invalid array value"}
	}
	return arr, nil
}

// GetInterface returns raw interface value
func (bj BinaryJSON) GetInterface() interface{} {
	return bj.Value
}

// Equals checks equality with another BinaryJSON
func (bj BinaryJSON) Equals(other BinaryJSON) bool {
	if bj.TypeCode != other.TypeCode {
		return false
	}
	return deepEqual(bj.Value, other.Value)
}

// deepEqual performs deep equality check
func deepEqual(a, b interface{}) bool {
	switch av := a.(type) {
	case map[string]interface{}:
		bv, ok := b.(map[string]interface{})
		if !ok || len(av) != len(bv) {
			return false
		}
		for k, v := range av {
			if !deepEqual(v, bv[k]) {
				return false
			}
		}
		return true
	case []interface{}:
		bv, ok := b.([]interface{})
		if !ok || len(av) != len(bv) {
			return false
		}
		for i := range av {
			if !deepEqual(av[i], bv[i]) {
				return false
			}
		}
		return true
	default:
		return a == b
	}
}
