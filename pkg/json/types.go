package json

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"

	"github.com/kasuganosora/sqlexec/pkg/builtin"
)

// JSONTypeCode represents the type code of a JSON value in binary format
// Compatible with MySQL 8.4 JSON binary format
type JSONTypeCode byte

const (
	TypeObject JSONTypeCode = 0x01 // large JSON object
	TypeArray  JSONTypeCode = 0x03 // large JSON array
	TypeLiteral JSONTypeCode = 0x04 // literal (true/false/null)
	TypeInt16   JSONTypeCode = 0x05
	TypeUint16  JSONTypeCode = 0x06
	TypeInt32   JSONTypeCode = 0x07
	TypeUint32  JSONTypeCode = 0x08
	TypeInt64   JSONTypeCode = 0x09
	TypeUint64  JSONTypeCode = 0x0A
	TypeDouble  JSONTypeCode = 0x0B
	TypeString  JSONTypeCode = 0x0C
)

// JSONLiteral represents literal values (true, false, null)
type JSONLiteral byte

const (
	LiteralNull JSONLiteral = 0x00
	LiteralTrue JSONLiteral = 0x01
	LiteralFalse JSONLiteral = 0x02
)

// BinaryJSON represents a JSON value in binary format
// This allows efficient random access without full deserialization
type BinaryJSON struct {
	TypeCode JSONTypeCode
	Value    interface{} // Using Go native types for simplicity
}

// NewBinaryJSON creates a BinaryJSON from Go native types
func NewBinaryJSON(data interface{}) (BinaryJSON, error) {
	if data == nil {
		return BinaryJSON{TypeCode: TypeLiteral, Value: LiteralNull}, nil
	}

	switch v := data.(type) {
	case bool:
		if v {
			return BinaryJSON{TypeCode: TypeLiteral, Value: LiteralTrue}, nil
		}
		return BinaryJSON{TypeCode: TypeLiteral, Value: LiteralFalse}, nil
	case int:
		return BinaryJSON{TypeCode: TypeInt64, Value: int64(v)}, nil
	case int8:
		return BinaryJSON{TypeCode: TypeInt16, Value: int16(v)}, nil
	case int16:
		return BinaryJSON{TypeCode: TypeInt16, Value: v}, nil
	case int32:
		return BinaryJSON{TypeCode: TypeInt32, Value: v}, nil
	case int64:
		return BinaryJSON{TypeCode: TypeInt64, Value: v}, nil
	case uint:
		return BinaryJSON{TypeCode: TypeUint64, Value: uint64(v)}, nil
	case uint8:
		return BinaryJSON{TypeCode: TypeUint16, Value: uint16(v)}, nil
	case uint16:
		return BinaryJSON{TypeCode: TypeUint16, Value: v}, nil
	case uint32:
		return BinaryJSON{TypeCode: TypeUint32, Value: v}, nil
	case uint64:
		return BinaryJSON{TypeCode: TypeUint64, Value: v}, nil
	case float32:
		return BinaryJSON{TypeCode: TypeDouble, Value: float64(v)}, nil
	case float64:
		return BinaryJSON{TypeCode: TypeDouble, Value: v}, nil
	case string:
		return BinaryJSON{TypeCode: TypeString, Value: v}, nil
	case []interface{}:
		return BinaryJSON{TypeCode: TypeArray, Value: v}, nil
	case map[string]interface{}:
		return BinaryJSON{TypeCode: TypeObject, Value: v}, nil
	default:
		return BinaryJSON{}, &JSONError{Code: ErrInvalidType, Message: fmt.Sprintf("unsupported JSON type: %T", v)}
	}
}

// ParseJSON parses a JSON string or interface{} into BinaryJSON
func ParseJSON(data interface{}) (BinaryJSON, error) {
	if data == nil {
		return BinaryJSON{TypeCode: TypeLiteral, Value: LiteralNull}, nil
	}

	// If it's a string, try to parse it as JSON
	if str, ok := data.(string); ok {
		var result interface{}
		if err := json.Unmarshal([]byte(str), &result); err != nil {
			return BinaryJSON{}, &JSONError{Code: ErrInvalidJSON, Message: err.Error()}
		}
		return NewBinaryJSON(result)
	}

	// If it's already a map or array, convert to BinaryJSON
	if bj, ok := data.(BinaryJSON); ok {
		return bj, nil
	}

	return NewBinaryJSON(data)
}

// ValidateJSON checks if the given value is valid JSON
func ValidateJSON(v interface{}) bool {
	_, err := ParseJSON(v)
	return err == nil
}

// Type returns the MySQL-compatible JSON type name
func (bj BinaryJSON) Type() string {
	switch bj.TypeCode {
	case TypeObject:
		return "OBJECT"
	case TypeArray:
		return "ARRAY"
	case TypeString:
		return "STRING"
	case TypeInt16, TypeInt32, TypeInt64, TypeUint16, TypeUint32, TypeUint64:
		return "INTEGER"
	case TypeDouble:
		return "DOUBLE"
	case TypeLiteral:
		if lit, ok := bj.Value.(JSONLiteral); ok {
			switch lit {
			case LiteralNull:
				return "NULL"
			case LiteralTrue, LiteralFalse:
				return "BOOLEAN"
			}
		}
		return "BOOLEAN"
	default:
		return "UNKNOWN"
	}
}

// MarshalJSON implements json.Marshaler interface
func (bj BinaryJSON) MarshalJSON() ([]byte, error) {
	return json.Marshal(bj.Value)
}

// UnmarshalJSON implements json.Unmarshaler interface
func (bj *BinaryJSON) UnmarshalJSON(data []byte) error {
	var result interface{}
	if err := json.Unmarshal(data, &result); err != nil {
		return err
	}
	parsed, err := NewBinaryJSON(result)
	if err != nil {
		return err
	}
	*bj = parsed
	return nil
}

// String returns the JSON string representation
func (bj BinaryJSON) String() string {
	if bj.Value == nil {
		return "null"
	}
	
	switch v := bj.Value.(type) {
	case JSONLiteral:
		switch v {
		case LiteralNull:
			return "null"
		case LiteralTrue:
			return "true"
		case LiteralFalse:
			return "false"
		}
	case string:
		return strconv.Quote(v)
	default:
		data, _ := json.Marshal(bj.Value)
		return string(data)
	}
}

// GetInterface returns the underlying Go interface{} value
func (bj BinaryJSON) GetInterface() interface{} {
	return bj.Value
}

// IsNull checks if this is a JSON null value
func (bj BinaryJSON) IsNull() bool {
	if bj.TypeCode != TypeLiteral {
		return false
	}
	if lit, ok := bj.Value.(JSONLiteral); ok {
		return lit == LiteralNull
	}
	return false
}

// IsArray checks if this is a JSON array
func (bj BinaryJSON) IsArray() bool {
	return bj.TypeCode == TypeArray
}

// IsObject checks if this is a JSON object
func (bj BinaryJSON) IsObject() bool {
	return bj.TypeCode == TypeObject
}

// IsString checks if this is a JSON string
func (bj BinaryJSON) IsString() bool {
	return bj.TypeCode == TypeString
}

// IsNumber checks if this is a JSON number
func (bj BinaryJSON) IsNumber() bool {
	switch bj.TypeCode {
	case TypeInt16, TypeInt32, TypeInt64, TypeUint16, TypeUint32, TypeUint64, TypeDouble:
		return true
	default:
		return false
	}
}

// IsBoolean checks if this is a JSON boolean
func (bj BinaryJSON) IsBoolean() bool {
	if bj.TypeCode != TypeLiteral {
		return false
	}
	if lit, ok := bj.Value.(JSONLiteral); ok {
		return lit == LiteralTrue || lit == LiteralFalse
	}
	return false
}

// GetFloat64 returns the float64 value if this is a number
func (bj BinaryJSON) GetFloat64() (float64, error) {
	if !bj.IsNumber() {
		return 0, &JSONError{Code: ErrTypeMismatch, Message: "value is not a number"}
	}
	
	switch v := bj.Value.(type) {
	case float64:
		return v, nil
	case int64:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int16:
		return float64(v), nil
	case uint64:
		return float64(v), nil
	case uint32:
		return float64(v), nil
	case uint16:
		return float64(v), nil
	default:
		return 0, &JSONError{Code: ErrTypeMismatch, Message: "cannot convert to float64"}
	}
}

// GetInt64 returns the int64 value if this is a number
func (bj BinaryJSON) GetInt64() (int64, error) {
	if !bj.IsNumber() {
		return 0, &JSONError{Code: ErrTypeMismatch, Message: "value is not a number"}
	}
	
	switch v := bj.Value.(type) {
	case int64:
		return v, nil
	case int32:
		return int64(v), nil
	case int16:
		return int64(v), nil
	case int:
		return int64(v), nil
	case uint64:
		if v > math.MaxInt64 {
			return 0, &JSONError{Code: ErrOverflow, Message: "uint64 overflow"}
		}
		return int64(v), nil
	case uint32:
		return int64(v), nil
	case uint16:
		return int64(v), nil
	case uint8:
		return int64(v), nil
	case float64:
		return int64(v), nil
	case float32:
		return int64(v), nil
	default:
		return 0, &JSONError{Code: ErrTypeMismatch, Message: "cannot convert to int64"}
	}
}

// GetString returns the string value if this is a string
func (bj BinaryJSON) GetString() (string, error) {
	if bj.TypeCode != TypeString {
		return "", &JSONError{Code: ErrTypeMismatch, Message: "value is not a string"}
	}
	
	if str, ok := bj.Value.(string); ok {
		return str, nil
	}
	return "", &JSONError{Code: ErrTypeMismatch, Message: "cannot convert to string"}
}

// GetBoolean returns the boolean value if this is a boolean
func (bj BinaryJSON) GetBoolean() (bool, error) {
	if !bj.IsBoolean() {
		return false, &JSONError{Code: ErrTypeMismatch, Message: "value is not a boolean"}
	}
	
	if lit, ok := bj.Value.(JSONLiteral); ok {
		return lit == LiteralTrue, nil
	}
	return false, &JSONError{Code: ErrTypeMismatch, Message: "cannot convert to boolean"}
}

// GetArray returns the array value if this is an array
func (bj BinaryJSON) GetArray() ([]interface{}, error) {
	if bj.TypeCode != TypeArray {
		return nil, &JSONError{Code: ErrTypeMismatch, Message: "value is not an array"}
	}
	
	if arr, ok := bj.Value.([]interface{}); ok {
		return arr, nil
	}
	return nil, &JSONError{Code: ErrTypeMismatch, Message: "cannot convert to array"}
}

// GetObject returns the object value if this is an object
func (bj BinaryJSON) GetObject() (map[string]interface{}, error) {
	if bj.TypeCode != TypeObject {
		return nil, &JSONError{Code: ErrTypeMismatch, Message: "value is not an object"}
	}
	
	if obj, ok := bj.Value.(map[string]interface{}); ok {
		return obj, nil
	}
	return nil, &JSONError{Code: ErrTypeMismatch, Message: "cannot convert to object"}
}

// ToBuiltinArg converts BinaryJSON to a format suitable for builtin function arguments
func (bj BinaryJSON) ToBuiltinArg() interface{} {
	if bj.IsNull() {
		return nil
	}
	return bj.Value
}

// FromBuiltinArg converts a builtin function argument to BinaryJSON
func FromBuiltinArg(arg interface{}) (BinaryJSON, error) {
	if arg == nil {
		return BinaryJSON{TypeCode: TypeLiteral, Value: LiteralNull}, nil
	}
	
	// If it's already BinaryJSON, return it directly
	if bj, ok := arg.(BinaryJSON); ok {
		return bj, nil
	}
	
	// Otherwise, convert from Go native types
	return NewBinaryJSON(arg)
}

// ConvertToFloat64 converts builtin arg to float64 with proper error handling
func ConvertToFloat64(arg interface{}) (float64, error) {
	if arg == nil {
		return 0, &JSONError{Code: ErrTypeMismatch, Message: "cannot convert NULL to float64"}
	}
	
	// Try builtin conversion first
	if f, err := builtin.ToFloat64(arg); err == nil {
		return f, nil
	}
	
	// Try BinaryJSON conversion
	bj, err := FromBuiltinArg(arg)
	if err != nil {
		return 0, err
	}
	
	return bj.GetFloat64()
}

// ConvertToInt64 converts builtin arg to int64 with proper error handling
func ConvertToInt64(arg interface{}) (int64, error) {
	if arg == nil {
		return 0, &JSONError{Code: ErrTypeMismatch, Message: "cannot convert NULL to int64"}
	}
	
	// Try builtin conversion first
	if i, err := builtin.ToInt64(arg); err == nil {
		return i, nil
	}
	
	// Try BinaryJSON conversion
	bj, err := FromBuiltinArg(arg)
	if err != nil {
		return 0, err
	}
	
	return bj.GetInt64()
}

// ConvertToString converts builtin arg to string with proper error handling
func ConvertToString(arg interface{}) string {
	if arg == nil {
		return "NULL"
	}
	
	// Try builtin conversion first
	if s := builtin.ToString(arg); s != "" {
		return s
	}
	
	// Try BinaryJSON conversion
	bj, err := FromBuiltinArg(arg)
	if err != nil {
		return fmt.Sprintf("%v", arg)
	}
	
	s, _ := bj.GetString()
	return s
}
