package memory

import (
	"encoding/binary"
	"fmt"
	"math"
	"time"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// page_codec implements a fast, zero-reflection binary codec for []domain.Row.
// It replaces encoding/gob which is extremely slow for map[string]interface{}
// due to heavy reflection (20ms+ for 4K rows). This codec handles the known
// value types directly and achieves ~10-50x faster serialization.
//
// Wire format:
//
//	[rowCount:uint32]
//	for each row:
//	  [fieldCount:uint16]
//	  for each field:
//	    [keyLen:uint16][key:bytes]
//	    [typeTag:byte][value:bytes]
//
// Type tags:
const (
	tagNil     byte = 0
	tagBool    byte = 1
	tagInt64   byte = 2
	tagFloat64 byte = 3
	tagString  byte = 4
	tagBytes   byte = 5
	tagTime    byte = 6
	tagInt     byte = 7
	tagFloat32 byte = 8
	tagInt32   byte = 9
)

// encodeRows serializes []domain.Row to a byte slice using a fast binary format.
func encodeRows(rows []domain.Row) ([]byte, error) {
	// Pre-estimate buffer size: ~200 bytes per row is a reasonable starting point
	buf := make([]byte, 0, len(rows)*200+4)

	// Row count
	buf = appendUint32(buf, uint32(len(rows)))

	for _, row := range rows {
		// Field count
		buf = appendUint16(buf, uint16(len(row)))

		for k, v := range row {
			// Key
			buf = appendUint16(buf, uint16(len(k)))
			buf = append(buf, k...)

			// Value
			var err error
			buf, err = appendValue(buf, v)
			if err != nil {
				return nil, err
			}
		}
	}

	return buf, nil
}

// decodeRows deserializes []domain.Row from a byte slice produced by encodeRows.
func decodeRows(data []byte) ([]domain.Row, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("page_codec: data too short")
	}

	pos := 0

	rowCount := readUint32(data, pos)
	pos += 4

	rows := make([]domain.Row, rowCount)

	for i := uint32(0); i < rowCount; i++ {
		if pos+2 > len(data) {
			return nil, fmt.Errorf("page_codec: unexpected EOF at row %d", i)
		}
		fieldCount := readUint16(data, pos)
		pos += 2

		row := make(domain.Row, fieldCount)

		for j := uint16(0); j < fieldCount; j++ {
			if pos+2 > len(data) {
				return nil, fmt.Errorf("page_codec: unexpected EOF at field key")
			}
			keyLen := readUint16(data, pos)
			pos += 2

			if pos+int(keyLen) > len(data) {
				return nil, fmt.Errorf("page_codec: unexpected EOF reading key")
			}
			key := string(data[pos : pos+int(keyLen)])
			pos += int(keyLen)

			var val interface{}
			var err error
			val, pos, err = readValue(data, pos)
			if err != nil {
				return nil, err
			}
			row[key] = val
		}

		rows[i] = row
	}

	return rows, nil
}

func appendValue(buf []byte, v interface{}) ([]byte, error) {
	switch val := v.(type) {
	case nil:
		buf = append(buf, tagNil)
	case bool:
		buf = append(buf, tagBool)
		if val {
			buf = append(buf, 1)
		} else {
			buf = append(buf, 0)
		}
	case int:
		buf = append(buf, tagInt)
		buf = appendInt64(buf, int64(val))
	case int32:
		buf = append(buf, tagInt32)
		buf = appendInt32(buf, val)
	case int64:
		buf = append(buf, tagInt64)
		buf = appendInt64(buf, val)
	case float32:
		buf = append(buf, tagFloat32)
		buf = appendUint32(buf, math.Float32bits(val))
	case float64:
		buf = append(buf, tagFloat64)
		buf = appendUint64(buf, math.Float64bits(val))
	case string:
		buf = append(buf, tagString)
		buf = appendUint32(buf, uint32(len(val)))
		buf = append(buf, val...)
	case []byte:
		buf = append(buf, tagBytes)
		buf = appendUint32(buf, uint32(len(val)))
		buf = append(buf, val...)
	case time.Time:
		buf = append(buf, tagTime)
		b, _ := val.MarshalBinary()
		buf = appendUint16(buf, uint16(len(b)))
		buf = append(buf, b...)
	default:
		return nil, fmt.Errorf("page_codec: unsupported type %T", v)
	}
	return buf, nil
}

func readValue(data []byte, pos int) (interface{}, int, error) {
	if pos >= len(data) {
		return nil, pos, fmt.Errorf("page_codec: unexpected EOF at value tag")
	}
	tag := data[pos]
	pos++

	switch tag {
	case tagNil:
		return nil, pos, nil
	case tagBool:
		if pos >= len(data) {
			return nil, pos, fmt.Errorf("page_codec: unexpected EOF at bool")
		}
		val := data[pos] != 0
		return val, pos + 1, nil
	case tagInt:
		if pos+8 > len(data) {
			return nil, pos, fmt.Errorf("page_codec: unexpected EOF at int")
		}
		val := int(readInt64(data, pos))
		return val, pos + 8, nil
	case tagInt32:
		if pos+4 > len(data) {
			return nil, pos, fmt.Errorf("page_codec: unexpected EOF at int32")
		}
		val := int32(readUint32(data, pos))
		return val, pos + 4, nil
	case tagInt64:
		if pos+8 > len(data) {
			return nil, pos, fmt.Errorf("page_codec: unexpected EOF at int64")
		}
		val := readInt64(data, pos)
		return val, pos + 8, nil
	case tagFloat32:
		if pos+4 > len(data) {
			return nil, pos, fmt.Errorf("page_codec: unexpected EOF at float32")
		}
		val := math.Float32frombits(readUint32(data, pos))
		return val, pos + 4, nil
	case tagFloat64:
		if pos+8 > len(data) {
			return nil, pos, fmt.Errorf("page_codec: unexpected EOF at float64")
		}
		val := math.Float64frombits(readUint64(data, pos))
		return val, pos + 8, nil
	case tagString:
		if pos+4 > len(data) {
			return nil, pos, fmt.Errorf("page_codec: unexpected EOF at string length")
		}
		sLen := readUint32(data, pos)
		pos += 4
		if pos+int(sLen) > len(data) {
			return nil, pos, fmt.Errorf("page_codec: unexpected EOF at string data")
		}
		val := string(data[pos : pos+int(sLen)])
		return val, pos + int(sLen), nil
	case tagBytes:
		if pos+4 > len(data) {
			return nil, pos, fmt.Errorf("page_codec: unexpected EOF at bytes length")
		}
		bLen := readUint32(data, pos)
		pos += 4
		if pos+int(bLen) > len(data) {
			return nil, pos, fmt.Errorf("page_codec: unexpected EOF at bytes data")
		}
		val := make([]byte, bLen)
		copy(val, data[pos:pos+int(bLen)])
		return val, pos + int(bLen), nil
	case tagTime:
		if pos+2 > len(data) {
			return nil, pos, fmt.Errorf("page_codec: unexpected EOF at time length")
		}
		tLen := readUint16(data, pos)
		pos += 2
		if pos+int(tLen) > len(data) {
			return nil, pos, fmt.Errorf("page_codec: unexpected EOF at time data")
		}
		var t time.Time
		if err := t.UnmarshalBinary(data[pos : pos+int(tLen)]); err != nil {
			return nil, pos, fmt.Errorf("page_codec: time unmarshal failed: %w", err)
		}
		return t, pos + int(tLen), nil
	default:
		return nil, pos, fmt.Errorf("page_codec: unknown tag %d", tag)
	}
}

// --- Binary encoding helpers (little-endian, inline-friendly) ---

func appendUint16(buf []byte, v uint16) []byte {
	return append(buf, byte(v), byte(v>>8))
}

func appendUint32(buf []byte, v uint32) []byte {
	return append(buf, byte(v), byte(v>>8), byte(v>>16), byte(v>>24))
}

func appendInt32(buf []byte, v int32) []byte {
	return appendUint32(buf, uint32(v))
}

func appendInt64(buf []byte, v int64) []byte {
	return appendUint64(buf, uint64(v))
}

func appendUint64(buf []byte, v uint64) []byte {
	return append(buf, byte(v), byte(v>>8), byte(v>>16), byte(v>>24),
		byte(v>>32), byte(v>>40), byte(v>>48), byte(v>>56))
}

func readUint16(data []byte, pos int) uint16 {
	return binary.LittleEndian.Uint16(data[pos:])
}

func readUint32(data []byte, pos int) uint32 {
	return binary.LittleEndian.Uint32(data[pos:])
}

func readUint64(data []byte, pos int) uint64 {
	return binary.LittleEndian.Uint64(data[pos:])
}

func readInt64(data []byte, pos int) int64 {
	return int64(binary.LittleEndian.Uint64(data[pos:]))
}
