package protocol

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
)

// 对象池，减少内存分配
var bufPool = sync.Pool{
	New: func() interface{} {
		return bytes.NewBuffer(make([]byte, 0, 256))
	},
}

func ReadStringByNullEnd(r *bytes.Buffer) (string, error) {
	var buf []byte
	for {
		b, err := r.ReadByte()
		if err != nil {
			return "", err
		}
		if b == 0 {
			break
		}
		buf = append(buf, b)
	}
	return string(buf), nil
}

func ReadStringByNullEndFromReader(r io.Reader) (string, error) {
	reader := bufio.NewReader(r)
	var buf []byte
	for {
		b, err := reader.ReadByte()
		if err != nil {
			return "", err
		}
		if b == 0 {
			break
		}
		buf = append(buf, b)
	}
	return string(buf), nil
}

func ReadStringByLenencFromReader[LengthType uint8 | uint16 | uint32 | uint64](r io.Reader) (string, error) {
	length, err := ReadLenencNumber[LengthType](r)
	if err != nil {
		return "", err
	}

	textBytes := make([]byte, length)
	_, err = io.ReadFull(r, textBytes)
	if err != nil {
		return "", err
	}

	return string(textBytes), nil
}

func ReadNumber[T uint8 | uint16 | uint32 | uint64 | uint | int | int8 | int16 | int32 | int64](r io.Reader, readLenght int) (T, error) {
	buf := make([]byte, readLenght)
	_, err := io.ReadFull(r, buf)
	if err != nil {
		return 0, err
	}

	var v T
	var anyV any = v

	switch anyV.(type) {
	case uint8:
		v = T(buf[0])
	case int8:
		v = T(int8(buf[0]))
	case uint16:
		v = T(binary.LittleEndian.Uint16(buf))
	case uint32:
		v = T(binary.LittleEndian.Uint32(buf))
	case uint64:
		v = T(binary.LittleEndian.Uint64(buf))
	case uint:
		v = T(binary.LittleEndian.Uint32(buf))
	case int:
		v = T(int(binary.LittleEndian.Uint32(buf)))
	case int16:
		v = T(int16(binary.LittleEndian.Uint16(buf)))
	case int32:
		v = T(int32(binary.LittleEndian.Uint32(buf)))
	case int64:
		v = T(int64(binary.LittleEndian.Uint64(buf)))
	default:
		return 0, errors.New("invalid type")
	}

	return v, nil
}

func ReadLenencNumber[T uint8 | uint16 | uint32 | uint64 | uint | int | int8 | int16 | int32 | int64](r io.Reader) (T, error) {
	// 读取第一个字节确定长度类型
	var b [1]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return 0, err
	}
	firstByte := b[0]

	// 根据第一个字节的值处理不同长度的整数
	switch {
	case firstByte < 0xfb: // 1字节整数
		return T(firstByte), nil

	case firstByte == 0xfc: // 2字节整数
		var buf [2]byte
		if _, err := io.ReadFull(r, buf[:]); err != nil {
			return 0, err
		}
		return T(binary.LittleEndian.Uint16(buf[:])), nil

	case firstByte == 0xfd: // 3字节整数 - 修复：正确处理3字节小端序
		var buf [3]byte
		if _, err := io.ReadFull(r, buf[:]); err != nil {
			return 0, err
		}
		// 手动组合3字节小端序整数
		val := uint32(buf[0]) | uint32(buf[1])<<8 | uint32(buf[2])<<16
		return T(val), nil

	case firstByte == 0xfe: // 8字节整数
		var buf [8]byte
		if _, err := io.ReadFull(r, buf[:]); err != nil {
			return 0, err
		}
		return T(binary.LittleEndian.Uint64(buf[:])), nil

	case firstByte == 0xfb: // 0xFB 保留值（不应出现）
		return 0, errors.New("invalid lenenc number: 0xfb is reserved for NULL")

	default: // 0xFF 和其他未定义值
		return 0, fmt.Errorf("invalid lenenc number prefix: 0x%x", firstByte)
	}
}

// 写入方法，与读取方法对应

func WriteStringByNullEnd(buf *bytes.Buffer, s string) error {
	buf.WriteString(s)
	buf.WriteByte(0)
	return nil
}

func WriteStringByLenenc(buf *bytes.Buffer, s string) error {
	// 写入长度（lenenc）
	length := len(s)
	if length < 0xfb {
		buf.WriteByte(byte(length))
	} else if length < 0x10000 {
		buf.WriteByte(0xfc)
		binary.Write(buf, binary.LittleEndian, uint16(length))
	} else if length < 0x1000000 { // 修复：16MB-1 (0x1000000)
		buf.WriteByte(0xfd)
		// 修复：正确写入3字节小端序
		buf.Write([]byte{byte(length), byte(length >> 8), byte(length >> 16)})
	} else {
		buf.WriteByte(0xfe)
		binary.Write(buf, binary.LittleEndian, uint64(length))
	}
	// 写入字符串内容
	buf.WriteString(s)
	return nil
}

func WriteNumber[T uint8 | uint16 | uint32 | uint64 | uint | int | int8 | int16 | int32 | int64](buf *bytes.Buffer, value T, writeLength int) error {
	switch writeLength {
	case 1:
		buf.WriteByte(byte(value))
	case 2:
		binary.Write(buf, binary.LittleEndian, uint16(value))
	case 4:
		binary.Write(buf, binary.LittleEndian, uint32(value))
	case 8:
		binary.Write(buf, binary.LittleEndian, uint64(value))
	default:
		return fmt.Errorf("unsupported write length: %d", writeLength)
	}
	return nil
}

func WriteLenencNumber[T uint8 | uint16 | uint32 | uint64 | uint | int | int8 | int16 | int32 | int64](buf *bytes.Buffer, value T) error {
	var val uint64
	switch v := any(value).(type) {
	case uint8:
		val = uint64(v)
	case uint16:
		val = uint64(v)
	case uint32:
		val = uint64(v)
	case uint64:
		val = v
	case uint:
		val = uint64(v)
	case int:
		val = uint64(v)
	case int8:
		val = uint64(v)
	case int16:
		val = uint64(v)
	case int32:
		val = uint64(v)
	case int64:
		val = uint64(v)
	default:
		return errors.New("invalid type for lenenc number")
	}

	// 根据值的大小选择编码方式
	if val < 0xfb {
		buf.WriteByte(byte(val))
	} else if val < 0x10000 {
		buf.WriteByte(0xfc)
		binary.Write(buf, binary.LittleEndian, uint16(val))
	} else if val < 0x1000000 { // 修复：16MB-1
		buf.WriteByte(0xfd)
		// 修复：正确写入3字节小端序
		buf.Write([]byte{byte(val), byte(val >> 8), byte(val >> 16)})
	} else {
		buf.WriteByte(0xfe)
		binary.Write(buf, binary.LittleEndian, val)
	}
	return nil
}

func WriteBinary(buf *bytes.Buffer, data []byte) error {
	buf.Write(data)
	return nil
}
