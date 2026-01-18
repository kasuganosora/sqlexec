package protocol

import (
	"bytes"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWriteStringByLenencEmptyString(t *testing.T) {
	buf := &bytes.Buffer{}

	// 测试空字符串
	err := WriteStringByLenenc(buf, "")
	assert.NoError(t, err)

	// 打印十六进制输出
	result := buf.Bytes()
	t.Logf("Empty string output: %s", hex.EncodeToString(result))

	// 验证输出
	assert.Equal(t, 1, len(result))
	assert.Equal(t, byte(0), result[0])

	// 测试读取
	readBuf := bytes.NewBuffer(result)
	length, err := ReadLenencNumber[uint8](readBuf)
	assert.NoError(t, err)
	assert.Equal(t, uint8(0), length)

	// 读取字符串内容
	textBytes := make([]byte, length)
	_, err = readBuf.Read(textBytes)
	assert.NoError(t, err)
	assert.Equal(t, "", string(textBytes))
}

func TestWriteStringByLenencNormalString(t *testing.T) {
	buf := &bytes.Buffer{}

	// 测试正常字符串
	testStr := "hello"
	err := WriteStringByLenenc(buf, testStr)
	assert.NoError(t, err)

	// 打印十六进制输出
	result := buf.Bytes()
	t.Logf("Normal string output: %s", hex.EncodeToString(result))

	// 验证输出长度
	assert.Equal(t, 6, len(result)) // 1字节长度 + 5字节字符串
	assert.Equal(t, byte(5), result[0])

	// 测试读取
	readBuf := bytes.NewBuffer(result)
	length, err := ReadLenencNumber[uint8](readBuf)
	assert.NoError(t, err)
	assert.Equal(t, uint8(5), length)

	// 读取字符串内容
	textBytes := make([]byte, length)
	_, err = readBuf.Read(textBytes)
	assert.NoError(t, err)
	assert.Equal(t, "hello", string(textBytes))
}

func TestConnectionAttributeItemEmptyString(t *testing.T) {
	// 测试空字符串的情况
	item := &ConnectionAttributeItem{
		Name:  "",
		Value: "",
	}

	data, err := item.Marshal()
	assert.NoError(t, err)

	// 打印十六进制输出
	t.Logf("Empty ConnectionAttributeItem output: %s", hex.EncodeToString(data))

	// 验证输出
	assert.Equal(t, 2, len(data))
	assert.Equal(t, byte(0), data[0]) // 空字符串长度
	assert.Equal(t, byte(0), data[1]) // 空字符串长度

	// 测试反序列化
	itemBack := &ConnectionAttributeItem{}
	err = itemBack.Unmarshal(bytes.NewReader(data))
	assert.NoError(t, err)
	assert.Equal(t, "", itemBack.Name)
	assert.Equal(t, "", itemBack.Value)
}

func TestConnectionAttributeItemEmptyName(t *testing.T) {
	// 测试空名称的情况
	item := &ConnectionAttributeItem{
		Name:  "",
		Value: "test",
	}

	data, err := item.Marshal()
	assert.NoError(t, err)

	// 打印十六进制输出
	t.Logf("Empty name ConnectionAttributeItem output: %s", hex.EncodeToString(data))

	// 验证输出
	assert.Equal(t, 6, len(data))
	assert.Equal(t, byte(0), data[0])                         // 空字符串长度
	assert.Equal(t, byte(4), data[1])                         // "test" 长度
	assert.Equal(t, []byte{0x74, 0x65, 0x73, 0x74}, data[2:]) // "test"

	// 测试反序列化
	itemBack := &ConnectionAttributeItem{}
	err = itemBack.Unmarshal(bytes.NewReader(data))
	assert.NoError(t, err)
	assert.Equal(t, "", itemBack.Name)
	assert.Equal(t, "test", itemBack.Value)
}
