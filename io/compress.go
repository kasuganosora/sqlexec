package io

import (
	"bytes"
	"compress/zlib"
	"io"
)

const (
	CompressionHeaderSize = 7 // 3字节长度+3字节原始长度+1字节序列号
)

// CompressPacket 压缩包
func (i *IO) CompressPacket(data []byte) ([]byte, error) {
	var buf bytes.Buffer

	// 创建 zlib 压缩器
	compressor, err := zlib.NewWriterLevel(&buf, zlib.BestCompression)
	if err != nil {
		return nil, err
	}

	// 写入数据
	if _, err := compressor.Write(data); err != nil {
		return nil, err
	}

	// 关闭压缩器
	if err := compressor.Close(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// DecompressPacket 解压包
func (i *IO) DecompressPacket(data []byte) ([]byte, error) {
	// 创建 zlib 解压器
	decompressor, err := zlib.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer decompressor.Close()

	// 读取解压后的数据
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, decompressor); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// compressPacket 压缩包（内部方法）- 修复：实现正确的MySQL压缩协议
func (i *IO) compressPacket(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	// 预留头部空间
	buf.Write(make([]byte, CompressionHeaderSize))

	compressor := zlib.NewWriter(&buf)
	if _, err := compressor.Write(data); err != nil {
		return nil, err
	}
	compressor.Close()

	compressed := buf.Bytes()
	payload := compressed[CompressionHeaderSize:]
	payloadLen := len(payload)
	uncompressedLen := len(data)

	// 填充压缩头
	compressed[0] = byte(payloadLen)
	compressed[1] = byte(payloadLen >> 8)
	compressed[2] = byte(payloadLen >> 16)
	compressed[3] = byte(uncompressedLen)
	compressed[4] = byte(uncompressedLen >> 8)
	compressed[5] = byte(uncompressedLen >> 16)
	compressed[6] = i.nextSequenceID()

	return compressed, nil
}

// decompressPacket 解压包（内部方法）- 修复：正确处理MySQL压缩包格式
func (i *IO) decompressPacket(data []byte) ([]byte, error) {
	if len(data) < CompressionHeaderSize {
		return nil, ErrInvalidPacket
	}

	// 解析压缩头
	payloadLen := uint32(data[0]) | uint32(data[1])<<8 | uint32(data[2])<<16
	uncompressedLen := uint32(data[3]) | uint32(data[4])<<8 | uint32(data[5])<<16
	_ = data[6] // sequenceID，暂时未使用

	// 验证数据长度
	if uint32(len(data)-CompressionHeaderSize) != payloadLen {
		return nil, ErrInvalidPacket
	}

	// 解压数据
	compressedData := data[CompressionHeaderSize:]
	decompressor, err := zlib.NewReader(bytes.NewReader(compressedData))
	if err != nil {
		return nil, err
	}
	defer decompressor.Close()

	var buf bytes.Buffer
	if _, err := io.Copy(&buf, decompressor); err != nil {
		return nil, err
	}

	decompressed := buf.Bytes()
	if uint32(len(decompressed)) != uncompressedLen {
		return nil, ErrInvalidPacket
	}

	return decompressed, nil
}

// nextSequenceID 获取下一个序列号
func (i *IO) nextSequenceID() uint8 {
	seq := i.writeSeq
	i.writeSeq++
	return seq
}
