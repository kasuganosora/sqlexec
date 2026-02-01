package protocol

import (
	"bytes"
	"testing"
)

func TestComInitDBPacketUnmarshal(t *testing.T) {
	// 构造 COM_INIT_DB 数据包
	// Header: 长度=19 (0x13 0x00 0x00), SequenceID=0
	// Payload: 0x02 (COM_INIT_DB) + "information_schema" (16字节)
	// 注意：MariaDB 客户端发送的数据包没有 null 终止符
	expectedSchemaName := "information_schema"
	payload := []byte{0x02}
	payload = append(payload, []byte(expectedSchemaName)...)
	
	packet := []byte{
		byte(len(payload)),       // PayloadLength 低8位
		byte(len(payload) >> 8),  // PayloadLength 中8位
		byte(len(payload) >> 16), // PayloadLength 高8位
		0x00,                     // SequenceID
	}
	packet = append(packet, payload...)
	
	comInitDB := &ComInitDBPacket{}
	err := comInitDB.Unmarshal(bytes.NewReader(packet))
	
	if err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}
	
	if comInitDB.SchemaName != expectedSchemaName {
		t.Errorf("Expected SchemaName=%q, got %q", expectedSchemaName, comInitDB.SchemaName)
	}
	
	t.Logf("Successfully parsed SchemaName=%q", comInitDB.SchemaName)
}

func TestComInitDBPacketUnmarshalWithNull(t *testing.T) {
	// 测试带 null 终止符的情况（某些客户端可能发送）
	expectedSchemaName := "test_db"
	payload := []byte{0x02}
	payload = append(payload, []byte(expectedSchemaName)...)
	payload = append(payload, 0x00) // null 终止符
	
	packet := []byte{
		byte(len(payload)),       // PayloadLength 低8位
		byte(len(payload) >> 8),  // PayloadLength 中8位
		byte(len(payload) >> 16), // PayloadLength 高8位
		0x00,                     // SequenceID
	}
	packet = append(packet, payload...)
	
	comInitDB := &ComInitDBPacket{}
	err := comInitDB.Unmarshal(bytes.NewReader(packet))
	
	if err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}
	
	if comInitDB.SchemaName != expectedSchemaName {
		t.Errorf("Expected SchemaName=%q, got %q", expectedSchemaName, comInitDB.SchemaName)
	}
	
	t.Logf("Successfully parsed SchemaName=%q", comInitDB.SchemaName)
}

func TestComInitDBPacketUnmarshalEmpty(t *testing.T) {
	// 测试空数据库名的情况
	payload := []byte{0x02} // 只有命令字节
	
	packet := []byte{
		byte(len(payload)),       // PayloadLength 低8位
		byte(len(payload) >> 8),  // PayloadLength 中8位
		byte(len(payload) >> 16), // PayloadLength 高8位
		0x00,                     // SequenceID
	}
	packet = append(packet, payload...)
	
	comInitDB := &ComInitDBPacket{}
	err := comInitDB.Unmarshal(bytes.NewReader(packet))
	
	if err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}
	
	if comInitDB.SchemaName != "" {
		t.Errorf("Expected empty SchemaName, got %q", comInitDB.SchemaName)
	}
	
	t.Logf("Successfully parsed empty SchemaName")
}
