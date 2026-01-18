package protocol

import (
	"bytes"
	"net"
	"time"
)

// NewHandshakePacket 创建一个新的握手包
func NewHandshakePacket() *HandshakeV10Packet {
	return &HandshakeV10Packet{
		Packet: Packet{
			SequenceID: 0,
		},
		ProtocolVersion:    10,
		ServerVersion:      "5.5.5-10.3.12-MariaDB",
		ThreadID:           uint32(time.Now().Unix()),
		AuthPluginDataPart: []byte{0x4a, 0x73, 0x29, 0x6c, 0x66, 0x3e, 0x41, 0x68},
		Filter:              0,
		CapabilityFlags1:    0xf7fe,
		CharacterSet:        8,
		StatusFlags:         2,
		CapabilityFlags2:    0x81bf,
		AuthPluginDataLen:   21,
		Reserved:            []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
		MariaDBCaps:         7,
		AuthPluginDataPart2: []byte{0x6a, 0x5a, 0x65, 0x6d, 0x74, 0x7c, 0x34, 0x2b, 0x7a, 0x49, 0x3a, 0x29, 0x00},
		AuthPluginName:      "mysql_native_password",
	}
}

// ReadPacket 读取一个MySQL协议包
func ReadPacket(conn net.Conn) (*Packet, error) {
	packet := &Packet{}
	err := packet.Unmarshal(conn)
	if err != nil {
		return nil, err
	}
	return packet, nil
}

// SendOK 发送一个OK响应包
func SendOK(conn net.Conn, sequenceID uint8) error {
	okPacket := &OkPacket{
		Packet: Packet{
			SequenceID: sequenceID,
		},
		OkInPacket: OkInPacket{
			Header:       0x00,
			AffectedRows: 0,
			LastInsertId: 0,
			StatusFlags:  0x0002, // SERVER_STATUS_AUTOCOMMIT
			Warnings:     0,
		},
	}
	return okPacket.Send(conn)
}

// SendError 发送一个错误包
func SendError(conn net.Conn, err error) error {
	errorPacket := &ErrorPacket{
		Packet: Packet{
			SequenceID: 1,
		},
		ErrorInPacket: ErrorInPacket{
			Header:       0xFF,
			ErrorCode:    1045,
			SqlStateMarker: "#",
			SqlState:     "28000",
			ErrorMessage: err.Error(),
		},
	}
	return errorPacket.Send(conn)
}

// Send 发送数据包
func (p *Packet) Send(conn net.Conn) error {
	data, err := p.MarshalBytes()
	if err != nil {
		return err
	}
	_, err = conn.Write(data)
	return err
}

// MarshalBytes 将包序列化为字节数组
func (p *Packet) MarshalBytes() ([]byte, error) {
	buf := new(bytes.Buffer)
	// 写入载荷长度（3字节小端）
	buf.WriteByte(byte(p.PayloadLength))
	buf.WriteByte(byte(p.PayloadLength >> 8))
	buf.WriteByte(byte(p.PayloadLength >> 16))
	// 写入序列ID
	buf.WriteByte(p.SequenceID)
	// 写入载荷
	if p.Payload != nil {
		buf.Write(p.Payload)
	}
	return buf.Bytes(), nil
}
