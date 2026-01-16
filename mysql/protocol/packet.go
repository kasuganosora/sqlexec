package protocol

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
)

type Packet struct {
	PayloadLength uint32 `mysql:"int<3>"`
	SequenceID    uint8  `mysql:"int<1>"`
	rawData      []byte // 保存原始数据
	Payload      []byte // 保存载荷数据
}

func (p *Packet) Unmarshal(r io.Reader) (err error) {
	buf := make([]byte, 4)
	_, err = io.ReadFull(r, buf)
	if err != nil {
		return err
	}
	// MySQL协议使用小端序
	p.PayloadLength = uint32(buf[0]) | uint32(buf[1])<<8 | uint32(buf[2])<<16
	p.SequenceID = buf[3]

	// 读取载荷数据（如果长度大于0）
	p.Payload = nil
	if p.PayloadLength > 0 && p.PayloadLength < 0xffffff {
		p.Payload = make([]byte, p.PayloadLength)
		_, err = io.ReadFull(r, p.Payload)
		if err != nil {
			return err
		}
	}
	return nil
}

// RawBytes 返回完整的原始字节数据（包括包头）
func (p *Packet) RawBytes() []byte {
	buf := new(bytes.Buffer)
	// 写入包头
	buf.Write([]byte{
		byte(p.PayloadLength),
		byte(p.PayloadLength >> 8),
		byte(p.PayloadLength >> 16),
		p.SequenceID,
	})
	// 写入载荷
	if p.Payload != nil {
		buf.Write(p.Payload)
	}
	return buf.Bytes()
}

// GetCommandType 获取包的命令类型（第一个字节）
func (p *Packet) GetCommandType() uint8 {
	if len(p.Payload) > 0 {
		return p.Payload[0]
	}
	return 0
}

// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_handshake_v10.html
// https://www.wireshark.org/docs/dfref/m/mysql.html
type HandshakeV10Packet struct {
	Packet
	ProtocolVersion    uint8  `mysql:"int<1>"`
	ServerVersion      string `mysql:"string<NUL>"`
	ThreadID           uint32 `mysql:"int<4>"`
	AuthPluginDataPart []byte `mysql:"binary<8>"` // 改为固定长度字节数组
	Filter             uint8  `mysql:"int<1>"`    // 实际为 capability_flags 的低8位
	CapabilityFlags1   uint16 `mysql:"int<2>"`    // 重命名为 CapabilityFlags1
	CharacterSet       uint8  `mysql:"int<1>"`
	StatusFlags        uint16 `mysql:"int<2>"`
	CapabilityFlags2   uint16 `mysql:"int<2>"` // 重命名为 CapabilityFlags2
	AuthPluginDataLen  uint8  `mysql:"int<1>"`
	Reserved           []byte `mysql:"binary<6>"` // 改为6字节，符合MariaDB协议
	// MariaDB 特定字段移到末尾并标记可选
	MariaDBCaps         uint32 `mysql:"int<4>,optional"`
	AuthPluginDataPart2 []byte `mysql:"binary<var>,optional"` // 动态长度
	AuthPluginName      string `mysql:"string<NUL>,optional"`
}

func (p *HandshakeV10Packet) Unmarshal(r io.Reader) (err error) {
	p.Packet.Unmarshal(r)
	buf := make([]byte, p.PayloadLength)
	_, err = io.ReadFull(r, buf)
	if err != nil {
		return err
	}

	nb := bytes.NewBuffer(buf)
	p.ProtocolVersion, _ = nb.ReadByte()
	p.ServerVersion, _ = ReadStringByNullEnd(nb)
	p.ThreadID, _ = ReadNumber[uint32](nb, 4)
	authPart := make([]byte, 8)
	nb.Read(authPart)
	p.AuthPluginDataPart = authPart
	p.Filter, _ = ReadNumber[uint8](nb, 1)
	p.CapabilityFlags1, _ = ReadNumber[uint16](nb, 2)
	p.CharacterSet, _ = ReadNumber[uint8](nb, 1)
	p.StatusFlags, _ = ReadNumber[uint16](nb, 2)
	p.CapabilityFlags2, _ = ReadNumber[uint16](nb, 2)
	p.AuthPluginDataLen, _ = ReadNumber[uint8](nb, 1)

	// 读取保留字段（6字节）
	reserved := make([]byte, 6)
	_, err = nb.Read(reserved)
	if err != nil {
		return err
	}
	p.Reserved = reserved

	// 读取 MariaDBCaps（4字节）
	p.MariaDBCaps, _ = ReadNumber[uint32](nb, 4)

	// 检查是否有额外的认证插件数据
	if p.AuthPluginDataLen > 8 {
		authPluginDataPart2Length := int(p.AuthPluginDataLen - 8)
		authDataPart2 := make([]byte, authPluginDataPart2Length)
		_, err = nb.Read(authDataPart2)
		if err != nil {
			return err
		}
		p.AuthPluginDataPart2 = authDataPart2
	}

	// 检查是否有认证插件名称
	if nb.Len() > 0 {
		p.AuthPluginName, _ = ReadStringByNullEnd(nb)
	}

	return nil
}

func (p *HandshakeV10Packet) Marshal() ([]byte, error) {
	buf := new(bytes.Buffer)

	// 修复：AuthPluginDataLen 应该是 AuthPluginDataPart (8字节) + AuthPluginDataPart2 的总长度
	if len(p.AuthPluginDataPart2) > 0 {
		p.AuthPluginDataPart2 = append(p.AuthPluginDataPart2, 0) // 添加0结尾
		p.AuthPluginDataLen = uint8(8 + len(p.AuthPluginDataPart2))
	}

	// 1. 写入 ProtocolVersion
	WriteNumber(buf, p.ProtocolVersion, 1)
	// 2. 写入 ServerVersion (以0结尾)
	WriteStringByNullEnd(buf, p.ServerVersion)
	// 3. 写入 ThreadID (4字节小端)
	WriteNumber(buf, p.ThreadID, 4)
	// 4. 写入 AuthPluginDataPart (9字节)+0
	WriteBinary(buf, append(p.AuthPluginDataPart, 0))

	// 6. 写入 CapabilityFlags1 (2字节小端)
	WriteNumber(buf, p.CapabilityFlags1, 2)
	// 7. 写入 CharacterSet
	WriteNumber(buf, p.CharacterSet, 1)
	// 8. 写入 StatusFlags (2字节小端)
	WriteNumber(buf, p.StatusFlags, 2)
	// 9. 写入 CapabilityFlags2 (2字节小端)
	WriteNumber(buf, p.CapabilityFlags2, 2)
	// 10. 写入 AuthPluginDataLen
	WriteNumber(buf, p.AuthPluginDataLen, 1)
	// 11. 写入 Reserved (6字节)
	WriteBinary(buf, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00})
	// 12. 写入 MariaDBCaps (4字节小端)
	WriteNumber(buf, p.MariaDBCaps, 4)
	// 13. 写入 AuthPluginDataPart2
	if len(p.AuthPluginDataPart2) > 0 {
		WriteBinary(buf, p.AuthPluginDataPart2)
	}
	// 14. 写入 AuthPluginName (以0结尾)
	if p.AuthPluginName != "" {
		WriteStringByNullEnd(buf, p.AuthPluginName)
	}

	// 组装Packet头部
	payload := buf.Bytes()
	packetBuf := new(bytes.Buffer)
	// PayloadLength 3字节小端
	packetBuf.Write([]byte{byte(len(payload)), byte(len(payload) >> 8), byte(len(payload) >> 16)})
	// SequenceID
	packetBuf.WriteByte(p.SequenceID)
	// Payload
	packetBuf.Write(payload)

	return packetBuf.Bytes(), nil
}

type HandshakeResponse struct {
	Packet
	ClientCapabilities         uint16                    `mysql:"int<2>"`
	ExtendedClientCapabilities uint16                    `mysql:"int<2>"`
	MaxPacketSize              uint32                    `mysql:"int<4>"`
	CharacterSet               uint8                     `mysql:"int<1>"`
	Reserved                   []byte                    `mysql:"binary<19>"`
	MariaDBCaps                uint32                    `mysql:"int<4>"`
	User                       string                    `mysql:"string<NUL>"`
	AuthResponse               string                    `mysql:"string<lenenc>"` // 通常是密码
	Database                   string                    `mysql:"string<NUL>"`
	ClientAuthPluginName       string                    `mysql:"string<NUL>"`
	ConnectionAttributesLength uint64                    `mysql:"int<lenenc>"`
	ConnectionAttributes       []ConnectionAttributeItem `mysql:"array"`
	ZstdCompressionLevel       uint8                     `mysql:"int<1>"`
}

func (p *HandshakeResponse) Unmarshal(r io.Reader, capabilities uint32) (err error) {
	p.Packet.Unmarshal(r)
	reader := bufio.NewReader(r)
	p.ClientCapabilities, _ = ReadNumber[uint16](reader, 2)
	p.ExtendedClientCapabilities, _ = ReadNumber[uint16](reader, 2)
	p.MaxPacketSize, _ = ReadNumber[uint32](reader, 4)
	p.CharacterSet, _ = ReadNumber[uint8](reader, 1)
	p.Reserved = make([]byte, 19)
	io.ReadFull(reader, p.Reserved)
	p.MariaDBCaps, _ = ReadNumber[uint32](reader, 4)
	// 读取用户名（NUL结尾字符串）
	p.User, _ = ReadStringByNullEndFromReader(reader)

	// 修复：根据能力标志正确处理认证响应
	switch {
	case capabilities&CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA != 0:
		// 长度编码的认证响应
		p.AuthResponse, _ = ReadStringByLenencFromReader[uint8](reader)
	case capabilities&CLIENT_SECURE_CONNECTION != 0:
		// 安全连接：1字节长度 + N字节内容
		authLen, _ := ReadNumber[uint8](reader, 1)
		authData := make([]byte, authLen)
		io.ReadFull(reader, authData)
		p.AuthResponse = hex.EncodeToString(authData)
	default:
		// 旧密码认证：NUL结尾字符串
		p.AuthResponse, _ = ReadStringByNullEndFromReader(reader)
	}

	if capabilities&CLIENT_CONNECT_WITH_DB != 0 {
		p.Database, _ = ReadStringByNullEndFromReader(reader)
	}

	if capabilities&CLIENT_PLUGIN_AUTH != 0 {
		p.ClientAuthPluginName, _ = ReadStringByNullEndFromReader(reader)
	}

	// 修复：连接属性解析使用有限读取器
	if capabilities&CLIENT_CONNECT_ATTRS != 0 {
		attrLen, _ := ReadLenencNumber[uint64](reader)
		p.ConnectionAttributesLength = attrLen
		p.ConnectionAttributes = make([]ConnectionAttributeItem, 0)

		// 使用有限读取器确保不读取额外数据
		attrReader := io.LimitReader(reader, int64(attrLen))
		for {
			item := &ConnectionAttributeItem{}
			err := item.Unmarshal(attrReader)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				return err
			}
			p.ConnectionAttributes = append(p.ConnectionAttributes, *item)
		}
	}

	if capabilities&CLIENT_ZSTD_COMPRESSION_ALGORITHM != 0 {
		p.ZstdCompressionLevel, _ = ReadNumber[uint8](reader, 1)
	}

	return nil
}

func (p *HandshakeResponse) Marshal() ([]byte, error) {
	buf := new(bytes.Buffer)

	// 1. 写入 ClientCapabilities (2字节小端)
	WriteNumber(buf, p.ClientCapabilities, 2)
	// 2. 写入 ExtendedClientCapabilities (2字节小端)
	WriteNumber(buf, p.ExtendedClientCapabilities, 2)
	// 3. 写入 MaxPacketSize (4字节小端)
	WriteNumber(buf, p.MaxPacketSize, 4)
	// 4. 写入 CharacterSet (1字节)
	WriteNumber(buf, p.CharacterSet, 1)
	// 5. 写入 Reserved (19字节)
	WriteBinary(buf, p.Reserved)
	// 6. 写入 MariaDBCaps (4字节小端)
	WriteNumber(buf, p.MariaDBCaps, 4)
	// 7. 写入 User (NUL结尾字符串)
	WriteStringByNullEnd(buf, p.User)
	// 8. 写入 AuthResponse (1字节长度+N字节内容)
	authRespBytes, err := hex.DecodeString(p.AuthResponse)
	if err != nil {
		return nil, err
	}
	WriteNumber(buf, uint8(len(authRespBytes)), 1)
	WriteBinary(buf, authRespBytes)
	// 9. 写入 Database (如果存在，NUL结尾字符串)
	if p.Database != "" {
		WriteStringByNullEnd(buf, p.Database)
	}
	// 10. 写入 ClientAuthPluginName (如果存在，NUL结尾字符串)
	if p.ClientAuthPluginName != "" {
		WriteStringByNullEnd(buf, p.ClientAuthPluginName)
	}
	// 11. 写入 ConnectionAttributes (如果存在)
	if len(p.ConnectionAttributes) > 0 {
		// 先序列化所有属性到一个临时buffer
		attrBuf := new(bytes.Buffer)
		for _, attr := range p.ConnectionAttributes {
			attrData, err := attr.Marshal()
			if err != nil {
				return nil, err
			}
			attrBuf.Write(attrData)
		}
		attrData := attrBuf.Bytes()
		// 写入属性长度（lenenc）
		WriteLenencNumber(buf, uint64(len(attrData)))
		// 写入属性数据
		WriteBinary(buf, attrData)
	}
	// 12. 写入 ZstdCompressionLevel (如果存在，1字节)
	if p.ZstdCompressionLevel != 0 {
		WriteNumber(buf, p.ZstdCompressionLevel, 1)
	}

	// 组装Packet头部
	payload := buf.Bytes()
	packetBuf := new(bytes.Buffer)
	// PayloadLength 3字节小端
	packetBuf.Write([]byte{byte(len(payload)), byte(len(payload) >> 8), byte(len(payload) >> 16)})
	// SequenceID
	packetBuf.WriteByte(p.SequenceID)
	// Payload
	packetBuf.Write(payload)

	return packetBuf.Bytes(), nil
}

type ConnectionAttributeItem struct {
	Name  string `mysql:"string<lenenc>"`
	Value string `mysql:"string<lenenc>"`
}

func (p *ConnectionAttributeItem) Unmarshal(r io.Reader) (err error) {
	p.Name, err = ReadStringByLenencFromReader[uint8](r)
	if err != nil {
		return
	}
	p.Value, err = ReadStringByLenencFromReader[uint8](r)
	if err != nil {
		return
	}
	return nil
}

func (p *ConnectionAttributeItem) Marshal() ([]byte, error) {
	buf := new(bytes.Buffer)

	// 写入Name (长度编码)
	WriteStringByLenenc(buf, p.Name)
	// 写入Value (长度编码)
	WriteStringByLenenc(buf, p.Value)

	return buf.Bytes(), nil
}

// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_ok_packet.html
// https://mariadb.com/docs/server/clients-and-utilities/server-client-software/client-libraries/clientserver-protocol/4-server-response-packets/ok_packet
type OkPacket struct {
	Packet
	OkInPacket
}

func (p *OkPacket) Unmarshal(r io.Reader, conditional uint32) (err error) {
	p.Packet.Unmarshal(r)
	p.OkInPacket.Unmarshal(r, conditional)
	return
}

func (p *OkPacket) Marshal() ([]byte, error) {
	buf := new(bytes.Buffer)

	// 写入 OK 包内容
	WriteNumber(buf, p.OkInPacket.Header, 1)
	WriteLenencNumber(buf, p.OkInPacket.AffectedRows)
	WriteLenencNumber(buf, p.OkInPacket.LastInsertId)

	// StatusFlags 和 Warnings 都需要在 CLIENT_PROTOCOL_41 时写入
	// 这里我们假设客户端支持 CLIENT_PROTOCOL_41，实际使用时应该传入条件参数
	WriteNumber(buf, p.OkInPacket.StatusFlags, 2)
	WriteNumber(buf, p.OkInPacket.Warnings, 2)

	if p.OkInPacket.Info != "" {
		WriteStringByLenenc(buf, p.OkInPacket.Info)
	}

	if p.OkInPacket.SessionStateInfo != "" {
		WriteStringByLenenc(buf, p.OkInPacket.SessionStateInfo)
	}

	// 组装Packet头部
	payload := buf.Bytes()
	packetBuf := new(bytes.Buffer)
	// PayloadLength 3字节小端
	packetBuf.Write([]byte{byte(len(payload)), byte(len(payload) >> 8), byte(len(payload) >> 16)})
	// SequenceID
	packetBuf.WriteByte(p.SequenceID)
	// Payload
	packetBuf.Write(payload)

	return packetBuf.Bytes(), nil
}

type OkInPacket struct {
	Header       uint8  `mysql:"int<1>"`
	AffectedRows uint64 `mysql:"int<lenenc>"` // 改为 uint64
	LastInsertId uint64 `mysql:"int<lenenc>"` // 改为 uint64
	StatusFlags  uint16 `mysql:"int<2>,conditional=CLIENT_PROTOCOL_41"`
	Warnings     uint16 `mysql:"int<2>,conditional=CLIENT_PROTOCOL_41"`
	Info         string `mysql:"string<lenenc>,optional"`
	// 8.0+ 新增字段
	SessionStateInfo string `mysql:"string<lenenc>,conditional=SERVER_SESSION_STATE_CHANGED"`
}

// IsAutoCommit 检查是否处于自动提交模式
func (p *OkInPacket) IsAutoCommit() bool {
	return p.StatusFlags&SERVER_STATUS_AUTOCOMMIT != 0
}

// IsInTransaction 检查是否在事务中
func (p *OkInPacket) IsInTransaction() bool {
	return p.StatusFlags&SERVER_STATUS_IN_TRANS != 0
}

// IsInTransactionReadOnly 检查是否在只读事务中
func (p *OkInPacket) IsInTransactionReadOnly() bool {
	return p.StatusFlags&SERVER_STATUS_IN_TRANS_READONLY != 0
}

// HasMoreResults 检查是否还有更多结果
func (p *OkInPacket) HasMoreResults() bool {
	return p.StatusFlags&SERVER_MORE_RESULTS_EXISTS != 0
}

// HasSessionStateChanged 检查会话状态是否发生变化
func (p *OkInPacket) HasSessionStateChanged() bool {
	return p.StatusFlags&SERVER_SESSION_STATE_CHANGED != 0
}

// SetAutoCommit 设置自动提交标志
func (p *OkInPacket) SetAutoCommit(autoCommit bool) {
	if autoCommit {
		p.StatusFlags |= SERVER_STATUS_AUTOCOMMIT
	} else {
		p.StatusFlags &^= SERVER_STATUS_AUTOCOMMIT
	}
}

// SetInTransaction 设置事务标志
func (p *OkInPacket) SetInTransaction(inTransaction bool) {
	if inTransaction {
		p.StatusFlags |= SERVER_STATUS_IN_TRANS
	} else {
		p.StatusFlags &^= SERVER_STATUS_IN_TRANS
	}
}

// SetInTransactionReadOnly 设置只读事务标志
func (p *OkInPacket) SetInTransactionReadOnly(readOnly bool) {
	if readOnly {
		p.StatusFlags |= SERVER_STATUS_IN_TRANS_READONLY
	} else {
		p.StatusFlags &^= SERVER_STATUS_IN_TRANS_READONLY
	}
}

// SetMoreResults 设置更多结果标志
func (p *OkInPacket) SetMoreResults(hasMore bool) {
	if hasMore {
		p.StatusFlags |= SERVER_MORE_RESULTS_EXISTS
	} else {
		p.StatusFlags &^= SERVER_MORE_RESULTS_EXISTS
	}
}

// SetSessionStateChanged 设置会话状态变化标志
func (p *OkInPacket) SetSessionStateChanged(changed bool) {
	if changed {
		p.StatusFlags |= SERVER_SESSION_STATE_CHANGED
	} else {
		p.StatusFlags &^= SERVER_SESSION_STATE_CHANGED
	}
}

// GetStatusFlagsDescription 获取状态标志的描述
func (p *OkInPacket) GetStatusFlagsDescription() []string {
	var descriptions []string

	if p.IsInTransaction() {
		descriptions = append(descriptions, "IN_TRANSACTION")
	}
	if p.IsAutoCommit() {
		descriptions = append(descriptions, "AUTOCOMMIT")
	}
	if p.HasMoreResults() {
		descriptions = append(descriptions, "MORE_RESULTS")
	}
	if p.IsInTransactionReadOnly() {
		descriptions = append(descriptions, "IN_TRANSACTION_READONLY")
	}
	if p.HasSessionStateChanged() {
		descriptions = append(descriptions, "SESSION_STATE_CHANGED")
	}

	return descriptions
}

func (p *OkInPacket) Unmarshal(r io.Reader, conditional uint32) (err error) {
	reader := bufio.NewReader(r)
	p.Header, _ = reader.ReadByte()
	p.AffectedRows, _ = ReadLenencNumber[uint64](reader)
	p.LastInsertId, _ = ReadLenencNumber[uint64](reader)
	if conditional&CLIENT_PROTOCOL_41 != 0 {
		p.StatusFlags, _ = ReadNumber[uint16](reader, 2)
	}
	if conditional&CLIENT_PROTOCOL_41 != 0 {
		p.Warnings, _ = ReadNumber[uint16](reader, 2)
	}

	p.Info, _ = ReadStringByLenencFromReader[uint8](reader)
	if conditional&SERVER_SESSION_STATE_CHANGED != 0 {
		p.SessionStateInfo, _ = ReadStringByLenencFromReader[uint8](reader)
	}
	return nil
}

// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_err_packet.html
type ErrorPacket struct {
	Packet
	ErrorInPacket
}

type ErrorInPacket struct {
	Header         uint8  `mysql:"int<1>"`
	ErrorCode      uint16 `mysql:"int<2>"`
	SqlStateMarker string `mysql:"string<1>,conditional=CLIENT_PROTOCOL_41"`
	SqlState       string `mysql:"string<5>,conditional=CLIENT_PROTOCOL_41"`
	ErrorMessage   string `mysql:"string<EOF>"`
}

func (p *ErrorInPacket) Unmarshal(r io.Reader, conditional uint32) (err error) {
	reader := bufio.NewReader(r)
	p.Header, _ = reader.ReadByte()
	p.ErrorCode, _ = ReadNumber[uint16](reader, 2)

	if conditional&CLIENT_PROTOCOL_41 != 0 {
		p.SqlStateMarker, _ = reader.ReadString(1)
		p.SqlState, _ = reader.ReadString(5)
	}

	p.ErrorMessage, _ = ReadStringByNullEndFromReader(reader)
	return nil
}

func (p *ErrorPacket) Marshal() ([]byte, error) {
	buf := new(bytes.Buffer)

	// 写入错误包内容
	WriteNumber(buf, p.ErrorInPacket.Header, 1)
	WriteNumber(buf, p.ErrorInPacket.ErrorCode, 2)

	if p.ErrorInPacket.SqlState != "" {
		WriteStringByNullEnd(buf, p.ErrorInPacket.SqlStateMarker)
		WriteStringByNullEnd(buf, p.ErrorInPacket.SqlState)
	}

	WriteStringByNullEnd(buf, p.ErrorInPacket.ErrorMessage)

	// 组装Packet头部
	payload := buf.Bytes()
	packetBuf := new(bytes.Buffer)
	// PayloadLength 3字节小端
	packetBuf.Write([]byte{byte(len(payload)), byte(len(payload) >> 8), byte(len(payload) >> 16)})
	// SequenceID
	packetBuf.WriteByte(p.SequenceID)
	// Payload
	packetBuf.Write(payload)

	return packetBuf.Bytes(), nil
}

// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_eof_packet.html
type EofPacket struct {
	Packet
	EofInPacket
}

func (p *EofPacket) Unmarshal(r io.Reader, conditional uint32) (err error) {
	if err = p.Packet.Unmarshal(r); err != nil {
		return err
	}

	if err = p.EofInPacket.Unmarshal(r, conditional); err != nil {
		return err
	}
	return nil
}

type EofInPacket struct {
	Header      uint8  `mysql:"int<1>"`
	Warnings    uint16 `mysql:"int<2>,conditional=CLIENT_PROTOCOL_41"`
	StatusFlags uint16 `mysql:"int<2>,conditional=CLIENT_PROTOCOL_41"`
}

// IsAutoCommit 检查是否处于自动提交模式
func (p *EofInPacket) IsAutoCommit() bool {
	return p.StatusFlags&SERVER_STATUS_AUTOCOMMIT != 0
}

// IsInTransaction 检查是否在事务中
func (p *EofInPacket) IsInTransaction() bool {
	return p.StatusFlags&SERVER_STATUS_IN_TRANS != 0
}

// IsInTransactionReadOnly 检查是否在只读事务中
func (p *EofInPacket) IsInTransactionReadOnly() bool {
	return p.StatusFlags&SERVER_STATUS_IN_TRANS_READONLY != 0
}

// HasMoreResults 检查是否还有更多结果
func (p *EofInPacket) HasMoreResults() bool {
	return p.StatusFlags&SERVER_MORE_RESULTS_EXISTS != 0
}

// HasSessionStateChanged 检查会话状态是否发生变化
func (p *EofInPacket) HasSessionStateChanged() bool {
	return p.StatusFlags&SERVER_SESSION_STATE_CHANGED != 0
}

// SetAutoCommit 设置自动提交标志
func (p *EofInPacket) SetAutoCommit(autoCommit bool) {
	if autoCommit {
		p.StatusFlags |= SERVER_STATUS_AUTOCOMMIT
	} else {
		p.StatusFlags &^= SERVER_STATUS_AUTOCOMMIT
	}
}

// SetInTransaction 设置事务标志
func (p *EofInPacket) SetInTransaction(inTransaction bool) {
	if inTransaction {
		p.StatusFlags |= SERVER_STATUS_IN_TRANS
	} else {
		p.StatusFlags &^= SERVER_STATUS_IN_TRANS
	}
}

// SetInTransactionReadOnly 设置只读事务标志
func (p *EofInPacket) SetInTransactionReadOnly(readOnly bool) {
	if readOnly {
		p.StatusFlags |= SERVER_STATUS_IN_TRANS_READONLY
	} else {
		p.StatusFlags &^= SERVER_STATUS_IN_TRANS_READONLY
	}
}

// SetMoreResults 设置更多结果标志
func (p *EofInPacket) SetMoreResults(hasMore bool) {
	if hasMore {
		p.StatusFlags |= SERVER_MORE_RESULTS_EXISTS
	} else {
		p.StatusFlags &^= SERVER_MORE_RESULTS_EXISTS
	}
}

// SetSessionStateChanged 设置会话状态变化标志
func (p *EofInPacket) SetSessionStateChanged(changed bool) {
	if changed {
		p.StatusFlags |= SERVER_SESSION_STATE_CHANGED
	} else {
		p.StatusFlags &^= SERVER_SESSION_STATE_CHANGED
	}
}

// GetStatusFlagsDescription 获取状态标志的描述
func (p *EofInPacket) GetStatusFlagsDescription() []string {
	var descriptions []string

	if p.IsInTransaction() {
		descriptions = append(descriptions, "IN_TRANSACTION")
	}
	if p.IsAutoCommit() {
		descriptions = append(descriptions, "AUTOCOMMIT")
	}
	if p.HasMoreResults() {
		descriptions = append(descriptions, "MORE_RESULTS")
	}
	if p.IsInTransactionReadOnly() {
		descriptions = append(descriptions, "IN_TRANSACTION_READONLY")
	}
	if p.HasSessionStateChanged() {
		descriptions = append(descriptions, "SESSION_STATE_CHANGED")
	}

	return descriptions
}

func (p *EofInPacket) Unmarshal(r io.Reader, conditional uint32) (err error) {
	reader := bufio.NewReader(r)
	p.Header, _ = reader.ReadByte()

	if conditional&CLIENT_PROTOCOL_41 != 0 {
		p.Warnings, _ = ReadNumber[uint16](reader, 2)
		p.StatusFlags, _ = ReadNumber[uint16](reader, 2)
	}

	return nil
}

func (p *EofPacket) Marshal() ([]byte, error) {
	buf := new(bytes.Buffer)

	// 写入 EOF 包内容
	WriteNumber(buf, p.EofInPacket.Header, 1)

	// 在 CLIENT_PROTOCOL_41 条件下，总是写入 Warnings 和 StatusFlags
	WriteNumber(buf, p.EofInPacket.Warnings, 2)
	WriteNumber(buf, p.EofInPacket.StatusFlags, 2)

	// 组装Packet头部
	payload := buf.Bytes()
	packetBuf := new(bytes.Buffer)
	// PayloadLength 3字节小端
	packetBuf.Write([]byte{byte(len(payload)), byte(len(payload) >> 8), byte(len(payload) >> 16)})
	// SequenceID
	packetBuf.WriteByte(p.SequenceID)
	// Payload
	packetBuf.Write(payload)

	return packetBuf.Bytes(), nil
}

// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query.html
type ComQueryPacket struct {
	Packet
	Command           uint8                 `mysql:"int<1>"` // 0x03
	ParameterCount    *uint32               `mysql:"int<lenenc>,omitempty"`
	ParameterSetCount *uint32               `mysql:"int<lenenc>,omitempty"`       // 如果使用占位符模式下 这个一直为1
	NullBitmap        *[]byte               `mysql:"binary<var>,omitempty"`       // 长度等于  (num_params + 7) / 8
	NewParamsBindFlag *uint8                `mysql:"int<1>,omitempty"`            //一直为 1
	Params            []ComQueryPacketParam `mysql:"array,omitempty"`             // new_params_bind_flag == true
	ParameterValues   []any                 `mysql:"array:binary<var>,omitempty"` // new_params_bind_flag == false
	Query             string                `mysql:"string<EOF>"`
}

type ComQueryPacketParam struct {
	ParamTypeAndFlag uint16 `mysql:"int<2>"`
	ParamName        string `mysql:"string<lenenc>"`
}

// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response_text_resultset.html
type TextResultSetPacket struct {
	Packet
	MetaDataFollows *uint8      `mysql:"int<1>,omitempty"`      // CLIENT_OPTIONAL_RESULTSET_METADATA
	ColumnCount     uint64      `mysql:"int<lenenc>,omitempty"` // 如果 meta_data_follows == 1 这个为 0
	FieldsMeta      []FieldMeta `mysql:"array,omitempty"`
	EofFieldsMeta   EofInPacket `mysql:"object"`
	// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response_text_resultset_row.html
	RowData []string       `mysql:"array:string<lenenc>,omitempty"`
	Error   *ErrorInPacket `mysql:"object,omitempty"`
	Ok      *OkInPacket    `mysql:"object,omitempty"`
	Eof     *EofInPacket   `mysql:"object,omitempty"`
}

// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response_text_resultset_column_definition.html
type FieldMeta struct {
	Catalog                   string  `mysql:"string<lenenc>"`
	Schema                    string  `mysql:"string<lenenc>"`
	Table                     string  `mysql:"string<lenenc>"`
	OrgTable                  string  `mysql:"string<lenenc>"`
	Name                      string  `mysql:"string<lenenc>"`
	OrgName                   string  `mysql:"string<lenenc>"`
	LengthOfFixedLengthFields uint32  `mysql:"int<lenenc>"`
	CharacterSet              uint16  `mysql:"int<2>"`
	ColumnLength              uint32  `mysql:"int<4>"`
	Type                      uint8   `mysql:"int<1>"`
	Flags                     uint16  `mysql:"int<2>"`
	Decimals                  uint8   `mysql:"int<1>"`
	Reserved                  string  `mysql:"string<2>"`
	DefaultValue              *string `mysql:"string<lenenc>,omitempty"` // 如果为 NULL 这个为 0xFB
}

type ComPacket struct {
	Packet
	Command uint8 `mysql:"int<1>"`
}

type ComInitDBPacket struct {
	ComPacket
	SchemaName string `mysql:"string<EOF>"`
}

type ComFieldListPacket struct {
	ComPacket
	Table    string `mysql:"string<NUL>"`
	Wildcard string `mysql:"string<EOF>"`
}

type ComSetOptionPacket struct {
	ComPacket
	OptionOperation uint16 `mysql:"int<2>"`
}

func (p *ComSetOptionPacket) Unmarshal(r io.Reader) error {
	if err := p.Packet.Unmarshal(r); err != nil {
		return err
	}

	reader := bufio.NewReader(r)
	p.Command, _ = reader.ReadByte()
	p.OptionOperation, _ = ReadNumber[uint16](reader, 2)
	return nil
}

func (p *ComSetOptionPacket) Marshal() ([]byte, error) {
	buf := new(bytes.Buffer)

	// 写入命令类型
	WriteNumber(buf, p.Command, 1)
	// 写入选项操作
	WriteNumber(buf, p.OptionOperation, 2)

	// 组装Packet头部
	payload := buf.Bytes()
	packetBuf := new(bytes.Buffer)
	// PayloadLength 3字节小端
	packetBuf.Write([]byte{byte(len(payload)), byte(len(payload) >> 8), byte(len(payload) >> 16)})
	// SequenceID
	packetBuf.WriteByte(p.SequenceID)
	// Payload
	packetBuf.Write(payload)

	return packetBuf.Bytes(), nil
}

// ColumnCountPacket 列数包
type ColumnCountPacket struct {
	Packet
	ColumnCount uint64 `mysql:"int<lenenc>"`
}

func (p *ColumnCountPacket) Unmarshal(r io.Reader) error {
	if err := p.Packet.Unmarshal(r); err != nil {
		return err
	}

	reader := bufio.NewReader(r)
	p.ColumnCount, _ = ReadLenencNumber[uint64](reader)
	return nil
}

func (p *ColumnCountPacket) Marshal() ([]byte, error) {
	buf := new(bytes.Buffer)

	// 写入列数（长度编码）
	WriteLenencNumber(buf, p.ColumnCount)

	// 组装Packet头部
	payload := buf.Bytes()
	packetBuf := new(bytes.Buffer)
	// PayloadLength 3字节小端
	packetBuf.Write([]byte{byte(len(payload)), byte(len(payload) >> 8), byte(len(payload) >> 16)})
	// SequenceID
	packetBuf.WriteByte(p.SequenceID)
	// Payload
	packetBuf.Write(payload)

	return packetBuf.Bytes(), nil
}

// FieldMetaPacket 字段元数据包
type FieldMetaPacket struct {
	Packet
	FieldMeta
}

func (p *FieldMetaPacket) Unmarshal(r io.Reader) error {
	if err := p.Packet.Unmarshal(r); err != nil {
		return err
	}

	reader := bufio.NewReader(r)

	// 读取字段元数据
	p.Catalog, _ = ReadStringByLenencFromReader[uint8](reader)
	p.Schema, _ = ReadStringByLenencFromReader[uint8](reader)
	p.Table, _ = ReadStringByLenencFromReader[uint8](reader)
	p.OrgTable, _ = ReadStringByLenencFromReader[uint8](reader)
	p.Name, _ = ReadStringByLenencFromReader[uint8](reader)
	p.OrgName, _ = ReadStringByLenencFromReader[uint8](reader)
	p.LengthOfFixedLengthFields, _ = ReadLenencNumber[uint32](reader)
	p.CharacterSet, _ = ReadNumber[uint16](reader, 2)
	p.ColumnLength, _ = ReadNumber[uint32](reader, 4)
	p.Type, _ = ReadNumber[uint8](reader, 1)
	p.Flags, _ = ReadNumber[uint16](reader, 2)
	p.Decimals, _ = ReadNumber[uint8](reader, 1)

	// 读取保留字段（2字节）
	reserved := make([]byte, 2)
	io.ReadFull(reader, reserved)
	p.Reserved = string(reserved)

	// 读取默认值（可选）
	// 检查是否还有数据可读
	peekBytes, err := reader.Peek(1)
	if err == nil && len(peekBytes) > 0 {
		defaultValue, _ := ReadStringByLenencFromReader[uint8](reader)
		p.DefaultValue = &defaultValue
	}

	return nil
}

func (p *FieldMetaPacket) Marshal() ([]byte, error) {
	buf := new(bytes.Buffer)

	// 写入字段元数据
	WriteStringByLenenc(buf, p.Catalog)
	WriteStringByLenenc(buf, p.Schema)
	WriteStringByLenenc(buf, p.Table)
	WriteStringByLenenc(buf, p.OrgTable)
	WriteStringByLenenc(buf, p.Name)
	WriteStringByLenenc(buf, p.OrgName)
	p.LengthOfFixedLengthFields = 0xc
	WriteLenencNumber(buf, p.LengthOfFixedLengthFields)
	WriteNumber(buf, p.CharacterSet, 2)
	WriteNumber(buf, p.ColumnLength, 4)
	WriteNumber(buf, p.Type, 1)
	WriteNumber(buf, p.Flags, 2)
	WriteNumber(buf, p.Decimals, 1)
	WriteBinary(buf, []byte{0x00, 0x00})

	if p.DefaultValue != nil {
		WriteStringByLenenc(buf, *p.DefaultValue)
	}

	// 组装Packet头部
	payload := buf.Bytes()
	packetBuf := new(bytes.Buffer)
	// PayloadLength 3字节小端
	packetBuf.Write([]byte{byte(len(payload)), byte(len(payload) >> 8), byte(len(payload) >> 16)})
	// SequenceID
	packetBuf.WriteByte(p.SequenceID)
	// Payload
	packetBuf.Write(payload)

	return packetBuf.Bytes(), nil
}

// RowDataPacket 数据行包
type RowDataPacket struct {
	Packet
	RowData []string `mysql:"array:string<lenenc>"`
}

func (p *RowDataPacket) Unmarshal(r io.Reader) error {
	if err := p.Packet.Unmarshal(r); err != nil {
		return err
	}

	reader := bufio.NewReader(r)

	// 读取行数据（长度编码字符串数组）
	p.RowData = make([]string, 0)
	for {
		// 检查是否还有数据可读
		peekBytes, err := reader.Peek(1)
		if err != nil || len(peekBytes) == 0 {
			break
		}

		value, err := ReadStringByLenencFromReader[uint8](reader)
		if err != nil {
			break
		}
		p.RowData = append(p.RowData, value)
	}

	return nil
}

func (p *RowDataPacket) Marshal() ([]byte, error) {
	buf := new(bytes.Buffer)

	// 写入行数据
	for _, value := range p.RowData {
		WriteStringByLenenc(buf, value)
	}

	// 组装Packet头部
	payload := buf.Bytes()
	packetBuf := new(bytes.Buffer)
	// PayloadLength 3字节小端
	packetBuf.Write([]byte{byte(len(payload)), byte(len(payload) >> 8), byte(len(payload) >> 16)})
	// SequenceID
	packetBuf.WriteByte(p.SequenceID)
	// Payload
	packetBuf.Write(payload)

	return packetBuf.Bytes(), nil
}

// COM_STMT_PREPARE 包 - 预处理语句
type ComStmtPreparePacket struct {
	Packet
	Command uint8  `mysql:"int<1>"` // 0x16
	Query   string `mysql:"string<EOF>"`
}

func (p *ComStmtPreparePacket) Unmarshal(r io.Reader) error {
	if err := p.Packet.Unmarshal(r); err != nil {
		return err
	}

	// 从Payload中读取Command和Query
	if len(p.Payload) >= 1 {
		p.Command = p.Payload[0]
		// Query是从第二个字节开始到包结尾
		if len(p.Payload) > 1 {
			p.Query = string(p.Payload[1:])
		}
	}
	return nil
}

func (p *ComStmtPreparePacket) Marshal() ([]byte, error) {
	buf := new(bytes.Buffer)

	// 写入命令类型
	WriteNumber(buf, p.Command, 1)
	// 写入查询字符串
	WriteStringByNullEnd(buf, p.Query)

	// 组装Packet头部
	payload := buf.Bytes()
	packetBuf := new(bytes.Buffer)
	// PayloadLength 3字节小端
	packetBuf.Write([]byte{byte(len(payload)), byte(len(payload) >> 8), byte(len(payload) >> 16)})
	// SequenceID
	packetBuf.WriteByte(p.SequenceID)
	// Payload
	packetBuf.Write(payload)

	return packetBuf.Bytes(), nil
}

// COM_STMT_EXECUTE 包 - 执行预处理语句
type ComStmtExecutePacket struct {
	Packet
	Command           uint8           `mysql:"int<1>"` // 0x17
	StatementID       uint32          `mysql:"int<4>"`
	Flags             uint8           `mysql:"int<1>"`
	IterationCount    uint32          `mysql:"int<4>"`
	NullBitmap        []byte          `mysql:"binary<var>"`
	NewParamsBindFlag uint8           `mysql:"int<1>"`
	ParamTypes        []StmtParamType `mysql:"array,omitempty"`
	ParamValues       []any           `mysql:"array,omitempty"`
}

type StmtParamType struct {
	Type uint8 `mysql:"int<1>"`
	Flag uint8 `mysql:"int<1>"`
}

func (p *ComStmtExecutePacket) Unmarshal(r io.Reader) error {
	// 先调用父类的 Unmarshal 来读取包结构和 payload
	if err := p.Packet.Unmarshal(r); err != nil {
		return err
	}

	// 从 Payload 中解析 COM_STMT_EXECUTE 的具体内容
	// Payload 格式: Command(1) + StatementID(4) + Flags(1) + IterationCount(4) + NullBitmap + NewParamsBindFlag(1) + [ParamTypes] + ParamValues
	if len(p.Payload) < 11 {
		return nil // 至少需要 11 字节
	}

	reader := bytes.NewReader(p.Payload)
	p.Command, _ = reader.ReadByte()
	p.StatementID, _ = ReadNumber[uint32](reader, 4)
	p.Flags, _ = ReadNumber[uint8](reader, 1)
	p.IterationCount, _ = ReadNumber[uint32](reader, 4)

	// 读取剩余的所有数据（包含 NULL Bitmap, NewParamsBindFlag, ParamTypes, ParamValues）
	remainingData, _ := io.ReadAll(reader)
	dataReader := bytes.NewReader(remainingData)

	// 我们需要猜测参数数量，这里假设最多256个参数
	maxParamCount := 256
	p.NullBitmap, _ = io.ReadAll(io.LimitReader(dataReader, int64((maxParamCount+7)/8)))
	p.NewParamsBindFlag, _ = dataReader.ReadByte()

	// 如果 NewParamsBindFlag = 1，读取参数类型
	if p.NewParamsBindFlag == 1 {
		p.ParamTypes = make([]StmtParamType, 0)
		for dataReader.Len() >= 2 {
			paramType := StmtParamType{}
			paramType.Type, _ = dataReader.ReadByte()
			paramType.Flag, _ = dataReader.ReadByte()
			p.ParamTypes = append(p.ParamTypes, paramType)
		}
	}

	// 根据 ParamTypes 的数量重新确定 NULL Bitmap 的长度
	paramCount := len(p.ParamTypes)
	if paramCount > 0 {
		nullBitmapLen := (paramCount + 7) / 8
		if len(p.NullBitmap) >= nullBitmapLen {
			p.NullBitmap = p.NullBitmap[:nullBitmapLen]
		}
	}

	// 读取参数值
	if len(p.ParamTypes) > 0 {
		p.ParamValues = make([]any, 0, len(p.ParamTypes))
		for i, paramType := range p.ParamTypes {
			// 检查是否为NULL
			byteIdx := i / 8
			bitIdx := uint(i % 8)
			isNull := (len(p.NullBitmap) > byteIdx) && (p.NullBitmap[byteIdx] & (1 << bitIdx)) != 0

			if isNull {
				p.ParamValues = append(p.ParamValues, nil)
				continue
			}

			// 根据参数类型读取值
			switch paramType.Type {
			case 0x01: // TINYINT
				val, _ := dataReader.ReadByte()
				p.ParamValues = append(p.ParamValues, int8(val))
			case 0x02: // SMALLINT
				val, _ := ReadNumber[uint16](dataReader, 2)
				p.ParamValues = append(p.ParamValues, int16(val))
			case 0x03: // INT
				val, _ := ReadNumber[uint32](dataReader, 4)
				p.ParamValues = append(p.ParamValues, int32(val))
			case 0x08: // BIGINT
				val, _ := ReadNumber[uint64](dataReader, 8)
				p.ParamValues = append(p.ParamValues, int64(val))
			case 0x0a: // FLOAT
				var val float32
				binary.Read(dataReader, binary.LittleEndian, &val)
				p.ParamValues = append(p.ParamValues, val)
			case 0x0b: // DOUBLE
				var val float64
				binary.Read(dataReader, binary.LittleEndian, &val)
				p.ParamValues = append(p.ParamValues, val)
			case 0x0f, 0xfd: // VARCHAR, VAR_STRING
				val, _ := ReadStringByLenencFromReader[uint8](dataReader)
				p.ParamValues = append(p.ParamValues, val)
			case 0x0c: // DATE
				val, _ := ReadNumber[uint8](dataReader, 1)
				if val == 0 {
					p.ParamValues = append(p.ParamValues, nil)
				} else {
					year, _ := ReadNumber[uint16](dataReader, 2)
					month, _ := dataReader.ReadByte()
					day, _ := dataReader.ReadByte()
					p.ParamValues = append(p.ParamValues, fmt.Sprintf("%04d-%02d-%02d", year, month, day))
				}
			case 0x0d: // TIME
				val, _ := ReadNumber[uint8](dataReader, 1)
				if val == 0 {
					p.ParamValues = append(p.ParamValues, "00:00:00")
				} else {
					// 读取时间值
					neg, _ := dataReader.ReadByte()
					_, _ = ReadNumber[uint32](dataReader, 4) // days
					hours, _ := dataReader.ReadByte()
					minutes, _ := dataReader.ReadByte()
					seconds, _ := dataReader.ReadByte()
					microseconds, _ := ReadNumber[uint32](dataReader, 4)
					p.ParamValues = append(p.ParamValues, fmt.Sprintf("%d%02d:%02d:%02d.%06d", neg, hours, minutes, seconds, microseconds))
				}
			case 0x0e: // DATETIME
				val, _ := ReadNumber[uint8](dataReader, 1)
				if val == 0 {
					p.ParamValues = append(p.ParamValues, nil)
				} else {
					year, _ := ReadNumber[uint16](dataReader, 2)
					month, _ := dataReader.ReadByte()
					day, _ := dataReader.ReadByte()
					hours, _ := dataReader.ReadByte()
					minutes, _ := dataReader.ReadByte()
					seconds, _ := dataReader.ReadByte()
					microseconds := uint32(0)
					if val >= 7 {
						microseconds, _ = ReadNumber[uint32](dataReader, 4)
					}
					p.ParamValues = append(p.ParamValues, fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d.%06d", year, month, day, hours, minutes, seconds, microseconds))
				}
			case 0xfb: // NULL
				p.ParamValues = append(p.ParamValues, nil)
			default:
				// 默认作为字符串读取
				val, _ := ReadStringByLenencFromReader[uint8](dataReader)
				p.ParamValues = append(p.ParamValues, val)
			}
		}
	}

	return nil
}

func (p *ComStmtExecutePacket) Marshal() ([]byte, error) {
	buf := new(bytes.Buffer)

	// 写入命令类型
	WriteNumber(buf, p.Command, 1)
	// 写入语句ID
	WriteNumber(buf, p.StatementID, 4)
	// 写入标志
	WriteNumber(buf, p.Flags, 1)
	// 写入迭代计数
	WriteNumber(buf, p.IterationCount, 4)

	// 计算参数数量
	paramCount := len(p.ParamTypes)
	if paramCount == 0 && len(p.ParamValues) > 0 {
		paramCount = len(p.ParamValues)
	}

	// 根据参数数量计算 NULL Bitmap 长度
	nullBitmapLen := (paramCount + 7) / 8

	// 确保 NullBitmap 长度正确
	if len(p.NullBitmap) < nullBitmapLen {
		// 扩展 NullBitmap
		newBitmap := make([]byte, nullBitmapLen)
		copy(newBitmap, p.NullBitmap)
		p.NullBitmap = newBitmap
	} else if len(p.NullBitmap) > nullBitmapLen {
		// 截断 NullBitmap
		p.NullBitmap = p.NullBitmap[:nullBitmapLen]
	}

	// 写入NULL位图
	WriteBinary(buf, p.NullBitmap)

	// 写入新参数绑定标志
	WriteNumber(buf, p.NewParamsBindFlag, 1)

	// 如果有参数类型和值，写入它们
	if p.NewParamsBindFlag == 1 {
		// 写入参数类型
		for _, paramType := range p.ParamTypes {
			WriteNumber(buf, paramType.Type, 1)
			WriteNumber(buf, paramType.Flag, 1)
		}

		// 写入参数值（根据类型）
		for i, value := range p.ParamValues {
			// 跳过已标记为NULL的参数
			byteIdx := i / 8
			bitIdx := uint(i % 8)
			if len(p.NullBitmap) > byteIdx && (p.NullBitmap[byteIdx] & (1 << bitIdx)) != 0 {
				continue
			}

			if i < len(p.ParamTypes) {
				switch p.ParamTypes[i].Type {
				case 0x01: // TINYINT
					if val, ok := value.(int8); ok {
						buf.WriteByte(byte(val))
					} else if val, ok := value.(int); ok {
						buf.WriteByte(byte(val))
					}
				case 0x02: // SMALLINT
					if val, ok := value.(int16); ok {
						binary.Write(buf, binary.LittleEndian, val)
					} else if val, ok := value.(int); ok {
						binary.Write(buf, binary.LittleEndian, int16(val))
					}
				case 0x03: // INT
					if val, ok := value.(int32); ok {
						binary.Write(buf, binary.LittleEndian, val)
					} else if val, ok := value.(int); ok {
						binary.Write(buf, binary.LittleEndian, int32(val))
					}
				case 0x08: // BIGINT
					if val, ok := value.(int64); ok {
						binary.Write(buf, binary.LittleEndian, val)
					} else if val, ok := value.(int); ok {
						binary.Write(buf, binary.LittleEndian, int64(val))
					}
				case 0x0a: // FLOAT
					if val, ok := value.(float32); ok {
						binary.Write(buf, binary.LittleEndian, val)
					}
				case 0x0b: // DOUBLE
					if val, ok := value.(float64); ok {
						binary.Write(buf, binary.LittleEndian, val)
					}
				case 0x0f, 0xfd: // VARCHAR, VAR_STRING
					if val, ok := value.(string); ok {
						WriteStringByLenenc(buf, val)
					}
				default:
					// 默认作为字符串
					if val, ok := value.(string); ok {
						WriteStringByLenenc(buf, val)
					}
				}
			} else {
				// 没有类型信息，默认作为字符串
				if val, ok := value.(string); ok {
					WriteStringByLenenc(buf, val)
				}
			}
		}
	}

	// 组装Packet头部
	payload := buf.Bytes()
	packetBuf := new(bytes.Buffer)
	// PayloadLength 3字节小端
	packetBuf.Write([]byte{byte(len(payload)), byte(len(payload) >> 8), byte(len(payload) >> 16)})
	// SequenceID
	packetBuf.WriteByte(p.SequenceID)
	// Payload
	packetBuf.Write(payload)

	return packetBuf.Bytes(), nil
}

// COM_STMT_PREPARE 响应包 - 预处理语句响应
// https://mariadb.com/docs/server/reference/clientserver-protocol/3-binary-protocol-prepared-statements/com_stmt_prepare
type StmtPrepareResponsePacket struct {
	Packet
	StatementID   uint32    `mysql:"int<4>"`
	ColumnCount   uint16    `mysql:"int<2>"`
	ParamCount    uint16    `mysql:"int<2>"`
	Reserved      uint8     `mysql:"int<1>"`
	WarningCount  uint16    `mysql:"int<2>"`
	Params        []FieldMeta `mysql:"array,omitempty"` // 参数元数据
	Columns       []FieldMeta `mysql:"array,omitempty"` // 列元数据
}

func (p *StmtPrepareResponsePacket) Unmarshal(r io.Reader) error {
	if err := p.Packet.Unmarshal(r); err != nil {
		return err
	}

	reader := bufio.NewReader(r)
	p.StatementID, _ = ReadNumber[uint32](reader, 4)
	p.ColumnCount, _ = ReadNumber[uint16](reader, 2)
	p.ParamCount, _ = ReadNumber[uint16](reader, 2)
	p.Reserved, _ = ReadNumber[uint8](reader, 1)
	p.WarningCount, _ = ReadNumber[uint16](reader, 2)

	// 读取参数元数据（如果有）
	p.Params = make([]FieldMeta, p.ParamCount)
	for i := uint16(0); i < p.ParamCount; i++ {
		paramMeta := FieldMeta{}
		paramMeta.Catalog, _ = ReadStringByLenencFromReader[uint8](reader)
		paramMeta.Schema, _ = ReadStringByLenencFromReader[uint8](reader)
		paramMeta.Table, _ = ReadStringByLenencFromReader[uint8](reader)
		paramMeta.OrgTable, _ = ReadStringByLenencFromReader[uint8](reader)
		paramMeta.Name, _ = ReadStringByLenencFromReader[uint8](reader)
		paramMeta.OrgName, _ = ReadStringByLenencFromReader[uint8](reader)
		paramMeta.LengthOfFixedLengthFields, _ = ReadLenencNumber[uint32](reader)
		paramMeta.CharacterSet, _ = ReadNumber[uint16](reader, 2)
		paramMeta.ColumnLength, _ = ReadNumber[uint32](reader, 4)
		paramMeta.Type, _ = ReadNumber[uint8](reader, 1)
		paramMeta.Flags, _ = ReadNumber[uint16](reader, 2)
		paramMeta.Decimals, _ = ReadNumber[uint8](reader, 1)
		// 读取保留字段（2字节）
		reserved := make([]byte, 2)
		io.ReadFull(reader, reserved)
		p.Params[i] = paramMeta
	}

	// 读取参数元数据结束包（EOF 或 OK）
	if p.ParamCount > 0 {
		// 读取并丢弃 EOF/OK 包
		peekByte, err := reader.Peek(1)
		if err == nil && len(peekByte) > 0 {
			if peekByte[0] == 0xfe || peekByte[0] == 0x00 {
				// EOF 或 OK 包
				eofPacket := &EofInPacket{}
				eofPacket.Header, _ = reader.ReadByte()
				if eofPacket.Header == 0xfe {
					eofPacket.Warnings, _ = ReadNumber[uint16](reader, 2)
					eofPacket.StatusFlags, _ = ReadNumber[uint16](reader, 2)
				} else if eofPacket.Header == 0x00 {
					// OK 包 - 读取受影响行数和插入ID
					io.ReadFull(reader, make([]byte, 2)) // 跳过
					eofPacket.Warnings, _ = ReadNumber[uint16](reader, 2)
					eofPacket.StatusFlags, _ = ReadNumber[uint16](reader, 2)
				}
			}
		}
	}

	// 读取列元数据（如果有）
	p.Columns = make([]FieldMeta, p.ColumnCount)
	for i := uint16(0); i < p.ColumnCount; i++ {
		colMeta := FieldMeta{}
		colMeta.Catalog, _ = ReadStringByLenencFromReader[uint8](reader)
		colMeta.Schema, _ = ReadStringByLenencFromReader[uint8](reader)
		colMeta.Table, _ = ReadStringByLenencFromReader[uint8](reader)
		colMeta.OrgTable, _ = ReadStringByLenencFromReader[uint8](reader)
		colMeta.Name, _ = ReadStringByLenencFromReader[uint8](reader)
		colMeta.OrgName, _ = ReadStringByLenencFromReader[uint8](reader)
		colMeta.LengthOfFixedLengthFields, _ = ReadLenencNumber[uint32](reader)
		colMeta.CharacterSet, _ = ReadNumber[uint16](reader, 2)
		colMeta.ColumnLength, _ = ReadNumber[uint32](reader, 4)
		colMeta.Type, _ = ReadNumber[uint8](reader, 1)
		colMeta.Flags, _ = ReadNumber[uint16](reader, 2)
		colMeta.Decimals, _ = ReadNumber[uint8](reader, 1)
		// 读取保留字段（2字节）
		reserved := make([]byte, 2)
		io.ReadFull(reader, reserved)
		p.Columns[i] = colMeta
	}

	return nil
}

func (p *StmtPrepareResponsePacket) Marshal() ([]byte, error) {
	buf := new(bytes.Buffer)

	// 写入语句ID
	WriteNumber(buf, p.StatementID, 4)
	// 写入列数
	WriteNumber(buf, p.ColumnCount, 2)
	// 写入参数数
	WriteNumber(buf, p.ParamCount, 2)
	// 写入保留字段
	WriteNumber(buf, p.Reserved, 1)
	// 写入警告数
	WriteNumber(buf, p.WarningCount, 2)

	// 写入参数元数据
	for _, param := range p.Params {
		WriteStringByLenenc(buf, param.Catalog)
		WriteStringByLenenc(buf, param.Schema)
		WriteStringByLenenc(buf, param.Table)
		WriteStringByLenenc(buf, param.OrgTable)
		WriteStringByLenenc(buf, param.Name)
		WriteStringByLenenc(buf, param.OrgName)
		WriteLenencNumber(buf, 0x0c)
		WriteNumber(buf, param.CharacterSet, 2)
		WriteNumber(buf, param.ColumnLength, 4)
		WriteNumber(buf, param.Type, 1)
		WriteNumber(buf, param.Flags, 2)
		WriteNumber(buf, param.Decimals, 1)
		WriteBinary(buf, []byte{0x00, 0x00})
	}

	// 写入参数结束包（如果存在参数）
	if p.ParamCount > 0 {
		eofBuf := new(bytes.Buffer)
		eofBuf.WriteByte(0x00) // OK header
		WriteLenencNumber(eofBuf, 0) // affected rows
		WriteLenencNumber(eofBuf, 0) // last insert id
		WriteNumber(eofBuf, 0, 2)    // status flags
		WriteNumber(eofBuf, 0, 2)    // warnings
		WriteBinary(buf, eofBuf.Bytes())
	}

	// 写入列元数据
	for _, col := range p.Columns {
		WriteStringByLenenc(buf, col.Catalog)
		WriteStringByLenenc(buf, col.Schema)
		WriteStringByLenenc(buf, col.Table)
		WriteStringByLenenc(buf, col.OrgTable)
		WriteStringByLenenc(buf, col.Name)
		WriteStringByLenenc(buf, col.OrgName)
		WriteLenencNumber(buf, 0x0c)
		WriteNumber(buf, col.CharacterSet, 2)
		WriteNumber(buf, col.ColumnLength, 4)
		WriteNumber(buf, col.Type, 1)
		WriteNumber(buf, col.Flags, 2)
		WriteNumber(buf, col.Decimals, 1)
		WriteBinary(buf, []byte{0x00, 0x00})
	}

	// 写入列结束包（如果存在列）
	if p.ColumnCount > 0 {
		eofBuf := new(bytes.Buffer)
		eofBuf.WriteByte(0x00) // OK header
		WriteLenencNumber(eofBuf, 0) // affected rows
		WriteLenencNumber(eofBuf, 0) // last insert id
		WriteNumber(eofBuf, 0, 2)    // status flags
		WriteNumber(eofBuf, 0, 2)    // warnings
		WriteBinary(buf, eofBuf.Bytes())
	}

	// 组装Packet头部
	payload := buf.Bytes()
	packetBuf := new(bytes.Buffer)
	// PayloadLength 3字节小端
	packetBuf.Write([]byte{byte(len(payload)), byte(len(payload) >> 8), byte(len(payload) >> 16)})
	// SequenceID
	packetBuf.WriteByte(p.SequenceID)
	// Payload
	packetBuf.Write(payload)

	return packetBuf.Bytes(), nil
}

// COM_STMT_EXECUTE 服务器响应 - 二进制结果集包
// https://mariadb.com/docs/server/reference/clientserver-protocol/3-binary-protocol-prepared-statements/com_stmt_execute
type BinaryResultSetPacket struct {
	Packet
	ColumnCount     uint64      `mysql:"int<lenenc>,omitempty"`
	FieldsMeta      []FieldMeta `mysql:"array,omitempty"`
	EofFieldsMeta   EofInPacket `mysql:"object,omitempty"`
	RowData         [][]any     `mysql:"array:binary<var>,omitempty"`
	Error           *ErrorInPacket `mysql:"object,omitempty"`
	Ok              *OkInPacket    `mysql:"object,omitempty"`
	Eof             *EofInPacket   `mysql:"object,omitempty"`
}

// COM_STMT_CLOSE 包 - 关闭预处理语句
type ComStmtClosePacket struct {
	Packet
	Command     uint8  `mysql:"int<1>"` // 0x19
	StatementID uint32 `mysql:"int<4>"`
}

func (p *ComStmtClosePacket) Unmarshal(r io.Reader) error {
	if err := p.Packet.Unmarshal(r); err != nil {
		return err
	}

	reader := bufio.NewReader(r)
	p.Command, _ = reader.ReadByte()
	p.StatementID, _ = ReadNumber[uint32](reader, 4)
	return nil
}

func (p *ComStmtClosePacket) Marshal() ([]byte, error) {
	buf := new(bytes.Buffer)

	// 写入命令类型
	WriteNumber(buf, p.Command, 1)
	// 写入语句ID
	WriteNumber(buf, p.StatementID, 4)

	// 组装Packet头部
	payload := buf.Bytes()
	packetBuf := new(bytes.Buffer)
	// PayloadLength 3字节小端
	packetBuf.Write([]byte{byte(len(payload)), byte(len(payload) >> 8), byte(len(payload) >> 16)})
	// SequenceID
	packetBuf.WriteByte(p.SequenceID)
	// Payload
	packetBuf.Write(payload)

	return packetBuf.Bytes(), nil
}

// COM_PING 包 - 心跳包
type ComPingPacket struct {
	Packet
	Command uint8 `mysql:"int<1>"` // 0x0e
}

func (p *ComPingPacket) Unmarshal(r io.Reader) error {
	if err := p.Packet.Unmarshal(r); err != nil {
		return err
	}

	reader := bufio.NewReader(r)
	p.Command, _ = reader.ReadByte()
	return nil
}

func (p *ComPingPacket) Marshal() ([]byte, error) {
	buf := new(bytes.Buffer)

	// 写入命令类型
	WriteNumber(buf, p.Command, 1)

	// 组装Packet头部
	payload := buf.Bytes()
	packetBuf := new(bytes.Buffer)
	// PayloadLength 3字节小端
	packetBuf.Write([]byte{byte(len(payload)), byte(len(payload) >> 8), byte(len(payload) >> 16)})
	// SequenceID
	packetBuf.WriteByte(p.SequenceID)
	// Payload
	packetBuf.Write(payload)

	return packetBuf.Bytes(), nil
}

// COM_QUIT 包 - 断开连接
type ComQuitPacket struct {
	Packet
	Command uint8 `mysql:"int<1>"` // 0x01
}

func (p *ComQuitPacket) Unmarshal(r io.Reader) error {
	if err := p.Packet.Unmarshal(r); err != nil {
		return err
	}

	reader := bufio.NewReader(r)
	p.Command, _ = reader.ReadByte()
	return nil
}

func (p *ComQuitPacket) Marshal() ([]byte, error) {
	buf := new(bytes.Buffer)

	// 写入命令类型
	WriteNumber(buf, p.Command, 1)

	// 组装Packet头部
	payload := buf.Bytes()
	packetBuf := new(bytes.Buffer)
	// PayloadLength 3字节小端
	packetBuf.Write([]byte{byte(len(payload)), byte(len(payload) >> 8), byte(len(payload) >> 16)})
	// SequenceID
	packetBuf.WriteByte(p.SequenceID)
	// Payload
	packetBuf.Write(payload)

	return packetBuf.Bytes(), nil
}

// COM_REFRESH 包 - 刷新
type ComRefreshPacket struct {
	Packet
	Command    uint8 `mysql:"int<1>"` // 0x07
	SubCommand uint8 `mysql:"int<1>"`
}

func (p *ComRefreshPacket) Unmarshal(r io.Reader) error {
	if err := p.Packet.Unmarshal(r); err != nil {
		return err
	}

	reader := bufio.NewReader(r)
	p.Command, _ = reader.ReadByte()
	p.SubCommand, _ = reader.ReadByte()
	return nil
}

func (p *ComRefreshPacket) Marshal() ([]byte, error) {
	buf := new(bytes.Buffer)

	// 写入命令类型
	WriteNumber(buf, p.Command, 1)
	// 写入子命令
	WriteNumber(buf, p.SubCommand, 1)

	// 组装Packet头部
	payload := buf.Bytes()
	packetBuf := new(bytes.Buffer)
	// PayloadLength 3字节小端
	packetBuf.Write([]byte{byte(len(payload)), byte(len(payload) >> 8), byte(len(payload) >> 16)})
	// SequenceID
	packetBuf.WriteByte(p.SequenceID)
	// Payload
	packetBuf.Write(payload)

	return packetBuf.Bytes(), nil
}

// COM_SHUTDOWN 包 - 关闭服务器
type ComShutdownPacket struct {
	Packet
	Command      uint8 `mysql:"int<1>"` // 0x08
	ShutdownType uint8 `mysql:"int<1>"`
}

func (p *ComShutdownPacket) Unmarshal(r io.Reader) error {
	if err := p.Packet.Unmarshal(r); err != nil {
		return err
	}

	reader := bufio.NewReader(r)
	p.Command, _ = reader.ReadByte()
	p.ShutdownType, _ = reader.ReadByte()
	return nil
}

func (p *ComShutdownPacket) Marshal() ([]byte, error) {
	buf := new(bytes.Buffer)

	// 写入命令类型
	WriteNumber(buf, p.Command, 1)
	// 写入关闭类型
	WriteNumber(buf, p.ShutdownType, 1)

	// 组装Packet头部
	payload := buf.Bytes()
	packetBuf := new(bytes.Buffer)
	// PayloadLength 3字节小端
	packetBuf.Write([]byte{byte(len(payload)), byte(len(payload) >> 8), byte(len(payload) >> 16)})
	// SequenceID
	packetBuf.WriteByte(p.SequenceID)
	// Payload
	packetBuf.Write(payload)

	return packetBuf.Bytes(), nil
}

// COM_STATISTICS 包 - 获取服务器统计信息
type ComStatisticsPacket struct {
	Packet
	Command uint8 `mysql:"int<1>"` // 0x09
}

func (p *ComStatisticsPacket) Unmarshal(r io.Reader) error {
	if err := p.Packet.Unmarshal(r); err != nil {
		return err
	}

	reader := bufio.NewReader(r)
	p.Command, _ = reader.ReadByte()
	return nil
}

func (p *ComStatisticsPacket) Marshal() ([]byte, error) {
	buf := new(bytes.Buffer)

	// 写入命令类型
	WriteNumber(buf, p.Command, 1)

	// 组装Packet头部
	payload := buf.Bytes()
	packetBuf := new(bytes.Buffer)
	// PayloadLength 3字节小端
	packetBuf.Write([]byte{byte(len(payload)), byte(len(payload) >> 8), byte(len(payload) >> 16)})
	// SequenceID
	packetBuf.WriteByte(p.SequenceID)
	// Payload
	packetBuf.Write(payload)

	return packetBuf.Bytes(), nil
}

// COM_PROCESS_INFO 包 - 获取进程信息
type ComProcessInfoPacket struct {
	Packet
	Command uint8 `mysql:"int<1>"` // 0x0a
}

func (p *ComProcessInfoPacket) Unmarshal(r io.Reader) error {
	if err := p.Packet.Unmarshal(r); err != nil {
		return err
	}

	reader := bufio.NewReader(r)
	p.Command, _ = reader.ReadByte()
	return nil
}

func (p *ComProcessInfoPacket) Marshal() ([]byte, error) {
	buf := new(bytes.Buffer)

	// 写入命令类型
	WriteNumber(buf, p.Command, 1)

	// 组装Packet头部
	payload := buf.Bytes()
	packetBuf := new(bytes.Buffer)
	// PayloadLength 3字节小端
	packetBuf.Write([]byte{byte(len(payload)), byte(len(payload) >> 8), byte(len(payload) >> 16)})
	// SequenceID
	packetBuf.WriteByte(p.SequenceID)
	// Payload
	packetBuf.Write(payload)

	return packetBuf.Bytes(), nil
}

// COM_PROCESS_KILL 包 - 终止进程
type ComProcessKillPacket struct {
	Packet
	Command   uint8  `mysql:"int<1>"` // 0x0c
	ProcessID uint32 `mysql:"int<4>"`
}

func (p *ComProcessKillPacket) Unmarshal(r io.Reader) error {
	if err := p.Packet.Unmarshal(r); err != nil {
		return err
	}

	reader := bufio.NewReader(r)
	p.Command, _ = reader.ReadByte()
	p.ProcessID, _ = ReadNumber[uint32](reader, 4)
	return nil
}

func (p *ComProcessKillPacket) Marshal() ([]byte, error) {
	buf := new(bytes.Buffer)

	// 写入命令类型
	WriteNumber(buf, p.Command, 1)
	// 写入进程ID
	WriteNumber(buf, p.ProcessID, 4)

	// 组装Packet头部
	payload := buf.Bytes()
	packetBuf := new(bytes.Buffer)
	// PayloadLength 3字节小端
	packetBuf.Write([]byte{byte(len(payload)), byte(len(payload) >> 8), byte(len(payload) >> 16)})
	// SequenceID
	packetBuf.WriteByte(p.SequenceID)
	// Payload
	packetBuf.Write(payload)

	return packetBuf.Bytes(), nil
}

// COM_DEBUG 包 - 调试
type ComDebugPacket struct {
	Packet
	Command uint8 `mysql:"int<1>"` // 0x0d
}

func (p *ComDebugPacket) Unmarshal(r io.Reader) error {
	if err := p.Packet.Unmarshal(r); err != nil {
		return err
	}

	reader := bufio.NewReader(r)
	p.Command, _ = reader.ReadByte()
	return nil
}

func (p *ComDebugPacket) Marshal() ([]byte, error) {
	buf := new(bytes.Buffer)

	// 写入命令类型
	WriteNumber(buf, p.Command, 1)

	// 组装Packet头部
	payload := buf.Bytes()
	packetBuf := new(bytes.Buffer)
	// PayloadLength 3字节小端
	packetBuf.Write([]byte{byte(len(payload)), byte(len(payload) >> 8), byte(len(payload) >> 16)})
	// SequenceID
	packetBuf.WriteByte(p.SequenceID)
	// Payload
	packetBuf.Write(payload)

	return packetBuf.Bytes(), nil
}

// COM_TIME 包 - 获取时间
type ComTimePacket struct {
	Packet
	Command uint8 `mysql:"int<1>"` // 0x0f
}

func (p *ComTimePacket) Unmarshal(r io.Reader) error {
	if err := p.Packet.Unmarshal(r); err != nil {
		return err
	}

	reader := bufio.NewReader(r)
	p.Command, _ = reader.ReadByte()
	return nil
}

func (p *ComTimePacket) Marshal() ([]byte, error) {
	buf := new(bytes.Buffer)

	// 写入命令类型
	WriteNumber(buf, p.Command, 1)

	// 组装Packet头部
	payload := buf.Bytes()
	packetBuf := new(bytes.Buffer)
	// PayloadLength 3字节小端
	packetBuf.Write([]byte{byte(len(payload)), byte(len(payload) >> 8), byte(len(payload) >> 16)})
	// SequenceID
	packetBuf.WriteByte(p.SequenceID)
	// Payload
	packetBuf.Write(payload)

	return packetBuf.Bytes(), nil
}

// COM_DELAYED_INSERT 包 - 延迟插入
type ComDelayedInsertPacket struct {
	Packet
	Command uint8 `mysql:"int<1>"` // 0x10
}

func (p *ComDelayedInsertPacket) Unmarshal(r io.Reader) error {
	if err := p.Packet.Unmarshal(r); err != nil {
		return err
	}

	reader := bufio.NewReader(r)
	p.Command, _ = reader.ReadByte()
	return nil
}

func (p *ComDelayedInsertPacket) Marshal() ([]byte, error) {
	buf := new(bytes.Buffer)

	// 写入命令类型
	WriteNumber(buf, p.Command, 1)

	// 组装Packet头部
	payload := buf.Bytes()
	packetBuf := new(bytes.Buffer)
	// PayloadLength 3字节小端
	packetBuf.Write([]byte{byte(len(payload)), byte(len(payload) >> 8), byte(len(payload) >> 16)})
	// SequenceID
	packetBuf.WriteByte(p.SequenceID)
	// Payload
	packetBuf.Write(payload)

	return packetBuf.Bytes(), nil
}

// COM_CHANGE_USER 包 - 切换用户
type ComChangeUserPacket struct {
	Packet
	Command      uint8  `mysql:"int<1>"` // 0x11
	User         string `mysql:"string<NUL>"`
	AuthResponse string `mysql:"string<lenenc>"`
	Database     string `mysql:"string<NUL>"`
	CharacterSet uint16 `mysql:"int<2>"`
}

func (p *ComChangeUserPacket) Unmarshal(r io.Reader) error {
	if err := p.Packet.Unmarshal(r); err != nil {
		return err
	}

	reader := bufio.NewReader(r)
	p.Command, _ = reader.ReadByte()
	p.User, _ = ReadStringByNullEndFromReader(reader)
	p.AuthResponse, _ = ReadStringByLenencFromReader[uint8](reader)
	p.Database, _ = ReadStringByNullEndFromReader(reader)
	p.CharacterSet, _ = ReadNumber[uint16](reader, 2)
	return nil
}

func (p *ComChangeUserPacket) Marshal() ([]byte, error) {
	buf := new(bytes.Buffer)

	// 写入命令类型
	WriteNumber(buf, p.Command, 1)
	// 写入用户名
	WriteStringByNullEnd(buf, p.User)
	// 写入认证响应
	WriteStringByLenenc(buf, p.AuthResponse)
	// 写入数据库名
	WriteStringByNullEnd(buf, p.Database)
	// 写入字符集
	WriteNumber(buf, p.CharacterSet, 2)

	// 组装Packet头部
	payload := buf.Bytes()
	packetBuf := new(bytes.Buffer)
	// PayloadLength 3字节小端
	packetBuf.Write([]byte{byte(len(payload)), byte(len(payload) >> 8), byte(len(payload) >> 16)})
	// SequenceID
	packetBuf.WriteByte(p.SequenceID)
	// Payload
	packetBuf.Write(payload)

	return packetBuf.Bytes(), nil
}

// COM_BINLOG_DUMP 包 - 二进制日志转储
type ComBinlogDumpPacket struct {
	Packet
	Command        uint8  `mysql:"int<1>"` // 0x12
	BinlogPos      uint32 `mysql:"int<4>"`
	Flags          uint16 `mysql:"int<2>"`
	ServerID       uint32 `mysql:"int<4>"`
	BinlogFilename string `mysql:"string<EOF>"`
}

func (p *ComBinlogDumpPacket) Unmarshal(r io.Reader) error {
	if err := p.Packet.Unmarshal(r); err != nil {
		return err
	}

	reader := bufio.NewReader(r)
	p.Command, _ = reader.ReadByte()
	p.BinlogPos, _ = ReadNumber[uint32](reader, 4)
	p.Flags, _ = ReadNumber[uint16](reader, 2)
	p.ServerID, _ = ReadNumber[uint32](reader, 4)
	p.BinlogFilename, _ = ReadStringByNullEndFromReader(reader)
	return nil
}

func (p *ComBinlogDumpPacket) Marshal() ([]byte, error) {
	buf := new(bytes.Buffer)

	// 写入命令类型
	WriteNumber(buf, p.Command, 1)
	// 写入二进制日志位置
	WriteNumber(buf, p.BinlogPos, 4)
	// 写入标志
	WriteNumber(buf, p.Flags, 2)
	// 写入服务器ID
	WriteNumber(buf, p.ServerID, 4)
	// 写入二进制日志文件名
	WriteStringByNullEnd(buf, p.BinlogFilename)

	// 组装Packet头部
	payload := buf.Bytes()
	packetBuf := new(bytes.Buffer)
	// PayloadLength 3字节小端
	packetBuf.Write([]byte{byte(len(payload)), byte(len(payload) >> 8), byte(len(payload) >> 16)})
	// SequenceID
	packetBuf.WriteByte(p.SequenceID)
	// Payload
	packetBuf.Write(payload)

	return packetBuf.Bytes(), nil
}

// COM_TABLE_DUMP 包 - 表转储
type ComTableDumpPacket struct {
	Packet
	Command  uint8  `mysql:"int<1>"` // 0x13
	Database string `mysql:"string<NUL>"`
	Table    string `mysql:"string<NUL>"`
}

func (p *ComTableDumpPacket) Unmarshal(r io.Reader) error {
	if err := p.Packet.Unmarshal(r); err != nil {
		return err
	}

	reader := bufio.NewReader(r)
	p.Command, _ = reader.ReadByte()
	p.Database, _ = ReadStringByNullEndFromReader(reader)
	p.Table, _ = ReadStringByNullEndFromReader(reader)
	return nil
}

func (p *ComTableDumpPacket) Marshal() ([]byte, error) {
	buf := new(bytes.Buffer)

	// 写入命令类型
	WriteNumber(buf, p.Command, 1)
	// 写入数据库名
	WriteStringByNullEnd(buf, p.Database)
	// 写入表名
	WriteStringByNullEnd(buf, p.Table)

	// 组装Packet头部
	payload := buf.Bytes()
	packetBuf := new(bytes.Buffer)
	// PayloadLength 3字节小端
	packetBuf.Write([]byte{byte(len(payload)), byte(len(payload) >> 8), byte(len(payload) >> 16)})
	// SequenceID
	packetBuf.WriteByte(p.SequenceID)
	// Payload
	packetBuf.Write(payload)

	return packetBuf.Bytes(), nil
}

// COM_CONNECT_OUT 包 - 连接输出
type ComConnectOutPacket struct {
	Packet
	Command uint8  `mysql:"int<1>"` // 0x14
	Host    string `mysql:"string<NUL>"`
	Port    uint16 `mysql:"int<2>"`
}

func (p *ComConnectOutPacket) Unmarshal(r io.Reader) error {
	if err := p.Packet.Unmarshal(r); err != nil {
		return err
	}

	reader := bufio.NewReader(r)
	p.Command, _ = reader.ReadByte()
	p.Host, _ = ReadStringByNullEndFromReader(reader)
	p.Port, _ = ReadNumber[uint16](reader, 2)
	return nil
}

func (p *ComConnectOutPacket) Marshal() ([]byte, error) {
	buf := new(bytes.Buffer)

	// 写入命令类型
	WriteNumber(buf, p.Command, 1)
	// 写入主机名
	WriteStringByNullEnd(buf, p.Host)
	// 写入端口
	WriteNumber(buf, p.Port, 2)

	// 组装Packet头部
	payload := buf.Bytes()
	packetBuf := new(bytes.Buffer)
	// PayloadLength 3字节小端
	packetBuf.Write([]byte{byte(len(payload)), byte(len(payload) >> 8), byte(len(payload) >> 16)})
	// SequenceID
	packetBuf.WriteByte(p.SequenceID)
	// Payload
	packetBuf.Write(payload)

	return packetBuf.Bytes(), nil
}

// COM_REGISTER_SLAVE 包 - 注册从服务器
type ComRegisterSlavePacket struct {
	Packet
	Command         uint8  `mysql:"int<1>"` // 0x15
	ServerID        uint32 `mysql:"int<4>"`
	Host            string `mysql:"string<NUL>"`
	User            string `mysql:"string<NUL>"`
	Password        string `mysql:"string<NUL>"`
	Port            uint16 `mysql:"int<2>"`
	ReplicationRank uint32 `mysql:"int<4>"`
	MasterID        uint32 `mysql:"int<4>"`
}

func (p *ComRegisterSlavePacket) Unmarshal(r io.Reader) error {
	if err := p.Packet.Unmarshal(r); err != nil {
		return err
	}

	reader := bufio.NewReader(r)
	p.Command, _ = reader.ReadByte()
	p.ServerID, _ = ReadNumber[uint32](reader, 4)
	p.Host, _ = ReadStringByNullEndFromReader(reader)
	p.User, _ = ReadStringByNullEndFromReader(reader)
	p.Password, _ = ReadStringByNullEndFromReader(reader)
	p.Port, _ = ReadNumber[uint16](reader, 2)
	p.ReplicationRank, _ = ReadNumber[uint32](reader, 4)
	p.MasterID, _ = ReadNumber[uint32](reader, 4)
	return nil
}

func (p *ComRegisterSlavePacket) Marshal() ([]byte, error) {
	buf := new(bytes.Buffer)

	// 写入命令类型
	WriteNumber(buf, p.Command, 1)
	// 写入服务器ID
	WriteNumber(buf, p.ServerID, 4)
	// 写入主机名
	WriteStringByNullEnd(buf, p.Host)
	// 写入用户名
	WriteStringByNullEnd(buf, p.User)
	// 写入密码
	WriteStringByNullEnd(buf, p.Password)
	// 写入端口
	WriteNumber(buf, p.Port, 2)
	// 写入复制等级
	WriteNumber(buf, p.ReplicationRank, 4)
	// 写入主服务器ID
	WriteNumber(buf, p.MasterID, 4)

	// 组装Packet头部
	payload := buf.Bytes()
	packetBuf := new(bytes.Buffer)
	// PayloadLength 3字节小端
	packetBuf.Write([]byte{byte(len(payload)), byte(len(payload) >> 8), byte(len(payload) >> 16)})
	// SequenceID
	packetBuf.WriteByte(p.SequenceID)
	// Payload
	packetBuf.Write(payload)

	return packetBuf.Bytes(), nil
}

// COM_STMT_SEND_LONG_DATA 包 - 发送长数据
type ComStmtSendLongDataPacket struct {
	Packet
	Command     uint8  `mysql:"int<1>"` // 0x18
	StatementID uint32 `mysql:"int<4>"`
	ParamID     uint16 `mysql:"int<2>"`
	Data        []byte `mysql:"binary<EOF>"`
}

func (p *ComStmtSendLongDataPacket) Unmarshal(r io.Reader) error {
	if err := p.Packet.Unmarshal(r); err != nil {
		return err
	}

	reader := bufio.NewReader(r)
	p.Command, _ = reader.ReadByte()
	p.StatementID, _ = ReadNumber[uint32](reader, 4)
	p.ParamID, _ = ReadNumber[uint16](reader, 2)

	// 读取剩余的所有数据
	remainingData, _ := io.ReadAll(reader)
	p.Data = remainingData
	return nil
}

func (p *ComStmtSendLongDataPacket) Marshal() ([]byte, error) {
	buf := new(bytes.Buffer)

	// 写入命令类型
	WriteNumber(buf, p.Command, 1)
	// 写入语句ID
	WriteNumber(buf, p.StatementID, 4)
	// 写入参数ID
	WriteNumber(buf, p.ParamID, 2)
	// 写入数据
	WriteBinary(buf, p.Data)

	// 组装Packet头部
	payload := buf.Bytes()
	packetBuf := new(bytes.Buffer)
	// PayloadLength 3字节小端
	packetBuf.Write([]byte{byte(len(payload)), byte(len(payload) >> 8), byte(len(payload) >> 16)})
	// SequenceID
	packetBuf.WriteByte(p.SequenceID)
	// Payload
	packetBuf.Write(payload)

	return packetBuf.Bytes(), nil
}

// COM_STMT_RESET 包 - 重置预处理语句
type ComStmtResetPacket struct {
	Packet
	Command     uint8  `mysql:"int<1>"` // 0x1a
	StatementID uint32 `mysql:"int<4>"`
}

func (p *ComStmtResetPacket) Unmarshal(r io.Reader) error {
	if err := p.Packet.Unmarshal(r); err != nil {
		return err
	}

	reader := bufio.NewReader(r)
	p.Command, _ = reader.ReadByte()
	p.StatementID, _ = ReadNumber[uint32](reader, 4)
	return nil
}

func (p *ComStmtResetPacket) Marshal() ([]byte, error) {
	buf := new(bytes.Buffer)

	// 写入命令类型
	WriteNumber(buf, p.Command, 1)
	// 写入语句ID
	WriteNumber(buf, p.StatementID, 4)

	// 组装Packet头部
	payload := buf.Bytes()
	packetBuf := new(bytes.Buffer)
	// PayloadLength 3字节小端
	packetBuf.Write([]byte{byte(len(payload)), byte(len(payload) >> 8), byte(len(payload) >> 16)})
	// SequenceID
	packetBuf.WriteByte(p.SequenceID)
	// Payload
	packetBuf.Write(payload)

	return packetBuf.Bytes(), nil
}

// COM_FETCH 包 - 获取数据
type ComFetchPacket struct {
	Packet
	Command     uint8  `mysql:"int<1>"` // 0x1c
	StatementID uint32 `mysql:"int<4>"`
	RowCount    uint32 `mysql:"int<4>"`
}

func (p *ComFetchPacket) Unmarshal(r io.Reader) error {
	if err := p.Packet.Unmarshal(r); err != nil {
		return err
	}

	reader := bufio.NewReader(r)
	p.Command, _ = reader.ReadByte()
	p.StatementID, _ = ReadNumber[uint32](reader, 4)
	p.RowCount, _ = ReadNumber[uint32](reader, 4)
	return nil
}

func (p *ComFetchPacket) Marshal() ([]byte, error) {
	buf := new(bytes.Buffer)

	// 写入命令类型
	WriteNumber(buf, p.Command, 1)
	// 写入语句ID
	WriteNumber(buf, p.StatementID, 4)
	// 写入行数
	WriteNumber(buf, p.RowCount, 4)

	// 组装Packet头部
	payload := buf.Bytes()
	packetBuf := new(bytes.Buffer)
	// PayloadLength 3字节小端
	packetBuf.Write([]byte{byte(len(payload)), byte(len(payload) >> 8), byte(len(payload) >> 16)})
	// SequenceID
	packetBuf.WriteByte(p.SequenceID)
	// Payload
	packetBuf.Write(payload)

	return packetBuf.Bytes(), nil
}

// COM_DAEMON 包 - 守护进程
type ComDaemonPacket struct {
	Packet
	Command uint8 `mysql:"int<1>"` // 0x1d
}

func (p *ComDaemonPacket) Unmarshal(r io.Reader) error {
	if err := p.Packet.Unmarshal(r); err != nil {
		return err
	}

	reader := bufio.NewReader(r)
	p.Command, _ = reader.ReadByte()
	return nil
}

func (p *ComDaemonPacket) Marshal() ([]byte, error) {
	buf := new(bytes.Buffer)

	// 写入命令类型
	WriteNumber(buf, p.Command, 1)

	// 组装Packet头部
	payload := buf.Bytes()
	packetBuf := new(bytes.Buffer)
	// PayloadLength 3字节小端
	packetBuf.Write([]byte{byte(len(payload)), byte(len(payload) >> 8), byte(len(payload) >> 16)})
	// SequenceID
	packetBuf.WriteByte(p.SequenceID)
	// Payload
	packetBuf.Write(payload)

	return packetBuf.Bytes(), nil
}

// COM_ERROR 包 - 错误包
type ComErrorPacket struct {
	Packet
	Command uint8 `mysql:"int<1>"` // 0xff
}

func (p *ComErrorPacket) Unmarshal(r io.Reader) error {
	if err := p.Packet.Unmarshal(r); err != nil {
		return err
	}

	reader := bufio.NewReader(r)
	p.Command, _ = reader.ReadByte()
	return nil
}

func (p *ComErrorPacket) Marshal() ([]byte, error) {
	buf := new(bytes.Buffer)

	// 写入命令类型
	WriteNumber(buf, p.Command, 1)

	// 组装Packet头部
	payload := buf.Bytes()
	packetBuf := new(bytes.Buffer)
	// PayloadLength 3字节小端
	packetBuf.Write([]byte{byte(len(payload)), byte(len(payload) >> 8), byte(len(payload) >> 16)})
	// SequenceID
	packetBuf.WriteByte(p.SequenceID)
	// Payload
	packetBuf.Write(payload)

	return packetBuf.Bytes(), nil
}

func (p *ComQueryPacket) Unmarshal(r io.Reader) error {
	if err := p.Packet.Unmarshal(r); err != nil {
		return err
	}

	reader := bufio.NewReader(r)
	p.Command, _ = reader.ReadByte()

	// 读取剩余的查询字符串（到包末尾）
	remainingBytes, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	p.Query = string(remainingBytes)
	return nil
}

func (p *ComQueryPacket) Marshal() ([]byte, error) {
	buf := new(bytes.Buffer)

	// 写入命令类型
	WriteNumber(buf, p.Command, 1)
	// 写入查询字符串
	WriteStringByNullEnd(buf, p.Query)

	// 组装Packet头部
	payload := buf.Bytes()
	packetBuf := new(bytes.Buffer)
	// PayloadLength 3字节小端
	packetBuf.Write([]byte{byte(len(payload)), byte(len(payload) >> 8), byte(len(payload) >> 16)})
	// SequenceID
	packetBuf.WriteByte(p.SequenceID)
	// Payload
	packetBuf.Write(payload)

	return packetBuf.Bytes(), nil
}

// NewReader 创建一个新的字节读取器
func NewReader(data []byte) io.Reader {
	return bytes.NewReader(data)
}

// CreateOkPacketWithStatus 创建一个带有特定状态标志的 OK 包
func CreateOkPacketWithStatus(affectedRows, lastInsertId uint64, autoCommit, inTransaction bool) *OkPacket {
	okPacket := &OkPacket{
		Packet: Packet{
			SequenceID: 1,
		},
		OkInPacket: OkInPacket{
			Header:       0x00, // OK 包头部
			AffectedRows: affectedRows,
			LastInsertId: lastInsertId,
			StatusFlags:  0,
			Warnings:     0,
		},
	}

	// 设置状态标志
	okPacket.OkInPacket.SetAutoCommit(autoCommit)
	okPacket.OkInPacket.SetInTransaction(inTransaction)

	return okPacket
}

// CreateOkPacketWithSessionState 创建一个带有会话状态变化的 OK 包
func CreateOkPacketWithSessionState(affectedRows, lastInsertId uint64, sessionStateInfo string) *OkPacket {
	okPacket := &OkPacket{
		Packet: Packet{
			SequenceID: 1,
		},
		OkInPacket: OkInPacket{
			Header:           0x00, // OK 包头部
			AffectedRows:     affectedRows,
			LastInsertId:     lastInsertId,
			StatusFlags:      0,
			Warnings:         0,
			SessionStateInfo: sessionStateInfo,
		},
	}

	// 设置会话状态变化标志
	okPacket.OkInPacket.SetSessionStateChanged(true)

	return okPacket
}

// CreateEofPacket 创建一个基本的 EOF 包
func CreateEofPacket(sequenceID uint8) *EofPacket {
	return &EofPacket{
		Packet: Packet{
			SequenceID: sequenceID,
		},
		EofInPacket: EofInPacket{
			Header:      EOF_MARKER,
			Warnings:    0,
			StatusFlags: 0,
		},
	}
}

// CreateEofPacketWithStatus 创建一个带有特定状态标志的 EOF 包
func CreateEofPacketWithStatus(sequenceID uint8, autoCommit, inTransaction bool) *EofPacket {
	eofPacket := &EofPacket{
		Packet: Packet{
			SequenceID: sequenceID,
		},
		EofInPacket: EofInPacket{
			Header:      EOF_MARKER,
			Warnings:    0,
			StatusFlags: 0,
		},
	}

	// 设置状态标志
	eofPacket.EofInPacket.SetAutoCommit(autoCommit)
	eofPacket.EofInPacket.SetInTransaction(inTransaction)

	return eofPacket
}

// CreateIntermediateEofPacket 创建一个中间 EOF 包（用于字段元数据之后）
func CreateIntermediateEofPacket(sequenceID uint8) *EofPacket {
	return CreateEofPacketWithStatus(sequenceID, true, false)
}

// CreateFinalEofPacket 创建一个最终 EOF 包（用于结果集结束）
func CreateFinalEofPacket(sequenceID uint8) *EofPacket {
	return CreateEofPacketWithStatus(sequenceID, true, false)
}
