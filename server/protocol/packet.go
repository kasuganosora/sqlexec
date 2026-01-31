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
	if err = p.Packet.Unmarshal(r); err != nil {
		return err
	}

	// 从 Packet.Payload 中读取 Handshake 数据
	nb := bytes.NewBuffer(p.Packet.Payload)
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
	// 使用Payload中的数据创建reader
	reader := bufio.NewReader(bytes.NewReader(p.Payload))
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
	if err = p.Packet.Unmarshal(r); err != nil {
		return err
	}

	// 从 Packet.Payload 中读取 OkInPacket 数据
	payloadReader := bytes.NewReader(p.Packet.Payload)
	if err = p.OkInPacket.Unmarshal(payloadReader, conditional); err != nil {
		return err
	}
	return nil
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
		p.Warnings, _ = ReadNumber[uint16](reader, 2)
	}

	p.Info, _ = ReadStringByLenencFromReader[uint8](reader)
	// 只有在 StatusFlags 包含 SERVER_SESSION_STATE_CHANGED 时才读取 SessionStateInfo
	if p.StatusFlags&SERVER_SESSION_STATE_CHANGED != 0 {
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

	// 根据MariaDB协议规范,只有当CLIENT_PROTOCOL_41启用且下一个字节是'#'时才读取SQL状态
	if conditional&CLIENT_PROTOCOL_41 != 0 {
		// 检查下一个字节是否为'#'
		peekBytes, err := reader.Peek(1)
		if err == nil && len(peekBytes) > 0 && peekBytes[0] == '#' {
			// 读取SQL状态标记('#')
			p.SqlStateMarker, _ = reader.ReadString(1)
			// 读取SQL状态(5字节)
			p.SqlState, _ = reader.ReadString(5)
		}
	}

	// 读取剩余数据作为错误消息（以NULL结尾）
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

	// 从 Packet.Payload 中读取 EofInPacket 数据
	payloadReader := bytes.NewReader(p.Packet.Payload)
	if err = p.EofInPacket.Unmarshal(payloadReader, conditional); err != nil {
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

// IsEofPacket 安全判断是否为EOF包
// 根据MariaDB文档，需要同时检查：
// 1. 包头为 0xFE
// 2. 包长度 < 9字节（防止与超长数据行混淆）
func IsEofPacket(packet []byte) bool {
	if len(packet) < 4 {
		return false
	}
	// 检查包长度（前3字节）
	packetLength := int(packet[0]) | int(packet[1])<<8 | int(packet[2])<<16
	// 检查包头（第4字节，索引3）
	header := packet[3]
	// EOF包必须是0xFE且长度小于9
	return header == 0xFE && packetLength < 9
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
	ExtendedMetadata          string  `mysql:"string<lenenc>,optional"` // MariaDB扩展元数据（如'point', 'json'）
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

	// 从 Packet.Payload 中读取数据
	reader := bufio.NewReader(bytes.NewReader(p.Packet.Payload))
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
	ColumnCount     uint64 `mysql:"int<lenenc>"`
	MetadataFollows *uint8  `mysql:"int<1>,omitempty"` // MARIADB_CLIENT_CACHE_METADATA能力
}

func (p *ColumnCountPacket) Unmarshal(r io.Reader, capabilities uint32) error {
	if err := p.Packet.Unmarshal(r); err != nil {
		return err
	}

	// 使用Payload中的数据创建reader
	reader := bufio.NewReader(bytes.NewReader(p.Payload))
	p.ColumnCount, _ = ReadLenencNumber[uint64](reader)

	// 如果支持MARIADB_CLIENT_CACHE_METADATA,读取metadata follows字节
	if capabilities&MARIADB_CLIENT_CACHE_METADATA != 0 {
		// 检查是否还有数据可读
		peekBytes, err := reader.Peek(1)
		if err == nil && len(peekBytes) > 0 {
			metadataFollows, _ := reader.ReadByte()
			p.MetadataFollows = &metadataFollows
		}
	}

	return nil
}

func (p *ColumnCountPacket) UnmarshalDefault(r io.Reader) error {
	// 兼容性调用,使用默认能力
	return p.Unmarshal(r, 0)
}

func (p *ColumnCountPacket) Marshal(capabilities uint32) ([]byte, error) {
	buf := new(bytes.Buffer)

	// 写入列数（长度编码）
	WriteLenencNumber(buf, p.ColumnCount)

	// 如果支持MARIADB_CLIENT_CACHE_METADATA且有metadata follows,写入该字节
	if capabilities&MARIADB_CLIENT_CACHE_METADATA != 0 && p.MetadataFollows != nil {
		buf.WriteByte(*p.MetadataFollows)
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

func (p *ColumnCountPacket) MarshalDefault() ([]byte, error) {
	// 兼容性调用,使用默认能力
	return p.Marshal(0)
}

// FieldMetaPacket 字段元数据包
type FieldMetaPacket struct {
	Packet
	FieldMeta
}

func (p *FieldMetaPacket) Unmarshal(r io.Reader, capabilities uint32) error {
	if err := p.Packet.Unmarshal(r); err != nil {
		return err
	}

	// 使用Payload中的数据创建reader
	reader := bufio.NewReader(bytes.NewReader(p.Payload))

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

	// 读取扩展元数据（如果支持）
	if capabilities&MARIADB_CLIENT_EXTENDED_METADATA != 0 {
	// 检查是否有扩展元数据
	peekBytes, err := reader.Peek(1)
	if err == nil && len(peekBytes) > 0 {
		// 扩展元数据格式: int<1> data_type + string value
		for {
			// 读取数据类型
			_, err := reader.ReadByte()
			if err != nil {
				break
			}

			// 读取值
			value, err := ReadStringByLenencFromReader[uint8](reader)
			if err != nil {
				break
			}

			// 0x00: type, 0x01: format
			// 这里简单存储扩展元数据,实际使用时可能需要更详细的解析
			if p.ExtendedMetadata == "" {
				p.ExtendedMetadata = value
			}
		}
	}
	}

	// 读取默认值（可选）
	// 检查是否还有数据可读
	peekBytes, err := reader.Peek(1)
	if err == nil && len(peekBytes) > 0 {
		defaultValue, _ := ReadStringByLenencFromReader[uint8](reader)
		p.DefaultValue = &defaultValue
	}

	return nil
}

func (p *FieldMetaPacket) Marshal(capabilities uint32) ([]byte, error) {
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

	// 写入扩展元数据（如果支持且有数据）
	if capabilities&MARIADB_CLIENT_EXTENDED_METADATA != 0 && p.ExtendedMetadata != "" {
		// 写入类型标识(0x00表示type)
		buf.WriteByte(0x00)
		// 写入扩展元数据值
		WriteStringByLenenc(buf, p.ExtendedMetadata)
	}

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

// UnmarshalDefault 兼容性调用,使用默认能力
func (p *FieldMetaPacket) UnmarshalDefault(r io.Reader) error {
	return p.Unmarshal(r, 0)
}

// MarshalDefault 兼容性调用,使用默认能力
func (p *FieldMetaPacket) MarshalDefault() ([]byte, error) {
	return p.Marshal(0)
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

	// 从 Packet.Payload 中读取行数据
	reader := bufio.NewReader(bytes.NewReader(p.Packet.Payload))

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
	// 写入查询字符串（直接写入字节数组，不添加null结束符）
	buf.WriteString(p.Query)

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

	// 使用更智能的方法确定 NULL bitmap 的长度
	// remainingData 包含：NULL Bitmap + NewParamsBindFlag + ParamTypes + ParamValues
	nullBitmap := make([]byte, 0)
	newParamsBindFlagOffset := -1

	// 定义有效的参数类型
	validTypes := map[byte]bool{
		0x01: true, // TINYINT
		0x02: true, // SMALLINT
		0x03: true, // INT
		0x04: true, // FLOAT
		0x05: true, // DOUBLE
		0x06: true, // NULL
		0x07: true, // TIMESTAMP
		0x08: true, // BIGINT
		0x09: true, // MEDIUMINT
		0x0a: true, // DATE
		0x0b: true, // TIME
		0x0c: true, // DATETIME
		0x0d: true, // YEAR
		0x0e: true, // NEWDATE
		0x0f: true, // VARCHAR
		0x10: true, // BIT
		0xf6: true, // NEWDECIMAL
		0xf7: true, // ENUM
		0xf8: true, // SET
		0xfc: true, // TINY_BLOB
		0xfd: true, // MEDIUM_BLOB, VAR_STRING
		0xfe: true, // LONG_BLOB, STRING
		0xff: true, // BLOB, GEOMETRY
	}

	// 查找 NewParamsBindFlag (0x00 或 0x01)
	// 从偏移 0 开始，从每个位置尝试
	for i := 0; i < len(remainingData) && i < 4; i++ {
		candidateFlag := remainingData[i]

		// 第一个字节（偏移0）总是 NULL bitmap 的一部分
		if i == 0 {
			nullBitmap = append(nullBitmap, candidateFlag)
			continue
		}

		// 只有 0x00 或 0x01 可能是 NewParamsBindFlag
		if candidateFlag == 0x00 || candidateFlag == 0x01 {
			// 检查这个字节后面是否跟着有效的参数类型
			if i+1+2 <= len(remainingData) {
				nextType := remainingData[i+1]
				nextFlag := remainingData[i+2]

				// 如果类型和标志都有效
				if validTypes[nextType] && nextFlag < 0x10 {
					// 找到了 NewParamsBindFlag！
					p.NewParamsBindFlag = candidateFlag
					newParamsBindFlagOffset = i
					break
				}
			}
		}

		// 如果不是 NewParamsBindFlag，这字节是 NULL bitmap 的一部分
		nullBitmap = append(nullBitmap, candidateFlag)
	}

	// 如果没有找到 NewParamsBindFlag，使用默认值
	if newParamsBindFlagOffset == -1 {
		// 默认策略：假设 NULL bitmap 是 1 字节，NewParamsBindFlag 是 0x01
		nullBitmap = []byte{remainingData[0]}
		p.NewParamsBindFlag = 1
		newParamsBindFlagOffset = 1
	}

	p.NullBitmap = nullBitmap

	// 更新 dataReader 的位置（跳过 NULL bitmap 和 NewParamsBindFlag）
	offset := newParamsBindFlagOffset + 1 // 跳过 NewParamsBindFlag
	if offset < len(remainingData) {
		dataReader = bytes.NewReader(remainingData[offset:])
	} else {
		// 没有更多数据了
		dataReader = bytes.NewReader([]byte{})
	}

	// 读取参数类型（如果NewParamsBindFlag = 1）
	if p.NewParamsBindFlag == 1 {
		// 读取参数类型，每2字节一个
		p.ParamTypes = make([]StmtParamType, 0)

		// 读取参数类型，直到数据不足以构成一个完整的参数类型（2字节）
		// 或者遇到无效的参数类型
		for dataReader.Len() >= 2 {
			paramType := StmtParamType{}
			paramType.Type, _ = dataReader.ReadByte()
			paramType.Flag, _ = dataReader.ReadByte()

			// 检查是否是有效的参数类型
			// 如果遇到无效的类型，停止读取
			if !validTypes[paramType.Type] || paramType.Flag >= 0x10 {
				// 无效的类型，回退
				dataReader.Seek(-2, io.SeekCurrent)
				break
			}

			p.ParamTypes = append(p.ParamTypes, paramType)
		}
	}

	// 读取参数值

	// 根据 ParamTypes 的数量重新确定 NULL Bitmap 的长度
	paramCount := len(p.ParamTypes)
	if paramCount > 0 {
		// MariaDB协议：NULL bitmap的第0,1位不使用，从第2位开始存储第1个参数的NULL标志
		// 所以对于n个参数，需要的字节数为 ceil((n + 2) / 8)
		requiredNullBitmapLen := (paramCount + 2 + 7) / 8
		if len(p.NullBitmap) < requiredNullBitmapLen {
			// 扩展NULL bitmap
			newBitmap := make([]byte, requiredNullBitmapLen)
			copy(newBitmap, p.NullBitmap)
			p.NullBitmap = newBitmap
		}
	}

	// 读取参数值

	// 读取参数值
	if len(p.ParamTypes) > 0 {
		p.ParamValues = make([]any, 0, len(p.ParamTypes))
		for i, paramType := range p.ParamTypes {
			// 根据MariaDB协议规范,NULL位图从第3位开始
			// 第n列对应位位置为 (n + 2)
			byteIdx := (i + 2) / 8
			bitIdx := uint((i + 2) % 8)
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
	// MariaDB协议：NULL bitmap的第0,1位不使用，从第2位开始存储第1个参数的NULL标志
	// 所以对于n个参数，需要的字节数为 ceil((n + 2) / 8)
	nullBitmapLen := (paramCount + 2 + 7) / 8

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
			// 根据MariaDB协议规范,NULL位图从第3位开始
			// 第n列对应位位置为 (n + 2)
			byteIdx := (i + 2) / 8
			bitIdx := uint((i + 2) % 8)
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

	// 从 Packet.Payload 中读取数据
	reader := bufio.NewReader(bytes.NewReader(p.Packet.Payload))
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
	RowData         []*BinaryRowDataPacket `mysql:"array,omitempty"`
	Error           *ErrorInPacket `mysql:"object,omitempty"`
	Ok              *OkInPacket    `mysql:"object,omitempty"`
	Eof             *EofInPacket   `mysql:"object,omitempty"`
}

// BinaryRowDataPacket - 二进制格式数据行包
// https://mariadb.com/docs/server/reference/clientserver-protocol/4-server-response-packets/resultset-row
type BinaryRowDataPacket struct {
	Packet
	NullBitmap []byte // NULL值位图
	Values     []any   // 列值
}

// UnmarshalBinaryRowData 解析二进制格式的行数据
// columnCount: 列数
// columnTypes: 列类型数组（从FieldMeta.Type获取）
func (p *BinaryRowDataPacket) Unmarshal(r io.Reader, columnCount uint64, columnTypes []uint8) error {
	if err := p.Packet.Unmarshal(r); err != nil {
		return err
	}

	// 从 Packet.Payload 中读取数据
	reader := bufio.NewReader(bytes.NewReader(p.Packet.Payload))

	// 1. 读取包头（固定为0x00）
	header, err := reader.ReadByte()
	if err != nil {
		return err
	}
	if header != 0x00 {
		return fmt.Errorf("invalid binary row header: expected 0x00, got 0x%02x", header)
	}

	// 2. 读取NULL位图
	nullBitmapSize := (columnCount + 7) / 8
	p.NullBitmap = make([]byte, nullBitmapSize)
	_, err = io.ReadFull(reader, p.NullBitmap)
	if err != nil {
		return err
	}

	// 3. 读取列值
	p.Values = make([]any, 0, columnCount)
	for i := uint64(0); i < columnCount; i++ {
		// 根据MariaDB协议规范,NULL位图从第3位开始
		// 第n列对应位位置为 (n + 2)
		byteIdx := (i + 2) / 8
		bitIdx := (i + 2) % 8
		if (len(p.NullBitmap) > int(byteIdx)) && (p.NullBitmap[byteIdx] & (1 << bitIdx)) != 0 {
			p.Values = append(p.Values, nil)
			continue
		}
		
		// 根据列类型读取值
		if i < uint64(len(columnTypes)) {
			value, err := p.readValueByType(reader, columnTypes[i])
			if err != nil {
				return err
			}
			p.Values = append(p.Values, value)
		} else {
			// 没有类型信息，作为字符串处理
			strValue, _ := ReadStringByLenencFromReader[uint8](reader)
			p.Values = append(p.Values, strValue)
		}
	}
	
	return nil
}

// readValueByType 根据列类型读取二进制值
func (p *BinaryRowDataPacket) readValueByType(reader *bufio.Reader, columnType uint8) (any, error) {
	switch columnType {
	case 0x01: // MYSQL_TYPE_TINY
		val, err := reader.ReadByte()
		if err != nil {
			return nil, err
		}
		return int8(val), nil
		
	case 0x02: // MYSQL_TYPE_SHORT
		val, err := ReadNumber[uint16](reader, 2)
		if err != nil {
			return nil, err
		}
		return int16(val), nil
		
	case 0x03: // MYSQL_TYPE_LONG
		val, err := ReadNumber[uint32](reader, 4)
		if err != nil {
			return nil, err
		}
		return int32(val), nil
		
	case 0x08: // MYSQL_TYPE_LONGLONG
		val, err := ReadNumber[uint64](reader, 8)
		if err != nil {
			return nil, err
		}
		return int64(val), nil
		
	case 0x04: // MYSQL_TYPE_FLOAT
		var val float32
		err := binary.Read(reader, binary.LittleEndian, &val)
		if err != nil {
			return nil, err
		}
		return val, nil
		
	case 0x05: // MYSQL_TYPE_DOUBLE
		var val float64
		err := binary.Read(reader, binary.LittleEndian, &val)
		if err != nil {
			return nil, err
		}
		return val, nil
		
	case 0x06: // MYSQL_TYPE_DATE, MYSQL_TYPE_NEWDATE
		return p.readBinaryDate(reader)
		
	case 0x07: // MYSQL_TYPE_TIME
		return p.readBinaryTime(reader)
		
	case 0x0c: // MYSQL_TYPE_DATETIME
		return p.readBinaryDateTime(reader)
		
	case 0x0f: // MYSQL_TYPE_VARCHAR
		return ReadStringByLenencFromReader[uint8](reader)

	case 0xfc: // MYSQL_TYPE_TINY_BLOB
		return p.readBinaryBlob(reader, 1)

	case 0xfd: // MYSQL_TYPE_BLOB, MYSQL_TYPE_MEDIUM_BLOB
		return p.readBinaryBlob(reader, 3)

	case 0xfe: // MYSQL_TYPE_LONG_BLOB, MYSQL_TYPE_STRING
		// MYSQL_TYPE_STRING使用长度编码字符串
		// 检查是否可能是字符串（长度编码的第一个字节 < 0xfb）
		peekByte, err := reader.Peek(1)
		if err == nil && len(peekByte) > 0 && peekByte[0] < 0xfb {
			// 可能是字符串
			return ReadStringByLenencFromReader[uint8](reader)
		}
		// 否则作为BLOB处理
		return p.readBinaryBlob(reader, 4)

	case 0xff: // MYSQL_TYPE_GEOMETRY
		return p.readBinaryBlob(reader, 4)

	case 0xf6: // MYSQL_TYPE_TIMESTAMP, MYSQL_TYPE_TIMESTAMP2
		return p.readBinaryTimestamp(reader)

	case 0xf7: // MYSQL_TYPE_DATETIME2
		return p.readBinaryDateTime2(reader)

	case 0xf8: // MYSQL_TYPE_TIME2
		return p.readBinaryTime2(reader)

	case 0xf9, 0xfa: // MYSQL_TYPE_NEWDECIMAL, MYSQL_TYPE_ENUM
		// 作为字符串处理
		return ReadStringByLenencFromReader[uint8](reader)

	case 0xfb: // MYSQL_TYPE_SET, MYSQL_TYPE_BIT
		// 需要根据字段长度判断是SET还是BIT
		// 简化处理:先尝试作为BIT
		return p.readBinaryBit(reader)
		
	default:
		// 默认作为字符串处理
		return ReadStringByLenencFromReader[uint8](reader)
	}
}

// readBinaryDate 读取二进制日期值
func (p *BinaryRowDataPacket) readBinaryDate(reader *bufio.Reader) (string, error) {
	length, err := reader.ReadByte()
	if err != nil {
		return "", err
	}
	
	if length == 0 {
		return "0000-00-00", nil
	}
	
	year, _ := ReadNumber[uint16](reader, 2)
	month, _ := reader.ReadByte()
	day, _ := reader.ReadByte()
	
	return fmt.Sprintf("%04d-%02d-%02d", year, month, day), nil
}

// readBinaryTime 读取二进制时间值
func (p *BinaryRowDataPacket) readBinaryTime(reader *bufio.Reader) (string, error) {
	length, err := reader.ReadByte()
	if err != nil {
		return "", err
	}
	
	if length == 0 {
		return "00:00:00", nil
	}
	
	neg, _ := reader.ReadByte()
	days, _ := ReadNumber[uint32](reader, 4)
	hours, _ := reader.ReadByte()
	minutes, _ := reader.ReadByte()
	seconds, _ := reader.ReadByte()
	
	var microseconds uint32
	if length == 12 {
		microseconds, _ = ReadNumber[uint32](reader, 4)
	}
	
	sign := "+"
	if neg == 1 {
		sign = "-"
	}
	
	return fmt.Sprintf("%s%dd %02d:%02d:%02d.%06d", sign, days, hours, minutes, seconds, microseconds), nil
}

// readBinaryDateTime 读取二进制日期时间值（旧格式）
func (p *BinaryRowDataPacket) readBinaryDateTime(reader *bufio.Reader) (string, error) {
	length, err := reader.ReadByte()
	if err != nil {
		return "", err
	}
	
	if length == 0 {
		return "0000-00-00 00:00:00", nil
	}
	
	year, _ := ReadNumber[uint16](reader, 2)
	month, _ := reader.ReadByte()
	day, _ := reader.ReadByte()
	hours, _ := reader.ReadByte()
	minutes, _ := reader.ReadByte()
	seconds, _ := reader.ReadByte()
	
	var microseconds uint32
	if length > 7 {
		microseconds, _ = ReadNumber[uint32](reader, 4)
	}
	
	if microseconds > 0 {
		return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d.%06d", year, month, day, hours, minutes, seconds, microseconds), nil
	}
	return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d", year, month, day, hours, minutes, seconds), nil
}

// readBinaryDateTime2 读取二进制日期时间值（新格式）
func (p *BinaryRowDataPacket) readBinaryDateTime2(reader *bufio.Reader) (string, error) {
	length, err := reader.ReadByte()
	if err != nil {
		return "", err
	}
	
	if length == 0 {
		return "0000-00-00 00:00:00", nil
	}
	
	year, _ := ReadNumber[uint16](reader, 2)
	month, _ := reader.ReadByte()
	day, _ := reader.ReadByte()
	hours, _ := reader.ReadByte()
	minutes, _ := reader.ReadByte()
	seconds, _ := reader.ReadByte()
	
	var microseconds uint32
	if length > 7 {
		microseconds, _ = ReadNumber[uint32](reader, 4)
	}
	
	if microseconds > 0 {
		return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d.%06d", year, month, day, hours, minutes, seconds, microseconds), nil
	}
	return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d", year, month, day, hours, minutes, seconds), nil
}

// readBinaryTime2 读取二进制时间值（新格式）
func (p *BinaryRowDataPacket) readBinaryTime2(reader *bufio.Reader) (string, error) {
	length, err := reader.ReadByte()
	if err != nil {
		return "", err
	}
	
	if length == 0 {
		return "00:00:00", nil
	}
	
	neg, _ := reader.ReadByte()
	days, _ := ReadNumber[uint32](reader, 4)
	hours, _ := reader.ReadByte()
	minutes, _ := reader.ReadByte()
	seconds, _ := reader.ReadByte()
	
	var microseconds uint32
	if length > 8 {
		microseconds, _ = ReadNumber[uint32](reader, 4)
	}
	
	sign := "+"
	if neg == 1 {
		sign = "-"
	}
	
	return fmt.Sprintf("%s%dd %02d:%02d:%02d.%06d", sign, days, hours, minutes, seconds, microseconds), nil
}

// readBinaryTimestamp 读取二进制时间戳值
func (p *BinaryRowDataPacket) readBinaryTimestamp(reader *bufio.Reader) (string, error) {
	// TIMESTAMP是4字节的UNIX时间戳
	val, err := ReadNumber[uint32](reader, 4)
	if err != nil {
		return "", err
	}
	
	// 简单返回时间戳值，实际应用中可以转换为可读格式
	return fmt.Sprintf("%d", val), nil
}

// readBinaryBlob 读取二进制BLOB值
func (p *BinaryRowDataPacket) readBinaryBlob(reader *bufio.Reader, lengthBytes int) ([]byte, error) {
	switch lengthBytes {
	case 1:
		length, err := reader.ReadByte()
		if err != nil {
			return nil, err
		}
		if length == 0xfb {
			// 空 blob
			return []byte{}, nil
		}
		data := make([]byte, length)
		_, err = io.ReadFull(reader, data)
		return data, err
		
	case 3:
		lengthBytesData := make([]byte, 3)
		_, readErr := io.ReadFull(reader, lengthBytesData)
		if readErr != nil {
			return nil, readErr
		}
		length := uint32(lengthBytesData[0]) | uint32(lengthBytesData[1])<<8 | uint32(lengthBytesData[2])<<16
		if length == 0xfbfbfb {
			// 空 blob
			return []byte{}, nil
		}
		data := make([]byte, length)
		_, readErr = io.ReadFull(reader, data)
		return data, readErr
		
	case 4:
		length, err := ReadNumber[uint32](reader, 4)
		if err != nil {
			return nil, err
		}
		data := make([]byte, length)
		_, err = io.ReadFull(reader, data)
		return data, err
		
	default:
		return nil, fmt.Errorf("unsupported blob length bytes: %d", lengthBytes)
	}
}

// readBinaryBit 读取二进制BIT值
// 注意:这个实现是简化的,实际使用时需要知道BIT字段的长度(bit数)
// BIT字段的长度信息应该在FieldMeta中获取
func (p *BinaryRowDataPacket) readBinaryBit(reader *bufio.Reader) (string, error) {
	// BIT类型:根据字段长度读取相应字节的二进制数据
	// 这里简化处理:读取1字节(8位)并转换为二进制字符串
	// 实际实现应该根据ColumnLength确定读取的字节数

	// 尝试读取为长度编码字符串(兼容某些实现)
	value, err := ReadStringByLenencFromReader[uint8](reader)
	if err == nil && len(value) > 0 {
		// 如果是字符串,尝试将字节转换为二进制表示
		data := []byte(value)
		if len(data) == 1 {
			return fmt.Sprintf("%08b", data[0]), nil
		}
		// 多字节BIT值,组合表示
		var result string
		for _, b := range data {
			result += fmt.Sprintf("%08b", b)
		}
		return result, nil
	}

	// 降级方案:直接读取1字节
	data, err := reader.ReadByte()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%08b", data), nil
}

// Marshal 序列化二进制行数据
func (p *BinaryRowDataPacket) Marshal(columnCount uint64, columnTypes []uint8) ([]byte, error) {
	buf := new(bytes.Buffer)
	
	// 1. 写入包头（0x00）
	buf.WriteByte(0x00)
	
	// 2. 写入NULL位图
	nullBitmapSize := (columnCount + 7) / 8
	nullBitmap := make([]byte, nullBitmapSize)
	for i, value := range p.Values {
		if value == nil {
			// 根据MariaDB协议规范,NULL位图从第3位开始
			// 第n列对应位位置为 (n + 2)
			byteIdx := (i + 2) / 8
			bitIdx := (i + 2) % 8
			nullBitmap[byteIdx] |= (1 << bitIdx)
		}
	}
	buf.Write(nullBitmap)
	
	// 3. 写入列值
	for i, value := range p.Values {
		if value == nil {
			continue
		}
		
		if i < len(columnTypes) {
			err := p.writeValueByType(buf, value, columnTypes[i])
			if err != nil {
				return nil, err
			}
		} else {
			// 默认作为字符串处理
			if str, ok := value.(string); ok {
				WriteStringByLenenc(buf, str)
			}
		}
	}
	
	// 组装Packet头部
	payload := buf.Bytes()
	packetBuf := new(bytes.Buffer)
	packetBuf.Write([]byte{byte(len(payload)), byte(len(payload) >> 8), byte(len(payload) >> 16)})
	packetBuf.WriteByte(p.SequenceID)
	packetBuf.Write(payload)
	
	return packetBuf.Bytes(), nil
}

// writeValueByType 根据列类型写入二进制值
func (p *BinaryRowDataPacket) writeValueByType(buf *bytes.Buffer, value any, columnType uint8) error {
	switch columnType {
	case 0x01: // MYSQL_TYPE_TINY
		if val, ok := value.(int8); ok {
			buf.WriteByte(byte(val))
		}
		
	case 0x02: // MYSQL_TYPE_SHORT
		if val, ok := value.(int16); ok {
			binary.Write(buf, binary.LittleEndian, val)
		}
		
	case 0x03: // MYSQL_TYPE_LONG
		if val, ok := value.(int32); ok {
			binary.Write(buf, binary.LittleEndian, val)
		}
		
	case 0x08: // MYSQL_TYPE_LONGLONG
		if val, ok := value.(int64); ok {
			binary.Write(buf, binary.LittleEndian, val)
		}
		
	case 0x04: // MYSQL_TYPE_FLOAT
		if val, ok := value.(float32); ok {
			binary.Write(buf, binary.LittleEndian, val)
		}
		
	case 0x05: // MYSQL_TYPE_DOUBLE
		if val, ok := value.(float64); ok {
			binary.Write(buf, binary.LittleEndian, val)
		}
		
	case 0x0f, 0xfd, 0xfe: // VARCHAR, VAR_STRING, STRING
		if val, ok := value.(string); ok {
			WriteStringByLenenc(buf, val)
		}
		
	default:
		// 默认作为字符串处理
		if val, ok := value.(string); ok {
			WriteStringByLenenc(buf, val)
		}
	}
	return nil
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

	// 从 Packet.Payload 中读取数据
	reader := bufio.NewReader(bytes.NewReader(p.Packet.Payload))
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

	// 从 Packet.Payload 中读取数据
	reader := bufio.NewReader(bytes.NewReader(p.Packet.Payload))
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

	// 从 Packet.Payload 中读取数据
	reader := bufio.NewReader(bytes.NewReader(p.Packet.Payload))
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

	// 从 Packet.Payload 中读取数据
	reader := bufio.NewReader(bytes.NewReader(p.Packet.Payload))
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

	// 从 Packet.Payload 中读取数据
	reader := bufio.NewReader(bytes.NewReader(p.Packet.Payload))
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
	// 写入二进制日志文件名（以 NULL 结尾）
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

	// 从 Packet.Payload 中读取数据
	reader := bufio.NewReader(bytes.NewReader(p.Packet.Payload))
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

	// 从 Packet.Payload 中读取数据
	reader := bufio.NewReader(bytes.NewReader(p.Packet.Payload))
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

	// 从 Packet.Payload 中读取数据
	reader := bufio.NewReader(bytes.NewReader(p.Packet.Payload))
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

	// 从 Packet.Payload 中读取数据
	reader := bufio.NewReader(bytes.NewReader(p.Packet.Payload))
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

// LOCAL_INFILE_Packet - 本地文件传输请求包
// https://mariadb.com/docs/server/reference/clientserver-protocol/4-server-response-packets/packet_local_infile
type LocalInfilePacket struct {
	Packet
	Header   uint8  `mysql:"int<1>"` // 固定值 0xFB
	Filename string `mysql:"string<NUL>"` // 服务器要求客户端发送的文件路径
}

func (p *LocalInfilePacket) Unmarshal(r io.Reader) error {
	if err := p.Packet.Unmarshal(r); err != nil {
		return err
	}

	// 从 Packet.Payload 中读取数据
	reader := bufio.NewReader(bytes.NewReader(p.Packet.Payload))
	p.Header, _ = reader.ReadByte()
	p.Filename, _ = ReadStringByNullEndFromReader(reader)
	return nil
}

func (p *LocalInfilePacket) Marshal() ([]byte, error) {
	buf := new(bytes.Buffer)

	// 写入头部
	WriteNumber(buf, p.Header, 1)
	// 写入文件名
	WriteStringByNullEnd(buf, p.Filename)

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

// ProgressReportPacket - 进度报告包（ERR_Packet的特殊形式）
// 当Error Code == 0xFFFF时，ERR_Packet变为进度报告
type ProgressReportPacket struct {
	Packet
	Header    uint8  `mysql:"int<1>"`   // 固定值 0xFF
	ErrorCode uint16 `mysql:"int<2>"`   // 0xFFFF 表示进度报告
	Stage     uint8  `mysql:"int<1>"`   // 当前阶段
	MaxStage  uint8  `mysql:"int<1>"`   // 最大阶段数
	Progress  uint32 `mysql:"int<3>"`   // 进度值
	Info      string `mysql:"string<NUL>"` // 进度信息
}

func (p *ProgressReportPacket) Unmarshal(r io.Reader) error {
	if err := p.Packet.Unmarshal(r); err != nil {
		return err
	}

	// 从 Packet.Payload 中读取数据
	reader := bufio.NewReader(bytes.NewReader(p.Packet.Payload))
	p.Header, _ = reader.ReadByte()
	p.ErrorCode, _ = ReadNumber[uint16](reader, 2)

	// 检查是否为进度报告（Error Code == 0xFFFF）
	if p.ErrorCode != 0xFFFF {
		return errors.New("not a progress report packet (error code != 0xFFFF)")
	}

	p.Stage, _ = ReadNumber[uint8](reader, 1)
	p.MaxStage, _ = ReadNumber[uint8](reader, 1)
	p.Progress, _ = ReadNumber[uint32](reader, 4)
	p.Info, _ = ReadStringByNullEndFromReader(reader)
	
	return nil
}

func (p *ProgressReportPacket) Marshal() ([]byte, error) {
	buf := new(bytes.Buffer)

	// 写入头部
	WriteNumber(buf, p.Header, 1)
	// 写入错误码（0xFFFF）
	WriteNumber(buf, p.ErrorCode, 2)
	// 写入阶段信息
	WriteNumber(buf, p.Stage, 1)
	WriteNumber(buf, p.MaxStage, 1)
	// 写入进度值（4字节小端）
	WriteNumber(buf, p.Progress, 4)
	// 写入进度信息
	WriteStringByNullEnd(buf, p.Info)

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

	// 从 Packet.Payload 中读取 ComQuery 数据
	if len(p.Payload) >= 1 {
		p.Command = p.Payload[0]
		// Query是从第二个字节开始到包结尾
		if len(p.Payload) > 1 {
			p.Query = string(p.Payload[1:])
		}
	}
	return nil
}

func (p *ComQueryPacket) Marshal() ([]byte, error) {
	buf := new(bytes.Buffer)

	// 写入命令类型
	WriteNumber(buf, p.Command, 1)
	// 写入查询字符串（不添加null终止符）
	buf.WriteString(p.Query)

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
