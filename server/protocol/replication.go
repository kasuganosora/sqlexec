package protocol

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

// ============================================
// MariaDB Replication Protocol Structures
// ============================================

// BinlogEventHeader 二进制日志事件头（19字节）
type BinlogEventHeader struct {
	Timestamp  uint32 // 事件创建时间（秒）
	EventType  uint8  // 事件类型代码
	ServerID   uint32 // 创建事件的服务器ID
	EventLength uint32 // 事件长度（头部+数据）
	NextPos    uint32 // 下一个事件在文件中的位置
	Flags      uint16 // 事件标志位
}

// Unmarshal 解析事件头
func (h *BinlogEventHeader) Unmarshal(r io.Reader) error {
	// 读取固定19字节头部
	buf := make([]byte, BINLOG_EVENT_HEADER_LENGTH)
	if _, err := io.ReadFull(r, buf); err != nil {
		return err
	}

	// 解析字段（小端序）
	h.Timestamp = binary.LittleEndian.Uint32(buf[0:4])
	h.EventType = buf[4]
	h.ServerID = binary.LittleEndian.Uint32(buf[5:9])
	h.EventLength = binary.LittleEndian.Uint32(buf[9:13])
	h.NextPos = binary.LittleEndian.Uint32(buf[13:17])
	h.Flags = binary.LittleEndian.Uint16(buf[17:19])

	return nil
}

// Marshal 序列化事件头
func (h *BinlogEventHeader) Marshal() ([]byte, error) {
	buf := make([]byte, BINLOG_EVENT_HEADER_LENGTH)

	// 写入字段（小端序）
	binary.LittleEndian.PutUint32(buf[0:4], h.Timestamp)
	buf[4] = h.EventType
	binary.LittleEndian.PutUint32(buf[5:9], h.ServerID)
	binary.LittleEndian.PutUint32(buf[9:13], h.EventLength)
	binary.LittleEndian.PutUint32(buf[13:17], h.NextPos)
	binary.LittleEndian.PutUint16(buf[17:19], h.Flags)

	return buf, nil
}

// ============================================
// FORMAT_DESCRIPTION_EVENT - 格式描述事件
// ============================================

type FormatDescriptionEvent struct {
	Header BinlogEventHeader

	// 固定数据部分
	BinlogFormatVersion   uint16   // 二进制日志格式版本（固定为4）
	ServerVersion        string   // 服务器版本字符串（50字节，以NULL填充）
	CreateTimestamp     uint32   // 创建时间戳
	HeaderLength        uint8    // 事件头长度（通常为19）
	EventTypePostHeader  []uint8  // 事件类型后长度数组（每个字节对应一个事件类型）

	// 校验和
	ChecksumAlgorithm uint8  // 校验和算法类型
	ChecksumValue    uint32 // CRC32 校验和值
}

// Unmarshal 解析 FORMAT_DESCRIPTION_EVENT
// 注意：这个方法假设 r 已经被读取过事件头，现在指向事件体
func (e *FormatDescriptionEvent) Unmarshal(r io.Reader) error {
	// Binlog 事件不使用 Packet 封装，直接读取
	reader := bufio.NewReader(r)

	// 读取固定字段
	e.BinlogFormatVersion, _ = ReadNumber[uint16](reader, 2)

	// 读取服务器版本（50字节）
	serverVersionBytes := make([]byte, 50)
	io.ReadFull(reader, serverVersionBytes)
	e.ServerVersion = string(serverVersionBytes)

	// 去除 NULL 填充
	e.ServerVersion = string(bytes.TrimRight([]byte(e.ServerVersion), "\x00"))

	// 读取创建时间戳
	e.CreateTimestamp, _ = ReadNumber[uint32](reader, 4)

	// 读取事件头长度
	e.HeaderLength, _ = ReadNumber[uint8](reader, 1)

	// 计算事件类型后长度数组的长度
	// 事件总长度 = 19（头部）+ 固定字段（57字节）+ 数组长度 + 校验和字段（5字节）
	// 数组长度 = 事件总长度 - 19 - 57 - 5 = 事件总长度 - 81
	arrayLength := int(e.Header.EventLength) - 19 - 57 - 5

	if arrayLength > 0 {
		// 读取事件类型后长度数组
		e.EventTypePostHeader = make([]uint8, arrayLength)
		io.ReadFull(reader, e.EventTypePostHeader)
	}

	// 读取校验和算法
	e.ChecksumAlgorithm, _ = ReadNumber[uint8](reader, 1)

	// 读取 CRC32 校验和（如果算法类型为 CRC32）
	if e.ChecksumAlgorithm == BINLOG_CHECKSUM_ALG_CRC32 {
		e.ChecksumValue, _ = ReadNumber[uint32](reader, 4)
	}

	return nil
}

// Marshal 序列化 FORMAT_DESCRIPTION_EVENT
func (e *FormatDescriptionEvent) Marshal() ([]byte, error) {
	buf := new(bytes.Buffer)

	// 序列化事件头
	headerBytes, _ := e.Header.Marshal()
	buf.Write(headerBytes)

	// 写入二进制日志格式版本
	binary.Write(buf, binary.LittleEndian, e.BinlogFormatVersion)

	// 写入服务器版本（50字节，以 NULL 填充）
	serverVersion := make([]byte, 50)
	copy(serverVersion, []byte(e.ServerVersion))
	buf.Write(serverVersion)

	// 写入创建时间戳
	binary.Write(buf, binary.LittleEndian, e.CreateTimestamp)

	// 写入事件头长度
	buf.WriteByte(e.HeaderLength)

	// 写入事件类型后长度数组
	buf.Write(e.EventTypePostHeader)

	// 写入校验和算法
	buf.WriteByte(e.ChecksumAlgorithm)

	// 写入 CRC32 校验和（如果算法类型为 CRC32）
	if e.ChecksumAlgorithm == BINLOG_CHECKSUM_ALG_CRC32 {
		binary.Write(buf, binary.LittleEndian, e.ChecksumValue)
	}

	return buf.Bytes(), nil
}

// ============================================
// GTID_EVENT - 全局事务标识符事件
// ============================================

type GtidEvent struct {
	Header BinlogEventHeader

	// 事件体字段
	GtidSeqNo      uint64 // GTID 序列号
	DomainID       uint32 // 复制域 ID
	Flags          uint8  // 标志位
	CommitID       *uint64 // 组提交 ID（可选）
	XaFormatID     *uint32 // XA 事务格式 ID（可选）
	GtridLength    *uint8  // 全局事务标识符长度（可选）
	BqualLength    *uint8  // 分支限定符长度（可选）
	XidData        []byte  // XID 数据（可选）
}

// Unmarshal 解析 GTID_EVENT
// 注意：这个方法假设 r 已经被读取过事件头，现在指向事件体
func (e *GtidEvent) Unmarshal(r io.Reader) error {
	// Binlog 事件不使用 Packet 封装，直接读取
	reader := bufio.NewReader(r)

	// 读取固定字段
	e.GtidSeqNo, _ = ReadNumber[uint64](reader, 8)
	e.DomainID, _ = ReadNumber[uint32](reader, 4)
	e.Flags, _ = ReadNumber[uint8](reader, 1)

	// 根据标志位读取可选字段
	if e.Flags&GTID_FL_GROUP_COMMIT_ID != 0 {
		// 读取 commit_id
		commitID, _ := ReadNumber[uint64](reader, 8)
		e.CommitID = &commitID
	}

	if e.Flags&(GTID_FL_PREPARED_XA|GTID_FL_COMPLETED_XA) != 0 {
		// 读取 XA 事务字段
		formatID, _ := ReadNumber[uint32](reader, 4)
		e.XaFormatID = &formatID

		gtridLen, _ := ReadNumber[uint8](reader, 1)
		e.GtridLength = &gtridLen

		bqualLen, _ := ReadNumber[uint8](reader, 1)
		e.BqualLength = &bqualLen

		if gtridLen+bqualLen > 0 {
			totalLen := int(gtridLen) + int(bqualLen)
			e.XidData = make([]byte, totalLen)
			io.ReadFull(reader, e.XidData)
		}
	} else {
		// 其他情况：读取6字节的 0 填充
		padding := make([]byte, 6)
		io.ReadFull(reader, padding)
	}

	return nil
}

// Marshal 序列化 GTID_EVENT
func (e *GtidEvent) Marshal() ([]byte, error) {
	buf := new(bytes.Buffer)

	// 序列化事件头
	headerBytes, _ := e.Header.Marshal()
	buf.Write(headerBytes)

	// 写入固定字段
	binary.Write(buf, binary.LittleEndian, e.GtidSeqNo)
	binary.Write(buf, binary.LittleEndian, e.DomainID)
	buf.WriteByte(e.Flags)

	// 根据标志位写入可选字段
	if e.Flags&GTID_FL_GROUP_COMMIT_ID != 0 && e.CommitID != nil {
		binary.Write(buf, binary.LittleEndian, *e.CommitID)
	}

	if e.Flags&(GTID_FL_PREPARED_XA|GTID_FL_COMPLETED_XA) != 0 {
		if e.XaFormatID != nil {
			binary.Write(buf, binary.LittleEndian, *e.XaFormatID)
		}
		if e.GtridLength != nil {
			buf.WriteByte(*e.GtridLength)
		}
		if e.BqualLength != nil {
			buf.WriteByte(*e.BqualLength)
		}
		buf.Write(e.XidData)
	} else {
		// 其他情况：写入6字节的 0 填充
		buf.Write([]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00})
	}

	return buf.Bytes(), nil
}

// String 返回 GTID 的字符串表示
func (e *GtidEvent) String() string {
	return fmt.Sprintf("%d-%d-%d", e.DomainID, e.Header.ServerID, e.GtidSeqNo)
}

// ============================================
// ROTATE_EVENT - 日志文件轮换事件
// ============================================

type RotateEvent struct {
	Header BinlogEventHeader

	// 事件体字段
	NextPosition uint64 // 下一个事件在下一个日志文件中的位置（固定为4）
	BinlogFile   string // 下一个二进制日志文件名
}

// Unmarshal 解析 ROTATE_EVENT
// 注意：这个方法假设 r 已经被读取过事件头，现在指向事件体
func (e *RotateEvent) Unmarshal(r io.Reader) error {
	// Binlog 事件不使用 Packet 封装，直接读取
	reader := bufio.NewReader(r)

	// 读取下一个位置（8字节）
	nextPosBuf := make([]byte, 8)
	io.ReadFull(reader, nextPosBuf)
	e.NextPosition = uint64(nextPosBuf[0]) | uint64(nextPosBuf[1])<<8 |
		uint64(nextPosBuf[2])<<16 | uint64(nextPosBuf[3])<<24 |
		uint64(nextPosBuf[4])<<32 | uint64(nextPosBuf[5])<<40 |
		uint64(nextPosBuf[6])<<48 | uint64(nextPosBuf[7])<<56

	// 读取日志文件名（以 NULL 结尾）
	var buf []byte
	for {
		b, err := reader.ReadByte()
		if err != nil {
			break
		}
		if b == 0 {
			break
		}
		buf = append(buf, b)
	}
	e.BinlogFile = string(buf)

	return nil
}

// Marshal 序列化 ROTATE_EVENT
func (e *RotateEvent) Marshal() ([]byte, error) {
	buf := new(bytes.Buffer)

	// 序列化事件头
	headerBytes, _ := e.Header.Marshal()
	buf.Write(headerBytes)

	// 写入下一个位置（8字节）
	binary.Write(buf, binary.LittleEndian, e.NextPosition)

	// 写入日志文件名
	buf.WriteString(e.BinlogFile)

	return buf.Bytes(), nil
}

// ============================================
// HEARTBEAT_LOG_EVENT - 心跳事件
// ============================================

type HeartbeatLogEvent struct {
	Header BinlogEventHeader

	// 事件体字段
	Timestamp string // 心跳时间戳（字符串格式）
}

// Unmarshal 解析 HEARTBEAT_LOG_EVENT
// 注意：这个方法假设 r 已经被读取过事件头，现在指向事件体
func (e *HeartbeatLogEvent) Unmarshal(r io.Reader) error {
	// Binlog 事件不使用 Packet 封装，直接读取
	reader := bufio.NewReader(r)

	// 读取心跳时间戳字符串
	e.Timestamp, _ = ReadStringByNullEndFromReader(reader)

	return nil
}

// Marshal 序列化 HEARTBEAT_LOG_EVENT
func (e *HeartbeatLogEvent) Marshal() ([]byte, error) {
	buf := new(bytes.Buffer)

	// 序列化事件头
	headerBytes, _ := e.Header.Marshal()
	buf.Write(headerBytes)

	// 写入心跳时间戳字符串（以 NULL 结尾）
	buf.WriteString(e.Timestamp)
	buf.WriteByte(0x00)

	return buf.Bytes(), nil
}

// ============================================
// QUERY_EVENT - 查询事件
// ============================================

type QueryEvent struct {
	Header BinlogEventHeader

	// 固定数据部分
	ThreadID         uint32 // 执行此语句的线程 ID
	ExecutionTime   uint32 // 执行时间（秒）
	DatabaseNameLen uint8  // 数据库名长度
	ErrorCode       uint16 // 错误代码
	StatusVarLen    uint16 // 状态变量块长度

	// 可变数据部分
	StatusVariables  []byte // 状态变量
	DatabaseName    string // 数据库名
	Query          string // SQL 语句
}

// Unmarshal 解析 QUERY_EVENT
// 注意：这个方法假设 r 已经被读取过事件头，现在指向事件体
func (e *QueryEvent) Unmarshal(r io.Reader) error {
	// Binlog 事件不使用 Packet 封装，直接读取
	reader := bufio.NewReader(r)

	// 读取固定字段（使用 ReadNumber 来避免读取偏移问题）
	e.ThreadID, _ = ReadNumber[uint32](reader, 4)
	e.ExecutionTime, _ = ReadNumber[uint32](reader, 4)
	e.DatabaseNameLen, _ = ReadNumber[uint8](reader, 1)
	e.ErrorCode, _ = ReadNumber[uint16](reader, 2)
	e.StatusVarLen, _ = ReadNumber[uint16](reader, 2)

	// 读取状态变量
	if e.StatusVarLen > 0 {
		e.StatusVariables = make([]byte, e.StatusVarLen)
		io.ReadFull(reader, e.StatusVariables)
	}

	// 读取数据库名
	if e.DatabaseNameLen > 0 {
		e.DatabaseName, _ = ReadStringFixedFromReader(reader, int(e.DatabaseNameLen))
		// 读取 NULL 终止符（仅在 DatabaseNameLen > 0 时）
		reader.ReadByte()
	}

	// 读取 SQL 语句（使用单一 reader，避免创建新的 bufio.Reader）
	var buf []byte
	for {
		b, err := reader.ReadByte()
		if err != nil {
			break
		}
		if b == 0 {
			break
		}
		buf = append(buf, b)
	}
	e.Query = string(buf)

	return nil
}

// Marshal 序列化 QUERY_EVENT
func (e *QueryEvent) Marshal() ([]byte, error) {
	buf := new(bytes.Buffer)

	// 序列化事件头
	headerBytes, _ := e.Header.Marshal()
	buf.Write(headerBytes)

	// 写入固定字段
	binary.Write(buf, binary.LittleEndian, e.ThreadID)
	binary.Write(buf, binary.LittleEndian, e.ExecutionTime)
	buf.WriteByte(e.DatabaseNameLen)
	binary.Write(buf, binary.LittleEndian, e.ErrorCode)
	binary.Write(buf, binary.LittleEndian, e.StatusVarLen)

	// 写入状态变量
	buf.Write(e.StatusVariables)

	// 写入数据库名
	buf.WriteString(e.DatabaseName)
	buf.WriteByte(0x00)

	// 写入 SQL 语句（以 NULL 结尾）
	buf.WriteString(e.Query)
	buf.WriteByte(0x00)

	return buf.Bytes(), nil
}

// ============================================
// TABLE_MAP_EVENT - 表映射事件
// ============================================

type TableMapEvent struct {
	Header BinlogEventHeader

	// 固定数据部分
	TableID      uint64 // 表 ID（6字节）
	Reserved     uint16 // 保留字段

	// 可变数据部分
	DatabaseNameLen uint8    // 数据库名长度
	DatabaseName    string   // 数据库名
	TableNameLen    uint8    // 表名长度
	TableName       string   // 表名
	ColumnCount     int      // 列数
	ColumnTypes     []uint8  // 列类型数组
	MetadataLen    int      // 元数据块长度
	Metadata       []byte   // 元数据块
	NullBitmap      []byte   // NULL 位图
	OptionalMetadata []byte   // 可选元数据块
}

// Unmarshal 解析 TABLE_MAP_EVENT
// 注意：这个方法假设 r 已经被读取过事件头，现在指向事件体
func (e *TableMapEvent) Unmarshal(r io.Reader) error {
	// Binlog 事件不使用 Packet 封装，直接读取
	reader := bufio.NewReader(r)

	// 读取固定字段
	tableIDBytes := make([]byte, 6)
	_, err := io.ReadFull(reader, tableIDBytes)
	if err != nil {
		return err
	}
	e.TableID = uint64(tableIDBytes[0]) | uint64(tableIDBytes[1])<<8 | uint64(tableIDBytes[2])<<16 |
		uint64(tableIDBytes[3])<<24 | uint64(tableIDBytes[4])<<32 | uint64(tableIDBytes[5])<<40
	e.Reserved, _ = ReadNumber[uint16](reader, 2)

	// 读取可变数据部分
	e.DatabaseNameLen, _ = ReadNumber[uint8](reader, 1)
	e.DatabaseName, _ = ReadStringFixedFromReader(reader, int(e.DatabaseNameLen))
	reader.ReadByte() // 读取 NULL 终止符

	e.TableNameLen, _ = ReadNumber[uint8](reader, 1)
	e.TableName, _ = ReadStringFixedFromReader(reader, int(e.TableNameLen))
	reader.ReadByte() // 读取 NULL 终止符

	// 读取列数（长度编码）
	colCount, _ := ReadLenencNumber[uint64](reader)
	e.ColumnCount = int(colCount)

	// 读取列类型数组
	e.ColumnTypes = make([]uint8, e.ColumnCount)
	io.ReadFull(reader, e.ColumnTypes)

	// 读取元数据块长度（长度编码）
	metadataLen, _ := ReadLenencNumber[uint64](reader)
	e.MetadataLen = int(metadataLen)

	// 读取元数据块
	if e.MetadataLen > 0 {
		e.Metadata = make([]byte, e.MetadataLen)
		io.ReadFull(reader, e.Metadata)
	}

	// 读取 NULL 位图（长度 = (n + 7) / 8）
	nullBitmapLen := (e.ColumnCount + 7) / 8
	e.NullBitmap = make([]byte, nullBitmapLen)
	io.ReadFull(reader, e.NullBitmap)

	// 读取可选元数据块（如果有剩余数据）
	remaining, _ := io.ReadAll(reader)
	if len(remaining) > 0 {
		e.OptionalMetadata = remaining
	}

	return nil
}

// Marshal 序列化 TABLE_MAP_EVENT
func (e *TableMapEvent) Marshal() ([]byte, error) {
	buf := new(bytes.Buffer)

	// 序列化事件头
	headerBytes, _ := e.Header.Marshal()
	buf.Write(headerBytes)

	// 写入固定字段
	binary.Write(buf, binary.LittleEndian, e.TableID)
	binary.Write(buf, binary.LittleEndian, e.Reserved)

	// 写入数据库名
	buf.WriteByte(e.DatabaseNameLen)
	buf.WriteString(e.DatabaseName)
	buf.WriteByte(0x00)

	// 写入表名
	buf.WriteByte(e.TableNameLen)
	buf.WriteString(e.TableName)
	buf.WriteByte(0x00)

	// 写入列数
	WriteLenencNumber(buf, uint64(e.ColumnCount))

	// 写入列类型数组
	buf.Write(e.ColumnTypes)

	// 写入元数据块
	WriteLenencNumber(buf, uint64(e.MetadataLen))
	buf.Write(e.Metadata)

	// 写入 NULL 位图
	buf.Write(e.NullBitmap)

	// 写入可选元数据块
	if len(e.OptionalMetadata) > 0 {
		buf.Write(e.OptionalMetadata)
	}

	return buf.Bytes(), nil
}

// ============================================
// XID_EVENT - XA 事务标识符提交事件
// ============================================

type XidEvent struct {
	Header BinlogEventHeader

	// 事件体字段
	XID uint64 // XA 事务标识符
}

// Unmarshal 解析 XID_EVENT
// 注意：这个方法假设 r 已经被读取过事件头，现在指向事件体
func (e *XidEvent) Unmarshal(r io.Reader) error {
	// Binlog 事件不使用 Packet 封装，直接读取
	reader := bufio.NewReader(r)

	// 读取 XID
	e.XID, _ = ReadNumber[uint64](reader, 8)

	return nil
}

// Marshal 序列化 XID_EVENT
func (e *XidEvent) Marshal() ([]byte, error) {
	buf := new(bytes.Buffer)

	// 序列化事件头
	headerBytes, _ := e.Header.Marshal()
	buf.Write(headerBytes)

	// 写入 XID（8字节）
	binary.Write(buf, binary.LittleEndian, e.XID)

	return buf.Bytes(), nil
}

// ============================================
// ReplicationNetworkStream - 复制网络流解析器
// ============================================

type ReplicationNetworkStream struct {
	reader         *bufio.Reader
	lastPacket     Packet
	statusByte     uint8
	currentEvent   []byte
	eventPosition  int
}

// NewReplicationNetworkStream 创建新的复制网络流解析器
func NewReplicationNetworkStream(r io.Reader) *ReplicationNetworkStream {
	return &ReplicationNetworkStream{
		reader: bufio.NewReader(r),
	}
}

// ReadEvent 读取下一个事件（包括网络协议头、状态字节和事件数据）
func (s *ReplicationNetworkStream) ReadEvent() (BinlogEventHeader, []byte, uint8, error) {
	var header BinlogEventHeader
	var eventData []byte
	var status uint8

	// 1. 读取网络协议头（4字节）
	packetBuf := make([]byte, 4)
	if _, err := io.ReadFull(s.reader, packetBuf); err != nil {
		return header, nil, 0, err
	}

	// 解析包头
	payloadLength := uint32(packetBuf[0]) | uint32(packetBuf[1])<<8 | uint32(packetBuf[2])<<16
	sequenceID := packetBuf[3]

	// 2. 读取状态字节（1字节）
	statusByte, err := s.reader.ReadByte()
	if err != nil {
		return header, nil, 0, err
	}
	status = statusByte

	// 检查状态
	if status == BINLOG_NETWORK_STATUS_ERR {
		return header, nil, status, fmt.Errorf("error status from server")
	}
	if status == BINLOG_NETWORK_STATUS_EOF {
		return header, nil, status, io.EOF
	}

	// 3. 读取事件数据（不包括状态字节）
	payload := make([]byte, payloadLength)
	if _, err := io.ReadFull(s.reader, payload); err != nil {
		return header, nil, status, err
	}

	// 4. Binlog 事件包的第一个字节是 OK 标记 (0x00)，需要跳过
	// MariaDB 可能返回两种格式：
	// a) 标准 MySQL 格式：[0x00][event_header][event_body]
	// b) MariaDB 原始格式：直接返回 binlog 文件内容（不含 OK 标记）
	if len(payload) > 0 && payload[0] == 0x00 {
		// 标准 MySQL 格式，跳过 OK 标记
		payload = payload[1:]
	} else if len(payload) >= 4 {
		// MariaDB 原始格式，检查是否是 binlog 文件内容
		// 可能是 Rotate Event 或其他 binlog 事件
		// 不跳过任何字节，直接解析
	}

	// 5. 解析事件头（前19字节）
	if len(payload) < 19 {
		return header, eventData, status, fmt.Errorf("payload too short for event header: %d bytes", len(payload))
	}

	headerReader := bytes.NewReader(payload)
	if err := header.Unmarshal(headerReader); err != nil {
		return header, nil, status, err
	}

	// 6. 事件数据 = payload[19:]
	eventData = payload[19:]

	// 保存最后一个包信息
	s.lastPacket = Packet{
		PayloadLength: payloadLength,
		SequenceID:    sequenceID,
		Payload:      payload,
	}

	return header, eventData, status, nil
}

// GetLastPacket 获取最后一个网络包
func (s *ReplicationNetworkStream) GetLastPacket() Packet {
	return s.lastPacket
}

// ParseEventHeader 仅解析事件头，不读取事件体
func (s *ReplicationNetworkStream) ParseEventHeader() (BinlogEventHeader, uint8, error) {
	var header BinlogEventHeader

	// 读取网络协议头（4字节）
	packetBuf := make([]byte, 4)
	if _, err := io.ReadFull(s.reader, packetBuf); err != nil {
		return header, 0, err
	}

	// 解析包头
	payloadLength := uint32(packetBuf[0]) | uint32(packetBuf[1])<<8 | uint32(packetBuf[2])<<16
	sequenceID := packetBuf[3]

	// 读取状态字节（1字节）
	statusByte, err := s.reader.ReadByte()
	if err != nil {
		return header, 0, err
	}

	// 读取事件头（前19字节）
	eventHeaderBuf := make([]byte, 19)
	if _, err := io.ReadFull(s.reader, eventHeaderBuf); err != nil {
		return header, 0, err
	}

	// 解析事件头
	headerReader := bytes.NewReader(eventHeaderBuf)
	if err := header.Unmarshal(headerReader); err != nil {
		return header, 0, err
	}

	// 保存最后一个包信息
	s.lastPacket = Packet{
		PayloadLength: payloadLength,
		SequenceID:    sequenceID,
	}

	return header, statusByte, nil
}

// SkipEventData 跳过当前事件的数据体
func (s *ReplicationNetworkStream) SkipEventData(header BinlogEventHeader) error {
	// 计算需要跳过的数据长度（事件总长度 - 事件头长度19 - 状态字节1）
	dataLength := int(header.EventLength) - BINLOG_EVENT_HEADER_LENGTH
	if dataLength <= 0 {
		return nil
	}

	discard := make([]byte, dataLength)
	_, err := io.ReadFull(s.reader, discard)
	return err
}
