---
name: analyze-mariadb-tidb-test-failures
overview: 结合 MariaDB 和 TiDB 源代码分析 5 个失败的 MySQL 协议测试用例的根因和修复方案
todos:
  - id: fix-test-com-binlog-dump
    content: 修复TestComBinlogDump测试数据，将文件名改为6位格式
    status: completed
  - id: fix-test-format-description
    content: 修复TestFormatDescriptionEventSimple，调整EventLength和数组数据
    status: completed
  - id: fix-query-event-unmarshal
    content: 修复QueryEvent的Unmarshal方法，确保读取NULL终止符
    status: completed
  - id: fix-read-binary-time
    content: 修复readBinaryTime格式化，使用%04d而非%d
    status: completed
  - id: fix-test-binary-row-data-packet-blob
    content: 修复TestBinaryRowDataPacketBlob，添加3字节长度前缀
    status: completed
---

## 需求概述

研究5个失败的测试用例，判断是用例问题还是程序问题，并提供修复方案。这些测试用例曾经是通过的，现在出现失败。

## 测试失败详情

### 1. TestComBinlogDump (replication_test.go:88)

- 期望: "mysql-bin.000019"
- 实际: "mysql-bin.00019"
- 问题: 测试数据文件名缺少一个零

### 2. TestFormatDescriptionEventSimple (replication_test.go:123)

- 期望: HeaderLength=19, EventTypePostHeader数组长度>0
- 实际: HeaderLength=0, EventTypePostHeader数组长度=0
- 问题: arrayLength计算结果为负数

### 3. TestQueryEventSimple (replication_test.go:239)

- 期望: Query="SELECT 1"
- 实际: Query=""
- 问题: 当DatabaseNameLen=0时，未读取NULL终止符

### 4. TestBinaryRowDataPacketTime (response_packet_test.go:428)

- 期望: "+0000 01:02:03.000000"
- 实际: "00:00:00"
- 问题: 读取逻辑错误和格式化代码错误

### 5. TestBinaryRowDataPacketBlob (response_packet_test.go:464)

- 期望: "abc"
- 实际: EOF错误，Values数组长度为0
- 问题: 测试数据缺少3字节长度前缀

## 技术栈

- 编程语言: Go
- 测试框架: testify/assert
- 目标协议: MySQL/MariaDB二进制协议和复制协议

## 问题分析结论

### 1. TestComBinlogDump - 用例问题

**问题原因**: 测试数据与期望值不匹配

- 测试数据字节序列: `mysql-bin.00019` (15字节)
- 期望值: `mysql-bin.000019` (6位数字格式)
- MariaDB源码确认: binlog文件名使用`%06lu`格式，总是6位数字

**修复方案**: 修改测试数据，增加一个零

```
'm', 'y', 's', 'q', 'l', '-', 'b', 'i', 'n', '.', '0', '0', '0', '0', '1', '9',
```

### 2. TestFormatDescriptionEventSimple - 用例问题 + 程序问题

**问题原因**:

- EventLength=78过小，导致arrayLength计算为负
- 计算公式: 78 - 19(头部) - 57(固定字段) - 5(校验和) = -3
- Unmarshal方法未检查arrayLength的有效性

**修复方案**:

1. 修改测试数据EventLength为合理的值（如85）
2. 相应增加EventTypePostHeader数组数据
3. 在Unmarshal方法中添加边界检查

### 3. TestQueryEventSimple - 程序问题

**问题原因**: replication.go:400-404行逻辑错误

- 当DatabaseNameLen=0时，跳过读取NULL终止符
- 导致第一个0x00被当作Query的第一个字符（NULL）
- MariaDB源码确认: 即使db_len=0，也会跳过1字节(NULL)

**修复方案**: 修改replication.go的Unmarshal方法

```
// 读取数据库名（无论长度是否为0，都读取NULL终止符）
e.DatabaseName, _ = ReadStringFixedFromReader(reader, int(e.DatabaseNameLen))
reader.ReadByte() // 总是读取NULL终止符
```

### 4. TestBinaryRowDataPacketTime - 程序问题

**问题原因**:

1. 读取逻辑错误: length=8时，读取6字节后数据不完整
2. 格式化错误: packet.go:2013使用`%d`而非`%04d`

**修复方案**: 修改packet.go:2013

```
return fmt.Sprintf("%s%04d %02d:%02d:%02d.%06d", sign, days, hours, minutes, seconds, microseconds), nil
```

### 5. TestBinaryRowDataPacketBlob - 用例问题

**问题原因**: 测试数据不符合MEDIUM_BLOB协议规范

- MEDIUM_BLOB(0xfd)需要3字节长度前缀
- 测试数据只提供1字节长度
- 导致readBinaryBlob读取时遇到EOF

**修复方案**: 修改测试数据

```
testData := []byte{
    0x00,                 // Header
    0x00,                 // NULL bitmap (no nulls)
    0x03, 0x00, 0x00,     // 3字节长度前缀
    'a', 'b', 'c',
}
```

## 架构设计

### 修复文件清单

```
server/protocol/
├── replication_test.go    [MODIFY] 修复TestComBinlogDump测试数据
├── replication.go         [MODIFY] 修复QueryEvent的Unmarshal方法
├── response_packet_test.go [MODIFY] 修复TestFormatDescriptionEventSimple、TestBinaryRowDataPacketTime、TestBinaryRowDataPacketBlob
└── packet.go              [MODIFY] 修复readBinaryTime格式化
```

### 实现策略

1. **TestComBinlogDump**: 直接修改测试数据，增加前导零
2. **TestFormatDescriptionEventSimple**: 调整EventLength和数组数据，添加验证
3. **TestQueryEventSimple**: 修复Unmarshal逻辑，确保总是读取NULL终止符
4. **TestBinaryRowDataPacketTime**: 修改格式化字符串为%04d
5. **TestBinaryRowDataPacketBlob**: 添加3字节长度前缀到测试数据

### 关键技术点

- MariaDB binlog文件名格式: 6位数字（%06lu）
- QUERY_EVENT数据库名后总是有NULL终止符（即使db_len=0）
- BLOB类型长度前缀: TINY_BLOB=1字节, MEDIUM_BLOB=3字节, LONG_BLOB=4字节
- TIME类型格式: +DDDD HH:MM:SS.mmmmmm (4位天数)