# COM_STMT_EXECUTE 测试指南

## 问题分析

`ComStmtExecutePacket` 存在以下问题：

1. **NULL Bitmap 处理**：根据 MariaDB 协议规范，NULL bitmap 的第 0、1 位不使用，从第 2 位开始存储第 1 个参数的 NULL 标志。即第 n 列对应位位置为 (n + 2)。

2. **参数类型处理**：需要确保参数类型和参数值正确序列化和反序列化。

3. **包格式验证**：需要验证实际的网络通信包格式是否符合协议规范。

## 测试方法

### 方法 1：使用测试客户端和服务器

**步骤 1：启动测试服务器**

打开一个终端，运行：
```powershell
cd d:/code/db
powershell -ExecutionPolicy Bypass -File run_server.ps1
```

**步骤 2：启动测试客户端**

打开另一个终端，运行：
```powershell
cd d:/code/db
powershell -ExecutionPolicy Bypass -File run_client.ps1
```

### 方法 2：使用现有 MySQL 服务器进行抓包测试

**步骤 1：启动 Wireshark 或其他抓包工具**

抓取 localhost:13307 的数据包。

**步骤 2：使用真实 MySQL 客户端**

```bash
mysql -h 127.0.0.1 -P 13307 -u root -p
```

执行预处理语句：
```sql
PREPARE stmt1 FROM 'SELECT * FROM users WHERE id = ?';
SET @id = 1;
EXECUTE stmt1 USING @id;
```

**步骤 3：分析抓包数据**

查看 COM_STMT_EXECUTE 包的实际格式，特别是：
- NULL bitmap 的位置和格式
- 参数类型的格式
- 参数值的格式

## 测试用例

测试客户端包含以下测试用例：

1. **测试 1：单个整型参数**
   - StatementID: 1
   - ParamTypes: [INT]
   - ParamValues: [123]

2. **测试 2：多个参数**
   - ParamTypes: [INT, VAR_STRING]
   - ParamValues: [456, "test"]

3. **测试 3：带 NULL 参数**
   - ParamTypes: [VAR_STRING]
   - ParamValues: [nil]
   - NullBitmap: 正确设置位标志

## 预期输出

服务器会显示：
- 收到的原始包数据 (hex 格式)
- 解析后的各个字段值
- 参数类型和参数值的详细信息

客户端会显示：
- 发送的请求数据 (hex 格式)
- 请求的详细结构
- 接收到的结果集

## 常见问题排查

### 问题 1：Unmarshal 返回错误

检查包格式是否符合协议规范，特别是：
- 包头是否正确（长度 + SequenceID）
- NULL bitmap 是否按协议要求设置
- 参数类型和值是否匹配

### 问题 2：参数值解析错误

检查：
- 参数类型值是否正确（参考 MySQL 类型常量）
- 参数值格式是否正确（例如字符串是否使用长度编码）
- NULL 参数的位标志是否正确

### 问题 3：与真实 MySQL 服务器不兼容

使用抓包工具对比：
- 真实 MySQL 发送的包格式
- 当前实现生成的包格式
- 找出差异并修正

## 下一步

1. 运行测试客户端和服务器
2. 检查输出中的包数据
3. 与真实 MySQL 服务器抓包对比
4. 修正 `ComStmtExecutePacket` 的实现
5. 再次测试验证修复效果
