# MySQL 协议分析

## 建立连接

### 1. S->C Server Greeting 包
* **Protocol**：协议版本号（通常为10，0x0A）
* **Version**：服务器版本号（如"5.7.35"或"8.0.26"）
* **ThreadID**：服务器为连接分配的唯一线程ID
* **Server Capabilities**：服务器支持的功能标志（低位2字节）
* **Server language**：字符编码（如0x21表示utf8_general_ci）
* **Extended Server Capabilities**：扩展功能标志（高位2字节）
* **Authentication Plugin length**：认证插件数据长度（通常为21字节）
* **Unused**：保留字段（全0）
* **MariaDB Extended Server Capabilities**：MariaDB特有功能标志（仅MariaDB存在）
* **Salt**：20字节加密盐（用于身份认证）
* **Authentication Plugin**：默认认证插件名（如"mysql_native_password"）

---

### 2. C->S Login Request
* **Client Capabilities**：客户端支持的功能标志（低位2字节）
* **Extended Client Capabilities**：客户端扩展功能标志（高位2字节）
* **Max Packet**：客户端最大数据包大小（默认0xFFFFFF，16MB）
* **MariaDB Extended Client Capabilities**：MariaDB客户端扩展标志
* **Username**：用户名（NULL结尾字符串）
* **Password**：加密密码（长度编码字符串，无密码时为空）
* **Client Auth Plugin**：客户端认证插件名
* **ConnectionAttributes**：连接属性键值对（可选）
* **Schema**：初始数据库名（当Client Capabilities的CLIENT_CONNECT_WITH_DB标志置位时存在）

---

### 2.1 登录失败 S->C Error Packet
* **Response Code**：0xFF（错误包标识）
* **Error Code**：MySQL错误码（如1045-访问被拒）
* **SQL State**：5字节SQL状态码（如"28000"）
* **Error Message**：错误描述文本（NULL结尾字符串）

### 2.1 登录成功 S->C OK Packet
* **Response Code**：0x00（成功标识）
* **Affected Rows**：受影响行数（登录时为0）
* **Server Status**：服务器状态标志（如0x0002-AUTOCOMMIT）
* **Warnings**：警告数量（通常为0）

---

### 2.2 C->S Query请求（登录后）
* **Command**：0x03（COM_QUERY标识）
* **Statement**："select @@version_comment limit 1"（SQL文本）

### 2.3 S->C 查询响应
返回结构：
1. 列数量包（Length Coded Binary）
2. 列定义包（Field Packet）
3. EOF包（协议<4.1）或OK包（协议≥4.1）
4. 行数据包
5. EOF/OK结束包

@@version_comment值示例：  
"MariaDB Server" 或 "MySQL Community Server"

---

## 3. 结束连接
### C->S Quit请求
* **Command**：0x01（COM_QUIT标识）

---

## 4. 数据库切换
### 4.1 查询当前数据库
```sql
SELECT DATABASE()
```
响应：包含当前库名的Field Packet

### 4.2 C->S 切换数据库请求
* **Command**：0x02（COM_INIT_DB）
* **Schema**：目标数据库名（NULL结尾字符串）

切换成功返回OK Packet（Response Code 0x00）

---

## Field Packet 详解
列定义包结构：
| 字段 | 类型 | 说明 |
|------|------|------|
| Catalog | 长度编码字符串 | 固定"def" |
| Schema | 长度编码字符串 | 数据库名 |
| Table | 长度编码字符串 | 虚拟表名 |
| OrgTable | 长度编码字符串 | 原始表名 |
| Name | 长度编码字符串 | 列别名 |
| OrgName | 长度编码字符串 | 原始列名 |
| Next Length | 1字节 | 固定0x0C |
| Charset | 2字节 | 列字符集编号 |
| Column Length | 4字节 | 最大长度 |
| Column Type | 1字节 | 数据类型（如0xFD=VAR_STRING） |
| Flags | 2字节 | 列标志（如NOT_NULL_FLAG） |
| Decimals | 1字节 | 小数位数 |
| Filler | 2字节 | 保留字段（全0） |

> **协议说明**：  
> - 长度编码字符串 = 长度字节 + 字符串内容  
> - MySQL 4.1+ 协议使用EOF包（0xFE），新协议改用OK包  
> - 能力标志使用位掩码（如CLIENT_PROTOCOL_41=0x00000200）  
> - 认证插件：MySQL 8.0默认使用caching_sha2_password

**示例交互流程**：
```
1. 连接 → Server Greeting
2. 客户端 → Login Request
3. 服务端 → OK Packet
4. 客户端 → COM_QUERY("SELECT...")
5. 服务端 → 列定义 → 行数据 → EOF/OK
6. 客户端 → COM_INIT_DB("new_db")
7. 服务端 → OK Packet
8. 客户端 → COM_QUIT
```