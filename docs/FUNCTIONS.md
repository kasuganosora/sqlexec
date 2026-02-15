# SQLExec 函数系统技术原理

本文档详细介绍 SQLExec 项目的函数系统架构、内建函数分类、注册机制、用户自定义函数（UDF）支持，以及 JSON 函数的实现原理。

---

## 目录

1. [函数系统架构](#1-函数系统架构)
2. [内建函数](#2-内建函数)
3. [函数注册机制](#3-函数注册机制)
4. [UDF 支持](#4-udf-支持)
5. [聚合函数](#5-聚合函数)
6. [JSON 函数](#6-json-函数)
7. [插件扩展体系](#7-插件扩展体系)

---

## 1. 函数系统架构

### 1.1 整体设计

SQLExec 的函数系统采用**双注册表**架构，分别管理标量函数与聚合函数，并通过统一的函数解析与调用链路集成到 SQL 查询执行流程中。

```
┌────────────────────────────────────────────────────┐
│                  SQL 查询引擎                       │
│   SELECT UPPER(name), COUNT(*) FROM t GROUP BY ... │
└────────┬──────────────────────────────┬────────────┘
         │ 标量函数调用                   │ 聚合函数调用
         ▼                              ▼
┌─────────────────┐           ┌─────────────────────┐
│ FunctionRegistry │           │ AggregateRegistry   │
│ (globalRegistry) │           │ (aggregateRegistry) │
│                  │           │                     │
│ map[string]      │           │ map[string]         │
│  *FunctionInfo   │           │  *AggregateFuncInfo │
└─────────────────┘           └─────────────────────┘
         │                              │
         ▼                              ▼
┌─────────────────┐           ┌─────────────────────┐
│  FunctionHandle  │           │ AggregateHandle     │
│ func([]any)      │           │ func(ctx, []any)    │
│  -> (any, error) │           │  -> error           │
└─────────────────┘           │ AggregateResult     │
                              │ func(ctx)            │
                              │  -> (any, error)     │
                              └─────────────────────┘
```

### 1.2 核心类型定义

系统中函数被分为三种类型（定义于 `pkg/builtin/functions.go`）：

```go
type FunctionType int

const (
    FunctionTypeScalar    FunctionType = iota // 标量函数：对每一行返回一个值
    FunctionTypeAggregate                     // 聚合函数：对一组行返回一个值
    FunctionTypeWindow                        // 窗口函数：预留
)
```

标量函数的处理签名为：

```go
type FunctionHandle func(args []interface{}) (interface{}, error)
```

每个已注册的函数通过 `FunctionInfo` 结构体描述其完整元数据：

```go
type FunctionInfo struct {
    Name        string              // 函数名（小写）
    Type        FunctionType        // 函数类型
    Signatures  []FunctionSignature // 签名列表（支持重载）
    Handler     FunctionHandle      // 处理函数
    Description string              // 函数描述
    Example     string              // 使用示例
    Category    string              // 所属类别
}
```

`FunctionSignature` 描述函数签名，包含参数类型列表和是否支持可变参数：

```go
type FunctionSignature struct {
    Name       string
    ReturnType string
    ParamTypes []string
    Variadic   bool
}
```

### 1.3 全局注册表

系统维护一个全局的 `FunctionRegistry` 实例，使用读写锁（`sync.RWMutex`）保证并发安全：

```go
var globalRegistry = NewFunctionRegistry()
```

所有内建函数在各自文件的 `init()` 函数中通过 `RegisterGlobal()` 注册到该全局注册表。查询执行时通过 `GetGlobal(name)` 查找函数。

### 1.4 函数类别体系

系统通过 `FunctionCategory` 常量定义了丰富的函数类别：

| 类别 | 常量 | 说明 |
|------|------|------|
| 数学函数 | `math` | 数值计算、三角函数、对数等 |
| 字符串函数 | `string` | 字符串操作与转换 |
| 日期时间函数 | `date` | 日期解析、格式化、运算 |
| 聚合函数 | `aggregate` | 分组聚合计算 |
| 控制流函数 | `control` | 条件判断、NULL 处理 |
| JSON 函数 | `json` | JSON 解析、提取、修改 |
| 系统函数 | `system` | 类型检测、UUID 生成等 |
| 金融函数 | `financial` | 货币时间价值、债券计算等 |
| 编码函数 | `encoding` | Base64、Hex、哈希等 |
| 位运算函数 | `bitwise` | 位操作与位计算 |
| 相似度函数 | `similarity` | 编辑距离、字符串相似度 |
| 向量函数 | `vector` | 向量距离计算 |
| ICU 函数 | `icu` | Unicode 规范化、排序规则 |
| 用户函数 | `user` | 用户自定义函数 |

### 1.5 函数解析优先级

扩展注册表 `FunctionRegistryExt` 支持多作用域函数查找，按以下优先级解析：

```
会话函数 (session) > 用户函数 (user) > 全局标量函数 > 全局聚合函数 > 别名
```

这意味着用户可以在会话级别覆盖全局函数的行为，实现灵活的函数定制。

---

## 2. 内建函数

### 2.1 字符串函数（string）

字符串函数注册于 `pkg/builtin/string_functions.go`，在 `init()` 中批量注册。

#### 基础操作

| 函数 | 说明 | 示例 |
|------|------|------|
| `CONCAT(s1, s2, ...)` | 连接字符串 | `CONCAT('Hello', ' ', 'World')` -> `'Hello World'` |
| `CONCAT_WS(sep, s1, s2, ...)` | 使用分隔符连接 | `CONCAT_WS(',', 'a', 'b', 'c')` -> `'a,b,c'` |
| `LENGTH(s)` | 返回字节长度 | `LENGTH('hello')` -> `5` |
| `CHAR_LENGTH(s)` | 返回字符长度 | `CHAR_LENGTH('hello')` -> `5` |
| `UPPER(s)` / `UCASE(s)` | 转大写 | `UPPER('hello')` -> `'HELLO'` |
| `LOWER(s)` / `LCASE(s)` | 转小写 | `LOWER('HELLO')` -> `'hello'` |

#### 裁剪与填充

| 函数 | 说明 | 示例 |
|------|------|------|
| `TRIM(s)` | 删除前后空格 | `TRIM('  hi  ')` -> `'hi'` |
| `LTRIM(s)` | 删除前导空格 | `LTRIM('  hi')` -> `'hi'` |
| `RTRIM(s)` | 删除尾部空格 | `RTRIM('hi  ')` -> `'hi'` |
| `LPAD(s, n, pad)` | 左填充 | `LPAD('hi', 5, '*')` -> `'***hi'` |
| `RPAD(s, n, pad)` | 右填充 | `RPAD('hi', 5, '*')` -> `'hi***'` |

#### 子串与搜索

| 函数 | 说明 | 示例 |
|------|------|------|
| `SUBSTRING(s, pos, len)` / `SUBSTR` | 提取子串（SQL 索引从 1 起） | `SUBSTRING('hello', 2, 3)` -> `'ell'` |
| `LEFT(s, n)` | 返回左边 n 个字符 | `LEFT('hello', 3)` -> `'hel'` |
| `RIGHT(s, n)` | 返回右边 n 个字符 | `RIGHT('hello', 3)` -> `'llo'` |
| `POSITION(sub IN s)` / `LOCATE` | 返回子串位置 | `POSITION('ll', 'hello')` -> `3` |
| `INSTR(s, sub)` | 返回子串位置 | `INSTR('hello', 'll')` -> `3` |

#### 变换

| 函数 | 说明 | 示例 |
|------|------|------|
| `REPLACE(s, old, new)` | 替换子串 | `REPLACE('hello', 'l', 'r')` -> `'herro'` |
| `REPEAT(s, n)` | 重复字符串 | `REPEAT('ab', 3)` -> `'ababab'` |
| `REVERSE(s)` | 反转字符串 | `REVERSE('hello')` -> `'olleh'` |
| `TRANSLATE(s, from, to)` | 逐字符替换映射 | `TRANSLATE('hello', 'el', 'ip')` -> `'hippo'` |

#### 判断与格式化

| 函数 | 说明 | 示例 |
|------|------|------|
| `STARTS_WITH(s, prefix)` | 前缀判断 | `STARTS_WITH('hello', 'he')` -> `true` |
| `ENDS_WITH(s, suffix)` | 后缀判断 | `ENDS_WITH('hello', 'lo')` -> `true` |
| `CONTAINS(s, sub)` | 包含判断 | `CONTAINS('hello', 'ell')` -> `true` |
| `FORMAT(fmt, ...)` / `PRINTF` | 格式化字符串 | `FORMAT('Hello %s', 'world')` -> `'Hello world'` |
| `ASCII(s)` / `ORD(s)` | 返回首字符 ASCII 值 | `ASCII('A')` -> `65` |
| `CHR(n)` / `CHAR(n)` | 码点转字符 | `CHR(65)` -> `'A'` |
| `UNICODE(s)` | 返回首字符 Unicode 码点 | `UNICODE('A')` -> `65` |
| `URL_ENCODE(s)` | URL 编码 | `URL_ENCODE('hello world')` -> `'hello+world'` |
| `URL_DECODE(s)` | URL 解码 | `URL_DECODE('hello+world')` -> `'hello world'` |

### 2.2 数学函数（math）

数学函数注册于 `pkg/builtin/math_functions.go`。

#### 基础运算

| 函数 | 说明 | 示例 |
|------|------|------|
| `ABS(x)` | 绝对值 | `ABS(-5)` -> `5` |
| `CEIL(x)` / `CEILING(x)` | 向上取整 | `CEIL(3.14)` -> `4` |
| `FLOOR(x)` | 向下取整 | `FLOOR(3.14)` -> `3` |
| `ROUND(x, d)` | 四舍五入 | `ROUND(3.14159, 2)` -> `3.14` |
| `TRUNCATE(x, d)` | 截断 | `TRUNCATE(3.14159, 2)` -> `3.14` |
| `MOD(x, y)` | 取模 | `MOD(10, 3)` -> `1` |
| `SIGN(x)` | 符号函数 | `SIGN(-10)` -> `-1` |

#### 幂与对数

| 函数 | 说明 | 示例 |
|------|------|------|
| `SQRT(x)` | 平方根 | `SQRT(16)` -> `4` |
| `CBRT(x)` | 立方根 | `CBRT(27)` -> `3` |
| `POW(x, y)` / `POWER(x, y)` | 幂运算 | `POW(2, 3)` -> `8` |
| `EXP(x)` | e 的 x 次方 | `EXP(1)` -> `2.718...` |
| `LOG(x)` / `LN(x)` | 自然对数 | `LN(10)` -> `2.302...` |
| `LOG10(x)` | 常用对数 | `LOG10(100)` -> `2` |
| `LOG2(x)` | 以 2 为底的对数 | `LOG2(8)` -> `3` |

#### 三角函数

| 函数 | 说明 | 函数 | 说明 |
|------|------|------|------|
| `SIN(x)` | 正弦 | `ASIN(x)` | 反正弦 |
| `COS(x)` | 余弦 | `ACOS(x)` | 反余弦 |
| `TAN(x)` | 正切 | `ATAN(x)` | 反正切 |
| `COT(x)` | 余切 | `ATAN2(y, x)` | 二参数反正切 |
| `SINH(x)` | 双曲正弦 | `ASINH(x)` | 反双曲正弦 |
| `COSH(x)` | 双曲余弦 | `ACOSH(x)` | 反双曲余弦 |
| `TANH(x)` | 双曲正切 | `ATANH(x)` | 反双曲正切 |

#### 其他

| 函数 | 说明 | 示例 |
|------|------|------|
| `PI()` | 圆周率 | `PI()` -> `3.14159...` |
| `DEGREES(x)` | 弧度转角度 | `DEGREES(PI())` -> `180` |
| `RADIANS(x)` | 角度转弧度 | `RADIANS(180)` -> `3.14159...` |
| `RAND()` | 随机数 [0, 1) | `RAND()` -> `0.123...` |
| `FACTORIAL(n)` | 阶乘 (n <= 20) | `FACTORIAL(5)` -> `120` |
| `GCD(a, b)` | 最大公约数 | `GCD(12, 8)` -> `4` |
| `LCM(a, b)` | 最小公倍数 | `LCM(4, 6)` -> `12` |
| `EVEN(x)` | 向远离零方向取最近偶数 | `EVEN(3)` -> `4` |
| `IS_FINITE(x)` | 是否有限值 | `IS_FINITE(1.0)` -> `true` |
| `IS_INFINITE(x)` | 是否无穷 | `IS_INFINITE(1.0/0.0)` -> `true` |
| `IS_NAN(x)` | 是否 NaN | `IS_NAN(0.0/0.0)` -> `true` |

### 2.3 日期时间函数（date）

日期时间函数注册于 `pkg/builtin/date_functions.go`，支持多种日期格式自动解析：

```go
formats := []string{
    "2006-01-02 15:04:05",
    "2006-01-02T15:04:05",
    "2006-01-02",
}
```

#### 获取当前时间

| 函数 | 说明 | 示例 |
|------|------|------|
| `NOW()` / `CURRENT_TIMESTAMP()` | 当前日期时间 | `NOW()` -> `'2024-01-01 12:00:00'` |
| `CURRENT_DATE()` / `CURDATE()` | 当前日期 | `CURDATE()` -> `'2024-01-01'` |
| `CURRENT_TIME()` / `CURTIME()` | 当前时间 | `CURTIME()` -> `'12:00:00'` |

#### 日期部分提取

| 函数 | 说明 | 示例 |
|------|------|------|
| `YEAR(d)` | 年份 | `YEAR('2024-01-01')` -> `2024` |
| `MONTH(d)` | 月份 | `MONTH('2024-03-15')` -> `3` |
| `DAY(d)` / `DAYOFMONTH(d)` | 日 | `DAY('2024-03-15')` -> `15` |
| `HOUR(d)` | 小时 | `HOUR('2024-01-01 12:30:00')` -> `12` |
| `MINUTE(d)` | 分钟 | `MINUTE('2024-01-01 12:30:00')` -> `30` |
| `SECOND(d)` | 秒 | `SECOND('2024-01-01 12:30:45')` -> `45` |
| `EXTRACT(part, d)` / `DATE_PART` | 通用提取 | `EXTRACT('year', '2024-03-15')` -> `2024` |
| `QUARTER(d)` | 季度 (1-4) | `QUARTER('2024-03-15')` -> `1` |
| `WEEK(d)` / `WEEKOFYEAR(d)` | ISO 周数 | `WEEK('2024-03-15')` -> `11` |
| `DAYOFWEEK(d)` | 星期几 (1=Sun) | `DAYOFWEEK('2024-01-01')` -> `2` |
| `DAYOFYEAR(d)` | 年中第几天 | `DAYOFYEAR('2024-03-01')` -> `61` |
| `DAYNAME(d)` | 星期名称 | `DAYNAME('2024-01-01')` -> `'Monday'` |
| `MONTHNAME(d)` | 月份名称 | `MONTHNAME('2024-03-15')` -> `'March'` |

#### 日期运算

| 函数 | 说明 | 示例 |
|------|------|------|
| `DATE_ADD(d, n, unit)` / `ADDDATE` | 日期加间隔 | `DATE_ADD('2024-01-01', 1, 'month')` -> `'2024-02-01'` |
| `DATE_SUB(d, n, unit)` / `SUBDATE` | 日期减间隔 | `DATE_SUB('2024-02-01', 1, 'month')` -> `'2024-01-01'` |
| `DATEDIFF(d1, d2)` | 日期差（天） | `DATEDIFF('2024-01-02', '2024-01-01')` -> `1` |
| `AGE(d1, d2)` | 年月日差异 | `AGE('2026-03-15', '2024-01-01')` -> `'2 years 2 months 14 days'` |

支持的时间间隔单位包括：`year`、`month`、`week`、`day`、`hour`、`minute`、`second`。

#### 日期截断与构造

| 函数 | 说明 | 示例 |
|------|------|------|
| `DATE_TRUNC(unit, d)` | 截断到指定精度 | `DATE_TRUNC('month', '2024-03-15')` -> `'2024-03-01'` |
| `LAST_DAY(d)` | 当月最后一天 | `LAST_DAY('2024-02-15')` -> `'2024-02-29'` |
| `MAKE_DATE(y, m, d)` | 构造日期 | `MAKE_DATE(2024, 3, 15)` -> `'2024-03-15'` |
| `MAKE_TIME(h, m, s)` | 构造时间 | `MAKE_TIME(12, 30, 45)` -> `'12:30:45'` |
| `MAKE_TIMESTAMP(y,m,d,h,mi,s)` | 构造时间戳 | `MAKE_TIMESTAMP(2024,3,15,12,30,45)` |

#### 时间戳转换

| 函数 | 说明 | 示例 |
|------|------|------|
| `UNIX_TIMESTAMP(d)` / `EPOCH` | 转 Unix 时间戳 | `UNIX_TIMESTAMP('2024-01-01')` -> `1704067200` |
| `FROM_UNIXTIME(n)` / `TO_TIMESTAMP` | 从 Unix 时间戳转换 | `FROM_UNIXTIME(1704067200)` -> `'2024-01-01'` |
| `TIME_BUCKET(interval, d)` | 按时间桶截断 | `TIME_BUCKET(3600, '2024-01-01 12:34:56')` -> `'2024-01-01 12:00:00'` |

#### 高级日期函数

`CENTURY`、`DECADE`、`MILLENNIUM`、`ERA`、`ISODOW`、`ISOYEAR`、`JULIAN_DAY`、`YEAR_WEEK` 等函数提供了更丰富的日期分析能力。

### 2.4 控制流函数（control）

控制流函数注册于 `pkg/builtin/control_functions.go`。

| 函数 | 说明 | 示例 |
|------|------|------|
| `COALESCE(v1, v2, ...)` | 返回第一个非 NULL 值 | `COALESCE(NULL, NULL, 'hello')` -> `'hello'` |
| `NULLIF(a, b)` | 相等返回 NULL | `NULLIF(1, 1)` -> `NULL` |
| `IFNULL(a, default)` / `NVL` | NULL 时返回默认值 | `IFNULL(NULL, 'default')` -> `'default'` |
| `IF(cond, then, else)` / `IIF` | 条件表达式 | `IF(1 > 0, 'yes', 'no')` -> `'yes'` |
| `GREATEST(v1, v2, ...)` | 返回最大值 | `GREATEST(1, 5, 3)` -> `5` |
| `LEAST(v1, v2, ...)` | 返回最小值 | `LEAST(1, 5, 3)` -> `1` |

### 2.5 编码函数（encoding）

编码函数注册于 `pkg/builtin/encoding_functions.go`。

| 函数 | 说明 | 示例 |
|------|------|------|
| `HEX(v)` | 十六进制编码 | `HEX('hello')` -> `'68656C6C6F'` |
| `UNHEX(s)` | 十六进制解码 | `UNHEX('68656C6C6F')` -> `'hello'` |
| `TO_BASE64(v)` / `BASE64` | Base64 编码 | `TO_BASE64('hello')` -> `'aGVsbG8='` |
| `FROM_BASE64(s)` | Base64 解码 | `FROM_BASE64('aGVsbG8=')` -> `'hello'` |
| `BIN(n)` | 整数转二进制串 | `BIN(10)` -> `'1010'` |
| `MD5(s)` | MD5 哈希 | `MD5('hello')` -> `'5d41402abc4b2a76...'` |
| `SHA1(s)` | SHA1 哈希 | `SHA1('hello')` -> `'aaf4c61ddcc5e8a2...'` |
| `SHA2(s, bits)` | SHA-256/512 哈希 | `SHA2('hello', 256)` -> `'2cf24dba...'` |
| `HASH(v)` | FNV-64a 哈希 | `HASH('hello')` -> `11831194018420276491` |
| `ENCODE(s, charset)` | 通用编码 | `ENCODE('hello', 'hex')` -> `'68656c6c6f'` |
| `DECODE(s, charset)` | 通用解码 | `DECODE('68656c6c6f', 'hex')` -> `'hello'` |

### 2.6 系统函数（system）

系统函数注册于 `pkg/builtin/system_functions.go`。

| 函数 | 说明 | 示例 |
|------|------|------|
| `TYPEOF(v)` | 返回 SQL 类型名称 | `TYPEOF(42)` -> `'INTEGER'` |
| `VERSION()` | 返回数据库版本 | `VERSION()` -> `'SQLExec 1.0.0'` |
| `CURRENT_DATABASE()` / `CURRENT_SCHEMA()` | 当前数据库名 | `CURRENT_DATABASE()` -> `'default'` |
| `UUID()` / `GEN_RANDOM_UUID()` | 生成 UUID v4 | `UUID()` -> `'550e8400-...'` |
| `SETSEED(n)` | 设置随机种子 | `SETSEED(0.5)` |
| `SLEEP(n)` | 暂停执行（秒，上限 300） | `SLEEP(1)` -> `0` |

### 2.7 金融函数（financial）

金融函数注册于 `pkg/builtin/financial_functions.go`，提供专业的金融计算能力。

#### 货币时间价值

| 函数 | 说明 | 示例 |
|------|------|------|
| `FV(rate, nper, pmt, [pv], [type])` | 未来值 | `FV(0.05, 10, -100)` -> `1257.79` |
| `PV(rate, nper, pmt, [fv], [type])` | 现值 | `PV(0.05, 10, -100)` -> `772.17` |
| `PMT(rate, nper, pv, [fv], [type])` | 每期还款额 | `PMT(0.05, 10, -1000)` -> `129.50` |
| `IPMT(rate, per, nper, pv, ...)` | 某期利息部分 | `IPMT(0.05, 1, 10, -1000)` -> `50.00` |
| `PPMT(rate, per, nper, pv, ...)` | 某期本金部分 | `PPMT(0.05, 1, 10, -1000)` -> `79.50` |
| `NPV(rate, v1, v2, ...)` | 净现值 | `NPV(0.1, -1000, 300, 400, 500)` -> `48.42` |
| `IRR(cashflows, [guess])` | 内部收益率 | `IRR('-1000,300,400,500,200')` |
| `XIRR(cashflows, dates, [guess])` | 非等间隔 IRR | `XIRR('-1000,500,600', '2024-01-01,...')` |
| `RATE(nper, pmt, pv, ...)` | 每期利率 | `RATE(10, -100, 800)` |
| `NPER(rate, pmt, pv, ...)` | 期数 | `NPER(0.05, -100, 800)` |

IRR 和 XIRR 使用牛顿迭代法求解，最多迭代 100 次，收敛精度为 1e-7。

#### 折旧

| 函数 | 说明 |
|------|------|
| `SLN(cost, salvage, life)` | 直线折旧 |
| `SYD(cost, salvage, life, per)` | 年数总和折旧 |
| `DDB(cost, salvage, life, per, [factor])` | 双倍余额递减折旧 |

#### 债券与收益

| 函数 | 说明 |
|------|------|
| `BOND_PRICE(face, coupon, ytm, years, freq)` | 债券价格 |
| `BOND_YIELD(face, coupon, price, years, freq)` | 到期收益率 |
| `BOND_DURATION(...)` | Macaulay 久期 |
| `BOND_MDURATION(...)` | 修正久期 |
| `BOND_CONVEXITY(...)` | 凸度 |

#### 基础金融计算

| 函数 | 说明 |
|------|------|
| `COMPOUND_INTEREST(P, r, n, t)` | 复利 A=P(1+r/n)^(nt) |
| `SIMPLE_INTEREST(P, r, t)` | 单利 I=P*r*t |
| `CAGR(begin, end, periods)` | 年均复合增长率 |
| `ROI(gain, cost)` | 投资回报率 (%) |
| `ROUND_BANKER(v, d)` | 银行家舍入 |
| `ROUND_CURRENCY(v, d)` | 货币舍入 |
| `ROUND_TRUNCATE(v, d)` | 截断舍入 |

### 2.8 位运算函数（bitwise）

| 函数 | 说明 | 示例 |
|------|------|------|
| `BIT_COUNT(n)` | 统计置位位数 | `BIT_COUNT(7)` -> `3` |
| `GET_BIT(n, pos)` | 获取指定位 | `GET_BIT(5, 0)` -> `1` |
| `SET_BIT(n, pos, val)` | 设置指定位 | `SET_BIT(5, 1, 1)` -> `7` |
| `BIT_LENGTH(s)` | 字符串位长度 | `BIT_LENGTH('hello')` -> `40` |

### 2.9 相似度函数（similarity）

| 函数 | 说明 | 示例 |
|------|------|------|
| `LEVENSHTEIN(s1, s2)` | 编辑距离 | `LEVENSHTEIN('kitten', 'sitting')` -> `3` |
| `DAMERAU_LEVENSHTEIN(s1, s2)` | 含转位的编辑距离 | `DAMERAU_LEVENSHTEIN('ca', 'abc')` -> `2` |
| `HAMMING(s1, s2)` | 汉明距离 | `HAMMING('karolin', 'kathrin')` -> `3` |
| `JACCARD(s1, s2)` | Jaccard 相似系数 | `JACCARD('night', 'nacht')` -> `0.25` |
| `JARO_SIMILARITY(s1, s2)` | Jaro 相似度 | |

### 2.10 向量函数（vector）

向量函数注册于 `pkg/builtin/vector_functions.go`，支持向量检索场景。

| 函数 | 说明 |
|------|------|
| `VEC_COSINE_DISTANCE(v1, v2)` | 余弦距离 |
| `VEC_L2_DISTANCE(v1, v2)` | 欧几里得距离 |
| `VEC_INNER_PRODUCT(v1, v2)` | 内积 |

### 2.11 ICU 国际化函数（icu）

| 函数 | 说明 |
|------|------|
| `ICU_SORT_KEY(s, collation)` | 生成排序键 |
| `COLLATION()` | 返回默认排序规则 |
| `UNICODE_NORMALIZE(s, [form])` | Unicode 规范化 (NFC/NFD/NFKC/NFKD) |
| `STRIP_ACCENTS(s)` | 去除变音符号 |
| `ICU_COMPARE(s1, s2, collation)` | 按排序规则比较 |

---

## 3. 函数注册机制

### 3.1 自动注册（init 机制）

SQLExec 利用 Go 语言的 `init()` 函数机制实现内建函数的自动注册。每个函数类别文件都在 `init()` 中定义函数列表并调用 `RegisterGlobal()`：

```go
// pkg/builtin/string_functions.go
func init() {
    stringFunctions := []*FunctionInfo{
        {
            Name:        "concat",
            Type:        FunctionTypeScalar,
            Signatures:  []FunctionSignature{
                {Name: "concat", ReturnType: "string", ParamTypes: []string{"string"}, Variadic: true},
            },
            Handler:     stringConcat,
            Description: "连接字符串",
            Example:     "CONCAT('Hello', ' ', 'World') -> 'Hello World'",
            Category:    "string",
        },
        // ... 更多函数
    }

    for _, fn := range stringFunctions {
        RegisterGlobal(fn)
    }
}
```

`RegisterGlobal` 将 `FunctionInfo` 注册到全局注册表：

```go
func RegisterGlobal(info *FunctionInfo) error {
    return globalRegistry.Register(info)
}
```

注册时会进行基本校验：函数名不能为空、处理函数不能为 nil。注册表使用读写锁保证并发安全。

### 3.2 扩展注册表 API

系统还提供了 `FunctionAPI` 作为高级注册接口（定义于 `pkg/builtin/api.go`），支持函数选项模式（Functional Options）：

```go
api := builtin.NewFunctionAPI()

// 注册标量函数
api.RegisterScalarFunction(
    "my_func",          // 函数名
    "MY_FUNC",          // 显示名
    "自定义函数描述",      // 描述
    myHandler,          // 处理函数
    WithCategory(CategoryMath),      // 类别
    WithReturnType("number"),        // 返回类型
    WithArgRange(1, 3),              // 参数范围
    WithVariadic(),                  // 可变参数
    WithParameter("x", "number", "输入值", true),  // 参数定义
    WithExample("MY_FUNC(42) -> 42"),              // 示例
)
```

可用的函数选项包括：

| 选项 | 说明 |
|------|------|
| `WithCategory(cat)` | 设置函数类别 |
| `WithVariadic()` | 标记为可变参数 |
| `WithMinArgs(n)` / `WithMaxArgs(n)` | 参数数量限制 |
| `WithArgRange(min, max)` | 参数范围（-1 表示无上限） |
| `WithReturnType(t)` | 返回类型 |
| `WithParameter(name, type, desc, required)` | 添加参数定义 |
| `WithExample(ex)` | 添加使用示例 |
| `WithTag(tag)` / `WithTags(tags)` | 添加标签 |

### 3.3 函数构建器模式

`FunctionBuilder`（定义于 `pkg/builtin/builder.go`）提供了链式调用风格的函数构建方式：

```go
err := builtin.MathFunctionBuilder("complex", "Complex", "复杂计算函数").
    WithParameter("x", "number", "X坐标", true).
    WithParameter("y", "number", "Y坐标", true).
    WithExample("SELECT complex(1, 2) FROM table").
    WithHandler(func(args []interface{}) (interface{}, error) {
        x, _ := toFloat64(args[0])
        y, _ := toFloat64(args[1])
        return x + y, nil
    }).
    Register(api)
```

预定义的构建器快捷方法：

```go
MathFunctionBuilder(name, display, desc)       // 数学函数构建器
StringFunctionBuilder(name, display, desc)     // 字符串函数构建器
DateFunctionBuilder(name, display, desc, ret)  // 日期函数构建器
AggregateFunctionBuilder(name, display, desc, ret) // 聚合函数构建器
```

### 3.4 函数别名

系统支持为已有函数创建别名：

```go
api.AddFunctionAlias("substr", "substring")  // SUBSTR 作为 SUBSTRING 的别名
```

查找函数时，若主名称未命中，会自动查找别名映射。很多内建函数直接在注册时提供了多个函数名指向同一个 Handler，例如 `UPPER`/`UCASE`、`LOWER`/`LCASE`、`SUBSTRING`/`SUBSTR` 等。

### 3.5 函数名标准化

所有函数名在注册和查询时都会被标准化为小写，因此函数调用不区分大小写：

```go
func normalizeName(name string) string {
    return toLowerCase(name)
}
```

---

## 4. UDF 支持

### 4.1 概述

用户自定义函数（UDF）系统定义于 `pkg/builtin/udf.go`，允许用户在运行时动态创建和注册自定义函数。UDF 支持两种语言：SQL 表达式和 Go 表达式。

### 4.2 UDF 元数据

每个 UDF 通过 `UDFMetadata` 描述：

```go
type UDFMetadata struct {
    Name        string         // 函数名称
    Parameters  []UDFParameter // 参数列表
    ReturnType  string         // 返回类型
    Body        string         // 函数体（SQL 表达式或计算逻辑）
    Determinism bool           // 是否确定性（相同输入总是产生相同输出）
    Description string         // 函数描述
    CreatedAt   time.Time      // 创建时间
    ModifiedAt  time.Time      // 修改时间
    Author      string         // 创建者
    Language    string         // 语言（SQL, GO）
}
```

UDF 参数定义：

```go
type UDFParameter struct {
    Name     string      // 参数名称
    Type     string      // 参数类型
    Optional bool        // 是否可选
    Required bool        // 是否必需
    Default  interface{} // 默认值
}
```

### 4.3 UDF 管理器

全局 UDF 管理器通过 `sync.Once` 实现单例模式：

```go
var (
    globalUDFManager *UDFManager
    udfOnce          sync.Once
)

func GetGlobalUDFManager() *UDFManager {
    udfOnce.Do(func() {
        globalUDFManager = NewUDFManager()
    })
    return globalUDFManager
}
```

UDF 管理器提供完整的 CRUD 操作：

- `Register(udf)` -- 注册 UDF（自动编译函数体）
- `Unregister(name)` -- 注销 UDF
- `Get(name)` -- 获取 UDF
- `List()` -- 列出所有 UDF
- `Exists(name)` -- 检查是否存在
- `Clear()` -- 清除所有 UDF

### 4.4 UDF 编译过程

注册 UDF 时，系统会自动编译函数体为可执行的 Handler。编译过程根据语言和表达式类型分发：

```
UDF 函数体
    │
    ├── Language = "SQL"
    │   ├── 简单表达式（参数引用）   -> evaluateSimpleExpression
    │   ├── 函数调用表达式           -> compileFunctionCall（调用内建函数）
    │   ├── 算术表达式               -> evaluateArithmeticExpression
    │   └── 复杂表达式               -> compileTemplateExpression（模板引擎）
    │
    └── Language = "GO"
        └── 算术表达式               -> evaluateArithmeticExpression
```

**简单表达式**：直接引用参数，如 `@param` 或 `:param`。

**函数调用**：可在 UDF 函数体中调用内建函数，如 `UPPER(@name)`。编译后的 Handler 会在运行时查找全局注册表中的内建函数并调用。

**算术表达式**：支持 `+`、`-`、`*`、`/` 四则运算，参数通过 `@param` 引用。

**模板表达式**：使用 Go 的 `text/template` 引擎处理复杂表达式。

### 4.5 UDF 构建器

`UDFBuilder` 提供链式调用创建 UDF：

```go
udf := builtin.NewUDFBuilder("add_tax").
    WithParameter("price", "number", false).
    WithParameter("rate", "number", true).   // 可选参数
    WithReturnType("number").
    WithBody("@price * (1 + @rate)").
    WithLanguage("SQL").
    WithDescription("计算含税价格").
    WithDeterminism(true).
    WithAuthor("admin").
    Build()

api.RegisterUDF(udf)
```

### 4.6 UDF 与全局注册表的集成

通过 `FunctionAPI.RegisterUDF()` 注册的 UDF 不仅存储在 UDF 管理器中，还会同时注册到函数注册表中，使得 UDF 可以在 SQL 查询中被正常调用：

```go
func (api *FunctionAPI) RegisterUDF(udf *UDFFunction) error {
    // 1. 注册到 UDF 管理器
    manager := GetGlobalUDFManager()
    manager.Register(udf)

    // 2. 包装 Handler 并注册到函数注册表
    wrappedHandler := func(args []any) (any, error) {
        return udf.Handler(args)
    }
    return api.RegisterScalarFunction(
        udf.Metadata.Name, udf.Metadata.Name, udf.Metadata.Description,
        wrappedHandler,
        WithCategory(CategoryUser),
        WithReturnType(udf.Metadata.ReturnType),
        WithTags([]string{"udf"}),
    )
}
```

---

## 5. 聚合函数

### 5.1 聚合函数架构

聚合函数与标量函数的关键区别在于其**有状态的处理模型**。聚合函数需要跨多行累积数据，最终产出单一结果。

系统定义了两个核心函数类型：

```go
// 累积函数：对每一行调用，更新上下文状态
type AggregateHandle func(ctx *AggregateContext, args []interface{}) error

// 结果函数：所有行处理完毕后调用，返回最终结果
type AggregateResult func(ctx *AggregateContext) (interface{}, error)
```

### 5.2 聚合上下文

`AggregateContext` 是聚合函数的状态容器，携带了各类聚合操作所需的中间状态：

```go
type AggregateContext struct {
    Count       int64           // 计数器
    Sum         float64         // 求和
    Min         interface{}     // 最小值
    Max         interface{}     // 最大值
    AvgSum      float64         // 平均值累积
    Values      []float64       // 数值列表（标准差、中位数等）
    Strings     []string        // 字符串列表（GROUP_CONCAT）
    AllValues   []interface{}   // 通用值列表（ARRAY_AGG、MODE）
    BoolAnd     *bool           // 逻辑与（BOOL_AND）
    BoolOr      *bool           // 逻辑或（BOOL_OR）
    Separator   string          // 分隔符（GROUP_CONCAT，默认 ","）
    ProductVal  float64         // 乘积（PRODUCT，初始 1.0）
    ProductInit bool            // 乘积是否已初始化
}
```

### 5.3 聚合函数生命周期

```
    ┌──────────────────┐
    │ NewAggregateCtx  │  1. 初始化上下文
    └────────┬─────────┘
             │
             ▼
    ┌──────────────────┐
    │  AggregateHandle │  2. 对每一行调用累积函数
    │  (逐行累积)       │     ctx 中的状态持续更新
    └────────┬─────────┘
             │  重复 N 次
             ▼
    ┌──────────────────┐
    │ AggregateResult  │  3. 所有行处理完毕，计算最终结果
    │  (产出结果)       │
    └──────────────────┘
```

### 5.4 基础聚合函数

注册于 `pkg/builtin/aggregate_functions.go`：

| 函数 | 说明 | 示例 |
|------|------|------|
| `COUNT(*)` / `COUNT(col)` | 计数（NULL 值被忽略） | `COUNT(*)` -> `100` |
| `SUM(col)` | 求和 | `SUM(price)` -> `1000.50` |
| `AVG(col)` | 平均值 | `AVG(price)` -> `100.05` |
| `MIN(col)` | 最小值 | `MIN(price)` -> `10.00` |
| `MAX(col)` | 最大值 | `MAX(price)` -> `1000.00` |
| `STDDEV(col)` | 总体标准差 | `STDDEV(price)` -> `50.25` |
| `VARIANCE(col)` | 总体方差 | `VARIANCE(price)` -> `2525.06` |
| `STDDEV_POP(col)` | 总体标准差 | 除以 N |
| `STDDEV_SAMP(col)` | 样本标准差 | 除以 N-1 |
| `VAR_POP(col)` | 总体方差 | 除以 N |
| `VAR_SAMP(col)` | 样本方差 | 除以 N-1 |
| `GROUP_CONCAT(col, [sep])` | 字符串连接 | `GROUP_CONCAT(name)` -> `'a,b,c'` |
| `STRING_AGG` / `LISTAGG` | GROUP_CONCAT 别名 | |
| `COUNT_IF(cond)` | 条件计数 | `COUNT_IF(is_active)` -> `5` |
| `BOOL_AND(col)` / `EVERY` | 逻辑与 | `BOOL_AND(is_active)` -> `true` |
| `BOOL_OR(col)` | 逻辑或 | `BOOL_OR(is_active)` -> `true` |
| `MEDIAN(col)` | 中位数 | `MEDIAN(price)` -> `50.00` |
| `PERCENTILE_CONT(p, col)` | 连续百分位（线性插值） | `PERCENTILE_CONT(0.5, price)` |
| `PERCENTILE_DISC(p, col)` | 离散百分位（最近值） | `PERCENTILE_DISC(0.5, price)` |
| `ARRAY_AGG(col)` / `LIST` | 值收集为数组 | `ARRAY_AGG(name)` -> `['a','b','c']` |
| `PRODUCT(col)` | 乘积 | `PRODUCT(quantity)` -> `120` |

### 5.5 高级聚合函数

注册于 `pkg/builtin/advanced_aggregate_functions.go`，使用 `advancedAggState` 扩展上下文：

```go
type advancedAggState struct {
    pairsX   []float64          // X 值对（相关性、协方差）
    pairsY   []float64          // Y 值对
    values   []float64          // 数值列表（偏度、峰度）
    freqMap  map[string]int     // 频率映射（众数、信息熵）
    distinct map[string]struct{} // 去重集合（近似去重计数）
}
```

| 函数 | 说明 | 示例 |
|------|------|------|
| `CORR(x, y)` | 皮尔逊相关系数 | `CORR(x, y)` -> `0.98` |
| `COVAR_POP(x, y)` | 总体协方差 | `COVAR_POP(x, y)` -> `2.5` |
| `COVAR_SAMP(x, y)` | 样本协方差 | `COVAR_SAMP(x, y)` -> `3.125` |
| `SKEWNESS(x)` | 偏度（三阶中心矩 / sigma^3） | `SKEWNESS(x)` -> `0.0` |
| `KURTOSIS(x)` | 超额峰度（四阶中心矩 / sigma^4 - 3） | `KURTOSIS(x)` -> `-1.2` |
| `MODE(x)` | 众数（最高频值） | `MODE(x)` -> `'apple'` |
| `ENTROPY(x)` | 信息熵 -sum(p*log2(p)) | `ENTROPY(x)` -> `1.585` |
| `APPROX_COUNT_DISTINCT(x)` | 近似去重计数 | `APPROX_COUNT_DISTINCT(x)` -> `42` |

### 5.6 聚合函数注册流程

聚合函数使用独立的注册表 `aggregateRegistry`，通过 `RegisterAggregate` 函数注册：

```go
var (
    aggregateRegistry   = make(map[string]*AggregateFunctionInfo)
    aggregateRegistryMu sync.RWMutex
)

func RegisterAggregate(info *AggregateFunctionInfo) {
    aggregateRegistryMu.Lock()
    aggregateRegistry[info.Name] = info
    aggregateRegistryMu.Unlock()
}
```

聚合函数在 `InitAggregateFunctions()` 中统一初始化，由 `InitBuiltinFunctions()` 调用。高级聚合函数则在各自文件的 `init()` 中直接注册。

### 5.7 聚合函数实现示例：AVG

```go
// 累积阶段：对每一行累加值和计数
func aggAvg(ctx *AggregateContext, args []interface{}) error {
    for _, arg := range args {
        if arg != nil {
            val, err := utils.ToFloat64(arg)
            if err != nil {
                return err
            }
            ctx.AvgSum += val
            ctx.Count++
        }
    }
    return nil
}

// 结果阶段：计算平均值
func aggAvgResult(ctx *AggregateContext) (interface{}, error) {
    if ctx.Count == 0 {
        return nil, nil  // 无数据时返回 NULL
    }
    return ctx.AvgSum / float64(ctx.Count), nil
}
```

### 5.8 聚合函数实现示例：PERCENTILE_CONT

百分位数函数展示了更复杂的累积与计算逻辑：

```go
// 累积阶段：第一个参数是百分位数 (0..1)，第二个是值
func aggPercentileCont(ctx *AggregateContext, args []interface{}) error {
    // 首次调用时存储百分位参数到 AvgSum
    if args[0] != nil && ctx.Count == 0 {
        p, _ := utils.ToFloat64(args[0])
        ctx.AvgSum = p
    }
    if args[1] != nil {
        val, _ := utils.ToFloat64(args[1])
        ctx.Values = append(ctx.Values, val)
    }
    ctx.Count++
    return nil
}

// 结果阶段：排序后线性插值
func aggPercentileContResult(ctx *AggregateContext) (interface{}, error) {
    sorted := make([]float64, len(ctx.Values))
    copy(sorted, ctx.Values)
    sort.Float64s(sorted)

    p := ctx.AvgSum  // 百分位数
    pos := p * float64(n-1)
    lower := int(math.Floor(pos))
    upper := lower + 1
    frac := pos - float64(lower)
    return sorted[lower] + frac*(sorted[upper]-sorted[lower]), nil
}
```

---

## 6. JSON 函数

### 6.1 JSON 类型系统

JSON 函数依赖 `pkg/json` 包，该包实现了完整的 JSON 二进制表示 `BinaryJSON`：

```go
type BinaryJSON struct {
    TypeCode TypeCode       // 类型代码
    Value    interface{}    // 实际值
}

type TypeCode byte

const (
    TypeLiteral TypeCode = iota  // null, true, false
    TypeObject                    // JSON 对象
    TypeArray                     // JSON 数组
    TypeString                    // JSON 字符串
    TypeInteger                   // JSON 整数
    TypeDouble                    // JSON 浮点数
    TypeOpaque                    // 不透明类型
)
```

`BinaryJSON` 提供了丰富的类型检查和值提取方法：`IsNull()`、`IsObject()`、`IsArray()`、`IsString()`、`IsNumber()`、`GetString()`、`GetFloat64()`、`GetObject()`、`GetArray()` 等。

### 6.2 JSON Path 表达式

JSON Path 系统（`pkg/json/path.go`）支持 MySQL 兼容的 `$` 路径语法：

```
$                  -- 根节点
$.key              -- 对象键访问
$[index]           -- 数组索引访问
$.*                -- 对象通配符（所有值）
$[*]               -- 数组通配符（所有元素）
$[last]            -- 数组最后一个元素
$[0 to 3]          -- 数组范围访问
$.key1.key2[0]     -- 链式路径
```

路径解析器将路径表达式解析为 `PathLeg` 序列：

```go
type Path struct {
    Legs []PathLeg
}

type PathLeg interface {
    Apply(bj BinaryJSON) ([]BinaryJSON, error)
    String() string
}
```

三种路径节点类型：

- `KeyLeg` -- 对象键访问，支持通配符 `*`
- `ArrayLeg` -- 数组索引访问，支持 `last`、负索引、通配符
- `RangeLeg` -- 数组范围访问 `[start to end]`

路径求值过程为逐级展开：从根节点开始，依次应用每个 `PathLeg`，每一步可能产生多个结果（通配符情况下），最终结果集即为所有匹配值。

### 6.3 JSON 函数清单

JSON 函数注册于 `pkg/builtin/json_functions.go`，均以 `json_` 为前缀。

#### 类型与验证

| 函数 | 说明 | 示例 |
|------|------|------|
| `JSON_TYPE(v)` | 返回 JSON 类型名 | `JSON_TYPE('{"a": 1}')` -> `'OBJECT'` |
| `JSON_VALID(v)` | 验证是否为有效 JSON | `JSON_VALID('{"a": 1}')` -> `1` |

#### 构造

| 函数 | 说明 | 示例 |
|------|------|------|
| `JSON_ARRAY(v1, v2, ...)` | 创建 JSON 数组 | `JSON_ARRAY(1, 2, 3)` -> `[1, 2, 3]` |
| `JSON_OBJECT(k1, v1, ...)` | 创建 JSON 对象（键值对） | `JSON_OBJECT('key', 'val')` -> `{"key": "val"}` |

#### 提取

| 函数 | 说明 | 示例 |
|------|------|------|
| `JSON_EXTRACT(doc, path, ...)` | 按路径提取值 | `JSON_EXTRACT('{"a": 1}', '$.a')` -> `1` |
| `JSON_KEYS(doc)` | 返回对象所有键 | `JSON_KEYS('{"a":1,"b":2}')` -> `["a","b"]` |
| `JSON_LENGTH(doc)` | 数组长度或对象键数 | `JSON_LENGTH('[1,2,3]')` -> `3` |
| `JSON_DEPTH(doc)` | 最大嵌套深度 | `JSON_DEPTH('{"a":{"b":1}}')` -> `2` |

`JSON_EXTRACT` 的实现流程：

1. 将输入解析为 `BinaryJSON`（字符串通过 `ParseJSON`，其他类型通过 `NewBinaryJSON`）
2. 解析 `$` 路径表达式
3. 在 `BinaryJSON` 上执行路径求值
4. 将结果序列化为 JSON 字符串返回

支持多路径提取：传入多个路径时返回 JSON 数组。

#### 搜索与判断

| 函数 | 说明 | 示例 |
|------|------|------|
| `JSON_CONTAINS(target, candidate)` | 是否包含目标值 | `JSON_CONTAINS('{"a":1}', 1)` -> `1` |
| `JSON_CONTAINS_PATH(doc, mode, path...)` | 路径是否存在 | `JSON_CONTAINS_PATH('{"a":1}', 'one', '$.a')` -> `1` |
| `JSON_MEMBER_OF(val, arr)` | 值是否为数组成员 | `JSON_MEMBER_OF(1, '[1,2,3]')` -> `1` |
| `JSON_OVERLAPS(arr1, arr2)` | 两数组是否有交集 | `JSON_OVERLAPS('[1,2]', '[2,3]')` -> `1` |
| `JSON_SEARCH(doc, mode, val)` | 在 JSON 中搜索值 | `JSON_SEARCH('{"a":"hello"}', 'one', 'hello')` |

`JSON_CONTAINS` 的检查逻辑：
1. 完全相等比较（使用深度比较 `deepEqual`）
2. 数组场景：检查 candidate 是否为数组中的某个元素
3. 对象场景：检查 candidate 是否为 target 的子集

`JSON_CONTAINS_PATH` 支持两种模式：
- `'one'` -- 至少一个路径存在即返回 true
- `'all'` -- 所有路径都必须存在

#### 修改

| 函数 | 说明 | 示例 |
|------|------|------|
| `JSON_SET(doc, path, val, ...)` | 设置值（插入或替换） | `JSON_SET('{"a":1}', '$.a', 2)` -> `{"a":2}` |
| `JSON_INSERT(doc, path, val, ...)` | 仅插入新值 | `JSON_INSERT('{"a":1}', '$.b', 2)` -> `{"a":1,"b":2}` |
| `JSON_REPLACE(doc, path, val, ...)` | 仅替换已存在值 | `JSON_REPLACE('{"a":1}', '$.a', 2)` -> `{"a":2}` |
| `JSON_REMOVE(doc, path, ...)` | 删除指定路径 | `JSON_REMOVE('{"a":1,"b":2}', '$.b')` -> `{"a":1}` |

三种修改语义的区别：
- `JSON_SET` -- 路径存在则替换，不存在则插入
- `JSON_INSERT` -- 路径不存在时才插入，已存在则跳过
- `JSON_REPLACE` -- 路径存在时才替换，不存在则跳过

所有修改函数支持多个 `(path, value)` 对，按序依次处理。

#### 合并

| 函数 | 说明 | 示例 |
|------|------|------|
| `JSON_MERGE_PRESERVE(d1, d2, ...)` | 保留所有值的合并 | `JSON_MERGE_PRESERVE('{"a":1}', '{"b":2}')` -> `{"a":1,"b":2}` |
| `JSON_MERGE_PATCH(d1, d2, ...)` | RFC 7396 合并（后者覆盖） | `JSON_MERGE_PATCH('{"a":1}', '{"a":2,"b":3}')` -> `{"a":2,"b":3}` |

#### 数组操作

| 函数 | 说明 | 示例 |
|------|------|------|
| `JSON_ARRAY_APPEND(doc, ...)` | 追加值到数组末尾 | `JSON_ARRAY_APPEND('[1,2]', '$', 3)` -> `[1,2,3]` |
| `JSON_ARRAY_INSERT(doc, path, idx, val)` | 在指定位置插入 | `JSON_ARRAY_INSERT('[1,3]', '$', 1, 2)` -> `[1,2,3]` |

#### 格式化与工具

| 函数 | 说明 | 示例 |
|------|------|------|
| `JSON_PRETTY(doc)` | 格式化输出 | `JSON_PRETTY('{"a":1}')` -> 缩进格式 |
| `JSON_QUOTE(s)` | 字符串转 JSON 字符串 | `JSON_QUOTE('hello')` -> `'"hello"'` |
| `JSON_UNQUOTE(s)` | 取消 JSON 字符串引号 | `JSON_UNQUOTE('"hello"')` -> `'hello'` |
| `JSON_STORAGE_SIZE(doc)` | 存储大小（字节） | `JSON_STORAGE_SIZE('{"a":1}')` -> `9` |

---

## 7. 插件扩展体系

### 7.1 插件架构

SQLExec 通过 `pkg/extensibility/plugin.go` 提供了完整的插件扩展体系，支持三种类型的插件：

```go
type Plugin interface {
    Name() string
    Version() string
    Initialize(config map[string]interface{}) error
    Start() error
    Stop() error
    IsRunning() bool
}

type FunctionPlugin interface {
    Plugin
    Register(name string, fn interface{}) error
    Unregister(name string) error
    Call(name string, args []interface{}) (interface{}, error)
    GetFunction(name string) (interface{}, error)
    ListFunctions() []string
}
```

### 7.2 函数插件

`FunctionPlugin` 接口允许通过外部插件注册自定义函数。插件管理器 `PluginManager` 负责插件的生命周期管理（注册、启动、停止、注销），使用读写锁保证并发安全。

系统还提供了 `BasePlugin` 作为插件基类，实现了通用的初始化、启动、停止逻辑，便于开发者快速创建自定义插件。

### 7.3 扩展点

除了函数插件，系统还支持：
- `DataSourcePlugin` -- 数据源插件（外部数据源对接）
- `MonitorPlugin` -- 监控插件（指标收集与事件记录）

---

## 附录：类型转换辅助函数

函数系统内部依赖 `pkg/utils` 包提供的类型转换函数：

| 函数 | 说明 |
|------|------|
| `utils.ToFloat64(v)` | 将 `interface{}` 转换为 `float64` |
| `utils.ToInt64(v)` | 将 `interface{}` 转换为 `int64` |
| `utils.ToString(v)` | 将 `interface{}` 转换为 `string` |
| `utils.CompareValuesForSort(a, b)` | 通用值比较（用于 MIN/MAX/GREATEST/LEAST） |

这些转换函数支持各种数值类型、字符串到数字的自动转换，是函数系统类型灵活性的基础。
