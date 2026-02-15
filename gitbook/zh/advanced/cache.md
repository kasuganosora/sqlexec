# 查询缓存

SQLExec 提供三层缓存机制，加速重复查询。

## 缓存层次

| 缓存层 | 缓存内容 | 默认 TTL | 默认大小 |
|--------|---------|----------|---------|
| **查询计划缓存** | 解析和优化后的执行计划 | 5 分钟 | 1000 条 |
| **结果缓存** | 查询结果数据 | 10 分钟 | 1000 条 |
| **Schema 缓存** | 表结构信息 | 1 小时 | 100 条 |

## 配置

### 服务器模式

```json
{
  "cache": {
    "query_cache": {
      "max_size": 1000,
      "ttl": "5m"
    },
    "result_cache": {
      "max_size": 1000,
      "ttl": "10m"
    },
    "schema_cache": {
      "max_size": 100,
      "ttl": "1h"
    }
  }
}
```

### 嵌入式模式

```go
db, _ := api.NewDB(&api.DBConfig{
    CacheEnabled: true,
    CacheSize:    1000,
    CacheTTL:     300,  // 秒
})
```

## 缓存统计

```go
stats := db.GetCacheStats()
fmt.Printf("命中: %d\n", stats.HitCount)
fmt.Printf("未命中: %d\n", stats.MissCount)
fmt.Printf("命中率: %.2f%%\n", stats.HitRate*100)
fmt.Printf("缓存条目: %d\n", stats.EntryCount)
```

## 手动清除

```go
// 清除所有缓存
db.ClearCache()

// 清除特定表的缓存
db.ClearTableCache("users")
```

## 自动失效

缓存在以下情况自动失效：

| 操作 | 影响的缓存 |
|------|-----------|
| INSERT / UPDATE / DELETE | 相关表的结果缓存 |
| CREATE TABLE / ALTER TABLE | Schema 缓存 + 相关结果缓存 |
| DROP TABLE / TRUNCATE | Schema 缓存 + 相关结果缓存 |
| TTL 过期 | 对应缓存条目 |

## 禁用缓存

### 全局禁用

```go
db, _ := api.NewDB(&api.DBConfig{
    CacheEnabled: false,
})
```

### 会话级禁用

```go
session := db.SessionWithOptions(&api.SessionOptions{
    CacheEnabled: false,
})
```
