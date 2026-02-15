# Query Cache

SQLExec provides a three-layer caching mechanism to accelerate repeated queries.

## Cache Layers

| Cache Layer | Cached Content | Default TTL | Default Size |
|--------|---------|----------|---------|
| **Query Plan Cache** | Parsed and optimized execution plans | 5 minutes | 1000 entries |
| **Result Cache** | Query result data | 10 minutes | 1000 entries |
| **Schema Cache** | Table structure information | 1 hour | 100 entries |

## Configuration

### Server Mode

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

### Embedded Mode

```go
db, _ := api.NewDB(&api.DBConfig{
    CacheEnabled: true,
    CacheSize:    1000,
    CacheTTL:     300,  // seconds
})
```

## Cache Statistics

```go
stats := db.GetCacheStats()
fmt.Printf("Hits: %d\n", stats.HitCount)
fmt.Printf("Misses: %d\n", stats.MissCount)
fmt.Printf("Hit rate: %.2f%%\n", stats.HitRate*100)
fmt.Printf("Cache entries: %d\n", stats.EntryCount)
```

## Manual Clearing

```go
// Clear all caches
db.ClearCache()

// Clear cache for a specific table
db.ClearTableCache("users")
```

## Automatic Invalidation

Caches are automatically invalidated in the following cases:

| Operation | Affected Cache |
|------|-----------|
| INSERT / UPDATE / DELETE | Result cache for the related table |
| CREATE TABLE / ALTER TABLE | Schema cache + related result cache |
| DROP TABLE / TRUNCATE | Schema cache + related result cache |
| TTL expiration | The corresponding cache entry |

## Disabling the Cache

### Global Disable

```go
db, _ := api.NewDB(&api.DBConfig{
    CacheEnabled: false,
})
```

### Session-Level Disable

```go
session := db.SessionWithOptions(&api.SessionOptions{
    CacheEnabled: false,
})
```
