package http

import (
	"context"
	"encoding/json"
	"fmt"
	gohttp "net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestServer 创建一个模拟 HTTP 数据源服务
func newTestServer() *httptest.Server {
	// 内存存储
	tables := map[string]*struct {
		Columns []domain.ColumnInfo
		Rows    []domain.Row
	}{
		"users": {
			Columns: []domain.ColumnInfo{
				{Name: "id", Type: "bigint", Nullable: false, Primary: true},
				{Name: "name", Type: "varchar(100)", Nullable: false},
				{Name: "email", Type: "varchar(255)", Nullable: true},
			},
			Rows: []domain.Row{
				{"id": float64(1), "name": "Alice", "email": "alice@example.com"},
				{"id": float64(2), "name": "Bob", "email": "bob@example.com"},
				{"id": float64(3), "name": "Charlie", "email": "charlie@example.com"},
			},
		},
		"orders": {
			Columns: []domain.ColumnInfo{
				{Name: "id", Type: "bigint", Primary: true},
				{Name: "user_id", Type: "bigint"},
				{Name: "amount", Type: "decimal(10,2)"},
			},
			Rows: []domain.Row{
				{"id": float64(1), "user_id": float64(1), "amount": float64(99.99)},
			},
		},
	}

	mux := gohttp.NewServeMux()

	// 健康检查
	mux.HandleFunc("GET /_health", func(w gohttp.ResponseWriter, r *gohttp.Request) {
		json.NewEncoder(w).Encode(HealthResponse{Status: "ok"})
	})

	// 表列表
	mux.HandleFunc("GET /_schema/tables", func(w gohttp.ResponseWriter, r *gohttp.Request) {
		names := make([]string, 0)
		for name := range tables {
			names = append(names, name)
		}
		json.NewEncoder(w).Encode(TablesResponse{Tables: names})
	})

	// 表结构
	mux.HandleFunc("GET /_schema/tables/", func(w gohttp.ResponseWriter, r *gohttp.Request) {
		tableName := strings.TrimPrefix(r.URL.Path, "/_schema/tables/")
		tbl, ok := tables[tableName]
		if !ok {
			w.WriteHeader(404)
			json.NewEncoder(w).Encode(ErrorResponse{Error: struct {
				Code    string `json:"code"`
				Message string `json:"message"`
			}{Code: "TABLE_NOT_FOUND", Message: fmt.Sprintf("table '%s' not found", tableName)}})
			return
		}
		json.NewEncoder(w).Encode(SchemaResponse{Name: tableName, Columns: tbl.Columns})
	})

	// 查询
	mux.HandleFunc("POST /_query/", func(w gohttp.ResponseWriter, r *gohttp.Request) {
		tableName := strings.TrimPrefix(r.URL.Path, "/_query/")
		tbl, ok := tables[tableName]
		if !ok {
			w.WriteHeader(404)
			json.NewEncoder(w).Encode(ErrorResponse{Error: struct {
				Code    string `json:"code"`
				Message string `json:"message"`
			}{Code: "TABLE_NOT_FOUND", Message: fmt.Sprintf("table '%s' not found", tableName)}})
			return
		}

		var req QueryRequest
		json.NewDecoder(r.Body).Decode(&req)

		rows := tbl.Rows

		// 简单过滤实现
		if len(req.Filters) > 0 {
			var filtered []domain.Row
			for _, row := range rows {
				match := true
				for _, f := range req.Filters {
					if f.Field == "" {
						continue
					}
					val, exists := row[f.Field]
					if !exists {
						match = false
						break
					}
					switch f.Operator {
					case "=":
						if fmt.Sprintf("%v", val) != fmt.Sprintf("%v", f.Value) {
							match = false
						}
					case "!=":
						if fmt.Sprintf("%v", val) == fmt.Sprintf("%v", f.Value) {
							match = false
						}
					}
				}
				if match {
					filtered = append(filtered, row)
				}
			}
			rows = filtered
		}

		total := int64(len(rows))

		// 分页
		if req.Offset > 0 && req.Offset < len(rows) {
			rows = rows[req.Offset:]
		}
		if req.Limit > 0 && req.Limit < len(rows) {
			rows = rows[:req.Limit]
		}

		json.NewEncoder(w).Encode(QueryResponse{
			Columns: tbl.Columns,
			Rows:    rows,
			Total:   total,
		})
	})

	// 插入
	mux.HandleFunc("POST /_insert/", func(w gohttp.ResponseWriter, r *gohttp.Request) {
		tableName := strings.TrimPrefix(r.URL.Path, "/_insert/")
		tbl, ok := tables[tableName]
		if !ok {
			w.WriteHeader(404)
			return
		}
		var req InsertRequest
		json.NewDecoder(r.Body).Decode(&req)
		tbl.Rows = append(tbl.Rows, req.Rows...)
		json.NewEncoder(w).Encode(MutationResponse{Affected: int64(len(req.Rows))})
	})

	// 更新
	mux.HandleFunc("POST /_update/", func(w gohttp.ResponseWriter, r *gohttp.Request) {
		tableName := strings.TrimPrefix(r.URL.Path, "/_update/")
		tbl, ok := tables[tableName]
		if !ok {
			w.WriteHeader(404)
			return
		}
		var req UpdateRequest
		json.NewDecoder(r.Body).Decode(&req)

		affected := int64(0)
		for i, row := range tbl.Rows {
			match := true
			for _, f := range req.Filters {
				if fmt.Sprintf("%v", row[f.Field]) != fmt.Sprintf("%v", f.Value) {
					match = false
					break
				}
			}
			if match {
				for k, v := range req.Updates {
					tbl.Rows[i][k] = v
				}
				affected++
			}
		}
		json.NewEncoder(w).Encode(MutationResponse{Affected: affected})
	})

	// 删除
	mux.HandleFunc("POST /_delete/", func(w gohttp.ResponseWriter, r *gohttp.Request) {
		tableName := strings.TrimPrefix(r.URL.Path, "/_delete/")
		tbl, ok := tables[tableName]
		if !ok {
			w.WriteHeader(404)
			return
		}
		var req DeleteRequest
		json.NewDecoder(r.Body).Decode(&req)

		var kept []domain.Row
		affected := int64(0)
		for _, row := range tbl.Rows {
			match := true
			for _, f := range req.Filters {
				if fmt.Sprintf("%v", row[f.Field]) != fmt.Sprintf("%v", f.Value) {
					match = false
					break
				}
			}
			if match {
				affected++
			} else {
				kept = append(kept, row)
			}
		}
		tbl.Rows = kept
		json.NewEncoder(w).Encode(MutationResponse{Affected: affected})
	})

	return httptest.NewServer(mux)
}

func TestHTTPDataSource_ConnectAndHealth(t *testing.T) {
	ts := newTestServer()
	defer ts.Close()

	ds := createTestDS(t, ts.URL, false)
	ctx := context.Background()

	err := ds.Connect(ctx)
	require.NoError(t, err)
	assert.True(t, ds.IsConnected())

	err = ds.Close(ctx)
	require.NoError(t, err)
	assert.False(t, ds.IsConnected())
}

func TestHTTPDataSource_ConnectFails(t *testing.T) {
	ds := createTestDS(t, "http://localhost:1", false)
	ctx := context.Background()

	err := ds.Connect(ctx)
	require.Error(t, err)
	assert.False(t, ds.IsConnected())
}

func TestHTTPDataSource_GetTables(t *testing.T) {
	ts := newTestServer()
	defer ts.Close()

	ds := createTestDS(t, ts.URL, false)
	ctx := context.Background()
	require.NoError(t, ds.Connect(ctx))

	tables, err := ds.GetTables(ctx)
	require.NoError(t, err)
	assert.Len(t, tables, 2)
	assert.Contains(t, tables, "users")
	assert.Contains(t, tables, "orders")
}

func TestHTTPDataSource_GetTableInfo(t *testing.T) {
	ts := newTestServer()
	defer ts.Close()

	ds := createTestDS(t, ts.URL, false)
	ctx := context.Background()
	require.NoError(t, ds.Connect(ctx))

	info, err := ds.GetTableInfo(ctx, "users")
	require.NoError(t, err)
	assert.Equal(t, "users", info.Name)
	assert.Len(t, info.Columns, 3)
	assert.Equal(t, "id", info.Columns[0].Name)
}

func TestHTTPDataSource_Query(t *testing.T) {
	ts := newTestServer()
	defer ts.Close()

	ds := createTestDS(t, ts.URL, false)
	ctx := context.Background()
	require.NoError(t, ds.Connect(ctx))

	// 查询所有
	result, err := ds.Query(ctx, "users", &domain.QueryOptions{})
	require.NoError(t, err)
	assert.Equal(t, int64(3), result.Total)
	assert.Len(t, result.Rows, 3)

	// 带过滤条件
	result, err = ds.Query(ctx, "users", &domain.QueryOptions{
		Filters: []domain.Filter{
			{Field: "name", Operator: "=", Value: "Alice"},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, int64(1), result.Total)
	assert.Len(t, result.Rows, 1)

	// 分页
	result, err = ds.Query(ctx, "users", &domain.QueryOptions{
		Limit: 2,
	})
	require.NoError(t, err)
	assert.Len(t, result.Rows, 2)
}

func TestHTTPDataSource_Insert(t *testing.T) {
	ts := newTestServer()
	defer ts.Close()

	ds := createTestDS(t, ts.URL, true)
	ctx := context.Background()
	require.NoError(t, ds.Connect(ctx))

	affected, err := ds.Insert(ctx, "users", []domain.Row{
		{"id": 4, "name": "Dave", "email": "dave@example.com"},
	}, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(1), affected)

	// 验证
	result, err := ds.Query(ctx, "users", &domain.QueryOptions{})
	require.NoError(t, err)
	assert.Equal(t, int64(4), result.Total)
}

func TestHTTPDataSource_Update(t *testing.T) {
	ts := newTestServer()
	defer ts.Close()

	ds := createTestDS(t, ts.URL, true)
	ctx := context.Background()
	require.NoError(t, ds.Connect(ctx))

	affected, err := ds.Update(ctx, "users",
		[]domain.Filter{{Field: "name", Operator: "=", Value: "Alice"}},
		domain.Row{"email": "alice_new@example.com"},
		nil,
	)
	require.NoError(t, err)
	assert.Equal(t, int64(1), affected)
}

func TestHTTPDataSource_Delete(t *testing.T) {
	ts := newTestServer()
	defer ts.Close()

	ds := createTestDS(t, ts.URL, true)
	ctx := context.Background()
	require.NoError(t, ds.Connect(ctx))

	affected, err := ds.Delete(ctx, "users",
		[]domain.Filter{{Field: "name", Operator: "=", Value: "Charlie"}},
		nil,
	)
	require.NoError(t, err)
	assert.Equal(t, int64(1), affected)

	// 验证
	result, err := ds.Query(ctx, "users", &domain.QueryOptions{})
	require.NoError(t, err)
	assert.Equal(t, int64(2), result.Total)
}

func TestHTTPDataSource_ReadOnly(t *testing.T) {
	ts := newTestServer()
	defer ts.Close()

	ds := createTestDS(t, ts.URL, false) // writable=false
	ctx := context.Background()
	require.NoError(t, ds.Connect(ctx))

	_, err := ds.Insert(ctx, "users", []domain.Row{{"id": 4}}, nil)
	require.Error(t, err)
	assert.IsType(t, &domain.ErrReadOnly{}, err)

	_, err = ds.Update(ctx, "users", nil, domain.Row{"name": "X"}, nil)
	require.Error(t, err)
	assert.IsType(t, &domain.ErrReadOnly{}, err)

	_, err = ds.Delete(ctx, "users", nil, nil)
	require.Error(t, err)
	assert.IsType(t, &domain.ErrReadOnly{}, err)
}

func TestHTTPDataSource_UnsupportedDDL(t *testing.T) {
	ts := newTestServer()
	defer ts.Close()

	ds := createTestDS(t, ts.URL, true)
	ctx := context.Background()
	require.NoError(t, ds.Connect(ctx))

	err := ds.CreateTable(ctx, &domain.TableInfo{Name: "test"})
	require.Error(t, err)
	assert.IsType(t, &domain.ErrUnsupportedOperation{}, err)

	err = ds.DropTable(ctx, "test")
	require.Error(t, err)
	assert.IsType(t, &domain.ErrUnsupportedOperation{}, err)

	err = ds.TruncateTable(ctx, "test")
	require.Error(t, err)
	assert.IsType(t, &domain.ErrUnsupportedOperation{}, err)

	_, err = ds.Execute(ctx, "SELECT 1")
	require.Error(t, err)
	assert.IsType(t, &domain.ErrUnsupportedOperation{}, err)
}

func TestHTTPDataSource_NotConnected(t *testing.T) {
	ts := newTestServer()
	defer ts.Close()

	ds := createTestDS(t, ts.URL, false)
	ctx := context.Background()
	// 不调用 Connect

	_, err := ds.GetTables(ctx)
	require.Error(t, err)
	assert.IsType(t, &domain.ErrNotConnected{}, err)

	_, err = ds.Query(ctx, "users", &domain.QueryOptions{})
	require.Error(t, err)
}

func TestHTTPDataSource_TableAlias(t *testing.T) {
	ts := newTestServer()
	defer ts.Close()

	dsCfg := &domain.DataSourceConfig{
		Type:     domain.DataSourceTypeHTTP,
		Name:     "test_ds",
		Host:     ts.URL,
		Writable: false,
		Options: map[string]interface{}{
			"table_alias": map[string]interface{}{
				"employees": "users", // SQL "employees" → HTTP "users"
			},
		},
	}

	httpCfg, err := ParseHTTPConfig(dsCfg)
	require.NoError(t, err)
	assert.Equal(t, "users", httpCfg.ResolveTableName("employees"))
	assert.Equal(t, "orders", httpCfg.ResolveTableName("orders")) // 不在映射中，直接透传

	ds, err := NewHTTPDataSource(dsCfg, httpCfg)
	require.NoError(t, err)

	ctx := context.Background()
	require.NoError(t, ds.Connect(ctx))

	// 通过别名查询
	info, err := ds.GetTableInfo(ctx, "employees")
	require.NoError(t, err)
	assert.Equal(t, "employees", info.Name) // 返回的是 SQL 名
	assert.Len(t, info.Columns, 3)

	result, err := ds.Query(ctx, "employees", &domain.QueryOptions{})
	require.NoError(t, err)
	assert.Equal(t, int64(3), result.Total)
}

func TestHTTPDataSource_FilterableDataSource(t *testing.T) {
	ts := newTestServer()
	defer ts.Close()

	ds := createTestDS(t, ts.URL, false)
	ctx := context.Background()
	require.NoError(t, ds.Connect(ctx))

	assert.True(t, ds.SupportsFiltering("users"))

	rows, total, err := ds.Filter(ctx, "users",
		domain.Filter{Field: "name", Operator: "=", Value: "Bob"},
		0, 10,
	)
	require.NoError(t, err)
	assert.Equal(t, int64(1), total)
	assert.Len(t, rows, 1)
}

func TestHTTPDataSource_ACL(t *testing.T) {
	ts := newTestServer()
	defer ts.Close()

	dsCfg := &domain.DataSourceConfig{
		Type:     domain.DataSourceTypeHTTP,
		Name:     "acl_test",
		Host:     ts.URL,
		Writable: false,
		Options: map[string]interface{}{
			"acl": map[string]interface{}{
				"allowed_users": []interface{}{"admin", "analyst"},
				"permissions": map[string]interface{}{
					"admin":   []interface{}{"SELECT", "INSERT", "UPDATE", "DELETE"},
					"analyst": []interface{}{"SELECT"},
				},
			},
		},
	}

	httpCfg, err := ParseHTTPConfig(dsCfg)
	require.NoError(t, err)

	// allowed user with permission
	assert.NoError(t, httpCfg.CheckACL("admin", "SELECT"))
	assert.NoError(t, httpCfg.CheckACL("admin", "INSERT"))
	assert.NoError(t, httpCfg.CheckACL("analyst", "SELECT"))

	// allowed user without permission
	assert.Error(t, httpCfg.CheckACL("analyst", "INSERT"))

	// denied user
	assert.Error(t, httpCfg.CheckACL("hacker", "SELECT"))
}

func TestHTTPDataSource_CustomHeaders(t *testing.T) {
	var receivedHeaders gohttp.Header

	ts := httptest.NewServer(gohttp.HandlerFunc(func(w gohttp.ResponseWriter, r *gohttp.Request) {
		receivedHeaders = r.Header
		if r.URL.Path == "/_health" {
			json.NewEncoder(w).Encode(HealthResponse{Status: "ok"})
			return
		}
		json.NewEncoder(w).Encode(QueryResponse{Total: 0})
	}))
	defer ts.Close()

	dsCfg := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeHTTP,
		Name: "header_test",
		Host: ts.URL,
		Options: map[string]interface{}{
			"auth_type":  "bearer",
			"auth_token": "my_secret",
			"headers": map[string]interface{}{
				"X-Custom":    "static-value",
				"X-Timestamp": "{{timestamp}}",
				"X-Method":    "{{upper(method)}}",
			},
		},
	}

	httpCfg, err := ParseHTTPConfig(dsCfg)
	require.NoError(t, err)

	ds, err := NewHTTPDataSource(dsCfg, httpCfg)
	require.NoError(t, err)

	ctx := context.Background()
	require.NoError(t, ds.Connect(ctx))

	assert.Equal(t, "static-value", receivedHeaders.Get("X-Custom"))
	assert.Regexp(t, `^\d+$`, receivedHeaders.Get("X-Timestamp"))
	assert.Equal(t, "GET", receivedHeaders.Get("X-Method"))
	assert.Equal(t, "Bearer my_secret", receivedHeaders.Get("Authorization"))
}

func TestHTTPDataSource_Retry(t *testing.T) {
	attempts := 0
	ts := httptest.NewServer(gohttp.HandlerFunc(func(w gohttp.ResponseWriter, r *gohttp.Request) {
		attempts++
		if r.URL.Path == "/_health" {
			if attempts <= 2 {
				w.WriteHeader(500)
				w.Write([]byte("server error"))
				return
			}
			json.NewEncoder(w).Encode(HealthResponse{Status: "ok"})
			return
		}
	}))
	defer ts.Close()

	dsCfg := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeHTTP,
		Name: "retry_test",
		Host: ts.URL,
		Options: map[string]interface{}{
			"retry_count":    float64(3),
			"retry_delay_ms": float64(10), // 快速重试
			"timeout_ms":     float64(5000),
		},
	}

	httpCfg, err := ParseHTTPConfig(dsCfg)
	require.NoError(t, err)

	ds, err := NewHTTPDataSource(dsCfg, httpCfg)
	require.NoError(t, err)

	ctx := context.Background()
	err = ds.Connect(ctx)
	require.NoError(t, err)
	assert.Equal(t, 3, attempts) // 2 failures + 1 success
}

func TestHTTPDataSource_GetDatabaseName(t *testing.T) {
	dsCfg := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeHTTP,
		Name: "myds",
		Host: "http://localhost",
		Options: map[string]interface{}{
			"database": "custom_db",
		},
	}
	httpCfg, err := ParseHTTPConfig(dsCfg)
	require.NoError(t, err)
	assert.Equal(t, "custom_db", httpCfg.Database)

	// 默认使用 Name
	dsCfg2 := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeHTTP,
		Name: "myds",
		Host: "http://localhost",
	}
	httpCfg2, err := ParseHTTPConfig(dsCfg2)
	require.NoError(t, err)
	assert.Equal(t, "myds", httpCfg2.Database)
}

func TestHTTPFactory(t *testing.T) {
	factory := NewHTTPFactory()
	assert.Equal(t, domain.DataSourceTypeHTTP, factory.GetType())

	ts := newTestServer()
	defer ts.Close()

	ds, err := factory.Create(&domain.DataSourceConfig{
		Type: domain.DataSourceTypeHTTP,
		Name: "factory_test",
		Host: ts.URL,
	})
	require.NoError(t, err)
	require.NotNil(t, ds)

	ctx := context.Background()
	require.NoError(t, ds.Connect(ctx))
	assert.True(t, ds.IsConnected())
}

func TestHTTPDataSource_ConfigParsing(t *testing.T) {
	dsCfg := &domain.DataSourceConfig{
		Type:     domain.DataSourceTypeHTTP,
		Name:     "full_config",
		Host:     "https://api.example.com",
		Username: "user",
		Password: "pass",
		Writable: true,
		Options: map[string]interface{}{
			"base_path": "/api/v1",
			"paths": map[string]interface{}{
				"tables": "/custom/tables",
				"query":  "/custom/query/{table}",
			},
			"auth_type":       "basic",
			"timeout_ms":      float64(5000),
			"retry_count":     float64(2),
			"retry_delay_ms":  float64(500),
			"tls_skip_verify": true,
			"database":        "erp_db",
			"table_alias": map[string]interface{}{
				"employees": "users",
			},
		},
	}

	cfg, err := ParseHTTPConfig(dsCfg)
	require.NoError(t, err)

	assert.Equal(t, "/api/v1", cfg.BasePath)
	assert.Equal(t, "/custom/tables", cfg.Paths.Tables)
	assert.Equal(t, "/custom/query/{table}", cfg.Paths.Query)
	assert.Equal(t, DefaultPathSchema, cfg.Paths.Schema) // 未设置，使用默认
	assert.Equal(t, "basic", cfg.AuthType)
	assert.Equal(t, 5000, cfg.TimeoutMs)
	assert.Equal(t, 2, cfg.RetryCount)
	assert.Equal(t, 500, cfg.RetryDelayMs)
	assert.True(t, cfg.TLSSkipVerify)
	assert.Equal(t, "erp_db", cfg.Database)
	assert.Equal(t, "users", cfg.TableAlias["employees"])
}

// createTestDS 创建测试用 HTTPDataSource
func createTestDS(t *testing.T, serverURL string, writable bool) *HTTPDataSource {
	t.Helper()
	dsCfg := &domain.DataSourceConfig{
		Type:     domain.DataSourceTypeHTTP,
		Name:     "test_ds",
		Host:     serverURL,
		Writable: writable,
	}
	httpCfg, err := ParseHTTPConfig(dsCfg)
	require.NoError(t, err)
	ds, err := NewHTTPDataSource(dsCfg, httpCfg)
	require.NoError(t, err)
	return ds
}
