# 资源层测试 (Resource Tests)

资源层测试使用底层 API 直接操作数据源，测试数据存储和查询逻辑。

## 测试文件

### generated_columns_test.go

**测试内容：** VIRTUAL 和 STORED 生成列功能

**主要测试：**
- VIRTUAL 列基础测试
- STORED 和 VIRTUAL 混合测试
- VIRTUAL 列 NULL 传播测试
- 复杂表达式测试
- UPDATE 操作级联更新测试
- SQL 解析 VIRTUAL 列语法测试
- 性能测试
- 错误处理测试
- 混合 STORED 和 VIRTUAL 列的多级依赖测试
- VIRTUAL 列在 WHERE 条件中测试
- VIRTUAL 列与 ORDER BY 测试
- 复杂数学表达式测试

**依赖：**
- `pkg/resource/memory` - 内存数据源
- `pkg/resource/domain` - 数据模型
- `pkg/parser` - SQL 解析器

**运行方式：**
```bash
go test ./server/testing/resource -run TestGeneratedColumns
```

### table_operations_test.go

**测试内容：** 表操作（CREATE/DROP/TRUNCATE）

**主要测试：**
- 创建表
- 表结构验证
- 插入数据并查询
- TRUNCATE TABLE 清空数据
- DROP TABLE 删除表
- 创建重复表
- 删除不存在的表
- 清空不存在的表
- 多表操作

**依赖：**
- `pkg/resource/memory` - 内存数据源
- `pkg/resource/domain` - 数据模型

**运行方式：**
```bash
go test ./server/testing/resource -run TestTableOperations
go test ./server/testing/resource -run TestMultipleTables
```

### privilege_simple_test.go

**测试内容：** 权限表可见性测试（使用 information_schema）

**主要测试：**
- USER_PRIVILEGES 表可见性
- TABLE_PRIVILEGES 表可见性
- COLUMN_PRIVILEGES 表可见性
- 权限数据验证

**依赖：**
- `pkg/information_schema` - 信息架构
- `pkg/resource/application` - 应用层
- `pkg/resource/domain` - 数据模型

**运行方式：**
```bash
go test ./server/testing/resource -run TestPrivilege
```

## 运行所有资源层测试

```bash
# 运行所有资源层测试
go test ./server/testing/resource

# 运行并显示详细输出
go test ./server/testing/resource -v

# 运行并显示覆盖率
go test ./server/testing/resource -cover
```

## 编写资源层测试的指南

### 1. 创建内存数据源

```go
func TestExample(t *testing.T) {
    ctx := context.Background()

    // 创建内存数据源
    ds := memory.NewMVCCDataSource(&domain.DataSourceConfig{
        Type:     domain.DataSourceTypeMemory,
        Name:     "test_memory",
        Writable: true,
    })
    err := ds.Connect(ctx)
    assert.NoError(t, err)
    t.Log("✓ 数据源连接成功")
}
```

### 2. 创建表

```go
func TestCreateTable(t *testing.T) {
    ctx := context.Background()
    ds := memory.NewMVCCDataSource(&domain.DataSourceConfig{
        Type:     domain.DataSourceTypeMemory,
        Name:     "test_memory",
        Writable: true,
    })
    err := ds.Connect(ctx)
    assert.NoError(t, err)

    // 创建表
    schema := &domain.TableInfo{
        Name:   "products",
        Schema: "test",
        Columns: []domain.ColumnInfo{
            {Name: "id", Type: "int64", Nullable: false, Primary: true},
            {Name: "name", Type: "string", Nullable: false},
            {Name: "price", Type: "float64", Nullable: true},
        },
    }

    err = ds.CreateTable(ctx, schema)
    assert.NoError(t, err)
    t.Log("✓ 表创建成功")

    // 验证表存在
    tables, err := ds.GetTables(ctx)
    assert.NoError(t, err)
    assert.Equal(t, 1, len(tables))
    assert.Equal(t, "products", tables[0])
}
```

### 3. 插入和查询数据

```go
func TestInsertAndQuery(t *testing.T) {
    ctx := context.Background()
    ds := memory.NewMVCCDataSource(&domain.DataSourceConfig{
        Type:     domain.DataSourceTypeMemory,
        Name:     "test_memory",
        Writable: true,
    })
    err := ds.Connect(ctx)
    assert.NoError(t, err)

    // 创建表
    schema := &domain.TableInfo{
        Name:   "users",
        Schema: "test",
        Columns: []domain.ColumnInfo{
            {Name: "id", Type: "int64", Nullable: false, Primary: true},
            {Name: "name", Type: "string", Nullable: false},
        },
    }
    err = ds.CreateTable(ctx, schema)
    assert.NoError(t, err)

    // 插入数据
    rows := []domain.Row{
        {"id": int64(1), "name": "Alice"},
        {"id": int64(2), "name": "Bob"},
    }

    affected, err := ds.Insert(ctx, "users", rows, &domain.InsertOptions{})
    assert.NoError(t, err)
    assert.Equal(t, int64(2), affected)
    t.Log("✓ 插入数据成功: 2 行")

    // 查询数据
    result, err := ds.Query(ctx, "users", &domain.QueryOptions{})
    assert.NoError(t, err)
    assert.Equal(t, 2, len(result.Rows))
    assert.Equal(t, "Alice", result.Rows[0]["name"])
    assert.Equal(t, "Bob", result.Rows[1]["name"])
}
```

### 4. 测试生成列

```go
func TestGeneratedColumns(t *testing.T) {
    ctx := context.Background()
    ds := memory.NewMVCCDataSource(&domain.DataSourceConfig{
        Type:     domain.DataSourceTypeMemory,
        Name:     "test_memory",
        Writable: true,
    })
    err := ds.Connect(ctx)
    assert.NoError(t, err)

    // 创建包含 VIRTUAL 列的表
    schema := &domain.TableInfo{
        Name:   "products",
        Schema: "test",
        Columns: []domain.ColumnInfo{
            {Name: "id", Type: "INT", Nullable: false, Primary: true},
            {Name: "name", Type: "VARCHAR(100)", Nullable: false},
            {Name: "price", Type: "DECIMAL(10,2)", Nullable: false},
            {Name: "quantity", Type: "INT", Nullable: false},
            {
                Name:          "total",
                Type:          "DECIMAL(10,2)",
                Nullable:      false,
                IsGenerated:   true,
                GeneratedType:  "VIRTUAL",
                GeneratedExpr:  "price * quantity",
                GeneratedDepends: []string{"price", "quantity"},
            },
        },
    }

    err = ds.CreateTable(ctx, schema)
    assert.NoError(t, err)

    // 插入数据（VIRTUAL 列不应被存储）
    rows := []domain.Row{
        {"id": int64(1), "name": "Apple", "price": 10.5, "quantity": int64(2)},
        {"id": int64(2), "name": "Banana", "price": 5.0, "quantity": int64(3)},
    }

    count, err := ds.Insert(ctx, "products", rows, nil)
    assert.NoError(t, err)
    assert.Equal(t, int64(2), count)

    // 查询数据并验证 VIRTUAL 列的计算
    queryResult, err := ds.Query(ctx, "products", &domain.QueryOptions{})
    assert.NoError(t, err)
    assert.Len(t, queryResult.Rows, 2)

    // 验证 VIRTUAL 列的计算结果
    assert.Equal(t, 21.0, queryResult.Rows[0]["total"], "Apple: 10.5 * 2 = 21.0")
    assert.Equal(t, 15.0, queryResult.Rows[1]["total"], "Banana: 5.0 * 3 = 15.0")
    t.Log("✓ VIRTUAL 列计算正确")
}
```

### 5. 测试表操作（CREATE/DROP/TRUNCATE）

```go
func TestTableOperations(t *testing.T) {
    ctx := context.Background()
    ds := memory.NewMVCCDataSource(&domain.DataSourceConfig{
        Type:     domain.DataSourceTypeMemory,
        Name:     "test_memory",
        Writable: true,
    })
    err := ds.Connect(ctx)
    assert.NoError(t, err)

    // 测试1: 创建表
    t.Run("创建表", func(t *testing.T) {
        schema := &domain.TableInfo{
            Name:   "products",
            Schema: "test",
            Columns: []domain.ColumnInfo{
                {Name: "id", Type: "int64", Nullable: false, Primary: true},
                {Name: "name", Type: "string", Nullable: false},
                {Name: "price", Type: "float64", Nullable: true},
            },
        }

        err = ds.CreateTable(ctx, schema)
        assert.NoError(t, err)

        // 验证表存在
        tables, err := ds.GetTables(ctx)
        assert.NoError(t, err)
        assert.Equal(t, 1, len(tables))
        assert.Equal(t, "products", tables[0])
    })

    // 测试2: 插入数据
    t.Run("插入数据", func(t *testing.T) {
        productsData := []domain.Row{
            {"id": int64(1), "name": "Product A", "price": float64(99.99)},
            {"id": int64(2), "name": "Product B", "price": float64(199.99)},
        }

        affected, err := ds.Insert(ctx, "products", productsData, &domain.InsertOptions{})
        assert.NoError(t, err)
        assert.Equal(t, int64(2), affected)

        // 查询验证
        result, err := ds.Query(ctx, "products", &domain.QueryOptions{})
        assert.NoError(t, err)
        assert.Equal(t, 2, len(result.Rows))
    })

    // 测试3: TRUNCATE TABLE
    t.Run("清空表", func(t *testing.T) {
        err := ds.TruncateTable(ctx, "products")
        assert.NoError(t, err)

        // 验证数据已清空
        result, err := ds.Query(ctx, "products", &domain.QueryOptions{})
        assert.NoError(t, err)
        assert.Equal(t, 0, len(result.Rows))

        // 验证表结构仍然存在
        tableInfo, err := ds.GetTableInfo(ctx, "products")
        assert.NoError(t, err)
        assert.Equal(t, "products", tableInfo.Name)
        assert.Equal(t, 3, len(tableInfo.Columns))
    })

    // 测试4: DROP TABLE
    t.Run("删除表", func(t *testing.T) {
        err := ds.DropTable(ctx, "products")
        assert.NoError(t, err)

        // 验证表不存在
        tables, err := ds.GetTables(ctx)
        assert.NoError(t, err)
        assert.Equal(t, 0, len(tables))

        // 验证不能查询已删除的表
        _, err = ds.Query(ctx, "products", &domain.QueryOptions{})
        assert.Error(t, err)
    })
}
```

### 6. 使用子测试

```go
func TestTableOperations(t *testing.T) {
    ctx := context.Background()
    ds := memory.NewMVCCDataSource(&domain.DataSourceConfig{
        Type:     domain.DataSourceTypeMemory,
        Name:     "test_memory",
        Writable: true,
    })
    err := ds.Connect(ctx)
    assert.NoError(t, err)

    t.Run("创建表", func(t *testing.T) {
        // 测试创建表
    })

    t.Run("插入数据", func(t *testing.T) {
        // 测试插入数据
    })

    t.Run("删除表", func(t *testing.T) {
        // 测试删除表
    })
}
```

## 注意事项

1. **包名** - 资源层测试文件使用 `package tests` 包
2. **Context** - 所有操作都需要传入 `context.Background()`
3. **数据清理** - 每个测试应该创建独立的数据源，避免测试之间的状态污染
4. **类型断言** - 使用正确的类型断言（如 `int64(1)` 而不是 `1`）
5. **错误验证** - 测试错误场景时要验证错误类型和消息
6. **数据验证** - 插入数据后要查询验证数据是否正确
7. **生成列** - 测试生成列时要验证计算结果的正确性
