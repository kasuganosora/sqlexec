# GORM 驱动测试运行指南

## 概述

本指南说明如何运行 GORM 驱动的测试套件。

## 前置条件

### 1. 确保项目依赖已安装
```bash
cd d:/code/db
go mod tidy
go mod download
```

### 2. 修复项目根目录的编译问题

项目根目录下的测试文件有重复的 `main` 函数声明，需要先解决：

**选项 1**：移除或重命名测试文件
```bash
# 重命名测试文件
mv test_parse.go test_parse.go.bak
mv test_information_schema.go test_information_schema.go.bak
mv test_use.go test_use.go.bak
```

**选项 2**：将测试文件移动到单独的目录
```bash
mkdir -p test_scripts
mv test_parse.go test_scripts/
mv test_information_schema.go test_scripts/
mv test_use.go test_scripts/
```

**选项 3**：使用构建标签
在每个测试文件中添加构建标签：
```go
//go:build !test
// +build !test
```

## 运行测试

### 1. 运行所有测试
```bash
cd d:/code/db/pkg/api/gorm
go test -v
```

### 2. 运行特定测试类
```bash
# 只运行 Dialector 测试
go test -v -run "TestDialector"

# 只运行 Migrator 测试
go test -v -run "TestMigrator"

# 只运行集成测试
go test -v -run "TestIntegration"

# 只运行边缘情况测试
go test -v -run "TestEdgeCases"

# 只运行简单测试
go test -v -run "TestNewDialector_Simple"
```

### 3. 运行特定测试函数
```bash
# 运行单个测试
go test -v -run "TestNewDialector_Simple"
go test -v -run "TestGORM_Open"
go test -v -run "TestDataTypeOf"
```

### 4. 生成覆盖率报告
```bash
# 生成覆盖率文件
go test -v -coverprofile=coverage.out -covermode=count

# 查看覆盖率百分比
go tool cover -func=coverage.out

# 生成 HTML 覆盖率报告
go tool cover -html=coverage.out -o coverage.html

# 在浏览器中打开报告
start coverage.html  # Windows
open coverage.html   # macOS
xdg-open coverage.html  # Linux
```

### 5. 运行基准测试
```bash
# 运行所有基准测试
go test -bench=. -benchmem

# 运行特定基准测试
go test -bench=BenchmarkCreate -benchmem
go test -bench=BenchmarkRead -benchmem

# 运行基准测试并保存结果
go test -bench=. -benchmem > benchmark_results.txt
```

### 6. 并行运行测试
```bash
# 使用所有 CPU 核心
go test -parallel -1

# 使用指定数量的并行
go test -parallel 4
```

## 测试文件说明

### 1. simple_test.go
**目的**：简化的单元测试，用于快速验证
**运行命令**：
```bash
go test -v -run "^TestNewDialector_Simple$|^TestGORM_Open$|^TestDataTypeOf$"
```

### 2. dialect_test.go
**目的**：Dialector 的单元测试
**运行命令**：
```bash
go test -v -run "^TestDialector"
```

### 3. migrator_test.go
**目的**：Migrator 的单元测试
**运行命令**：
```bash
go test -v -run "^TestMigrator"
```

### 4. integration_test.go
**目的**：端到端集成测试
**运行命令**：
```bash
go test -v -run "^TestIntegration"
```

### 5. gorm_test.go
**目的**：高级功能和综合工作流测试
**运行命令**：
```bash
go test -v -run "^TestGORM"
```

### 6. benchmark_test.go
**目的**：性能基准测试
**运行命令**：
```bash
go test -bench=. -benchmem
```

### 7. edge_cases_test.go
**目的**：边缘情况和异常场景测试
**运行命令**：
```bash
go test -v -run "^TestEdgeCases"
```

## 预期覆盖率

### 按文件预估覆盖率

| 文件 | 覆盖率 | 说明 |
|------|---------|------|
| dialect.go | 100% | 所有方法都有对应测试 |
| migrator.go | 100% | 所有方法都有对应测试 |
| examples.go | 85% | 大部分示例被集成测试覆盖 |
| **总计** | **90%+** | 超过 80% 目标 |

### 按功能覆盖率

| 功能 | 覆盖率 |
|------|---------|
| Dialector 方法 | 100% |
| Migrator 方法 | 100% |
| CRUD 操作 | 95% |
| 事务处理 | 90% |
| 查询功能 | 95% |
| 聚合功能 | 90% |
| 原生 SQL | 90% |
| 边缘情况 | 85% |

## 测试输出说明

### 成功的测试输出
```
=== RUN   TestNewDialector_Simple
--- PASS: TestNewDialector_Simple (0.02s)
PASS
ok      github.com/kasuganosora/sqlexec/pkg/api/gorm    0.123s
```

### 失败的测试输出
```
=== RUN   TestNewDialector_Simple
    dialect_test.go:15: Dialector should not be nil
--- FAIL: TestNewDialector_Simple (0.01s)
FAIL
```

### 跳过的测试输出
```
=== RUN   BenchmarkAutoMigrate
--- SKIP: BenchmarkAutoMigrate (0.00s)
```

## 常见问题

### 1. 编译错误：main 函数重复
**错误信息**：
```
main redeclared in this block
```

**解决方案**：参考"前置条件"部分的说明

### 2. 找不到包
**错误信息**：
```
no required module provides package xxx
```

**解决方案**：
```bash
go mod tidy
```

### 3. 测试超时
**错误信息**：
```
timeout waiting for test to complete
```

**解决方案**：增加超时时间
```bash
go test -timeout 30m
```

### 4. 内存不足
**错误信息**：
```
out of memory
```

**解决方案**：
- 减少并行测试数量
- 分批运行测试
```bash
go test -parallel 1
```

## CI/CD 集成

### GitHub Actions 示例

```yaml
name: Run GORM Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Set up Go
        uses: actions/setup-go@v2
        with:
          go-version: 1.21

      - name: Install dependencies
        run: go mod download

      - name: Run tests
        run: |
          cd pkg/api/gorm
          go test -v -coverprofile=coverage.out -covermode=count

      - name: Check coverage
        run: |
          cd pkg/api/gorm
          go tool cover -func=coverage.out | grep total

      - name: Upload coverage
        uses: codecov/codecov-action@v2
        with:
          files: ./pkg/api/gorm/coverage.out
```

## 性能基准

### 基准测试结果示例
```
BenchmarkCreate-8           1000    1234567 ns/op    456789 B/op    12345 allocs/op
BenchmarkRead-8            2000    2345678 ns/op    567890 B/op    23456 allocs/op
BenchmarkUpdate-8          1500    3456789 ns/op    678901 B/op    34567 allocs/op
```

### 解释
- `1000`：迭代次数
- `1234567 ns/op`：每次操作耗时（纳秒）
- `456789 B/op`：每次操作分配的字节数
- `12345 allocs/op`：每次操作的内存分配次数

### 性能目标
- 创建操作：< 2ms/op
- 读取操作：< 1ms/op
- 更新操作：< 1.5ms/op
- 删除操作：< 1ms/op
- 事务操作：< 3ms/op

## 持续改进

### 测试维护清单
- [ ] 定期更新测试用例
- [ ] 添加新功能的测试
- [ ] 修复失败的测试
- [ ] 提高测试覆盖率
- [ ] 优化测试性能
- [ ] 改进测试文档

### 下一步
1. 修复编译错误
2. 运行完整测试套件
3. 生成覆盖率报告
4. 分析覆盖率报告
5. 添加缺失的测试
6. 优化性能瓶颈
7. 建立持续集成

## 联系和支持

如有问题或建议，请：
1. 查看 TEST_SUMMARY.md 了解测试概况
2. 查看 README.md 了解使用方法
3. 查看 IMPLEMENTATION.md 了解实现细节
4. 查看示例文件了解用法示例
