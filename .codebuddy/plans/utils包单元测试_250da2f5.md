---
name: utils包单元测试
overview: 为 pkg/utils 包的8个文件创建完整的单元测试，覆盖约32个函数，要求覆盖率达到85%以上，重点测试边界条件和异常情况。
todos:
  - id: create-string-tests
    content: 为 pkg/utils/string.go 创建单元测试，覆盖所有字符串操作函数和边界情况
    status: completed
  - id: create-compare-tests
    content: 为 pkg/utils/compare.go 创建单元测试，覆盖数值和字符串比较函数
    status: completed
  - id: create-converter-tests
    content: 为 pkg/utils/converter.go 创建单元测试，覆盖类型转换和空值处理
    status: completed
  - id: create-crypto-tests
    content: 为 pkg/utils/crypto.go 创建单元测试，覆盖密码哈希生成和验证
    status: completed
  - id: create-error-tests
    content: 为 pkg/utils/error.go 创建单元测试，覆盖错误码映射逻辑
    status: completed
  - id: create-filter-tests
    content: 为 pkg/utils/filter.go 创建单元测试，覆盖过滤器匹配逻辑
    status: completed
  - id: create-net-tests
    content: 为 pkg/utils/net.go 创建单元测试，覆盖地址解析边界情况
    status: completed
  - id: create-charset-tests
    content: 为 pkg/utils/charset.go 创建单元测试，覆盖字符集ID和名称转换
    status: completed
  - id: run-tests-and-check-coverage
    content: 运行所有测试并检查覆盖率是否达到85%以上
    status: completed
    dependencies:
      - create-string-tests
      - create-compare-tests
      - create-converter-tests
      - create-crypto-tests
      - create-error-tests
      - create-filter-tests
      - create-net-tests
      - create-charset-tests
---

## Product Overview

为 pkg/utils 包创建全面的单元测试，确保代码覆盖率达到 85% 以上。

## Core Features

- 为 8 个 utils 文件创建完整的单元测试文件
- 覆盖所有 32 个函数和常量
- 重点关注边界检测：空值、nil、特殊字符、极值、异常输入
- 测试覆盖：字符集转换、数值比较、类型转换、密码哈希、错误映射、字符串操作、网络解析

## Test Coverage Focus

- 边界条件：空字符串、nil值、最大/最小值
- 异常情况：无效输入、格式错误、类型不匹配
- 功能完整性：所有函数路径、所有常量定义

## Tech Stack

- Go 1.x 测试框架
- testing 包内置测试工具

## Implementation Approach

为 pkg/utils 包中的 8 个文件分别创建对应的 _test.go 文件，使用表驱动测试方法确保全面覆盖各种边界情况。每个测试文件将包含：

- 常规功能测试（正常路径）
- 边界值测试（空值、极值、特殊字符）
- 异常情况测试（无效输入、类型不匹配）
- 表驱动测试（多场景批量验证）

## Implementation Notes

- 使用表驱动测试减少代码重复，提高可维护性
- 每个函数至少覆盖：正常输入、边界值、异常情况三类场景
- 对于复杂逻辑函数，使用子测试覆盖不同代码路径
- 确保测试文件与被测文件在同一目录下
- 使用 t.Helper() 辅助函数标记测试辅助函数

## Directory Structure

```
pkg/utils/
├── charset.go
├── charset_test.go              # [NEW] 字符集转换函数测试
├── compare.go
├── compare_test.go              # [NEW] 比较函数测试（CompareValues, CompareValuesForSort）
├── converter.go
├── converter_test.go            # [NEW] 类型转换函数测试（ToString, ToInt64, ToFloat64）
├── crypto.go
├── crypto_test.go               # [NEW] 密码哈希和验证测试
├── error.go
├── error_test.go                # [NEW] 错误码映射测试
├── filter.go
├── filter_test.go               # [NEW] 过滤器匹配测试
├── net.go
├── net_test.go                  # [NEW] 网络地址解析测试
├── string.go
└── string_test.go              # [NEW] 字符串操作测试（StartsWith, EndsWith, Contains等）
```

## Key Code Structures

测试文件结构模板（以 string_test.go 为例）：

```
package utils

import (
	"testing"
)

func TestStartsWith(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		prefix   string
		expected bool
	}{
		{"正常匹配", "hello world", "hello", true},
		{"空前缀匹配", "hello", "", true},
		{"不匹配", "hello", "world", false},
		{"空字符串", "", "", true},
		{"前缀长于字符串", "hi", "hello", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := StartsWith(tt.s, tt.prefix); got != tt.expected {
				t.Errorf("StartsWith(%q, %q) = %v, want %v", tt.s, tt.prefix, got, tt.expected)
			}
		})
	}
}
```