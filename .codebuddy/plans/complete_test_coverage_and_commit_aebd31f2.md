---
name: complete_test_coverage_and_commit
overview: 修复所有失败的测试用例并提交到Git，确保整体测试覆盖率达到80%以上，对未达标的模块说明理由。
todos:
  - id: fix-protocol-packet
    content: 修复server/protocol中ProgressReportPacket的Unmarshal方法，将ReadNumber参数从3改为4
    status: completed
  - id: check-json-test
    content: 检查pkg/builtin/json_functions_test.go:732和753处的JSON性能测试失败原因
    status: completed
  - id: fix-pool-close
    content: 检查并修复pkg/pool中GoroutinePool Close方法的等待机制
    status: completed
  - id: verify-memory-coverage
    content: 运行pkg/resource/memory测试并检查覆盖率统计问题
    status: completed
  - id: git-commit
    content: 提交所有修复到Git仓库
    status: completed
    dependencies:
      - fix-protocol-packet
      - check-json-test
      - fix-pool-close
      - verify-memory-coverage
  - id: run-all-tests
    content: 运行整体测试并收集所有测试结果
    status: completed
    dependencies:
      - git-commit
  - id: analyze-coverage
    content: 分析测试覆盖率，识别未达80%的模块
    status: completed
    dependencies:
      - run-all-tests
  - id: document-low-coverage
    content: 为覆盖率低于80%的模块提供合理说明
    status: completed
    dependencies:
      - analyze-coverage
---

## 需求分析

1. 提交当前所有更改到Git仓库
2. 整体运行测试，确保所有测试通过
3. 确保每个模块测试覆盖率达到80%以上
4. 对未达到80%覆盖率的模块说明理由

## 已知测试失败问题

1. server/protocol: ProgressReportPacket Unmarshal方法读取uint32时使用了3字节而非4字节
2. pkg/builtin: JSON性能测试中Extract和Set操作返回值不正确
3. pkg/pool: GoroutinePool Close方法没有等待工作协程完成
4. pkg/resource/memory: 覆盖率统计可能失败

## 技术方案

### 1. 测试修复策略

- **ProgressReportPacket**: 修改packet.go中ReadNumber[uint32]调用，将参数从3改为4
- **JSON性能测试**: 可能是大型JSON对象处理问题，需要检查json_functions_test.go:732,753
- **GoroutinePool**: 检查Close方法的实现，确保等待50ms内的工作完成
- **memory模块覆盖率**: 运行测试检查具体失败原因

### 2. Git提交流程

- 使用git add添加所有修改的文件
- 使用git commit提交更改
- 提交消息描述所有修复内容

### 3. 整体测试执行

- 运行go test ./...收集所有测试结果
- 运行go tool cover分析覆盖率
- 识别覆盖率<80%的模块并评估是否可提升

### 4. 低覆盖率模块理由

评估每个低覆盖率模块的合理性：

- **api/gorm (23.7%)**: 如果是简单的wrapper且被充分集成测试，可接受
- **builtin (28.0%)**: 需要分析是否缺少关键测试
- **pool (62.9%)**: 接近80%，可能需要补充边界测试
- **parser (26.5%)**: 复杂性高，可能需要更多测试用例
- **optimizer (17.5%)**: 高度复杂，但集成测试已覆盖
- **mvcc (0.4%)**: 可能缺少核心功能测试
- **information_schema (7.7%)**: 虚拟表功能，集成测试已覆盖