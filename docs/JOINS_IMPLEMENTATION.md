# JOIN实现原理文档

## 概述

本文档详细说明查询优化器中各种JOIN算法的实现原理、复杂度分析和使用场景。

## 1. HashJoin（哈希连接）

### 1.1 算法原理

**两阶段执行模型**：
```
阶段1: Build (构建)
  - 扫描较小表（Build表）
  - 为连接键计算哈希值
  - 构建哈希表: key → [row1, row2, ...]

阶段2: Probe (探测)
  - 扫描较大表（Probe表）
  - 为每行计算哈希值
  - 在哈希表中查找匹配行
  - 产生结果
```

### 1.2 代码结构

```go
type PhysicalHashJoin struct {
    JoinType   JoinType      // INNER, LEFT, RIGHT
    Conditions  []*JoinCondition
    children   []PhysicalPlan
}

func (p *PhysicalHashJoin) Execute(ctx context.Context) (*QueryResult, error) {
    // 1. 获取左表（Build表）
    leftResult := p.children[0].Execute(ctx)

    // 2. 构建哈希表
    hashTable := make(map[interface{}][]Row)
    for _, row := range leftResult.Rows {
        key := row[leftJoinCol]
        hashTable[key] = append(hashTable[key], row)
    }

    // 3. 获取右表（Probe表）
    rightResult := p.children[1].Execute(ctx)

    // 4. 探测哈希表
    output := []Row{}
    for _, rightRow := range rightResult.Rows {
        key := rightRow[rightJoinCol]
        if matches, exists := hashTable[key]; exists {
            // 找到匹配：连接所有匹配的左行
            for _, leftRow := range matches {
                output = append(output, mergeRow(leftRow, rightRow))
            }
        } else if p.JoinType != JoinTypeInner {
            // 未找到匹配：LEFT/RIGHT JOIN处理
            output = append(output, mergeWithNull(leftRow, rightRow))
        }
    }

    return output, nil
}
```

### 1.3 时间复杂度

| 阶段 | 操作 | 复杂度 | 说明 |
|-------|------|--------|------|
| Build | 构建哈希表 | O(N) | N为左表行数 |
| Probe | 探测哈希表 | O(M) | M为右表行数，假设无碰撞 |
| 总体 | - | O(N + M) |

**空间复杂度**: O(N)，需要存储左表的哈希表

### 1.4 适用场景

✅ **推荐使用**:
- 无序数据
- 大表与小表连接
- 等值连接条件
- 一张表明显小于另一张表

❌ **不推荐**:
- 两张表都非常小（< 100行）
- 数据已排序（考虑MergeJoin）
- 非等值连接（如范围条件）

### 1.5 实现细节

#### INNER JOIN
```go
// 两边都必须有匹配
if matches, exists := hashTable[key]; exists {
    for _, leftRow := range matches {
        output = append(output, mergeRow(leftRow, rightRow))
    }
}
// 没有匹配的行被丢弃
```

#### LEFT JOIN
```go
// 左表所有行保留
for _, leftRow := range leftResult.Rows {
    key := leftRow[leftJoinCol]
    if matches, exists := hashTable[key]; exists {
        // 有匹配：连接
        for _, rightRow := range matches {
            output = append(output, mergeRow(leftRow, rightRow))
        }
    } else {
        // 无匹配：左行 + 右NULL
        output = append(output, mergeWithNull(leftRow, rightSchema))
    }
}
```

#### RIGHT JOIN
```go
// 右表所有行保留
for _, rightRow := range rightResult.Rows {
    key := rightRow[rightJoinCol]
    if matches, exists := hashTable[key]; exists {
        // 有匹配：连接
        for _, leftRow := range matches {
            output = append(output, mergeRow(leftRow, rightRow))
        }
    } else {
        // 无匹配：左NULL + 右行
        output = append(output, mergeWithNull(leftSchema, rightRow))
    }
}
```

## 2. MergeJoin（归并连接）

### 2.1 算法原理

**两路归并排序**：
```
前提：两个输入表已按连接键排序

过程:
  1. 两个指针分别指向左表和右表的当前行
  2. 比较两行连接键的大小
  3. 根据比较结果和JOIN类型决定输出
  4. 相等：合并两行并推进两个指针
  5. 不等：推进较小值的一边指针
  6. 重复直到一个表遍历完
```

### 2.2 代码结构

```go
type PhysicalMergeJoin struct {
    JoinType   JoinType
    Conditions  []*JoinCondition
    children   []PhysicalPlan
}

func (p *PhysicalMergeJoin) Execute(ctx context.Context) (*QueryResult, error) {
    // 1. 执行左右表
    leftResult := p.children[0].Execute(ctx)
    rightResult := p.children[1].Execute(ctx)

    // 2. 对两边按连接键排序（如果有序数据可跳过）
    leftRows := p.sortByColumn(leftResult.Rows, leftJoinCol)
    rightRows := p.sortByColumn(rightResult.Rows, rightJoinCol)

    // 3. 两路归并
    i, j := 0, 0
    leftCount := len(leftRows)
    rightCount := len(rightRows)
    output := make([]Row, 0, leftCount+rightCount)

    for i < leftCount && j < rightCount {
        leftVal := leftRows[i][leftCol]
        rightVal := rightRows[j][rightCol]

        cmp := compareValues(leftVal, rightVal)
        if cmp < 0 {
            // 左值小：取左行，继续比较
            output = append(output, mergeRow(leftRows[i], rightRows[j]))
            i++
        } else if cmp > 0 {
            // 右值小：取右行，继续比较
            output = append(output, mergeRow(leftRows[i], rightRows[j]))
            j++
        } else {
            // 相等：合并并推进两个指针
            output = append(output, mergeRow(leftRows[i], rightRows[j]))
            i++
            j++
        }
    }

    // 处理LEFT/RIGHT JOIN的剩余行...
    return output, nil
}
```

### 2.3 时间复杂度

| 操作 | 复杂度 | 说明 |
|------|--------|------|
| 排序（如需要） | O(N log N + M log M) | N为左表行数，M为右表行数 |
| 归并 | O(N + M) | 两表行数之和 |
| 总体 | O(N log N + M log M) 或 O(N + M) | 如果数据已有序 |

**空间复杂度**: O(1)，只需要输出缓冲区

### 2.4 适用场景

✅ **强烈推荐**:
- 数据已有序（索引扫描结果）
- 两张表大小相近
- 等值连接条件
- 内存受限（不需要构建哈希表）

❌ **不推荐**:
- 数据完全无序（排序成本高）
- 大表与小表连接（HashJoin更优）
- 复杂的连接条件

### 2.5 实现细节

#### INNER JOIN（归并）
```go
// 相等：合并并双指针前进
if cmp == 0 {
    output = append(output, mergeRow(leftRows[i], rightRows[j]))
    i++
    j++
}
```

#### LEFT JOIN（归并）
```go
// 保证左表所有行输出
for i < leftCount {
    leftVal := leftRows[i][leftCol]
    
    // 查找所有匹配的右行
    for j < rightCount && compareValues(leftVal, rightRows[j][rightCol]) == 0 {
        output = append(output, mergeRow(leftRows[i], rightRows[j]))
        j++
    }
    
    // 如果右表已遍历完，或左值小于右表所有值
    if j >= rightCount || leftVal < rightRows[j][rightCol] {
        // 无匹配：左行 + 右NULL
        if !hasMatched {
            output = append(output, mergeWithNull(leftRows[i], rightSchema))
        }
        break
    }
    
    i++
    hasMatched = false
}
```

## 3. IndexJoin（索引连接）

### 3.1 算法原理

**基于索引查找**：
```
过程:
  1. 使用索引在一张表（Probe表）中快速查找
  2. 遍历另一张表（Build表）
  3. 对于每行，使用索引在Probe表中查找匹配
  4. 产生结果
```

### 3.2 伪代码

```go
func (p *PhysicalIndexJoin) Execute(ctx context.Context) (*QueryResult, error) {
    // 1. 遍历左表（Build表）
    for _, leftRow := range leftResult.Rows {
        leftVal := leftRow[leftJoinCol]
        
        // 2. 使用索引在右表中查找
        matchingRows := rightTable.LookupByIndex(rightJoinCol, leftVal)
        
        // 3. 产生结果
        for _, rightRow := range matchingRows {
            output = append(output, mergeRow(leftRow, rightRow))
        }
    }
    
    return output, nil
}
```

### 3.3 时间复杂度

| 操作 | 复杂度 | 说明 |
|------|--------|------|
| 索引查找 | O(log M) | M为右表行数 |
| 主表遍历 | O(N) | N为左表行数 |
| 总体 | O(N log M) | |

**空间复杂度**: O(1)，利用现有索引

### 3.4 适用场景

✅ **强烈推荐**:
- Probe表有合适的索引
- 大表与小表连接（比HashJoin更优）
- 连接键有高选择性

❌ **不推荐**:
- Probe表没有索引
- 索引选择性差

## 4. JOIN算法对比

| 特性 | HashJoin | MergeJoin | IndexJoin |
|------|----------|-----------|-----------|
| **时间复杂度** | O(N + M) | O(N log N + M log M) 或 O(N + M) | O(N log M) |
| **空间复杂度** | O(N) | O(1) | O(1) |
| **前提条件** | 无 | 数据已有序 | Probe表有索引 |
| **等值连接** | ✅ | ✅ | ✅ |
| **不等值连接** | ❌ | ❌ | ❌ |
| **大表+小表** | ✅ 最优 | ⚠️ 排序成本高 | ✅ 最优 |
| **相近大小表** | ✅ | ✅ 最优 | ✅ |
| **内存敏感** | ❌ 需要哈希表 | ✅ | ✅ |
| **实现复杂度** | 🟡 中 | 🟢 简单 | 🟡 中 |

**推荐决策树**:
```
                        Probe表有索引？
                         /           \
                       /              \
                    是/                \否
                   /                    \
              使用IndexJoin          大表<<小表？
                                 /       \
                               是/        \否
                         使用HashJoin    考虑排序成本
                                            /           \
                                       已有序?      \
                                       /             \
                                     是/              \否
                                 使用MergeJoin   使用HashJoin
```

## 5. 优化规则

### 5.1 JOIN重排序（Join Reorder）

**贪心算法**：
```
目标: 选择JOIN顺序，使总成本最小

算法:
  1. 选择基数最小的表作为起点
  2. 每次选择与已选表集JOIN成本最小的表
  3. 重复直到所有表被选入

成本估算:
  Cost = ScanCost + MatchCost
  MatchCost ≈ OutputRows / AverageNDV
```

**示例**:
```sql
-- 原始: A JOIN B JOIN C
-- 优化顺序（如果B最小）: B JOIN A JOIN C
```

### 5.2 半连接重写（Semi-Join Rewrite）

**EXISTS → JOIN**:
```sql
-- 原始
SELECT * FROM A WHERE EXISTS (SELECT 1 FROM B WHERE B.id = A.b_id)

-- 重写为
SELECT A.* FROM A 
INNER JOIN (SELECT DISTINCT b_id FROM B) AS B 
ON A.b_id = B.b_id
```

**IN → JOIN**:
```sql
-- 原始
SELECT * FROM A WHERE A.b_id IN (SELECT id FROM B)

-- 重写为
SELECT A.* FROM A 
INNER JOIN (SELECT DISTINCT id FROM B) AS B 
ON A.b_id = B.id
```

### 5.3 JOIN消除（Join Elimination）

**消除场景**:
```sql
-- 1:1外键主键（可消除）
SELECT * FROM A JOIN B ON A.b_id = B.id

-- 消除为
SELECT * FROM A -- B列已包含在A中（通过外键）
```

**消除条件**:
- JOIN条件为等值
- 1:1关系（外键-主键）
- 一边表很小或可以推导

## 6. 性能优化建议

### 6.1 选择JOIN算法

| 场景 | 推荐算法 | 原因 |
|-------|-----------|--------|
| 小表(100) + 大表(1M) | HashJoin | O(N+M)最优 |
| 有序表 + 相近大小 | MergeJoin | 无额外内存 |
| Probe表有B+树索引 | IndexJoin | O(log M)查找 |
| 无序表 + 内存受限 | HashJoin（分批）| 减少内存占用 |
| 大表+大表 | 分批HashJoin | 避免OOM |

### 6.2 构建侧选择

**规则**:
- 选择较小的表作为Build表（减少哈希表大小）
- 考虑列宽（宽列增加哈希表内存占用）
- 选择选择性高的列作为连接键（减少匹配数）

### 6.3 NULL值处理

```go
// 确保NULL不等于NULL（三值逻辑）
func compareForJoin(a, b interface{}) int {
    if a == nil && b == nil {
        return 0 // NULL == NULL 为true
    }
    if a == nil {
        return -1 // NULL < any value
    }
    if b == nil {
        return 1 // any value > NULL
    }
    return compareValues(a, b)
}
```

### 6.4 列名冲突处理

```go
// 自动添加前缀解决冲突
for _, col := range rightResult.Columns {
    if _, exists := leftResult.ColumnNames[col.Name]; exists {
        // 冲突：添加"right_"前缀
        mergedColumns = append(mergedColumns, 
            ColumnInfo{Name: "right_" + col.Name, ...})
    } else {
        mergedColumns = append(mergedColumns, col)
    }
}
```

## 7. 测试用例

### 7.1 HashJoin测试
```go
func TestHashJoinInner() {
    // INNER JOIN: 两边都有匹配
    // 验证：只返回匹配的行
}

func TestHashJoinLeft() {
    // LEFT JOIN: 左表所有行
    // 验证：无匹配的左行，右列全为NULL
}

func TestHashJoinRight() {
    // RIGHT JOIN: 右表所有行
    // 验证：无匹配的右行，左列全为NULL
}
```

### 7.2 MergeJoin测试
```go
func TestMergeJoinSorted() {
    // 输入已排序
    // 验证：归并结果有序
}

func TestMergeJoinPerformance() {
    // 对比HashJoin和MergeJoin在有序数据上的性能
    // 预期：MergeJoin更快
}
```

### 7.3 JOIN重排序测试
```go
func TestJoinReorder() {
    // 测试多表JOIN的不同顺序
    // 验证：优化后的顺序成本更低
}
```

## 8. 参考实现

### 8.1 TiDB实现

- **HashJoin**: `executor/hash_join_executor.go`
- **MergeJoin**: `executor/merge_join_executor.go`
- **IndexJoin**: `executor/index_join_executor.go`
- **Join重排序**: `core/operator/logicalop/rule_join_reorder.go`

### 8.2 本项目实现

- **HashJoin**: `mysql/optimizer/physical_scan.go`
- **MergeJoin**: `mysql/optimizer/merge_join.go`
- **JOIN重排序**: `mysql/optimizer/join_reorder.go`
- **半连接重写**: `mysql/optimizer/semi_join_rewrite.go`
- **JOIN消除**: `mysql/optimizer/join_elimination.go`

## 总结

本优化器实现了完整的JOIN算法体系：

✅ **已实现**:
- HashJoin（INNER, LEFT, RIGHT）
- MergeJoin（所有类型）
- JOIN重排序规则
- 半连接重写规则
- JOIN消除规则

⏸️ **待实现**:
- IndexJoin（需要索引支持）
- 更复杂的JOIN重排序算法（动态规划）
- 外连接转内连接的更精确规则

这些实现为查询优化提供了坚实的JOIN执行能力！
