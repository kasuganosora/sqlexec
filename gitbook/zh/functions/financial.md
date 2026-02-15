# 金融函数

SQLExec 提供了一组金融计算函数，用于净现值、现值、终值、年金等常见的财务分析计算。

## 函数列表

| 函数 | 说明 | 示例 |
|------|------|------|
| `NPV(rate, values)` | 计算净现值 | `SELECT NPV(0.1, '[-1000, 300, 400, 500]');` |
| `PV(rate, nper, pmt)` | 计算现值 | `SELECT PV(0.05, 10, -1000);` |
| `FV(rate, nper, pmt)` | 计算终值（未来值） | `SELECT FV(0.05, 10, -1000);` |
| `PMT(rate, nper, pv)` | 计算每期还款金额 | `SELECT PMT(0.05, 10, 10000);` |
| `RATE(nper, pmt, pv)` | 计算每期利率 | `SELECT RATE(10, -1000, 8000);` |
| `NPER(rate, pmt, pv)` | 计算还款期数 | `SELECT NPER(0.05, -1000, 8000);` |

## 详细说明

### NPV -- 净现值

计算一系列未来现金流在给定折现率下的净现值。净现值为正表示投资可行。

**参数：**
- `rate`：每期折现率
- `values`：现金流数组（字符串格式），第一个值通常为初始投资（负值）

```sql
-- 评估投资项目：初始投入 10000，未来 4 年现金流
SELECT NPV(0.1, '[-10000, 3000, 4000, 4000, 3000]') AS npv;
-- 结果约 1154.47，NPV > 0，投资可行

-- 比较两个投资方案
SELECT 'A' AS plan,
       NPV(0.08, '[-50000, 15000, 18000, 20000, 22000]') AS npv
UNION ALL
SELECT 'B' AS plan,
       NPV(0.08, '[-30000, 10000, 12000, 12000, 10000]') AS npv;
```

### PV -- 现值

计算在给定利率和期数下，未来一系列等额支付的现值。用于评估当前需要投入多少才能获得未来的固定收入。

**参数：**
- `rate`：每期利率
- `nper`：总期数
- `pmt`：每期支付金额（流出为负，流入为正）

```sql
-- 计算年利率 5%、每年收入 1000 元、持续 10 年的现值
SELECT PV(0.05, 10, 1000) AS present_value;

-- 评估退休金：每月领取 5000 元，持续 20 年（月利率 0.4%）
SELECT PV(0.004, 240, 5000) AS retirement_fund;
```

### FV -- 终值

计算在给定利率和期数下，每期等额投入的未来总价值。用于规划储蓄和投资目标。

**参数：**
- `rate`：每期利率
- `nper`：总期数
- `pmt`：每期投入金额（流出为负）

```sql
-- 每月存入 1000 元，年利率 6%（月利率 0.5%），存 10 年的终值
SELECT FV(0.005, 120, -1000) AS future_value;

-- 每年投资 10000 元，年回报率 8%，20 年后的总价值
SELECT FV(0.08, 20, -10000) AS investment_value;
```

### PMT -- 每期还款金额

计算在给定利率和期数下，偿还贷款本金所需的每期等额还款金额。

**参数：**
- `rate`：每期利率
- `nper`：总期数
- `pv`：贷款本金（现值）

```sql
-- 贷款 500000 元，年利率 4.9%（月利率约 0.408%），分 30 年还清
SELECT PMT(0.049/12, 360, 500000) AS monthly_payment;

-- 不同贷款期限的月供对比
SELECT '10年' AS term, PMT(0.049/12, 120, 500000) AS monthly
UNION ALL
SELECT '20年' AS term, PMT(0.049/12, 240, 500000) AS monthly
UNION ALL
SELECT '30年' AS term, PMT(0.049/12, 360, 500000) AS monthly;
```

### RATE -- 每期利率

计算在给定期数、每期支付和现值下的每期利率。用于反推投资回报率或贷款实际利率。

**参数：**
- `nper`：总期数
- `pmt`：每期支付金额
- `pv`：现值（本金）

```sql
-- 贷款 80000 元，每月还 1000 元，分 10 年还清，计算月利率
SELECT RATE(120, -1000, 80000) AS monthly_rate;

-- 投资 10000 元，每年获得 1500 元回报，持续 10 年，计算年回报率
SELECT RATE(10, 1500, -10000) AS annual_return;
```

### NPER -- 还款期数

计算在给定利率和每期支付下，偿还贷款本金所需的期数。

**参数：**
- `rate`：每期利率
- `pmt`：每期支付金额
- `pv`：贷款本金（现值）

```sql
-- 贷款 100000 元，月利率 0.5%，每月还 2000 元，需要多少个月还清
SELECT NPER(0.005, -2000, 100000) AS months_needed;

-- 目标存款 100 万，年利率 5%，每年存 50000，需要多少年
SELECT NPER(0.05, -50000, 0) AS years_to_goal;
```

## 综合应用

```sql
-- 贷款方案综合分析
SELECT loan_amount,
       annual_rate,
       years,
       PMT(annual_rate/12, years*12, loan_amount) AS monthly_payment,
       PMT(annual_rate/12, years*12, loan_amount) * years * 12 AS total_payment,
       PMT(annual_rate/12, years*12, loan_amount) * years * 12 - loan_amount AS total_interest
FROM loan_plans;
```

## 注意事项

- 现金流方向：流入为正值，流出为负值。例如贷款本金为正（流入），还款金额为负（流出）。
- 利率与期数单位须一致：若期数为月，则利率应为月利率（年利率 / 12）。
- `NPV` 的 `values` 参数使用 JSON 数组字符串格式。
- 金融函数的计算结果为浮点数，实际应用中可配合 `ROUND` 函数保留合适的小数位。
