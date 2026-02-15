# Financial Functions

SQLExec provides a set of financial calculation functions for net present value, present value, future value, annuity, and other common financial analysis computations.

## Function List

| Function | Description | Example |
|----------|-------------|---------|
| `NPV(rate, values)` | Calculate net present value | `SELECT NPV(0.1, '[-1000, 300, 400, 500]');` |
| `PV(rate, nper, pmt)` | Calculate present value | `SELECT PV(0.05, 10, -1000);` |
| `FV(rate, nper, pmt)` | Calculate future value | `SELECT FV(0.05, 10, -1000);` |
| `PMT(rate, nper, pv)` | Calculate periodic payment amount | `SELECT PMT(0.05, 10, 10000);` |
| `RATE(nper, pmt, pv)` | Calculate the periodic interest rate | `SELECT RATE(10, -1000, 8000);` |
| `NPER(rate, pmt, pv)` | Calculate the number of payment periods | `SELECT NPER(0.05, -1000, 8000);` |

## Detailed Description

### NPV -- Net Present Value

Calculates the net present value of a series of future cash flows at a given discount rate. A positive NPV indicates a viable investment.

**Parameters:**
- `rate`: Discount rate per period
- `values`: Cash flow array (string format); the first value is typically the initial investment (negative)

```sql
-- Evaluate an investment project: initial investment of 10000, cash flows over 4 years
SELECT NPV(0.1, '[-10000, 3000, 4000, 4000, 3000]') AS npv;
-- Result is approximately 1154.47; NPV > 0, investment is viable

-- Compare two investment plans
SELECT 'A' AS plan,
       NPV(0.08, '[-50000, 15000, 18000, 20000, 22000]') AS npv
UNION ALL
SELECT 'B' AS plan,
       NPV(0.08, '[-30000, 10000, 12000, 12000, 10000]') AS npv;
```

### PV -- Present Value

Calculates the present value of a series of equal periodic payments at a given interest rate and number of periods. Used to evaluate how much needs to be invested now to receive a fixed income in the future.

**Parameters:**
- `rate`: Interest rate per period
- `nper`: Total number of periods
- `pmt`: Payment amount per period (outflow is negative, inflow is positive)

```sql
-- Calculate the present value of receiving 1000 per year for 10 years at 5% annual interest
SELECT PV(0.05, 10, 1000) AS present_value;

-- Evaluate a pension: receiving 5000 per month for 20 years (monthly rate 0.4%)
SELECT PV(0.004, 240, 5000) AS retirement_fund;
```

### FV -- Future Value

Calculates the total future value of equal periodic contributions at a given interest rate and number of periods. Used for planning savings and investment goals.

**Parameters:**
- `rate`: Interest rate per period
- `nper`: Total number of periods
- `pmt`: Contribution amount per period (outflow is negative)

```sql
-- Deposit 1000 per month at 6% annual interest (0.5% monthly rate) for 10 years
SELECT FV(0.005, 120, -1000) AS future_value;

-- Invest 10000 per year at 8% annual return for 20 years
SELECT FV(0.08, 20, -10000) AS investment_value;
```

### PMT -- Periodic Payment Amount

Calculates the equal periodic payment required to repay a loan principal at a given interest rate and number of periods.

**Parameters:**
- `rate`: Interest rate per period
- `nper`: Total number of periods
- `pv`: Loan principal (present value)

```sql
-- Loan of 500000 at 4.9% annual interest (approximately 0.408% monthly), repaid over 30 years
SELECT PMT(0.049/12, 360, 500000) AS monthly_payment;

-- Compare monthly payments across different loan terms
SELECT '10 years' AS term, PMT(0.049/12, 120, 500000) AS monthly
UNION ALL
SELECT '20 years' AS term, PMT(0.049/12, 240, 500000) AS monthly
UNION ALL
SELECT '30 years' AS term, PMT(0.049/12, 360, 500000) AS monthly;
```

### RATE -- Periodic Interest Rate

Calculates the periodic interest rate given the number of periods, periodic payment, and present value. Used to determine the actual return rate on an investment or the effective interest rate on a loan.

**Parameters:**
- `nper`: Total number of periods
- `pmt`: Payment amount per period
- `pv`: Present value (principal)

```sql
-- Loan of 80000, monthly payment of 1000, repaid over 10 years; calculate monthly rate
SELECT RATE(120, -1000, 80000) AS monthly_rate;

-- Investment of 10000 with annual return of 1500 for 10 years; calculate annual return rate
SELECT RATE(10, 1500, -10000) AS annual_return;
```

### NPER -- Number of Payment Periods

Calculates the number of periods required to repay a loan principal at a given interest rate and periodic payment.

**Parameters:**
- `rate`: Interest rate per period
- `pmt`: Payment amount per period
- `pv`: Loan principal (present value)

```sql
-- Loan of 100000 at 0.5% monthly rate, paying 2000 per month; how many months to repay
SELECT NPER(0.005, -2000, 100000) AS months_needed;

-- Target savings of 1,000,000 at 5% annual rate, saving 50000 per year; how many years needed
SELECT NPER(0.05, -50000, 0) AS years_to_goal;
```

## Comprehensive Examples

```sql
-- Comprehensive loan plan analysis
SELECT loan_amount,
       annual_rate,
       years,
       PMT(annual_rate/12, years*12, loan_amount) AS monthly_payment,
       PMT(annual_rate/12, years*12, loan_amount) * years * 12 AS total_payment,
       PMT(annual_rate/12, years*12, loan_amount) * years * 12 - loan_amount AS total_interest
FROM loan_plans;
```

## Notes

- Cash flow direction: inflows are positive, outflows are negative. For example, a loan principal is positive (inflow), and repayment amounts are negative (outflow).
- The rate and period units must be consistent: if periods are in months, then the rate should be a monthly rate (annual rate / 12).
- The `values` parameter of `NPV` uses JSON array string format.
- Financial function results are floating-point numbers. In practice, use the `ROUND` function to retain an appropriate number of decimal places.
