package builtin

import (
	"fmt"
	"math"
	"time"
)

func init() {
	financialFunctions := []*FunctionInfo{
		// ============ 货币时间价值 (Time Value of Money) ============
		{
			Name: "fv",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "fv", ReturnType: "number", ParamTypes: []string{"number", "number", "number"}, Variadic: true},
			},
			Handler:     financialFV,
			Description: "计算未来值 (Future Value)",
			Example:     "FV(0.05, 10, -100) -> 1257.79",
			Category:    "financial",
		},
		{
			Name: "pv",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "pv", ReturnType: "number", ParamTypes: []string{"number", "number", "number"}, Variadic: true},
			},
			Handler:     financialPV,
			Description: "计算现值 (Present Value)",
			Example:     "PV(0.05, 10, -100) -> 772.17",
			Category:    "financial",
		},
		{
			Name: "pmt",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "pmt", ReturnType: "number", ParamTypes: []string{"number", "number", "number"}, Variadic: true},
			},
			Handler:     financialPMT,
			Description: "计算每期还款额 (Payment)",
			Example:     "PMT(0.05, 10, -1000) -> 129.50",
			Category:    "financial",
		},
		{
			Name: "ipmt",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "ipmt", ReturnType: "number", ParamTypes: []string{"number", "number", "number", "number"}, Variadic: true},
			},
			Handler:     financialIPMT,
			Description: "计算某期利息部分 (Interest Payment)",
			Example:     "IPMT(0.05, 1, 10, -1000) -> 50.00",
			Category:    "financial",
		},
		{
			Name: "ppmt",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "ppmt", ReturnType: "number", ParamTypes: []string{"number", "number", "number", "number"}, Variadic: true},
			},
			Handler:     financialPPMT,
			Description: "计算某期本金部分 (Principal Payment)",
			Example:     "PPMT(0.05, 1, 10, -1000) -> 79.50",
			Category:    "financial",
		},
		{
			Name: "npv",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "npv", ReturnType: "number", ParamTypes: []string{"number", "number"}, Variadic: true},
			},
			Handler:     financialNPV,
			Description: "计算净现值 (Net Present Value)",
			Example:     "NPV(0.1, -1000, 300, 400, 500) -> 48.42",
			Category:    "financial",
		},
		{
			Name: "irr",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "irr", ReturnType: "number", ParamTypes: []string{"string"}, Variadic: true},
			},
			Handler:     financialIRR,
			Description: "计算内部收益率 (Internal Rate of Return)",
			Example:     "IRR('-1000,300,400,500,200') -> 0.1065",
			Category:    "financial",
		},
		{
			Name: "xirr",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "xirr", ReturnType: "number", ParamTypes: []string{"string", "string"}, Variadic: true},
			},
			Handler:     financialXIRR,
			Description: "计算非等间隔现金流内部收益率",
			Example:     "XIRR('-1000,500,600', '2024-01-01,2024-07-01,2025-01-01') -> 0.0886",
			Category:    "financial",
		},
		{
			Name: "rate",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "rate", ReturnType: "number", ParamTypes: []string{"number", "number", "number"}, Variadic: true},
			},
			Handler:     financialRATE,
			Description: "计算每期利率",
			Example:     "RATE(10, -100, 800) -> 0.0435",
			Category:    "financial",
		},
		{
			Name: "nper",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "nper", ReturnType: "number", ParamTypes: []string{"number", "number", "number"}, Variadic: true},
			},
			Handler:     financialNPER,
			Description: "计算期数",
			Example:     "NPER(0.05, -100, 800) -> 11.05",
			Category:    "financial",
		},
		// ============ 折旧 (Depreciation) ============
		{
			Name: "sln",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "sln", ReturnType: "number", ParamTypes: []string{"number", "number", "number"}, Variadic: false},
			},
			Handler:     financialSLN,
			Description: "直线折旧 (Straight-Line Depreciation)",
			Example:     "SLN(10000, 1000, 5) -> 1800",
			Category:    "financial",
		},
		{
			Name: "syd",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "syd", ReturnType: "number", ParamTypes: []string{"number", "number", "number", "number"}, Variadic: false},
			},
			Handler:     financialSYD,
			Description: "年数总和折旧 (Sum-of-Years-Digits Depreciation)",
			Example:     "SYD(10000, 1000, 5, 1) -> 3000",
			Category:    "financial",
		},
		{
			Name: "ddb",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "ddb", ReturnType: "number", ParamTypes: []string{"number", "number", "number", "number"}, Variadic: true},
			},
			Handler:     financialDDB,
			Description: "双倍余额递减折旧 (Double Declining Balance)",
			Example:     "DDB(10000, 1000, 5, 1) -> 4000",
			Category:    "financial",
		},
		// ============ 银行舍入 (Banking Rounding) ============
		{
			Name: "round_banker",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "round_banker", ReturnType: "number", ParamTypes: []string{"number", "number"}, Variadic: false},
			},
			Handler:     financialRoundBanker,
			Description: "银行家舍入法（四舍六入五成双）",
			Example:     "ROUND_BANKER(2.5, 0) -> 2",
			Category:    "financial",
		},
		{
			Name: "round_currency",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "round_currency", ReturnType: "number", ParamTypes: []string{"number", "number"}, Variadic: false},
			},
			Handler:     financialRoundCurrency,
			Description: "货币舍入（四舍五入）",
			Example:     "ROUND_CURRENCY(2.5, 0) -> 3",
			Category:    "financial",
		},
		{
			Name: "round_truncate",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "round_truncate", ReturnType: "number", ParamTypes: []string{"number", "number"}, Variadic: false},
			},
			Handler:     financialRoundTruncate,
			Description: "截断式舍入（向零截断）",
			Example:     "ROUND_TRUNCATE(2.9, 0) -> 2",
			Category:    "financial",
		},
		// ============ 债券与收益计算 (Bond & Yield) ============
		{
			Name: "bond_price",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "bond_price", ReturnType: "number", ParamTypes: []string{"number", "number", "number", "number", "number"}, Variadic: false},
			},
			Handler:     financialBondPrice,
			Description: "债券价格（折现现金流）",
			Example:     "BOND_PRICE(1000, 0.05, 0.06, 10, 2) -> 925.61",
			Category:    "financial",
		},
		{
			Name: "bond_yield",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "bond_yield", ReturnType: "number", ParamTypes: []string{"number", "number", "number", "number", "number"}, Variadic: true},
			},
			Handler:     financialBondYield,
			Description: "债券到期收益率 YTM",
			Example:     "BOND_YIELD(1000, 0.05, 925.61, 10, 2) -> 0.06",
			Category:    "financial",
		},
		{
			Name: "bond_duration",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "bond_duration", ReturnType: "number", ParamTypes: []string{"number", "number", "number", "number", "number"}, Variadic: false},
			},
			Handler:     financialBondDuration,
			Description: "Macaulay 久期",
			Example:     "BOND_DURATION(1000, 0.05, 0.06, 10, 2) -> 7.80",
			Category:    "financial",
		},
		{
			Name: "bond_mduration",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "bond_mduration", ReturnType: "number", ParamTypes: []string{"number", "number", "number", "number", "number"}, Variadic: false},
			},
			Handler:     financialBondMDuration,
			Description: "修正久期 (Modified Duration)",
			Example:     "BOND_MDURATION(1000, 0.05, 0.06, 10, 2) -> 7.57",
			Category:    "financial",
		},
		{
			Name: "bond_convexity",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "bond_convexity", ReturnType: "number", ParamTypes: []string{"number", "number", "number", "number", "number"}, Variadic: false},
			},
			Handler:     financialBondConvexity,
			Description: "凸度（利率风险二阶度量）",
			Example:     "BOND_CONVEXITY(1000, 0.05, 0.06, 10, 2) -> 73.96",
			Category:    "financial",
		},
		// ============ 基础金融计算 ============
		{
			Name: "compound_interest",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "compound_interest", ReturnType: "number", ParamTypes: []string{"number", "number", "number", "number"}, Variadic: false},
			},
			Handler:     financialCompoundInterest,
			Description: "复利计算 A=P(1+r/n)^(nt)",
			Example:     "COMPOUND_INTEREST(1000, 0.05, 12, 10) -> 1647.01",
			Category:    "financial",
		},
		{
			Name: "simple_interest",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "simple_interest", ReturnType: "number", ParamTypes: []string{"number", "number", "number"}, Variadic: false},
			},
			Handler:     financialSimpleInterest,
			Description: "单利计算 I=P*r*t",
			Example:     "SIMPLE_INTEREST(1000, 0.05, 3) -> 150",
			Category:    "financial",
		},
		{
			Name: "cagr",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "cagr", ReturnType: "number", ParamTypes: []string{"number", "number", "number"}, Variadic: false},
			},
			Handler:     financialCAGR,
			Description: "年均复合增长率 (Compound Annual Growth Rate)",
			Example:     "CAGR(1000, 2000, 5) -> 0.1487",
			Category:    "financial",
		},
		{
			Name: "roi",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "roi", ReturnType: "number", ParamTypes: []string{"number", "number"}, Variadic: false},
			},
			Handler:     financialROI,
			Description: "投资回报率 (Return on Investment) (%)",
			Example:     "ROI(500, 1000) -> 50",
			Category:    "financial",
		},
	}

	for _, fn := range financialFunctions {
		RegisterGlobal(fn)
	}
}

// ============ 内部辅助函数 ============

// computePMT computes PMT (shared by PMT, IPMT, PPMT)
func computePMT(rate, nper, pv, fv, pmtType float64) float64 {
	if rate == 0 {
		return -(pv + fv) / nper
	}
	pvif := math.Pow(1+rate, nper)
	return (-pv*pvif - fv) / ((1 + rate*pmtType) * (pvif - 1) / rate)
}

// parseCashFlows parses a comma-separated string or slice into []float64
func parseCashFlows(arg interface{}) ([]float64, error) {
	switch v := arg.(type) {
	case string:
		return parseCashFlowString(v), nil
	case []interface{}:
		result := make([]float64, len(v))
		for i, item := range v {
			val, err := toFloat64(item)
			if err != nil {
				return nil, fmt.Errorf("invalid cash flow at index %d: %w", i, err)
			}
			result[i] = val
		}
		return result, nil
	default:
		return nil, fmt.Errorf("cash flows must be a comma-separated string or array, got %T", arg)
	}
}

// parseCashFlowString parses "v1,v2,v3" into []float64
func parseCashFlowString(s string) []float64 {
	var result []float64
	start := 0
	for i := 0; i <= len(s); i++ {
		if i == len(s) || s[i] == ',' {
			token := trimSpaces(s[start:i])
			if token != "" {
				val, err := toFloat64(token)
				if err == nil {
					result = append(result, val)
				}
			}
			start = i + 1
		}
	}
	return result
}

// parseDateStrings parses a comma-separated date string into []time.Time
func parseDateStrings(arg interface{}) ([]time.Time, error) {
	s, ok := arg.(string)
	if !ok {
		return nil, fmt.Errorf("dates must be a comma-separated string, got %T", arg)
	}

	var result []time.Time
	start := 0
	for i := 0; i <= len(s); i++ {
		if i == len(s) || s[i] == ',' {
			token := trimSpaces(s[start:i])
			if token != "" {
				t, err := time.Parse("2006-01-02", token)
				if err != nil {
					return nil, fmt.Errorf("invalid date %q: %w", token, err)
				}
				result = append(result, t)
			}
			start = i + 1
		}
	}
	return result, nil
}

// trimSpaces trims leading and trailing spaces from a string
func trimSpaces(s string) string {
	start, end := 0, len(s)
	for start < end && s[start] == ' ' {
		start++
	}
	for end > start && s[end-1] == ' ' {
		end--
	}
	return s[start:end]
}

// computeBondPrice computes bond price given parameters
func computeBondPrice(face, couponRate, ytm float64, years, freq int) float64 {
	n := years * freq
	coupon := face * couponRate / float64(freq)
	yPerPeriod := ytm / float64(freq)

	price := 0.0
	for t := 1; t <= n; t++ {
		price += coupon / math.Pow(1+yPerPeriod, float64(t))
	}
	price += face / math.Pow(1+yPerPeriod, float64(n))
	return price
}

// ============ 货币时间价值函数实现 ============

// financialFV: FV(rate, nper, pmt [, pv [, type]])
func financialFV(args []interface{}) (interface{}, error) {
	if len(args) < 3 || len(args) > 5 {
		return nil, fmt.Errorf("FV() requires 3-5 arguments, got %d", len(args))
	}
	rate, err := toFloat64(args[0])
	if err != nil {
		return nil, fmt.Errorf("FV() rate: %w", err)
	}
	nper, err := toFloat64(args[1])
	if err != nil {
		return nil, fmt.Errorf("FV() nper: %w", err)
	}
	pmt, err := toFloat64(args[2])
	if err != nil {
		return nil, fmt.Errorf("FV() pmt: %w", err)
	}
	pv := 0.0
	if len(args) >= 4 {
		pv, err = toFloat64(args[3])
		if err != nil {
			return nil, fmt.Errorf("FV() pv: %w", err)
		}
	}
	pmtType := 0.0
	if len(args) >= 5 {
		pmtType, err = toFloat64(args[4])
		if err != nil {
			return nil, fmt.Errorf("FV() type: %w", err)
		}
	}

	if rate == 0 {
		return -(pv + pmt*nper), nil
	}
	pvif := math.Pow(1+rate, nper)
	return -pv*pvif - pmt*(1+rate*pmtType)*(pvif-1)/rate, nil
}

// financialPV: PV(rate, nper, pmt [, fv [, type]])
func financialPV(args []interface{}) (interface{}, error) {
	if len(args) < 3 || len(args) > 5 {
		return nil, fmt.Errorf("PV() requires 3-5 arguments, got %d", len(args))
	}
	rate, err := toFloat64(args[0])
	if err != nil {
		return nil, fmt.Errorf("PV() rate: %w", err)
	}
	nper, err := toFloat64(args[1])
	if err != nil {
		return nil, fmt.Errorf("PV() nper: %w", err)
	}
	pmt, err := toFloat64(args[2])
	if err != nil {
		return nil, fmt.Errorf("PV() pmt: %w", err)
	}
	fv := 0.0
	if len(args) >= 4 {
		fv, err = toFloat64(args[3])
		if err != nil {
			return nil, fmt.Errorf("PV() fv: %w", err)
		}
	}
	pmtType := 0.0
	if len(args) >= 5 {
		pmtType, err = toFloat64(args[4])
		if err != nil {
			return nil, fmt.Errorf("PV() type: %w", err)
		}
	}

	if rate == 0 {
		return -(fv + pmt*nper), nil
	}
	pvif := math.Pow(1+rate, nper)
	return (-fv - pmt*(1+rate*pmtType)*(pvif-1)/rate) / pvif, nil
}

// financialPMT: PMT(rate, nper, pv [, fv [, type]])
func financialPMT(args []interface{}) (interface{}, error) {
	if len(args) < 3 || len(args) > 5 {
		return nil, fmt.Errorf("PMT() requires 3-5 arguments, got %d", len(args))
	}
	rate, err := toFloat64(args[0])
	if err != nil {
		return nil, fmt.Errorf("PMT() rate: %w", err)
	}
	nper, err := toFloat64(args[1])
	if err != nil {
		return nil, fmt.Errorf("PMT() nper: %w", err)
	}
	if nper == 0 {
		return nil, fmt.Errorf("PMT() nper cannot be zero")
	}
	pv, err := toFloat64(args[2])
	if err != nil {
		return nil, fmt.Errorf("PMT() pv: %w", err)
	}
	fv := 0.0
	if len(args) >= 4 {
		fv, err = toFloat64(args[3])
		if err != nil {
			return nil, fmt.Errorf("PMT() fv: %w", err)
		}
	}
	pmtType := 0.0
	if len(args) >= 5 {
		pmtType, err = toFloat64(args[4])
		if err != nil {
			return nil, fmt.Errorf("PMT() type: %w", err)
		}
	}

	return computePMT(rate, nper, pv, fv, pmtType), nil
}

// financialIPMT: IPMT(rate, per, nper, pv [, fv [, type]])
func financialIPMT(args []interface{}) (interface{}, error) {
	if len(args) < 4 || len(args) > 6 {
		return nil, fmt.Errorf("IPMT() requires 4-6 arguments, got %d", len(args))
	}
	rate, err := toFloat64(args[0])
	if err != nil {
		return nil, fmt.Errorf("IPMT() rate: %w", err)
	}
	per, err := toFloat64(args[1])
	if err != nil {
		return nil, fmt.Errorf("IPMT() per: %w", err)
	}
	nper, err := toFloat64(args[2])
	if err != nil {
		return nil, fmt.Errorf("IPMT() nper: %w", err)
	}
	if nper == 0 {
		return nil, fmt.Errorf("IPMT() nper cannot be zero")
	}
	if per < 1 || per > nper {
		return nil, fmt.Errorf("IPMT() per must be between 1 and nper")
	}
	pv, err := toFloat64(args[3])
	if err != nil {
		return nil, fmt.Errorf("IPMT() pv: %w", err)
	}
	fv := 0.0
	if len(args) >= 5 {
		fv, err = toFloat64(args[4])
		if err != nil {
			return nil, fmt.Errorf("IPMT() fv: %w", err)
		}
	}
	pmtType := 0.0
	if len(args) >= 6 {
		pmtType, err = toFloat64(args[5])
		if err != nil {
			return nil, fmt.Errorf("IPMT() type: %w", err)
		}
	}

	pmt := computePMT(rate, nper, pv, fv, pmtType)

	var ipmt float64
	if pmtType == 1 {
		// Beginning of period
		if per == 1 {
			ipmt = 0
		} else {
			ipmt = (financialFVInternal(rate, per-2, pmt, pv, 1) - pmt) * rate
		}
	} else {
		ipmt = financialFVInternal(rate, per-1, pmt, pv, 0) * rate
	}
	return ipmt, nil
}

// financialFVInternal is internal FV calculation
func financialFVInternal(rate, nper, pmt, pv, pmtType float64) float64 {
	if rate == 0 {
		return -(pv + pmt*nper)
	}
	pvif := math.Pow(1+rate, nper)
	return -pv*pvif - pmt*(1+rate*pmtType)*(pvif-1)/rate
}

// financialPPMT: PPMT(rate, per, nper, pv [, fv [, type]])
func financialPPMT(args []interface{}) (interface{}, error) {
	if len(args) < 4 || len(args) > 6 {
		return nil, fmt.Errorf("PPMT() requires 4-6 arguments, got %d", len(args))
	}
	rate, err := toFloat64(args[0])
	if err != nil {
		return nil, fmt.Errorf("PPMT() rate: %w", err)
	}
	nper, err := toFloat64(args[2])
	if err != nil {
		return nil, fmt.Errorf("PPMT() nper: %w", err)
	}
	if nper == 0 {
		return nil, fmt.Errorf("PPMT() nper cannot be zero")
	}

	fv := 0.0
	if len(args) >= 5 {
		fv, err = toFloat64(args[4])
		if err != nil {
			return nil, fmt.Errorf("PPMT() fv: %w", err)
		}
	}
	pmtType := 0.0
	if len(args) >= 6 {
		pmtType, err = toFloat64(args[5])
		if err != nil {
			return nil, fmt.Errorf("PPMT() type: %w", err)
		}
	}
	pv, err := toFloat64(args[3])
	if err != nil {
		return nil, fmt.Errorf("PPMT() pv: %w", err)
	}

	pmt := computePMT(rate, nper, pv, fv, pmtType)

	// Get IPMT by calling financialIPMT
	ipmtResult, err := financialIPMT(args)
	if err != nil {
		return nil, err
	}
	ipmt, _ := toFloat64(ipmtResult)

	return pmt - ipmt, nil
}

// financialNPV: NPV(rate, value1, value2, ...)
func financialNPV(args []interface{}) (interface{}, error) {
	if len(args) < 2 {
		return nil, fmt.Errorf("NPV() requires at least 2 arguments, got %d", len(args))
	}
	rate, err := toFloat64(args[0])
	if err != nil {
		return nil, fmt.Errorf("NPV() rate: %w", err)
	}

	npv := 0.0
	for i := 1; i < len(args); i++ {
		val, err := toFloat64(args[i])
		if err != nil {
			return nil, fmt.Errorf("NPV() value%d: %w", i, err)
		}
		npv += val / math.Pow(1+rate, float64(i))
	}

	return npv, nil
}

// financialIRR: IRR(cash_flows_string [, guess])
func financialIRR(args []interface{}) (interface{}, error) {
	if len(args) < 1 || len(args) > 2 {
		return nil, fmt.Errorf("IRR() requires 1-2 arguments, got %d", len(args))
	}

	cashFlows, err := parseCashFlows(args[0])
	if err != nil {
		return nil, fmt.Errorf("IRR() %w", err)
	}
	if len(cashFlows) < 2 {
		return nil, fmt.Errorf("IRR() requires at least 2 cash flows")
	}

	guess := 0.1
	if len(args) >= 2 {
		guess, err = toFloat64(args[1])
		if err != nil {
			return nil, fmt.Errorf("IRR() guess: %w", err)
		}
	}

	rate := guess
	for iter := 0; iter < 100; iter++ {
		f := 0.0
		df := 0.0
		for i, cf := range cashFlows {
			f += cf / math.Pow(1+rate, float64(i))
			if i > 0 {
				df -= float64(i) * cf / math.Pow(1+rate, float64(i+1))
			}
		}
		if math.Abs(df) < 1e-14 {
			return nil, fmt.Errorf("IRR() derivative too small, cannot converge")
		}
		newRate := rate - f/df
		if math.Abs(newRate-rate) < 1e-7 {
			return newRate, nil
		}
		rate = newRate
	}

	return nil, fmt.Errorf("IRR() did not converge after 100 iterations")
}

// financialXIRR: XIRR(cash_flows, dates [, guess])
func financialXIRR(args []interface{}) (interface{}, error) {
	if len(args) < 2 || len(args) > 3 {
		return nil, fmt.Errorf("XIRR() requires 2-3 arguments, got %d", len(args))
	}

	cashFlows, err := parseCashFlows(args[0])
	if err != nil {
		return nil, fmt.Errorf("XIRR() cash flows: %w", err)
	}
	dates, err := parseDateStrings(args[1])
	if err != nil {
		return nil, fmt.Errorf("XIRR() dates: %w", err)
	}
	if len(cashFlows) != len(dates) {
		return nil, fmt.Errorf("XIRR() cash flows count (%d) must match dates count (%d)", len(cashFlows), len(dates))
	}
	if len(cashFlows) < 2 {
		return nil, fmt.Errorf("XIRR() requires at least 2 cash flows")
	}

	guess := 0.1
	if len(args) >= 3 {
		guess, err = toFloat64(args[2])
		if err != nil {
			return nil, fmt.Errorf("XIRR() guess: %w", err)
		}
	}

	t0 := dates[0]
	rate := guess

	for iter := 0; iter < 100; iter++ {
		f := 0.0
		df := 0.0
		for i, cf := range cashFlows {
			years := dates[i].Sub(t0).Hours() / (24 * 365)
			denom := math.Pow(1+rate, years)
			f += cf / denom
			if years != 0 {
				df -= years * cf / math.Pow(1+rate, years+1)
			}
		}
		if math.Abs(df) < 1e-14 {
			return nil, fmt.Errorf("XIRR() derivative too small, cannot converge")
		}
		newRate := rate - f/df
		if math.Abs(newRate-rate) < 1e-7 {
			return newRate, nil
		}
		rate = newRate
	}

	return nil, fmt.Errorf("XIRR() did not converge after 100 iterations")
}

// financialRATE: RATE(nper, pmt, pv [, fv [, type [, guess]]])
func financialRATE(args []interface{}) (interface{}, error) {
	if len(args) < 3 || len(args) > 6 {
		return nil, fmt.Errorf("RATE() requires 3-6 arguments, got %d", len(args))
	}
	nper, err := toFloat64(args[0])
	if err != nil {
		return nil, fmt.Errorf("RATE() nper: %w", err)
	}
	if nper == 0 {
		return nil, fmt.Errorf("RATE() nper cannot be zero")
	}
	pmt, err := toFloat64(args[1])
	if err != nil {
		return nil, fmt.Errorf("RATE() pmt: %w", err)
	}
	pv, err := toFloat64(args[2])
	if err != nil {
		return nil, fmt.Errorf("RATE() pv: %w", err)
	}
	fv := 0.0
	if len(args) >= 4 {
		fv, err = toFloat64(args[3])
		if err != nil {
			return nil, fmt.Errorf("RATE() fv: %w", err)
		}
	}
	pmtType := 0.0
	if len(args) >= 5 {
		pmtType, err = toFloat64(args[4])
		if err != nil {
			return nil, fmt.Errorf("RATE() type: %w", err)
		}
	}
	guess := 0.1
	if len(args) >= 6 {
		guess, err = toFloat64(args[5])
		if err != nil {
			return nil, fmt.Errorf("RATE() guess: %w", err)
		}
	}

	rate := guess
	for iter := 0; iter < 100; iter++ {
		pvif := math.Pow(1+rate, nper)
		y := pv*pvif + pmt*(1+rate*pmtType)*(pvif-1)/rate + fv
		// derivative
		dpvif := nper * math.Pow(1+rate, nper-1)
		dy := pv*dpvif + pmt*pmtType*(pvif-1)/rate + pmt*(1+rate*pmtType)*(dpvif*rate-(pvif-1))/(rate*rate)
		if math.Abs(dy) < 1e-14 {
			return nil, fmt.Errorf("RATE() derivative too small, cannot converge")
		}
		newRate := rate - y/dy
		if math.Abs(newRate-rate) < 1e-7 {
			return newRate, nil
		}
		rate = newRate
	}

	return nil, fmt.Errorf("RATE() did not converge after 100 iterations")
}

// financialNPER: NPER(rate, pmt, pv [, fv [, type]])
func financialNPER(args []interface{}) (interface{}, error) {
	if len(args) < 3 || len(args) > 5 {
		return nil, fmt.Errorf("NPER() requires 3-5 arguments, got %d", len(args))
	}
	rate, err := toFloat64(args[0])
	if err != nil {
		return nil, fmt.Errorf("NPER() rate: %w", err)
	}
	pmt, err := toFloat64(args[1])
	if err != nil {
		return nil, fmt.Errorf("NPER() pmt: %w", err)
	}
	pv, err := toFloat64(args[2])
	if err != nil {
		return nil, fmt.Errorf("NPER() pv: %w", err)
	}
	fv := 0.0
	if len(args) >= 4 {
		fv, err = toFloat64(args[3])
		if err != nil {
			return nil, fmt.Errorf("NPER() fv: %w", err)
		}
	}
	pmtType := 0.0
	if len(args) >= 5 {
		pmtType, err = toFloat64(args[4])
		if err != nil {
			return nil, fmt.Errorf("NPER() type: %w", err)
		}
	}

	if rate == 0 {
		if pmt == 0 {
			return nil, fmt.Errorf("NPER() pmt cannot be zero when rate is zero")
		}
		return -(pv + fv) / pmt, nil
	}

	z := pmt * (1 + rate*pmtType) / rate
	num := -fv + z
	den := pv + z
	if den == 0 || num/den <= 0 {
		return nil, fmt.Errorf("NPER() cannot compute: invalid parameters")
	}
	return math.Log(num/den) / math.Log(1+rate), nil
}

// ============ 折旧函数实现 ============

// financialSLN: SLN(cost, salvage, life)
func financialSLN(args []interface{}) (interface{}, error) {
	if len(args) != 3 {
		return nil, fmt.Errorf("SLN() requires exactly 3 arguments, got %d", len(args))
	}
	cost, err := toFloat64(args[0])
	if err != nil {
		return nil, fmt.Errorf("SLN() cost: %w", err)
	}
	salvage, err := toFloat64(args[1])
	if err != nil {
		return nil, fmt.Errorf("SLN() salvage: %w", err)
	}
	life, err := toFloat64(args[2])
	if err != nil {
		return nil, fmt.Errorf("SLN() life: %w", err)
	}
	if life == 0 {
		return nil, fmt.Errorf("SLN() life cannot be zero")
	}
	return (cost - salvage) / life, nil
}

// financialSYD: SYD(cost, salvage, life, per)
func financialSYD(args []interface{}) (interface{}, error) {
	if len(args) != 4 {
		return nil, fmt.Errorf("SYD() requires exactly 4 arguments, got %d", len(args))
	}
	cost, err := toFloat64(args[0])
	if err != nil {
		return nil, fmt.Errorf("SYD() cost: %w", err)
	}
	salvage, err := toFloat64(args[1])
	if err != nil {
		return nil, fmt.Errorf("SYD() salvage: %w", err)
	}
	life, err := toFloat64(args[2])
	if err != nil {
		return nil, fmt.Errorf("SYD() life: %w", err)
	}
	per, err := toFloat64(args[3])
	if err != nil {
		return nil, fmt.Errorf("SYD() per: %w", err)
	}
	if life <= 0 {
		return nil, fmt.Errorf("SYD() life must be positive")
	}
	if per < 1 || per > life {
		return nil, fmt.Errorf("SYD() per must be between 1 and life")
	}
	sumYears := life * (life + 1) / 2
	return (cost - salvage) * (life - per + 1) / sumYears, nil
}

// financialDDB: DDB(cost, salvage, life, per [, factor])
func financialDDB(args []interface{}) (interface{}, error) {
	if len(args) < 4 || len(args) > 5 {
		return nil, fmt.Errorf("DDB() requires 4-5 arguments, got %d", len(args))
	}
	cost, err := toFloat64(args[0])
	if err != nil {
		return nil, fmt.Errorf("DDB() cost: %w", err)
	}
	salvage, err := toFloat64(args[1])
	if err != nil {
		return nil, fmt.Errorf("DDB() salvage: %w", err)
	}
	life, err := toFloat64(args[2])
	if err != nil {
		return nil, fmt.Errorf("DDB() life: %w", err)
	}
	per, err := toFloat64(args[3])
	if err != nil {
		return nil, fmt.Errorf("DDB() per: %w", err)
	}
	factor := 2.0
	if len(args) >= 5 {
		factor, err = toFloat64(args[4])
		if err != nil {
			return nil, fmt.Errorf("DDB() factor: %w", err)
		}
	}

	if life <= 0 {
		return nil, fmt.Errorf("DDB() life must be positive")
	}
	if per < 1 || per > life {
		return nil, fmt.Errorf("DDB() per must be between 1 and life")
	}

	rate := factor / life
	bookValue := cost
	for i := 1.0; i < per; i++ {
		depreciation := bookValue * rate
		if bookValue-depreciation < salvage {
			depreciation = bookValue - salvage
		}
		bookValue -= depreciation
	}
	depreciation := bookValue * rate
	if bookValue-depreciation < salvage {
		depreciation = bookValue - salvage
	}
	if depreciation < 0 {
		depreciation = 0
	}
	return depreciation, nil
}

// ============ 银行舍入函数实现 ============

// financialRoundBanker: ROUND_BANKER(value, decimals) — 四舍六入五成双
func financialRoundBanker(args []interface{}) (interface{}, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("ROUND_BANKER() requires exactly 2 arguments, got %d", len(args))
	}
	value, err := toFloat64(args[0])
	if err != nil {
		return nil, fmt.Errorf("ROUND_BANKER() value: %w", err)
	}
	decimals, err := toFloat64(args[1])
	if err != nil {
		return nil, fmt.Errorf("ROUND_BANKER() decimals: %w", err)
	}
	d := int(decimals)
	multiplier := math.Pow(10, float64(d))
	return math.RoundToEven(value*multiplier) / multiplier, nil
}

// financialRoundCurrency: ROUND_CURRENCY(value, decimals) — 四舍五入
func financialRoundCurrency(args []interface{}) (interface{}, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("ROUND_CURRENCY() requires exactly 2 arguments, got %d", len(args))
	}
	value, err := toFloat64(args[0])
	if err != nil {
		return nil, fmt.Errorf("ROUND_CURRENCY() value: %w", err)
	}
	decimals, err := toFloat64(args[1])
	if err != nil {
		return nil, fmt.Errorf("ROUND_CURRENCY() decimals: %w", err)
	}
	d := int(decimals)
	multiplier := math.Pow(10, float64(d))
	return math.Round(value*multiplier) / multiplier, nil
}

// financialRoundTruncate: ROUND_TRUNCATE(value, decimals) — 向零截断
func financialRoundTruncate(args []interface{}) (interface{}, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("ROUND_TRUNCATE() requires exactly 2 arguments, got %d", len(args))
	}
	value, err := toFloat64(args[0])
	if err != nil {
		return nil, fmt.Errorf("ROUND_TRUNCATE() value: %w", err)
	}
	decimals, err := toFloat64(args[1])
	if err != nil {
		return nil, fmt.Errorf("ROUND_TRUNCATE() decimals: %w", err)
	}
	d := int(decimals)
	multiplier := math.Pow(10, float64(d))
	return math.Trunc(value*multiplier) / multiplier, nil
}

// ============ 债券与收益计算函数实现 ============

// financialBondPrice: BOND_PRICE(face, coupon_rate, ytm, years, freq)
func financialBondPrice(args []interface{}) (interface{}, error) {
	if len(args) != 5 {
		return nil, fmt.Errorf("BOND_PRICE() requires exactly 5 arguments, got %d", len(args))
	}
	face, err := toFloat64(args[0])
	if err != nil {
		return nil, fmt.Errorf("BOND_PRICE() face: %w", err)
	}
	couponRate, err := toFloat64(args[1])
	if err != nil {
		return nil, fmt.Errorf("BOND_PRICE() coupon_rate: %w", err)
	}
	ytm, err := toFloat64(args[2])
	if err != nil {
		return nil, fmt.Errorf("BOND_PRICE() ytm: %w", err)
	}
	years, err := toFloat64(args[3])
	if err != nil {
		return nil, fmt.Errorf("BOND_PRICE() years: %w", err)
	}
	freq, err := toFloat64(args[4])
	if err != nil {
		return nil, fmt.Errorf("BOND_PRICE() freq: %w", err)
	}
	if freq <= 0 {
		return nil, fmt.Errorf("BOND_PRICE() freq must be positive")
	}

	return computeBondPrice(face, couponRate, ytm, int(years), int(freq)), nil
}

// financialBondYield: BOND_YIELD(face, coupon_rate, price, years, freq [, guess])
func financialBondYield(args []interface{}) (interface{}, error) {
	if len(args) < 5 || len(args) > 6 {
		return nil, fmt.Errorf("BOND_YIELD() requires 5-6 arguments, got %d", len(args))
	}
	face, err := toFloat64(args[0])
	if err != nil {
		return nil, fmt.Errorf("BOND_YIELD() face: %w", err)
	}
	couponRate, err := toFloat64(args[1])
	if err != nil {
		return nil, fmt.Errorf("BOND_YIELD() coupon_rate: %w", err)
	}
	price, err := toFloat64(args[2])
	if err != nil {
		return nil, fmt.Errorf("BOND_YIELD() price: %w", err)
	}
	years, err := toFloat64(args[3])
	if err != nil {
		return nil, fmt.Errorf("BOND_YIELD() years: %w", err)
	}
	freq, err := toFloat64(args[4])
	if err != nil {
		return nil, fmt.Errorf("BOND_YIELD() freq: %w", err)
	}
	if freq <= 0 {
		return nil, fmt.Errorf("BOND_YIELD() freq must be positive")
	}

	guess := 0.05
	if len(args) >= 6 {
		guess, err = toFloat64(args[5])
		if err != nil {
			return nil, fmt.Errorf("BOND_YIELD() guess: %w", err)
		}
	}

	yrsInt := int(years)
	freqInt := int(freq)
	ytm := guess

	for iter := 0; iter < 100; iter++ {
		p := computeBondPrice(face, couponRate, ytm, yrsInt, freqInt)
		// Numerical derivative
		dy := 0.0001
		p2 := computeBondPrice(face, couponRate, ytm+dy, yrsInt, freqInt)
		dp := (p2 - p) / dy
		if math.Abs(dp) < 1e-14 {
			return nil, fmt.Errorf("BOND_YIELD() derivative too small, cannot converge")
		}
		newYtm := ytm - (p-price)/dp
		if math.Abs(newYtm-ytm) < 1e-7 {
			return newYtm, nil
		}
		ytm = newYtm
	}

	return nil, fmt.Errorf("BOND_YIELD() did not converge after 100 iterations")
}

// financialBondDuration: BOND_DURATION(face, coupon_rate, ytm, years, freq)
func financialBondDuration(args []interface{}) (interface{}, error) {
	if len(args) != 5 {
		return nil, fmt.Errorf("BOND_DURATION() requires exactly 5 arguments, got %d", len(args))
	}
	face, err := toFloat64(args[0])
	if err != nil {
		return nil, fmt.Errorf("BOND_DURATION() face: %w", err)
	}
	couponRate, err := toFloat64(args[1])
	if err != nil {
		return nil, fmt.Errorf("BOND_DURATION() coupon_rate: %w", err)
	}
	ytm, err := toFloat64(args[2])
	if err != nil {
		return nil, fmt.Errorf("BOND_DURATION() ytm: %w", err)
	}
	years, err := toFloat64(args[3])
	if err != nil {
		return nil, fmt.Errorf("BOND_DURATION() years: %w", err)
	}
	freq, err := toFloat64(args[4])
	if err != nil {
		return nil, fmt.Errorf("BOND_DURATION() freq: %w", err)
	}
	if freq <= 0 {
		return nil, fmt.Errorf("BOND_DURATION() freq must be positive")
	}

	n := int(years) * int(freq)
	coupon := face * couponRate / freq
	yPerPeriod := ytm / freq

	weightedSum := 0.0
	priceSum := 0.0
	for t := 1; t <= n; t++ {
		pv := coupon / math.Pow(1+yPerPeriod, float64(t))
		weightedSum += float64(t) * pv
		priceSum += pv
	}
	// Add face value
	pvFace := face / math.Pow(1+yPerPeriod, float64(n))
	weightedSum += float64(n) * pvFace
	priceSum += pvFace

	if priceSum == 0 {
		return nil, fmt.Errorf("BOND_DURATION() bond price is zero")
	}

	// Macaulay duration in years
	return weightedSum / priceSum / freq, nil
}

// financialBondMDuration: BOND_MDURATION(face, coupon_rate, ytm, years, freq)
func financialBondMDuration(args []interface{}) (interface{}, error) {
	// Modified Duration = Macaulay Duration / (1 + ytm/freq)
	durationResult, err := financialBondDuration(args)
	if err != nil {
		return nil, err
	}
	duration, _ := toFloat64(durationResult)

	ytm, _ := toFloat64(args[2])
	freq, _ := toFloat64(args[4])

	return duration / (1 + ytm/freq), nil
}

// financialBondConvexity: BOND_CONVEXITY(face, coupon_rate, ytm, years, freq)
func financialBondConvexity(args []interface{}) (interface{}, error) {
	if len(args) != 5 {
		return nil, fmt.Errorf("BOND_CONVEXITY() requires exactly 5 arguments, got %d", len(args))
	}
	face, err := toFloat64(args[0])
	if err != nil {
		return nil, fmt.Errorf("BOND_CONVEXITY() face: %w", err)
	}
	couponRate, err := toFloat64(args[1])
	if err != nil {
		return nil, fmt.Errorf("BOND_CONVEXITY() coupon_rate: %w", err)
	}
	ytm, err := toFloat64(args[2])
	if err != nil {
		return nil, fmt.Errorf("BOND_CONVEXITY() ytm: %w", err)
	}
	years, err := toFloat64(args[3])
	if err != nil {
		return nil, fmt.Errorf("BOND_CONVEXITY() years: %w", err)
	}
	freq, err := toFloat64(args[4])
	if err != nil {
		return nil, fmt.Errorf("BOND_CONVEXITY() freq: %w", err)
	}
	if freq <= 0 {
		return nil, fmt.Errorf("BOND_CONVEXITY() freq must be positive")
	}

	n := int(years) * int(freq)
	coupon := face * couponRate / freq
	yPerPeriod := ytm / freq
	freqSq := freq * freq

	convexSum := 0.0
	price := 0.0
	for t := 1; t <= n; t++ {
		tf := float64(t)
		pv := coupon / math.Pow(1+yPerPeriod, tf)
		convexSum += tf * (tf + 1) * pv / math.Pow(1+yPerPeriod, 2)
		price += pv
	}
	// Add face value
	nf := float64(n)
	pvFace := face / math.Pow(1+yPerPeriod, nf)
	convexSum += nf * (nf + 1) * pvFace / math.Pow(1+yPerPeriod, 2)
	price += pvFace

	if price == 0 {
		return nil, fmt.Errorf("BOND_CONVEXITY() bond price is zero")
	}

	return convexSum / (price * freqSq), nil
}

// ============ 基础金融计算函数实现 ============

// financialCompoundInterest: COMPOUND_INTEREST(principal, rate, n, t)
// A = P * (1 + r/n)^(n*t)
func financialCompoundInterest(args []interface{}) (interface{}, error) {
	if len(args) != 4 {
		return nil, fmt.Errorf("COMPOUND_INTEREST() requires exactly 4 arguments, got %d", len(args))
	}
	principal, err := toFloat64(args[0])
	if err != nil {
		return nil, fmt.Errorf("COMPOUND_INTEREST() principal: %w", err)
	}
	rate, err := toFloat64(args[1])
	if err != nil {
		return nil, fmt.Errorf("COMPOUND_INTEREST() rate: %w", err)
	}
	n, err := toFloat64(args[2])
	if err != nil {
		return nil, fmt.Errorf("COMPOUND_INTEREST() n: %w", err)
	}
	if n == 0 {
		return nil, fmt.Errorf("COMPOUND_INTEREST() n cannot be zero")
	}
	t, err := toFloat64(args[3])
	if err != nil {
		return nil, fmt.Errorf("COMPOUND_INTEREST() t: %w", err)
	}
	return principal * math.Pow(1+rate/n, n*t), nil
}

// financialSimpleInterest: SIMPLE_INTEREST(principal, rate, time)
// I = P * r * t
func financialSimpleInterest(args []interface{}) (interface{}, error) {
	if len(args) != 3 {
		return nil, fmt.Errorf("SIMPLE_INTEREST() requires exactly 3 arguments, got %d", len(args))
	}
	principal, err := toFloat64(args[0])
	if err != nil {
		return nil, fmt.Errorf("SIMPLE_INTEREST() principal: %w", err)
	}
	rate, err := toFloat64(args[1])
	if err != nil {
		return nil, fmt.Errorf("SIMPLE_INTEREST() rate: %w", err)
	}
	t, err := toFloat64(args[2])
	if err != nil {
		return nil, fmt.Errorf("SIMPLE_INTEREST() time: %w", err)
	}
	return principal * rate * t, nil
}

// financialCAGR: CAGR(begin_value, end_value, periods)
// CAGR = (end/begin)^(1/periods) - 1
func financialCAGR(args []interface{}) (interface{}, error) {
	if len(args) != 3 {
		return nil, fmt.Errorf("CAGR() requires exactly 3 arguments, got %d", len(args))
	}
	beginValue, err := toFloat64(args[0])
	if err != nil {
		return nil, fmt.Errorf("CAGR() begin_value: %w", err)
	}
	endValue, err := toFloat64(args[1])
	if err != nil {
		return nil, fmt.Errorf("CAGR() end_value: %w", err)
	}
	periods, err := toFloat64(args[2])
	if err != nil {
		return nil, fmt.Errorf("CAGR() periods: %w", err)
	}
	if beginValue <= 0 {
		return nil, fmt.Errorf("CAGR() begin_value must be positive")
	}
	if endValue <= 0 {
		return nil, fmt.Errorf("CAGR() end_value must be positive")
	}
	if periods <= 0 {
		return nil, fmt.Errorf("CAGR() periods must be positive")
	}
	return math.Pow(endValue/beginValue, 1/periods) - 1, nil
}

// financialROI: ROI(gain, cost)
// ROI = (gain - cost) / cost * 100
func financialROI(args []interface{}) (interface{}, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("ROI() requires exactly 2 arguments, got %d", len(args))
	}
	gain, err := toFloat64(args[0])
	if err != nil {
		return nil, fmt.Errorf("ROI() gain: %w", err)
	}
	cost, err := toFloat64(args[1])
	if err != nil {
		return nil, fmt.Errorf("ROI() cost: %w", err)
	}
	if cost == 0 {
		return nil, fmt.Errorf("ROI() cost cannot be zero")
	}
	return (gain - cost) / cost * 100, nil
}
