package builtin

import (
	"math"
	"testing"
)

// financialAlmostEqual checks if two float64 values are approximately equal
func financialAlmostEqual(a, b, tolerance float64) bool {
	return math.Abs(a-b) < tolerance
}

// ============ 注册验证 ============

func TestFinancial_Registration(t *testing.T) {
	expectedFunctions := []string{
		"fv", "pv", "pmt", "ipmt", "ppmt", "npv", "irr", "xirr", "rate", "nper",
		"sln", "syd", "ddb",
		"round_banker", "round_currency", "round_truncate",
		"bond_price", "bond_yield", "bond_duration", "bond_mduration", "bond_convexity",
		"compound_interest", "simple_interest", "cagr", "roi",
	}

	for _, name := range expectedFunctions {
		fn, ok := GetGlobal(name)
		if !ok || fn == nil {
			t.Errorf("function %q not registered", name)
			continue
		}
		if fn.Category != "financial" {
			t.Errorf("function %q has category %q, expected \"financial\"", name, fn.Category)
		}
	}
}

func TestFinancial_Count(t *testing.T) {
	count := 0
	for _, name := range []string{
		"fv", "pv", "pmt", "ipmt", "ppmt", "npv", "irr", "xirr", "rate", "nper",
		"sln", "syd", "ddb",
		"round_banker", "round_currency", "round_truncate",
		"bond_price", "bond_yield", "bond_duration", "bond_mduration", "bond_convexity",
		"compound_interest", "simple_interest", "cagr", "roi",
	} {
		if _, ok := GetGlobal(name); ok {
			count++
		}
	}
	if count != 25 {
		t.Errorf("expected 25 financial functions registered, got %d", count)
	}
}

// ============ FV 测试 ============

func TestFinancial_FV(t *testing.T) {
	tests := []struct {
		name     string
		args     []interface{}
		expected float64
		wantErr  bool
	}{
		{
			name:     "basic FV",
			args:     []interface{}{0.05, 10.0, -100.0},
			expected: 1257.789,
		},
		{
			name:     "FV with pv",
			args:     []interface{}{0.05, 10.0, -100.0, -1000.0},
			expected: 2886.684,
		},
		{
			name:     "FV with zero rate",
			args:     []interface{}{0.0, 10.0, -100.0},
			expected: 1000.0,
		},
		{
			name:     "FV with type=1 (beginning)",
			args:     []interface{}{0.05, 10.0, -100.0, 0.0, 1.0},
			expected: 1320.679,
		},
		{
			name:    "too few args",
			args:    []interface{}{0.05, 10.0},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := financialFV(tt.args)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			val, _ := toFloat64(result)
			if !financialAlmostEqual(val, tt.expected, 1.0) {
				t.Errorf("FV(%v) = %f, expected ~%f", tt.args, val, tt.expected)
			}
		})
	}
}

// ============ PV 测试 ============

func TestFinancial_PV(t *testing.T) {
	tests := []struct {
		name     string
		args     []interface{}
		expected float64
		wantErr  bool
	}{
		{
			name:     "basic PV",
			args:     []interface{}{0.05, 10.0, -100.0},
			expected: 772.173,
		},
		{
			name:     "PV with zero rate",
			args:     []interface{}{0.0, 10.0, -100.0},
			expected: 1000.0,
		},
		{
			name:    "too few args",
			args:    []interface{}{0.05, 10.0},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := financialPV(tt.args)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			val, _ := toFloat64(result)
			if !financialAlmostEqual(val, tt.expected, 1.0) {
				t.Errorf("PV(%v) = %f, expected ~%f", tt.args, val, tt.expected)
			}
		})
	}
}

// ============ PMT 测试 ============

func TestFinancial_PMT(t *testing.T) {
	tests := []struct {
		name     string
		args     []interface{}
		expected float64
		wantErr  bool
	}{
		{
			name:     "basic PMT - loan",
			args:     []interface{}{0.05, 10.0, -1000.0},
			expected: 129.505,
		},
		{
			name:     "PMT with zero rate",
			args:     []interface{}{0.0, 10.0, -1000.0},
			expected: 100.0,
		},
		{
			name:     "PMT with fv",
			args:     []interface{}{0.05, 10.0, -1000.0, 500.0},
			expected: 90.340,
		},
		{
			name:    "nper is zero",
			args:    []interface{}{0.05, 0.0, -1000.0},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := financialPMT(tt.args)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			val, _ := toFloat64(result)
			if !financialAlmostEqual(val, tt.expected, 1.0) {
				t.Errorf("PMT(%v) = %f, expected ~%f", tt.args, val, tt.expected)
			}
		})
	}
}

// ============ IPMT + PPMT 一致性 ============

func TestFinancial_IPMT_PPMT_Consistency(t *testing.T) {
	// IPMT + PPMT should equal PMT for each period
	rate := 0.05
	nper := 10.0
	pv := -1000.0

	pmtResult, err := financialPMT([]interface{}{rate, nper, pv})
	if err != nil {
		t.Fatalf("PMT error: %v", err)
	}
	pmtVal, _ := toFloat64(pmtResult)

	for per := 1.0; per <= nper; per++ {
		ipmtResult, err := financialIPMT([]interface{}{rate, per, nper, pv})
		if err != nil {
			t.Fatalf("IPMT error at per=%v: %v", per, err)
		}
		ppmtResult, err := financialPPMT([]interface{}{rate, per, nper, pv})
		if err != nil {
			t.Fatalf("PPMT error at per=%v: %v", per, err)
		}
		ipmtVal, _ := toFloat64(ipmtResult)
		ppmtVal, _ := toFloat64(ppmtResult)

		sum := ipmtVal + ppmtVal
		if !financialAlmostEqual(sum, pmtVal, 0.01) {
			t.Errorf("per=%v: IPMT(%f) + PPMT(%f) = %f, expected PMT=%f",
				per, ipmtVal, ppmtVal, sum, pmtVal)
		}
	}
}

func TestFinancial_IPMT(t *testing.T) {
	tests := []struct {
		name    string
		args    []interface{}
		wantErr bool
	}{
		{
			name: "first period",
			args: []interface{}{0.05, 1.0, 10.0, -1000.0},
		},
		{
			name: "last period",
			args: []interface{}{0.05, 10.0, 10.0, -1000.0},
		},
		{
			name:    "per out of range",
			args:    []interface{}{0.05, 0.0, 10.0, -1000.0},
			wantErr: true,
		},
		{
			name:    "per > nper",
			args:    []interface{}{0.05, 11.0, 10.0, -1000.0},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := financialIPMT(tt.args)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

// ============ NPV 测试 ============

func TestFinancial_NPV(t *testing.T) {
	tests := []struct {
		name     string
		args     []interface{}
		expected float64
		wantErr  bool
	}{
		{
			name:     "basic NPV",
			args:     []interface{}{0.1, -1000.0, 300.0, 400.0, 500.0},
			expected: -19.124,
		},
		{
			name:     "NPV with positive values",
			args:     []interface{}{0.08, 100.0, 200.0, 300.0},
			expected: 502.210,
		},
		{
			name:    "too few args",
			args:    []interface{}{0.1},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := financialNPV(tt.args)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			val, _ := toFloat64(result)
			if !financialAlmostEqual(val, tt.expected, 1.0) {
				t.Errorf("NPV(%v) = %f, expected ~%f", tt.args, val, tt.expected)
			}
		})
	}
}

// ============ IRR 测试 ============

func TestFinancial_IRR(t *testing.T) {
	tests := []struct {
		name     string
		args     []interface{}
		expected float64
		wantErr  bool
	}{
		{
			name:     "basic IRR",
			args:     []interface{}{"-1000,300,400,500,200"},
			expected: 0.1532,
		},
		{
			name:     "IRR with guess",
			args:     []interface{}{"-1000,300,400,500,200", 0.05},
			expected: 0.1532,
		},
		{
			name:     "simple IRR",
			args:     []interface{}{"-100,110"},
			expected: 0.10,
		},
		{
			name:    "too few cash flows",
			args:    []interface{}{"-1000"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := financialIRR(tt.args)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			val, _ := toFloat64(result)
			if !financialAlmostEqual(val, tt.expected, 0.01) {
				t.Errorf("IRR(%v) = %f, expected ~%f", tt.args, val, tt.expected)
			}
		})
	}
}

// ============ XIRR 测试 ============

func TestFinancial_XIRR(t *testing.T) {
	tests := []struct {
		name     string
		args     []interface{}
		expected float64
		wantErr  bool
	}{
		{
			name:     "basic XIRR",
			args:     []interface{}{"-1000,500,600", "2024-01-01,2024-07-01,2025-01-01"},
			expected: 0.1318,
		},
		{
			name:     "XIRR with guess",
			args:     []interface{}{"-1000,500,600", "2024-01-01,2024-07-01,2025-01-01", 0.05},
			expected: 0.1318,
		},
		{
			name:    "mismatched counts",
			args:    []interface{}{"-1000,500", "2024-01-01"},
			wantErr: true,
		},
		{
			name:    "invalid date format",
			args:    []interface{}{"-1000,500", "not-a-date,also-bad"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := financialXIRR(tt.args)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			val, _ := toFloat64(result)
			if !financialAlmostEqual(val, tt.expected, 0.02) {
				t.Errorf("XIRR(%v) = %f, expected ~%f", tt.args, val, tt.expected)
			}
		})
	}
}

// ============ RATE 测试 ============

func TestFinancial_RATE(t *testing.T) {
	tests := []struct {
		name     string
		args     []interface{}
		expected float64
		wantErr  bool
	}{
		{
			name:     "basic RATE",
			args:     []interface{}{10.0, -100.0, 800.0},
			expected: 0.0435,
		},
		{
			name:    "nper is zero",
			args:    []interface{}{0.0, -100.0, 800.0},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := financialRATE(tt.args)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			val, _ := toFloat64(result)
			if !financialAlmostEqual(val, tt.expected, 0.01) {
				t.Errorf("RATE(%v) = %f, expected ~%f", tt.args, val, tt.expected)
			}
		})
	}
}

// ============ NPER 测试 ============

func TestFinancial_NPER(t *testing.T) {
	tests := []struct {
		name     string
		args     []interface{}
		expected float64
		wantErr  bool
	}{
		{
			name:     "basic NPER",
			args:     []interface{}{0.05, -100.0, 800.0},
			expected: 10.47,
		},
		{
			name:     "NPER with zero rate",
			args:     []interface{}{0.0, -100.0, 1000.0},
			expected: 10.0,
		},
		{
			name:    "zero rate and zero pmt",
			args:    []interface{}{0.0, 0.0, 1000.0},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := financialNPER(tt.args)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			val, _ := toFloat64(result)
			if !financialAlmostEqual(val, tt.expected, 0.1) {
				t.Errorf("NPER(%v) = %f, expected ~%f", tt.args, val, tt.expected)
			}
		})
	}
}

// ============ 折旧函数测试 ============

func TestFinancial_SLN(t *testing.T) {
	tests := []struct {
		name     string
		args     []interface{}
		expected float64
		wantErr  bool
	}{
		{
			name:     "basic SLN",
			args:     []interface{}{10000.0, 1000.0, 5.0},
			expected: 1800.0,
		},
		{
			name:     "SLN zero salvage",
			args:     []interface{}{10000.0, 0.0, 10.0},
			expected: 1000.0,
		},
		{
			name:    "SLN zero life",
			args:    []interface{}{10000.0, 1000.0, 0.0},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := financialSLN(tt.args)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			val, _ := toFloat64(result)
			if !financialAlmostEqual(val, tt.expected, 0.01) {
				t.Errorf("SLN(%v) = %f, expected %f", tt.args, val, tt.expected)
			}
		})
	}
}

func TestFinancial_SYD(t *testing.T) {
	tests := []struct {
		name     string
		args     []interface{}
		expected float64
		wantErr  bool
	}{
		{
			name:     "SYD first year",
			args:     []interface{}{10000.0, 1000.0, 5.0, 1.0},
			expected: 3000.0,
		},
		{
			name:     "SYD last year",
			args:     []interface{}{10000.0, 1000.0, 5.0, 5.0},
			expected: 600.0,
		},
		{
			name:    "SYD per out of range",
			args:    []interface{}{10000.0, 1000.0, 5.0, 6.0},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := financialSYD(tt.args)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			val, _ := toFloat64(result)
			if !financialAlmostEqual(val, tt.expected, 0.01) {
				t.Errorf("SYD(%v) = %f, expected %f", tt.args, val, tt.expected)
			}
		})
	}
}

func TestFinancial_DDB(t *testing.T) {
	tests := []struct {
		name     string
		args     []interface{}
		expected float64
		wantErr  bool
	}{
		{
			name:     "DDB first year",
			args:     []interface{}{10000.0, 1000.0, 5.0, 1.0},
			expected: 4000.0,
		},
		{
			name:     "DDB second year",
			args:     []interface{}{10000.0, 1000.0, 5.0, 2.0},
			expected: 2400.0,
		},
		{
			name:     "DDB with factor",
			args:     []interface{}{10000.0, 1000.0, 5.0, 1.0, 1.5},
			expected: 3000.0,
		},
		{
			name:    "DDB per out of range",
			args:    []interface{}{10000.0, 1000.0, 5.0, 6.0},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := financialDDB(tt.args)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			val, _ := toFloat64(result)
			if !financialAlmostEqual(val, tt.expected, 1.0) {
				t.Errorf("DDB(%v) = %f, expected ~%f", tt.args, val, tt.expected)
			}
		})
	}
}

// ============ 银行舍入测试 ============

func TestFinancial_RoundBanker(t *testing.T) {
	// 四舍六入五成双：0.5→0, 1.5→2, 2.5→2, 3.5→4
	tests := []struct {
		name     string
		args     []interface{}
		expected float64
	}{
		{"0.5 → 0", []interface{}{0.5, 0.0}, 0.0},
		{"1.5 → 2", []interface{}{1.5, 0.0}, 2.0},
		{"2.5 → 2", []interface{}{2.5, 0.0}, 2.0},
		{"3.5 → 4", []interface{}{3.5, 0.0}, 4.0},
		{"4.5 → 4", []interface{}{4.5, 0.0}, 4.0},
		{"-0.5 → 0", []interface{}{-0.5, 0.0}, 0.0},
		{"-1.5 → -2", []interface{}{-1.5, 0.0}, -2.0},
		{"2.35 with 1 decimal → 2.4", []interface{}{2.35, 1.0}, 2.4},
		{"2.45 with 1 decimal → 2.4", []interface{}{2.45, 1.0}, 2.4},
		{"normal rounding 2.6 → 3", []interface{}{2.6, 0.0}, 3.0},
		{"normal rounding 2.4 → 2", []interface{}{2.4, 0.0}, 2.0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := financialRoundBanker(tt.args)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			val, _ := toFloat64(result)
			if val != tt.expected {
				t.Errorf("ROUND_BANKER(%v) = %f, expected %f", tt.args, val, tt.expected)
			}
		})
	}
}

func TestFinancial_RoundCurrency(t *testing.T) {
	tests := []struct {
		name     string
		args     []interface{}
		expected float64
	}{
		{"2.5 → 3", []interface{}{2.5, 0.0}, 3.0},
		{"2.4 → 2", []interface{}{2.4, 0.0}, 2.0},
		{"2.6 → 3", []interface{}{2.6, 0.0}, 3.0},
		{"-2.5 → -3", []interface{}{-2.5, 0.0}, -3.0},
		{"2.345 with 2 decimals → 2.35", []interface{}{2.345, 2.0}, 2.35},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := financialRoundCurrency(tt.args)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			val, _ := toFloat64(result)
			if !financialAlmostEqual(val, tt.expected, 0.001) {
				t.Errorf("ROUND_CURRENCY(%v) = %f, expected %f", tt.args, val, tt.expected)
			}
		})
	}
}

func TestFinancial_RoundTruncate(t *testing.T) {
	tests := []struct {
		name     string
		args     []interface{}
		expected float64
	}{
		{"2.9 → 2", []interface{}{2.9, 0.0}, 2.0},
		{"2.1 → 2", []interface{}{2.1, 0.0}, 2.0},
		{"-2.9 → -2", []interface{}{-2.9, 0.0}, -2.0},
		{"-2.1 → -2", []interface{}{-2.1, 0.0}, -2.0},
		{"2.567 with 2 decimals → 2.56", []interface{}{2.567, 2.0}, 2.56},
		{"2.999 with 1 decimal → 2.9", []interface{}{2.999, 1.0}, 2.9},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := financialRoundTruncate(tt.args)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			val, _ := toFloat64(result)
			if !financialAlmostEqual(val, tt.expected, 0.001) {
				t.Errorf("ROUND_TRUNCATE(%v) = %f, expected %f", tt.args, val, tt.expected)
			}
		})
	}
}

// ============ 债券函数测试 ============

func TestFinancial_BondPrice(t *testing.T) {
	tests := []struct {
		name     string
		args     []interface{}
		expected float64
		wantErr  bool
	}{
		{
			name:     "par bond",
			args:     []interface{}{1000.0, 0.05, 0.05, 10.0, 2.0},
			expected: 1000.0,
		},
		{
			name:     "discount bond",
			args:     []interface{}{1000.0, 0.05, 0.06, 10.0, 2.0},
			expected: 925.613,
		},
		{
			name:     "premium bond",
			args:     []interface{}{1000.0, 0.08, 0.06, 10.0, 2.0},
			expected: 1148.775,
		},
		{
			name:    "zero freq",
			args:    []interface{}{1000.0, 0.05, 0.06, 10.0, 0.0},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := financialBondPrice(tt.args)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			val, _ := toFloat64(result)
			if !financialAlmostEqual(val, tt.expected, 1.0) {
				t.Errorf("BOND_PRICE(%v) = %f, expected ~%f", tt.args, val, tt.expected)
			}
		})
	}
}

func TestFinancial_BondYield(t *testing.T) {
	// BOND_YIELD should find the yield that produces the given price
	face := 1000.0
	coupon := 0.05
	years := 10.0
	freq := 2.0

	// First compute price at 6% yield
	priceArgs := []interface{}{face, coupon, 0.06, years, freq}
	priceResult, err := financialBondPrice(priceArgs)
	if err != nil {
		t.Fatalf("BOND_PRICE error: %v", err)
	}
	price, _ := toFloat64(priceResult)

	// Now find yield from that price
	yieldResult, err := financialBondYield([]interface{}{face, coupon, price, years, freq})
	if err != nil {
		t.Fatalf("BOND_YIELD error: %v", err)
	}
	ytm, _ := toFloat64(yieldResult)
	if !financialAlmostEqual(ytm, 0.06, 0.001) {
		t.Errorf("BOND_YIELD = %f, expected ~0.06", ytm)
	}
}

func TestFinancial_BondDuration(t *testing.T) {
	result, err := financialBondDuration([]interface{}{1000.0, 0.05, 0.06, 10.0, 2.0})
	if err != nil {
		t.Fatalf("BOND_DURATION error: %v", err)
	}
	val, _ := toFloat64(result)
	// Macaulay duration for this bond should be around 7.8 years
	if val < 7.0 || val > 9.0 {
		t.Errorf("BOND_DURATION = %f, expected ~7.8", val)
	}
}

func TestFinancial_BondMDuration_Consistency(t *testing.T) {
	// Modified Duration = Macaulay Duration / (1 + ytm/freq)
	args := []interface{}{1000.0, 0.05, 0.06, 10.0, 2.0}

	durResult, err := financialBondDuration(args)
	if err != nil {
		t.Fatalf("BOND_DURATION error: %v", err)
	}
	dur, _ := toFloat64(durResult)

	mdurResult, err := financialBondMDuration(args)
	if err != nil {
		t.Fatalf("BOND_MDURATION error: %v", err)
	}
	mdur, _ := toFloat64(mdurResult)

	expectedMdur := dur / (1 + 0.06/2.0)
	if !financialAlmostEqual(mdur, expectedMdur, 0.001) {
		t.Errorf("BOND_MDURATION = %f, expected %f (duration/%f)", mdur, expectedMdur, 1+0.06/2.0)
	}
}

func TestFinancial_BondConvexity(t *testing.T) {
	result, err := financialBondConvexity([]interface{}{1000.0, 0.05, 0.06, 10.0, 2.0})
	if err != nil {
		t.Fatalf("BOND_CONVEXITY error: %v", err)
	}
	val, _ := toFloat64(result)
	// Convexity for this bond should be positive and in a reasonable range
	if val < 50 || val > 120 {
		t.Errorf("BOND_CONVEXITY = %f, expected value between 50 and 120", val)
	}
}

// ============ 基础金融函数测试 ============

func TestFinancial_CompoundInterest(t *testing.T) {
	tests := []struct {
		name     string
		args     []interface{}
		expected float64
		wantErr  bool
	}{
		{
			name:     "monthly compounding",
			args:     []interface{}{1000.0, 0.05, 12.0, 10.0},
			expected: 1647.01,
		},
		{
			name:     "annual compounding",
			args:     []interface{}{1000.0, 0.05, 1.0, 10.0},
			expected: 1628.89,
		},
		{
			name:    "n is zero",
			args:    []interface{}{1000.0, 0.05, 0.0, 10.0},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := financialCompoundInterest(tt.args)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			val, _ := toFloat64(result)
			if !financialAlmostEqual(val, tt.expected, 1.0) {
				t.Errorf("COMPOUND_INTEREST(%v) = %f, expected ~%f", tt.args, val, tt.expected)
			}
		})
	}
}

func TestFinancial_SimpleInterest(t *testing.T) {
	tests := []struct {
		name     string
		args     []interface{}
		expected float64
		wantErr  bool
	}{
		{
			name:     "basic simple interest",
			args:     []interface{}{1000.0, 0.05, 3.0},
			expected: 150.0,
		},
		{
			name:     "zero rate",
			args:     []interface{}{1000.0, 0.0, 5.0},
			expected: 0.0,
		},
		{
			name:    "wrong args count",
			args:    []interface{}{1000.0, 0.05},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := financialSimpleInterest(tt.args)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			val, _ := toFloat64(result)
			if !financialAlmostEqual(val, tt.expected, 0.01) {
				t.Errorf("SIMPLE_INTEREST(%v) = %f, expected %f", tt.args, val, tt.expected)
			}
		})
	}
}

func TestFinancial_CAGR(t *testing.T) {
	tests := []struct {
		name     string
		args     []interface{}
		expected float64
		wantErr  bool
	}{
		{
			name:     "basic CAGR",
			args:     []interface{}{1000.0, 2000.0, 5.0},
			expected: 0.1487,
		},
		{
			name:     "CAGR double in 1 year",
			args:     []interface{}{100.0, 200.0, 1.0},
			expected: 1.0,
		},
		{
			name:    "zero begin value",
			args:    []interface{}{0.0, 2000.0, 5.0},
			wantErr: true,
		},
		{
			name:    "zero periods",
			args:    []interface{}{1000.0, 2000.0, 0.0},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := financialCAGR(tt.args)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			val, _ := toFloat64(result)
			if !financialAlmostEqual(val, tt.expected, 0.01) {
				t.Errorf("CAGR(%v) = %f, expected ~%f", tt.args, val, tt.expected)
			}
		})
	}
}

func TestFinancial_ROI(t *testing.T) {
	tests := []struct {
		name     string
		args     []interface{}
		expected float64
		wantErr  bool
	}{
		{
			name:     "50% ROI",
			args:     []interface{}{1500.0, 1000.0},
			expected: 50.0,
		},
		{
			name:     "100% ROI",
			args:     []interface{}{2000.0, 1000.0},
			expected: 100.0,
		},
		{
			name:     "negative ROI",
			args:     []interface{}{800.0, 1000.0},
			expected: -20.0,
		},
		{
			name:    "zero cost",
			args:    []interface{}{500.0, 0.0},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := financialROI(tt.args)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			val, _ := toFloat64(result)
			if !financialAlmostEqual(val, tt.expected, 0.01) {
				t.Errorf("ROI(%v) = %f, expected %f", tt.args, val, tt.expected)
			}
		})
	}
}

// ============ 错误参数测试 ============

func TestFinancial_ErrorHandling(t *testing.T) {
	errorTests := []struct {
		name string
		fn   func([]interface{}) (interface{}, error)
		args []interface{}
	}{
		{"FV invalid rate", financialFV, []interface{}{"bad", 10.0, -100.0}},
		{"PV invalid nper", financialPV, []interface{}{0.05, "bad", -100.0}},
		{"PMT invalid pv", financialPMT, []interface{}{0.05, 10.0, "bad"}},
		{"IPMT too few args", financialIPMT, []interface{}{0.05, 1.0}},
		{"PPMT too few args", financialPPMT, []interface{}{0.05, 1.0}},
		{"NPV invalid value", financialNPV, []interface{}{0.1, "bad"}},
		{"SLN wrong args", financialSLN, []interface{}{10000.0, 1000.0}},
		{"SYD wrong args", financialSYD, []interface{}{10000.0}},
		{"DDB wrong args", financialDDB, []interface{}{10000.0}},
		{"ROUND_BANKER wrong args", financialRoundBanker, []interface{}{2.5}},
		{"ROUND_CURRENCY wrong args", financialRoundCurrency, []interface{}{2.5}},
		{"ROUND_TRUNCATE wrong args", financialRoundTruncate, []interface{}{2.5}},
		{"BOND_PRICE wrong args", financialBondPrice, []interface{}{1000.0}},
		{"BOND_YIELD wrong args", financialBondYield, []interface{}{1000.0}},
		{"BOND_DURATION wrong args", financialBondDuration, []interface{}{1000.0}},
		{"BOND_CONVEXITY wrong args", financialBondConvexity, []interface{}{1000.0}},
		{"COMPOUND_INTEREST wrong args", financialCompoundInterest, []interface{}{1000.0}},
		{"SIMPLE_INTEREST wrong args", financialSimpleInterest, []interface{}{1000.0}},
		{"CAGR wrong args", financialCAGR, []interface{}{1000.0}},
		{"ROI wrong args", financialROI, []interface{}{500.0}},
		{"RATE wrong args", financialRATE, []interface{}{10.0}},
		{"NPER wrong args", financialNPER, []interface{}{0.05}},
	}

	for _, tt := range errorTests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tt.fn(tt.args)
			if err == nil {
				t.Errorf("expected error for %s, got nil", tt.name)
			}
		})
	}
}

// ============ 辅助函数测试 ============

func TestFinancial_TrimSpaces(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"  hello  ", "hello"},
		{"hello", "hello"},
		{"  ", ""},
		{"", ""},
	}
	for _, tt := range tests {
		result := trimSpaces(tt.input)
		if result != tt.expected {
			t.Errorf("trimSpaces(%q) = %q, expected %q", tt.input, result, tt.expected)
		}
	}
}

func TestFinancial_ParseCashFlows(t *testing.T) {
	tests := []struct {
		name    string
		input   interface{}
		count   int
		wantErr bool
	}{
		{"comma string", "-1000,300,400,500", 4, false},
		{"with spaces", "-1000, 300, 400", 3, false},
		{"invalid type", 12345, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parseCashFlows(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(result) != tt.count {
				t.Errorf("got %d values, expected %d", len(result), tt.count)
			}
		})
	}
}
