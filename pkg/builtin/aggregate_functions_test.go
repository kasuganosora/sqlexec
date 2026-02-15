package builtin

import (
	"math"
	"testing"
)

func init() {
	InitAggregateFunctions()
}

// ---- GROUP_CONCAT / STRING_AGG / LISTAGG ----

func TestAggGroupConcat(t *testing.T) {
	ctx := NewAggregateContext()
	aggGroupConcat(ctx, []interface{}{"hello", nil})
	aggGroupConcat(ctx, []interface{}{"world", nil})
	aggGroupConcat(ctx, []interface{}{"foo", nil})

	result, err := aggGroupConcatResult(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != "hello,world,foo" {
		t.Errorf("expected 'hello,world,foo', got %v", result)
	}
}

func TestAggGroupConcatCustomSeparator(t *testing.T) {
	ctx := NewAggregateContext()
	aggGroupConcat(ctx, []interface{}{"a", " | "})
	aggGroupConcat(ctx, []interface{}{"b", " | "})

	result, err := aggGroupConcatResult(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != "a | b" {
		t.Errorf("expected 'a | b', got %v", result)
	}
}

func TestAggGroupConcatEmpty(t *testing.T) {
	ctx := NewAggregateContext()
	result, err := aggGroupConcatResult(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != nil {
		t.Errorf("expected nil for empty group_concat, got %v", result)
	}
}

func TestAggGroupConcatSkipsNil(t *testing.T) {
	ctx := NewAggregateContext()
	aggGroupConcat(ctx, []interface{}{"a", nil})
	aggGroupConcat(ctx, []interface{}{nil, nil})
	aggGroupConcat(ctx, []interface{}{"b", nil})

	result, err := aggGroupConcatResult(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != "a,b" {
		t.Errorf("expected 'a,b', got %v", result)
	}
}

func TestAggGroupConcatNoArgs(t *testing.T) {
	ctx := NewAggregateContext()
	err := aggGroupConcat(ctx, []interface{}{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	result, err := aggGroupConcatResult(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != nil {
		t.Errorf("expected nil, got %v", result)
	}
}

func TestAggGroupConcatRegisteredAliases(t *testing.T) {
	for _, name := range []string{"group_concat", "string_agg", "listagg"} {
		info, ok := GetAggregate(name)
		if !ok {
			t.Errorf("aggregate %q should be registered", name)
			continue
		}
		if info.Handler == nil || info.Result == nil {
			t.Errorf("aggregate %q has nil handler or result", name)
		}
	}
}

// ---- COUNT_IF ----

func TestAggCountIf(t *testing.T) {
	ctx := NewAggregateContext()
	aggCountIf(ctx, []interface{}{true})
	aggCountIf(ctx, []interface{}{false})
	aggCountIf(ctx, []interface{}{true})
	aggCountIf(ctx, []interface{}{1})
	aggCountIf(ctx, []interface{}{0})
	aggCountIf(ctx, []interface{}{nil})

	result, err := aggCountIfResult(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != int64(3) {
		t.Errorf("expected 3, got %v", result)
	}
}

func TestAggCountIfAllFalse(t *testing.T) {
	ctx := NewAggregateContext()
	aggCountIf(ctx, []interface{}{false})
	aggCountIf(ctx, []interface{}{0})
	aggCountIf(ctx, []interface{}{""})

	result, err := aggCountIfResult(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != int64(0) {
		t.Errorf("expected 0, got %v", result)
	}
}

func TestAggCountIfNoArgs(t *testing.T) {
	ctx := NewAggregateContext()
	err := aggCountIf(ctx, []interface{}{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	result, _ := aggCountIfResult(ctx)
	if result != int64(0) {
		t.Errorf("expected 0, got %v", result)
	}
}

func TestAggCountIfStrings(t *testing.T) {
	ctx := NewAggregateContext()
	aggCountIf(ctx, []interface{}{"hello"})   // truthy
	aggCountIf(ctx, []interface{}{""})         // falsy
	aggCountIf(ctx, []interface{}{"0"})        // falsy
	aggCountIf(ctx, []interface{}{"false"})    // falsy
	aggCountIf(ctx, []interface{}{"anything"}) // truthy

	result, _ := aggCountIfResult(ctx)
	if result != int64(2) {
		t.Errorf("expected 2, got %v", result)
	}
}

// ---- BOOL_AND / EVERY ----

func TestAggBoolAndAllTrue(t *testing.T) {
	ctx := NewAggregateContext()
	aggBoolAnd(ctx, []interface{}{true})
	aggBoolAnd(ctx, []interface{}{true})
	aggBoolAnd(ctx, []interface{}{true})

	result, err := aggBoolAndResult(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != true {
		t.Errorf("expected true, got %v", result)
	}
}

func TestAggBoolAndOneFalse(t *testing.T) {
	ctx := NewAggregateContext()
	aggBoolAnd(ctx, []interface{}{true})
	aggBoolAnd(ctx, []interface{}{false})
	aggBoolAnd(ctx, []interface{}{true})

	result, err := aggBoolAndResult(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != false {
		t.Errorf("expected false, got %v", result)
	}
}

func TestAggBoolAndEmpty(t *testing.T) {
	ctx := NewAggregateContext()
	result, err := aggBoolAndResult(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != nil {
		t.Errorf("expected nil for empty bool_and, got %v", result)
	}
}

func TestAggBoolAndSkipsNil(t *testing.T) {
	ctx := NewAggregateContext()
	aggBoolAnd(ctx, []interface{}{true})
	aggBoolAnd(ctx, []interface{}{nil})
	aggBoolAnd(ctx, []interface{}{true})

	result, err := aggBoolAndResult(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != true {
		t.Errorf("expected true (nil skipped), got %v", result)
	}
}

func TestAggBoolAndRegisteredAsEvery(t *testing.T) {
	info, ok := GetAggregate("every")
	if !ok {
		t.Fatal("aggregate 'every' should be registered")
	}
	if info.Handler == nil || info.Result == nil {
		t.Fatal("aggregate 'every' has nil handler or result")
	}
}

func TestAggBoolAndNoArgs(t *testing.T) {
	ctx := NewAggregateContext()
	err := aggBoolAnd(ctx, []interface{}{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	result, _ := aggBoolAndResult(ctx)
	if result != nil {
		t.Errorf("expected nil, got %v", result)
	}
}

// ---- BOOL_OR ----

func TestAggBoolOrOneTrue(t *testing.T) {
	ctx := NewAggregateContext()
	aggBoolOr(ctx, []interface{}{false})
	aggBoolOr(ctx, []interface{}{true})
	aggBoolOr(ctx, []interface{}{false})

	result, err := aggBoolOrResult(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != true {
		t.Errorf("expected true, got %v", result)
	}
}

func TestAggBoolOrAllFalse(t *testing.T) {
	ctx := NewAggregateContext()
	aggBoolOr(ctx, []interface{}{false})
	aggBoolOr(ctx, []interface{}{false})

	result, err := aggBoolOrResult(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != false {
		t.Errorf("expected false, got %v", result)
	}
}

func TestAggBoolOrEmpty(t *testing.T) {
	ctx := NewAggregateContext()
	result, err := aggBoolOrResult(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != nil {
		t.Errorf("expected nil for empty bool_or, got %v", result)
	}
}

func TestAggBoolOrSkipsNil(t *testing.T) {
	ctx := NewAggregateContext()
	aggBoolOr(ctx, []interface{}{false})
	aggBoolOr(ctx, []interface{}{nil})
	aggBoolOr(ctx, []interface{}{false})

	result, err := aggBoolOrResult(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != false {
		t.Errorf("expected false (nil skipped), got %v", result)
	}
}

// ---- STDDEV_POP ----

func TestAggStdDevPop(t *testing.T) {
	ctx := NewAggregateContext()
	for _, v := range []float64{2, 4, 4, 4, 5, 5, 7, 9} {
		aggStdDevPop(ctx, []interface{}{v})
	}

	result, err := aggStdDevPopResult(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// population stddev of {2,4,4,4,5,5,7,9} = 2.0
	val, ok := result.(float64)
	if !ok {
		t.Fatalf("expected float64, got %T", result)
	}
	if !almostEqual(val, 2.0, 1e-9) {
		t.Errorf("expected ~2.0, got %v", val)
	}
}

func TestAggStdDevPopEmpty(t *testing.T) {
	ctx := NewAggregateContext()
	result, err := aggStdDevPopResult(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != nil {
		t.Errorf("expected nil, got %v", result)
	}
}

func TestAggStdDevPopSingleValue(t *testing.T) {
	ctx := NewAggregateContext()
	aggStdDevPop(ctx, []interface{}{5.0})
	result, err := aggStdDevPopResult(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	val := result.(float64)
	if !almostEqual(val, 0.0, 1e-9) {
		t.Errorf("expected 0.0, got %v", val)
	}
}

// ---- STDDEV_SAMP ----

func TestAggStdDevSamp(t *testing.T) {
	ctx := NewAggregateContext()
	for _, v := range []float64{2, 4, 4, 4, 5, 5, 7, 9} {
		aggStdDevSamp(ctx, []interface{}{v})
	}

	result, err := aggStdDevSampResult(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	val, ok := result.(float64)
	if !ok {
		t.Fatalf("expected float64, got %T", result)
	}
	// sample stddev = sqrt(32/7) ≈ 2.13809
	expected := math.Sqrt(32.0 / 7.0)
	if !almostEqual(val, expected, 1e-4) {
		t.Errorf("expected ~%v, got %v", expected, val)
	}
}

func TestAggStdDevSampSingleValue(t *testing.T) {
	ctx := NewAggregateContext()
	aggStdDevSamp(ctx, []interface{}{5.0})
	result, err := aggStdDevSampResult(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != nil {
		t.Errorf("expected nil for single-value stddev_samp, got %v", result)
	}
}

func TestAggStdDevSampEmpty(t *testing.T) {
	ctx := NewAggregateContext()
	result, err := aggStdDevSampResult(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != nil {
		t.Errorf("expected nil, got %v", result)
	}
}

// ---- VAR_POP ----

func TestAggVarPop(t *testing.T) {
	ctx := NewAggregateContext()
	for _, v := range []float64{2, 4, 4, 4, 5, 5, 7, 9} {
		aggVarPop(ctx, []interface{}{v})
	}

	result, err := aggVarPopResult(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	val := result.(float64)
	// population variance = 4.0
	if !almostEqual(val, 4.0, 1e-9) {
		t.Errorf("expected 4.0, got %v", val)
	}
}

func TestAggVarPopEmpty(t *testing.T) {
	ctx := NewAggregateContext()
	result, err := aggVarPopResult(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != nil {
		t.Errorf("expected nil, got %v", result)
	}
}

// ---- VAR_SAMP ----

func TestAggVarSamp(t *testing.T) {
	ctx := NewAggregateContext()
	for _, v := range []float64{2, 4, 4, 4, 5, 5, 7, 9} {
		aggVarSamp(ctx, []interface{}{v})
	}

	result, err := aggVarSampResult(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	val := result.(float64)
	// sample variance = 32/7 ≈ 4.57143
	expected := 32.0 / 7.0
	if !almostEqual(val, expected, 1e-4) {
		t.Errorf("expected ~%v, got %v", expected, val)
	}
}

func TestAggVarSampSingleValue(t *testing.T) {
	ctx := NewAggregateContext()
	aggVarSamp(ctx, []interface{}{5.0})
	result, err := aggVarSampResult(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != nil {
		t.Errorf("expected nil for single-value var_samp, got %v", result)
	}
}

func TestAggVarSampEmpty(t *testing.T) {
	ctx := NewAggregateContext()
	result, err := aggVarSampResult(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != nil {
		t.Errorf("expected nil, got %v", result)
	}
}

// ---- MEDIAN ----

func TestAggMedianOdd(t *testing.T) {
	ctx := NewAggregateContext()
	for _, v := range []float64{1, 3, 5, 7, 9} {
		aggMedian(ctx, []interface{}{v})
	}

	result, err := aggMedianResult(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	val := result.(float64)
	if !almostEqual(val, 5.0, 1e-9) {
		t.Errorf("expected 5.0, got %v", val)
	}
}

func TestAggMedianEven(t *testing.T) {
	ctx := NewAggregateContext()
	for _, v := range []float64{1, 3, 5, 7} {
		aggMedian(ctx, []interface{}{v})
	}

	result, err := aggMedianResult(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	val := result.(float64)
	// median of {1,3,5,7} = (3+5)/2 = 4.0
	if !almostEqual(val, 4.0, 1e-9) {
		t.Errorf("expected 4.0, got %v", val)
	}
}

func TestAggMedianSingleValue(t *testing.T) {
	ctx := NewAggregateContext()
	aggMedian(ctx, []interface{}{42.0})
	result, err := aggMedianResult(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	val := result.(float64)
	if val != 42.0 {
		t.Errorf("expected 42.0, got %v", val)
	}
}

func TestAggMedianEmpty(t *testing.T) {
	ctx := NewAggregateContext()
	result, err := aggMedianResult(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != nil {
		t.Errorf("expected nil, got %v", result)
	}
}

func TestAggMedianUnsorted(t *testing.T) {
	ctx := NewAggregateContext()
	for _, v := range []float64{9, 1, 7, 3, 5} {
		aggMedian(ctx, []interface{}{v})
	}
	result, _ := aggMedianResult(ctx)
	val := result.(float64)
	if !almostEqual(val, 5.0, 1e-9) {
		t.Errorf("expected 5.0, got %v", val)
	}
}

// ---- PERCENTILE_CONT ----

func TestAggPercentileCont50(t *testing.T) {
	ctx := NewAggregateContext()
	vals := []float64{10, 20, 30, 40, 50}
	for _, v := range vals {
		aggPercentileCont(ctx, []interface{}{0.5, v})
	}

	result, err := aggPercentileContResult(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	val := result.(float64)
	// 0.5 * (5-1) = 2.0 => sorted[2] = 30
	if !almostEqual(val, 30.0, 1e-9) {
		t.Errorf("expected 30.0, got %v", val)
	}
}

func TestAggPercentileCont25(t *testing.T) {
	ctx := NewAggregateContext()
	vals := []float64{10, 20, 30, 40, 50}
	for _, v := range vals {
		aggPercentileCont(ctx, []interface{}{0.25, v})
	}

	result, err := aggPercentileContResult(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	val := result.(float64)
	// pos = 0.25 * 4 = 1.0 => sorted[1] = 20
	if !almostEqual(val, 20.0, 1e-9) {
		t.Errorf("expected 20.0, got %v", val)
	}
}

func TestAggPercentileContBoundary0(t *testing.T) {
	ctx := NewAggregateContext()
	for _, v := range []float64{10, 20, 30} {
		aggPercentileCont(ctx, []interface{}{0.0, v})
	}
	result, _ := aggPercentileContResult(ctx)
	val := result.(float64)
	if !almostEqual(val, 10.0, 1e-9) {
		t.Errorf("expected 10.0, got %v", val)
	}
}

func TestAggPercentileContBoundary1(t *testing.T) {
	ctx := NewAggregateContext()
	for _, v := range []float64{10, 20, 30} {
		aggPercentileCont(ctx, []interface{}{1.0, v})
	}
	result, _ := aggPercentileContResult(ctx)
	val := result.(float64)
	if !almostEqual(val, 30.0, 1e-9) {
		t.Errorf("expected 30.0, got %v", val)
	}
}

func TestAggPercentileContEmpty(t *testing.T) {
	ctx := NewAggregateContext()
	result, err := aggPercentileContResult(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != nil {
		t.Errorf("expected nil, got %v", result)
	}
}

func TestAggPercentileContInterpolation(t *testing.T) {
	ctx := NewAggregateContext()
	for _, v := range []float64{0, 10} {
		aggPercentileCont(ctx, []interface{}{0.5, v})
	}
	result, _ := aggPercentileContResult(ctx)
	val := result.(float64)
	// pos = 0.5 * 1 = 0.5 => 0 + 0.5*(10-0) = 5.0
	if !almostEqual(val, 5.0, 1e-9) {
		t.Errorf("expected 5.0, got %v", val)
	}
}

// ---- PERCENTILE_DISC ----

func TestAggPercentileDisc50(t *testing.T) {
	ctx := NewAggregateContext()
	for _, v := range []float64{10, 20, 30, 40, 50} {
		aggPercentileDisc(ctx, []interface{}{0.5, v})
	}

	result, err := aggPercentileDiscResult(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	val := result.(float64)
	// ceil(0.5*5)-1 = ceil(2.5)-1 = 3-1 = 2 => sorted[2] = 30
	if !almostEqual(val, 30.0, 1e-9) {
		t.Errorf("expected 30.0, got %v", val)
	}
}

func TestAggPercentileDiscBoundary0(t *testing.T) {
	ctx := NewAggregateContext()
	for _, v := range []float64{10, 20, 30} {
		aggPercentileDisc(ctx, []interface{}{0.0, v})
	}
	result, _ := aggPercentileDiscResult(ctx)
	val := result.(float64)
	if !almostEqual(val, 10.0, 1e-9) {
		t.Errorf("expected 10.0, got %v", val)
	}
}

func TestAggPercentileDiscBoundary1(t *testing.T) {
	ctx := NewAggregateContext()
	for _, v := range []float64{10, 20, 30} {
		aggPercentileDisc(ctx, []interface{}{1.0, v})
	}
	result, _ := aggPercentileDiscResult(ctx)
	val := result.(float64)
	if !almostEqual(val, 30.0, 1e-9) {
		t.Errorf("expected 30.0, got %v", val)
	}
}

func TestAggPercentileDiscEmpty(t *testing.T) {
	ctx := NewAggregateContext()
	result, err := aggPercentileDiscResult(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != nil {
		t.Errorf("expected nil, got %v", result)
	}
}

// ---- ARRAY_AGG / LIST ----

func TestAggArrayAgg(t *testing.T) {
	ctx := NewAggregateContext()
	aggArrayAgg(ctx, []interface{}{"a"})
	aggArrayAgg(ctx, []interface{}{42})
	aggArrayAgg(ctx, []interface{}{nil})
	aggArrayAgg(ctx, []interface{}{3.14})

	result, err := aggArrayAggResult(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	arr, ok := result.([]interface{})
	if !ok {
		t.Fatalf("expected []interface{}, got %T", result)
	}
	if len(arr) != 4 {
		t.Fatalf("expected 4 elements, got %d", len(arr))
	}
	if arr[0] != "a" {
		t.Errorf("arr[0] expected 'a', got %v", arr[0])
	}
	if arr[1] != 42 {
		t.Errorf("arr[1] expected 42, got %v", arr[1])
	}
	if arr[2] != nil {
		t.Errorf("arr[2] expected nil, got %v", arr[2])
	}
	if arr[3] != 3.14 {
		t.Errorf("arr[3] expected 3.14, got %v", arr[3])
	}
}

func TestAggArrayAggEmpty(t *testing.T) {
	ctx := NewAggregateContext()
	result, err := aggArrayAggResult(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != nil {
		t.Errorf("expected nil for empty array_agg, got %v", result)
	}
}

func TestAggArrayAggNoArgs(t *testing.T) {
	ctx := NewAggregateContext()
	err := aggArrayAgg(ctx, []interface{}{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	result, _ := aggArrayAggResult(ctx)
	if result != nil {
		t.Errorf("expected nil, got %v", result)
	}
}

func TestAggArrayAggRegisteredAsList(t *testing.T) {
	for _, name := range []string{"array_agg", "list"} {
		info, ok := GetAggregate(name)
		if !ok {
			t.Errorf("aggregate %q should be registered", name)
			continue
		}
		if info.Handler == nil || info.Result == nil {
			t.Errorf("aggregate %q has nil handler or result", name)
		}
	}
}

// ---- PRODUCT ----

func TestAggProduct(t *testing.T) {
	ctx := NewAggregateContext()
	aggProduct(ctx, []interface{}{2.0})
	aggProduct(ctx, []interface{}{3.0})
	aggProduct(ctx, []interface{}{4.0})

	result, err := aggProductResult(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	val := result.(float64)
	if !almostEqual(val, 24.0, 1e-9) {
		t.Errorf("expected 24.0, got %v", val)
	}
}

func TestAggProductWithIntegers(t *testing.T) {
	ctx := NewAggregateContext()
	aggProduct(ctx, []interface{}{5})
	aggProduct(ctx, []interface{}{6})

	result, err := aggProductResult(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	val := result.(float64)
	if !almostEqual(val, 30.0, 1e-9) {
		t.Errorf("expected 30.0, got %v", val)
	}
}

func TestAggProductWithZero(t *testing.T) {
	ctx := NewAggregateContext()
	aggProduct(ctx, []interface{}{5.0})
	aggProduct(ctx, []interface{}{0.0})
	aggProduct(ctx, []interface{}{3.0})

	result, err := aggProductResult(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	val := result.(float64)
	if !almostEqual(val, 0.0, 1e-9) {
		t.Errorf("expected 0.0, got %v", val)
	}
}

func TestAggProductEmpty(t *testing.T) {
	ctx := NewAggregateContext()
	result, err := aggProductResult(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != nil {
		t.Errorf("expected nil for empty product, got %v", result)
	}
}

func TestAggProductSkipsNil(t *testing.T) {
	ctx := NewAggregateContext()
	aggProduct(ctx, []interface{}{3.0})
	aggProduct(ctx, []interface{}{nil})
	aggProduct(ctx, []interface{}{4.0})

	result, _ := aggProductResult(ctx)
	val := result.(float64)
	if !almostEqual(val, 12.0, 1e-9) {
		t.Errorf("expected 12.0, got %v", val)
	}
}

func TestAggProductSingleValue(t *testing.T) {
	ctx := NewAggregateContext()
	aggProduct(ctx, []interface{}{7.0})

	result, _ := aggProductResult(ctx)
	val := result.(float64)
	if !almostEqual(val, 7.0, 1e-9) {
		t.Errorf("expected 7.0, got %v", val)
	}
}

// ---- isTruthy helper ----

func TestIsTruthyValues(t *testing.T) {
	tests := []struct {
		name     string
		val      interface{}
		expected bool
	}{
		{"nil", nil, false},
		{"true", true, true},
		{"false", false, false},
		{"int 1", int(1), true},
		{"int 0", int(0), false},
		{"int64 1", int64(1), true},
		{"int64 0", int64(0), false},
		{"float64 1.0", float64(1.0), true},
		{"float64 0.0", float64(0.0), false},
		{"non-empty string", "hello", true},
		{"empty string", "", false},
		{"string 0", "0", false},
		{"string false", "false", false},
		{"uint 0", uint(0), false},
		{"uint 1", uint(1), true},
		{"struct", struct{}{}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isTruthy(tt.val)
			if got != tt.expected {
				t.Errorf("isTruthy(%v) = %v, want %v", tt.val, got, tt.expected)
			}
		})
	}
}

// ---- NewAggregateContext defaults ----

func TestAggNewContextDefaults(t *testing.T) {
	ctx := NewAggregateContext()
	if ctx.Separator != "," {
		t.Errorf("expected default separator ',', got %q", ctx.Separator)
	}
	if ctx.ProductVal != 1.0 {
		t.Errorf("expected default ProductVal 1.0, got %v", ctx.ProductVal)
	}
	if ctx.ProductInit {
		t.Error("expected ProductInit to be false")
	}
	if ctx.BoolAnd != nil {
		t.Error("expected BoolAnd to be nil")
	}
	if ctx.BoolOr != nil {
		t.Error("expected BoolOr to be nil")
	}
	if ctx.Strings == nil {
		t.Error("expected Strings to be initialized")
	}
	if ctx.AllValues == nil {
		t.Error("expected AllValues to be initialized")
	}
}

// ---- Registration check for all new aggregate functions ----

func TestAggAllNewFunctionsRegistered(t *testing.T) {
	names := []string{
		"group_concat", "string_agg", "listagg",
		"count_if",
		"bool_and", "every",
		"bool_or",
		"stddev_pop", "stddev_samp",
		"var_pop", "var_samp",
		"median",
		"percentile_cont", "percentile_disc",
		"array_agg", "list",
		"product",
	}
	for _, name := range names {
		info, ok := GetAggregate(name)
		if !ok {
			t.Errorf("aggregate %q should be registered", name)
			continue
		}
		if info.Handler == nil {
			t.Errorf("aggregate %q has nil Handler", name)
		}
		if info.Result == nil {
			t.Errorf("aggregate %q has nil Result", name)
		}
		if info.Category != "aggregate" {
			t.Errorf("aggregate %q category = %q, want 'aggregate'", name, info.Category)
		}
	}
}
