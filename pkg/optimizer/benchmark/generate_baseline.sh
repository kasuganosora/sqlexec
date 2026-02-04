#!/bin/bash

# 基准测试结果收集脚本
# 运行关键基准测试并生成 baseline.json

cd d:/code/db

echo "开始运行关键基准测试..."
echo "================================================"
echo ""

# 运行基准测试并保存结果
echo "1. 单表查询基准..."
go test -bench=BenchmarkSingleTable -benchmem -run=^$ -benchtime=100ms ./pkg/optimizer/ 2>&1 | grep "^BenchmarkSingleTable.*ns/op" > benchmark/single_table_results.txt

echo "2. 两表JOIN基准..."
go test -bench=BenchmarkJoin2Table -benchmem -run=^$ -benchtime=100ms ./pkg/optimizer/ 2>&1 | grep "^BenchmarkJoin2Table.*ns/op" > benchmark/join2_table_results.txt

echo "3. 复杂查询基准..."
go test -bench=BenchmarkComplexQuery -benchmem -run=^$ -benchtime=100ms ./pkg/optimizer/ 2>&1 | grep "^BenchmarkComplexQuery.*ns/op" > benchmark/complex_query_results.txt

echo "4. 聚合查询基准..."
go test -bench=BenchmarkAggregate -benchmem -run=^$ -benchtime=100ms ./pkg/optimizer/ 2>&1 | grep "^BenchmarkAggregate.*ns/op" > benchmark/aggregate_results.txt

echo "5. 并行执行基准..."
go test -bench=BenchmarkParallel_Scan -benchmem -run=^$ -benchtime=100ms ./pkg/optimizer/ 2>&1 | grep "^BenchmarkParallel.*ns/op" > benchmark/parallel_results.txt

echo ""
echo "基准测试完成！"
echo "================================================"
echo ""
echo "所有结果文件已保存在 benchmark/ 目录"
echo ""
echo "单表查询结果:"
cat benchmark/single_table_results.txt
echo ""
echo "两表JOIN结果:"
cat benchmark/join2_table_results.txt
echo ""
echo "复杂查询结果:"
cat benchmark/complex_query_results.txt
echo ""
echo "聚合查询结果:"
cat benchmark/aggregate_results.txt
echo ""
echo "并行执行结果:"
cat benchmark/parallel_results.txt
