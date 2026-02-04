#!/bin/bash

# 基准测试运行脚本
# 运行所有基准测试并保存结果到 baseline.json

# 设置输出目录
BENCHMARK_DIR="benchmark"
OUTPUT_FILE="${BENCHMARK_DIR}/baseline.json"
RAW_OUTPUT="${BENCHMARK_DIR}/raw_output.txt"

# 创建benchmark目录
mkdir -p "${BENCHMARK_DIR}"

echo "开始运行性能基准测试..."
echo "================================================"
echo ""

# 运行基准测试并保存原始输出
# 使用较短的benchtime以加快测试速度
go test -bench=. -benchmem -run=^$ -benchtime=200ms ./pkg/optimizer/ 2>&1 | tee "${RAW_OUTPUT}"

# 提取基准测试结果（排除DEBUG日志）
grep "^Benchmark" "${RAW_OUTPUT}" > "${BENCHMARK_DIR}/filtered_output.txt"

echo ""
echo "================================================"
echo "基准测试完成！"
echo ""
echo "原始输出已保存到: ${RAW_OUTPUT}"
echo "过滤后的基准数据已保存到: ${BENCHMARK_DIR}/filtered_output.txt"
echo ""
echo "关键基准测试结果:"
echo "================================================"
cat "${BENCHMARK_DIR}/filtered_output.txt"

# 如果需要生成JSON格式，可以运行 go test 命令的 -json 选项
echo ""
echo "提示: 要生成JSON格式的基准结果，可以使用:"
echo "go test -bench=. -benchmem -run=^$ -json ./pkg/optimizer/ > ${BENCHMARK_DIR}/benchmarks.json"
