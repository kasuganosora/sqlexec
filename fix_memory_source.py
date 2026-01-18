#!/usr/bin/env python3
import sys

# 读取文件
with open('d:/code/db/mysql/resource/memory_source.go', 'r', encoding='utf-8') as f:
    lines = f.readlines()

# 找到第二个compareEqual函数定义（从第911行开始）
# 我们需要删除第594-620行的旧compareEqual函数
new_lines = []
skip_until = 594
skip_count = 0
in_compare_equal = False

for i, line in enumerate(lines):
    line_num = i + 1

    if skip_count > 0 and line_num < 594:
        # 还在跳过区域
        skip_count -= 1
        continue

    # 开始跳过旧compareEqual函数
    if line_num == 594 and 'func compareEqual(' in line:
        in_compare_equal = True
        skip_count = 10  # 预计跳过10行（594-603）
        continue

    # 结束跳过
    if in_compare_equal and skip_count <= 0 and line_num > 603:
        in_compare_equal = False

    # 如果还在跳过中
    if skip_count > 0:
        skip_count -= 1
        continue

    new_lines.append(line)

# 写回文件
with open('d:/code/db/mysql/resource/memory_source.go', 'w', encoding='utf-8') as f:
    f.writelines(new_lines)

print(f"处理完成，共{len(new_lines)}行")
