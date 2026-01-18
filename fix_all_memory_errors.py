#!/usr/bin/env python3
"""
修复memory_source.go中的所有语法错误
主要问题：compareEqual函数重复定义且没有正确闭合
"""

# 读取文件
with open('d:/code/db/mysql/resource/memory_source.go', 'r', encoding='utf-8') as f:
    lines = f.readlines()

# 找到所有 compareEqual 函数的行号
compare_equal_lines = []
for i, line in enumerate(lines, 1):
    if line.strip().startswith('func compareEqual'):
        compare_equal_lines.append(i)
    print(f"Found compareEqual at line {i}")

# 我们保留第一个compareEqual（从第884行），删除其他的（第885行开始的）
# 第884行的compareEqual是正确的，删除第885行开始的那个
if len(compare_equal_lines) > 1:
    # 找到第二个（从第885行开始）
    first_bad_start = compare_equal_lines[1]
    print(f"Removing duplicate compareEqual starting at line {first_bad_start}")

    # 找到对应函数的结束
    brace_count = 0
    end_line = first_bad_start
    for i in range(first_bad_start - 1, len(lines)):
        if 'return' in lines[i]:
            brace_count += 1
        if brace_count >= 2 and lines[i].strip() == '}':
            end_line = i + 1
            break

    print(f"Removing lines {first_bad_start} to {end_line}")
    new_lines = lines[:first_bad_start-1] + lines[end_line:]

    # 写回文件
    with open('d:/code/db/mysql/resource/memory_source.go', 'w', encoding='utf-8') as f:
        f.writelines(new_lines)

    print(f"Fixed! Removed {len(lines) - len(new_lines)} lines")
else:
    print("Found only one compareEqual function, no need to fix")
