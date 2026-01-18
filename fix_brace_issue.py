#!/usr/bin/env python3
# 直接删除第594行的空闭合大括号
with open('d:/code/db/mysql/resource/memory_source.go', 'r', encoding='utf-8') as f:
    lines = f.readlines()

# 删除第594行（从0开始计数，即593）
new_lines = lines[:593] + lines[594:]

with open('d:/code/db/mysql/resource/memory_source.go', 'w', encoding='utf-8') as f:
    f.writelines(new_lines)

print(f"Deleted line 594 (empty closing brace). Total lines: {len(new_lines)}")
