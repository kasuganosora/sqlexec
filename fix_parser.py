#!/usr/bin/env python3
import re

def fix_fragment_issue(content, line_num, indent):
    """修复Fragment为空的问题"""
    # 找到对应行并替换
    lines = content.split('\n')
    for i in range(len(lines)):
        if i == line_num - 1:
            lines[i] = '\t\t\tif d.isTautologyCondition(stmt.Where) {'
            lines.insert(i + 1, '\t\t\t\tresult.IsDetected = true')
            lines.insert(i + 2, '\t\t\t\twhereText := stmt.Where.OriginalText()')
            lines.insert(i + 3, '\t\t\t\tif whereText == "" {')
            lines.insert(i + 4, '\t\t\t\t\twhereText = "WHERE clause with tautology condition"')
            lines.insert(i + 5, '\t\t\t\t}')
            lines.insert(i + 6, '\t\t\t\tresult.Details = append(result.Details, InjectionDetail{')
            lines.insert(i + 7, '\t\t\t\t\tPattern:  "tautology_injection",')
            lines.insert(i + 8, '\t\t\t\t\tPosition: stmt.OriginTextPosition(),')
            lines.insert(i + 9, '\t\t\t\t\tLength:   len(whereText),')
            lines.insert(i + 10, '\t\t\t\t\tFragment: whereText,')
            lines.insert(i + 11, '\t\t\t\t})')
            lines.insert(i + 12, '\t\t\t}')
            lines.insert(i + 13, '\t\t}')
            # 删除原来的行
            for _ in range(15):
                lines.pop(i + 14)
            break
    return '\n'.join(lines)

with open('pkg/security/sql_injection_parser.go', 'r', encoding='utf-8') as f:
    content = f.read()

# 修复analyzeSelectStmt中的Fragment问题
lines = content.split('\n')
for i in range(len(lines)):
    if 'result.IsDetected = true' in lines[i] and i > 90:
        # 查找Fragment使用Where.OriginalText()的行
        if i + 2 < len(lines) and 'Fragment:' in lines[i+2]:
            # 检查是否使用了Where.Text()
            if 'stmt.Where.Text()' in lines[i+2] or 'Where.Text()' in lines[i+2]:
                # 替换Where.Text()为Where.OriginalText()并添加空字符串检查
                fragment_line = lines[i+2]
                if 'stmt.Where.Text()' in fragment_line:
                    new_line = fragment_line.replace('stmt.Where.Text()', 'whereText')
                else:
                    new_line = fragment_line.replace('Where.Text()', 'whereText')

                # 在result.IsDetected = true后添加whereText定义和检查
                indent = '\t\t\t'
                new_lines = [
                    indent + 'result.IsDetected = true',
                    indent + 'whereText := stmt.Where.OriginalText()',
                    indent + 'if whereText == "" {',
                    indent + '\twhereText = "WHERE clause with tautology condition"',
                    indent + '}',
                    indent + 'result.Details = append(result.Details, InjectionDetail{',
                    indent + '\tPattern:  "tautology_injection",',
                    indent + '\tPosition: stmt.OriginTextPosition(),',
                    indent + '\tLength:   len(whereText),',
                    indent + '\tFragment: whereText,',
                    indent + '})',
                    indent + '}'
                ]

                # 替换相关行
                lines[i:i+4] = new_lines
                break

content = '\n'.join(lines)

with open('pkg/security/sql_injection_parser.go', 'w', encoding='utf-8') as f:
    f.write(content)

print('Fixed Fragment issues')
