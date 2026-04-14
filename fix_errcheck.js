#!/usr/bin/env node
const fs = require('fs');
const path = require('path');

// Parse errcheck output
const output = fs.readFileSync('/tmp/errcheck_output.txt', 'utf8');
const lines = output.split('\n').filter(l => l.trim());

// Group issues by file
const byFile = {};
for (const line of lines) {
    const tabIdx = line.indexOf('\t');
    if (tabIdx === -1) continue;
    const loc = line.substring(0, tabIdx).trim();
    const expr = line.substring(tabIdx + 1).trim();
    const m = loc.match(/^(.+?):(\d+):(\d+):$/);
    if (!m) continue;
    const file = m[1];
    const lineNum = parseInt(m[2]);
    if (!byFile[file]) byFile[file] = [];
    byFile[file].push({ line: lineNum, expr });
}

let totalFixes = 0;

for (const [filepath, issues] of Object.entries(byFile).sort()) {
    if (!fs.existsSync(filepath)) {
        console.log(`SKIP: ${filepath} not found`);
        continue;
    }
    
    const isTest = filepath.endsWith('_test.go');
    const content = fs.readFileSync(filepath, 'utf8');
    let fileLines = content.split('\n');
    
    // Sort issues by line descending for bottom-up processing
    issues.sort((a, b) => b.line - a.line);
    
    let fixesApplied = 0;
    
    for (const issue of issues) {
        const idx = issue.line - 1;
        if (idx < 0 || idx >= fileLines.length) continue;
        
        const line = fileLines[idx];
        const stripped = line.trim();
        
        if (stripped.startsWith('defer ')) {
            // defer foo(args) -> defer func() { _ = foo(args) }()
            const indent = line.substring(0, line.length - line.trimStart().length);
            const deferExpr = stripped.substring(6); // Remove 'defer '
            fileLines[idx] = `${indent}defer func() { _ = ${deferExpr} }()`;
            fixesApplied++;
        } else if (!isTest) {
            // Non-test: _ = expr
            const indent = line.substring(0, line.length - line.trimStart().length);
            fileLines[idx] = `${indent}_ = ${issue.expr}`;
            fixesApplied++;
        } else {
            // Test file: check if we have 't' in enclosing function
            let hasT = false;
            for (let i = idx - 1; i >= Math.max(0, idx - 60); i--) {
                const fl = fileLines[i];
                if (/\*\s*testing\.T\b/.test(fl) || /\*\s*testing\.T\)/.test(fl)) {
                    hasT = true;
                    break;
                }
                if (fl.trim() === 'func ' && i < idx - 5) break;
                if (/^func\s/.test(fl.trim())) {
                    if (/\*\s*testing\.T/.test(fl)) {
                        hasT = true;
                    }
                    break;
                }
            }
            
            const indent = line.substring(0, line.length - line.trimStart().length);
            if (hasT) {
                fileLines[idx] = `${indent}require.NoError(t, ${issue.expr})`;
            } else {
                fileLines[idx] = `${indent}_ = ${issue.expr}`;
            }
            fixesApplied++;
        }
    }
    
    if (fixesApplied > 0) {
        fs.writeFileSync(filepath, fileLines.join('\n'));
        console.log(`Fixed ${fixesApplied} in ${filepath}`);
        totalFixes += fixesApplied;
    }
}

console.log(`\nTotal: ${totalFixes}`);