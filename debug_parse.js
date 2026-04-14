const fs = require('fs');
const output = fs.readFileSync('/tmp/errcheck_output.txt', 'utf8');
const lines = output.split('\n').filter(l => l.trim());

const w = fs.createWriteStream('/tmp/debug_results.txt');
const first5 = lines.slice(0, 5);
for (let i = 0; i < first5.length; i++) {
    const line = first5[i];
    const tabIdx = line.indexOf('\t');
    if (tabIdx === -1) {
        w.write(`Line ${i}: No tab found\n`);
        w.write(`  First 40 chars: ${line.substring(0, 40).replace(/\t/g, '<TAB>')}\n`);
        continue;
    }
    const loc = line.substring(0, tabIdx).trim();
    const expr = line.substring(tabIdx + 1).trim();
    w.write(`Line ${i}: loc="${loc}" expr="${expr}"\n`);
    const m = loc.match(/^(.+?):(\d+):(\d+)$/);
    w.write(`  match=${!!m} file=${m?m[1]:'n/a'}\n`);
}
w.end();