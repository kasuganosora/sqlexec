const fs = require('fs');
const lines = fs.readFileSync('/tmp/build_out.txt', 'utf8').split('\n');
const w = fs.createWriteStream('/tmp/build_clean.txt');
for (const line of lines) {
    w.write(line.replace(/\t/g, '  ') + '\n');
}
w.end();