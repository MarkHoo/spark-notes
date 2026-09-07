// 专项验证：点击"第 9 章 → 9.4 步骤 3：落 ODS 层（Parquet 按天分区）"小节应定位到真实内容锚点
const fs = require('fs');
const path = require('path');
global.window = global;
const SITE = path.join(__dirname, '..');
fs.readdirSync(path.join(SITE, 'data')).forEach(f => eval(fs.readFileSync(path.join(SITE, 'data', f), 'utf8')));
eval(fs.readFileSync(path.join(SITE, 'js', 'highlight.js'), 'utf8'));
eval(fs.readFileSync(path.join(SITE, 'js', 'markdown.js'), 'utf8'));

function slugifyJS(text) {
  return 'sec-' + text.replace(/[^\w\u4e00-\u9fa5]+/g, '-').replace(/^-+|-+$/g, '').slice(0, 60);
}

const key = 'ch09-综合实战案例';
const title = '9.4 步骤 3：落 ODS 层（Parquet 按天分区）';
const md = global.BOOK_DATA[key];
const rendered = global.Markdown.render(md).html;
const slug = slugifyJS(title);

let fail = 0;
const hasId = rendered.includes('id="' + slug + '"');
console.log('① 锚点 id 存在:', hasId ? '✓' : '✗', '(' + slug + ')');
if (!hasId) fail++;

const m = rendered.match(new RegExp('<h2[^>]*id="' + slug + '"[^>]*>([^<]*)</h2>'));
console.log('② h2 文本:', m ? '✓ ' + m[1] : '✗');
if (!m) fail++;

const pos = rendered.indexOf('id="' + slug + '"');
const pct = (pos / rendered.length * 100).toFixed(1);
const inRange = parseFloat(pct) < 60;
console.log('③ 锚点在正文位置: ' + pct + '%（<60% 为真实内容）', inRange ? '✓' : '✗');
if (!inRange) fail++;

const after = rendered.slice(pos, pos + 200).replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim();
console.log('④ 小节开头内容: ', after.slice(0, 55));

const mdNoCode = md.replace(/```[\s\S]*?```/g, '');
const re = /^##\s+(.+)$/gm;
let m2, total = 0, missing = 0;
while ((m2 = re.exec(mdNoCode)) !== null) {
  total++;
  const t = m2[1].replace(/\s*★+.*$/, '').trim();
  if (!rendered.includes('id="' + slugifyJS(t) + '"')) { missing++; console.log('  ✗ 缺:', t); }
}
console.log('⑤ 第9章小节锚点: ' + (total - missing) + '/' + total + ' 存在', missing === 0 ? '✓' : '✗');
if (missing) fail++;

console.log('\n' + (fail === 0 ? '✅ 9.4 小节定位专项验证通过（点击将滚动到该小节真实内容）' : '❌ 失败 ' + fail));
process.exit(fail === 0 ? 0 : 1);
