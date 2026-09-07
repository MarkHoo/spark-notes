// 网站逻辑真实验证脚本（Node）：渲染全部章节 + 搜索 + 高亮
const fs = require('fs');
const path = require('path');

const SITE = path.join(__dirname, '..');
global.window = global;

// 加载数据
const dataDir = path.join(SITE, 'data');
const files = fs.readdirSync(dataDir).filter(f => f.endsWith('.js'));
files.forEach(f => eval(fs.readFileSync(path.join(dataDir, f), 'utf8')));
const dataCount = Object.keys(global.BOOK_DATA || {}).length;
console.log('数据章节数:', dataCount);

// 加载 JS 模块
eval(fs.readFileSync(path.join(SITE, 'js', 'highlight.js'), 'utf8'));
eval(fs.readFileSync(path.join(SITE, 'js', 'markdown.js'), 'utf8'));
eval(fs.readFileSync(path.join(SITE, 'js', 'search.js'), 'utf8'));

let fail = 0;

// 1) 渲染全部章节
console.log('\n=== 渲染测试 ===');
for (const k of Object.keys(global.BOOK_DATA)) {
  try {
    const r = global.Markdown.render(global.BOOK_DATA[k]);
    const opens = (r.html.match(/<pre>/g) || []).length;
    const closes = (r.html.match(/<\/pre>/g) || []).length;
    const tbl = (r.html.match(/<table>/g) || []).length;
    const tblClose = (r.html.match(/<\/table>/g) || []).length;
    if (!r.html.trim()) throw new Error('渲染为空');
    if (opens !== closes) throw new Error('pre 不配对: ' + opens + '/' + closes);
    if (tbl !== tblClose) throw new Error('table 不配对: ' + tbl + '/' + tblClose);
    console.log('  ✓', k, '(' + r.html.length + ' 字符, 小节数=' + r.headings.length + ')');
  } catch (e) {
    fail++;
    console.log('  ✗', k, '→', e.message);
  }
}

// 2) 搜索测试
console.log('\n=== 搜索测试 ===');
global.Search.buildIndex();
const tests = [
  ['窗口函数', 6],
  ['broadcast 广播', 8],
  ['ROW_NUMBER', 6],
  ['Parquet', 7],
  ['GROUP BY', 3],
  ['JSON 解析', 4],
  ['拉链表', 9],
];
for (const [q, expectChapter] of tests) {
  const res = global.Search.search(q);
  const hitExpect = res.some(r => r.key.indexOf('ch' + String(expectChapter).padStart(2, '0')) === 0);
  console.log('  搜索「' + q + '」→', res.length, '条命中' + (hitExpect ? '（含第' + expectChapter + '章 ✓）' : '（缺预期章节 ✗）'));
  if (!res.length || !hitExpect) fail++;
}
// 多关键词 AND
const multi = global.Search.search('窗口 排序');
console.log('  搜索「窗口 排序」→', multi.length, '条命中（AND 语义）');
if (!multi.length) fail++;

// 3) 高亮测试
console.log('\n=== 高亮测试 ===');
const hl = global.Highlighter.highlight('def add(a, b) { return a + b } // 注释 "字符串"', 'python');
console.log('  python 高亮含 span:', hl.includes('<span'));
console.log('  输出样例:', hl.slice(0, 80));
if (!hl.includes('<span')) fail++;

// 4) 列表渲染测试
console.log('\n=== 列表渲染测试 ===');
function count(html, tag) { return (html.match(new RegExp('<' + tag + '( |>|$)', 'g')) || []).length; }
// 嵌套列表
const nestedMd = '- 一级项 A\n  - 二级项 A1\n  - 二级项 A2\n- 一级项 B\n1. 有序一\n2. 有序二';
const nl = global.Markdown.render(nestedMd).html;
const ulOpen = count(nl, 'ul'), ulClose = count(nl, '/ul');
const liOpen = count(nl, 'li'), liClose = count(nl, '/li');
const olOpen = count(nl, 'ol'), olClose = count(nl, '/ol');
console.log('  嵌套列表 ul:', ulOpen, '/', ulClose, ' ol:', olOpen, '/', olClose, ' li:', liOpen, '/', liClose);
if (ulOpen !== ulClose || olOpen !== olClose || liOpen !== liClose || ulOpen !== 2 || olOpen !== 1 || liOpen !== 6) {
  console.log('  ✗ 列表结构异常:', nl);
  fail++;
} else {
  console.log('  ✓ 列表结构正确（嵌套列表共享 ul，同级 li 正确闭合）');
}
// 真实章节列表检查（第 2 章含大量列表）
const ch2 = global.Markdown.render(global.BOOK_DATA['ch02-DataFrame核心操作']).html;
if (count(ch2, 'ul') !== count(ch2, '/ul') || count(ch2, 'li') !== count(ch2, '/li')) {
  console.log('  ✗ 第2章列表不配对');
  fail++;
} else {
  console.log('  ✓ 第2章列表配对正确（ul=' + count(ch2, 'ul') + ', li=' + count(ch2, 'li') + '）');
}

// 5) 锚点一致性测试：渲染各章 h2 id 与 main.js slug 算法对比（跳过代码块内的 ##）
console.log('\n=== 锚点一致性测试 ===');
function slugifyJS(text) {
  return 'sec-' + text.replace(/[^\w\u4e00-\u9fa5]+/g, '-').replace(/^-+|-+$/g, '').slice(0, 60);
}
let anchorFail = 0;
for (const k of Object.keys(global.BOOK_DATA)) {
  const md = global.BOOK_DATA[k];
  const rendered = global.Markdown.render(md);
  // 剥离代码块后再提取 ## 标题
  const mdNoCode = md.replace(/```[\s\S]*?```/g, '');
  const re = /^##\s+(.+)$/gm;
  let m2;
  while ((m2 = re.exec(mdNoCode)) !== null) {
    const title = m2[1].replace(/\s*★+.*$/, '').trim();
    const slug = slugifyJS(title);
    if (!rendered.html.includes('id="' + slug + '"')) {
      console.log('  ✗ ' + k + ' 缺锚点: ' + title + ' → ' + slug);
      anchorFail++;
    }
  }
}
console.log('  锚点检查: ' + (anchorFail === 0 ? '全部 h2 锚点一致 ✓' : anchorFail + ' 处不一致 ✗'));
if (anchorFail) fail += anchorFail;

console.log('\n' + (fail === 0 ? '✅ 全部测试通过' : '❌ 失败项: ' + fail));
process.exit(fail === 0 ? 0 : 1);
