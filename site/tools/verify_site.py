# -*- coding: utf-8 -*-
"""网站验证脚本：调用 Node 验证 JS 渲染/搜索/高亮 + Python 静态检查"""
import os, re, subprocess, sys

BASE = os.path.dirname(os.path.abspath(__file__))
SITE = os.path.join(BASE, '..')
fail = 0

# Windows 控制台默认 GBK，统一改用 UTF-8 输出
try:
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')
except Exception:
    pass

print("=== 1) 静态资源完整性 ===")
required = ['index.html', 'css/style.css', 'js/markdown.js', 'js/highlight.js', 'js/search.js', 'js/main.js']
for r in required:
    ok = os.path.isfile(os.path.join(SITE, r))
    print(f"  {'✓' if ok else '✗'} {r}")
    if not ok: fail += 1

data_dir = os.path.join(SITE, 'data')
data_files = [f for f in os.listdir(data_dir) if f.endswith('.js')]
print(f"  data 文件数: {len(data_files)}（应为 10）")
if len(data_files) != 10: fail += 1

print("=== 2) data JS 转义正确性 ===")
for f in data_files:
    content = open(os.path.join(data_dir, f), encoding='utf-8').read()
    m = re.match(r"window\.BOOK_DATA = window\.BOOK_DATA \|\| \{\};\nwindow\.BOOK_DATA\['[^']*'\] = '(.*)';$", content, re.S)
    if not m:
        print(f"  ✗ {f} 结构不正确")
        fail += 1
        continue
    body = m.group(1)
    # 检查裸单引号（前面不是反斜杠）
    bad = re.findall(r"(?<!\\)'", body)
    # 检查裸换行
    if '\n' in body:
        print(f"  ✗ {f} 含裸换行")
        fail += 1
    if bad:
        print(f"  ✗ {f} 含 {len(bad)} 个未转义单引号")
        fail += 1
print("  data 转义检查完成")

print("=== 3) HTML 引用一致性 ===")
html = open(os.path.join(SITE, 'index.html'), encoding='utf-8').read()
refs = re.findall(r'(?:src|href)="([^"]+)"', html)
missing = [r for r in refs if not r.startswith('#') and not os.path.exists(os.path.join(SITE, r))]
print(f"  引用资源 {len(refs)} 个，缺失 {len(missing)}" + (f"：{missing}" if missing else " ✓"))
if missing: fail += len(missing)

print("=== 4) Node 逻辑验证（渲染 10 章 + 搜索 + 高亮）===")
r = subprocess.run(['node', os.path.join(BASE, 'site-test.js')], capture_output=True, text=True, encoding='utf-8')
out = r.stdout + r.stderr
print(out[-2000:])
if r.returncode != 0:
    print("  Node 验证失败")
    fail += 1

print("=== 5) Node 运行时验证（DOM 模拟执行 main.js）===")
r = subprocess.run(['node', os.path.join(BASE, 'test-runtime.js')], capture_output=True, text=True, encoding='utf-8')
out = r.stdout + r.stderr
print(out[-800:])
if r.returncode != 0:
    print("  运行时验证失败")
    fail += 1

print("\n" + ("✅ 网站验证全部通过" if fail == 0 else f"❌ 失败项: {fail}"))
sys.exit(0 if fail == 0 else 1)
