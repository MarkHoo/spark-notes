# -*- coding: utf-8 -*-
"""构建静态网站数据：直接读取各章节文件 → site/data/*.js
用法: python build_site.py
"""
import os

BASE = os.path.dirname(os.path.abspath(__file__))  # site/tools
SITE = os.path.dirname(BASE)                        # site
BOOK = os.path.dirname(SITE)                        # Spark-SQL-notes
DATA_DIR = os.path.join(SITE, 'data')

# 章节文件（顺序即网站目录顺序）：key = 文件名去 .md
CHAPTER_FILES = [
    'README.md',
    'ch01-认识Spark-SQL.md', 'ch02-DataFrame核心操作.md', 'ch03-SQL查询与临时视图.md',
    'ch04-高频内置函数精讲.md', 'ch05-Join详解.md', 'ch06-窗口函数实战.md',
    'ch07-读写外部数据源.md', 'ch08-性能调优实践.md', 'ch09-综合实战案例.md',
]


def js_escape(text):
    """转义为 JS 单引号字符串内容"""
    text = text.replace('\\', '\\\\')
    text = text.replace("'", "\\'")
    text = text.replace('\r', '')
    text = text.replace('\n', '\\n')
    text = text.replace('</script>', '<\\/script>')
    return text


def main():
    blocks = []
    for fname in CHAPTER_FILES:
        path = os.path.join(BOOK, fname)
        if not os.path.isfile(path):
            print('缺少章节文件:', fname)
            return 1
        with open(path, encoding='utf-8') as fh:
            blocks.append((fname[:-3], fh.read().strip()))

    # 清空旧数据
    for f in os.listdir(DATA_DIR):
        if f.endswith('.js'):
            os.remove(os.path.join(DATA_DIR, f))

    for name, block in blocks:
        escaped = js_escape(block)
        js = "window.BOOK_DATA = window.BOOK_DATA || {};\nwindow.BOOK_DATA['%s'] = '%s';\n" % (name, escaped)
        with open(os.path.join(DATA_DIR, name + '.js'), 'w', encoding='utf-8', newline='\n') as fh:
            fh.write(js)

    print('生成数据文件: %d 个' % len(blocks))
    for name, block in blocks:
        print('  %s (%d 字符)' % (name, len(block)))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
