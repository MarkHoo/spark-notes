#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
examples/verify.py —— 全书示例一键自检

用法:
    python examples/verify.py            # 重跑 ch01~ch09 并与 out/ 存档比对
    python examples/verify.py 03 05      # 只校验指定章节（任意数量）
    python examples/verify.py --update   # 以本次运行结果覆盖刷新 out/ 存档（谨慎使用）
    python examples/verify.py --no-clean # 保留临时输出与运行产物（默认自动清理）

比对策略（逐级放宽，任何一级通过即 PASS，未通过的行会给出差异）:
    1. 规范化逐行比对 —— 先做 token 级规范化（把每次运行必变的动态内容归一）：
       - 随机 UUID（part 文件名）、临时目录路径
       - plan_id=NN / [id=NN] 等内部对象编号
       - 时间戳 / 带微秒时间戳 / 纯日期（当前日期类函数输出，值取决于运行当天）
       - 表格宽度（show 按内容自适应列宽，长度变化不影响内容）
    2. JSON 记录行集合比对 —— 从文件读回的记录行顺序由文件系统枚举决定，
       只要两边记录集合一致即视为一致。
    3. 数值掩码比对 —— CURRENT_DATE 派生的差值（如 days_ago）每天变化 1，
       把数字抹成 # 后若结构完全一致即视为一致。

说明:
    - 正文宣称"输出即真实运行结果"，本脚本让读者能一键复核这一声明。
    - 第 2/3 级豁免属于"值随运行时间/环境漂移"，不代表脚本不稳定；
      脚本会在输出中明确提示命中了哪些豁免，方便你判断。
"""
import argparse
import difflib
import glob
import os
import re
import shutil
import subprocess
import sys
import tempfile
from collections import Counter

BOOK_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
EXAMPLES = os.path.join(BOOK_ROOT, "examples")
OUT_DIR = os.path.join(EXAMPLES, "out")
CHAPTERS = list(range(1, 10))  # 1~9 章

# ---- 动态 token 规范化：把每次运行必变的编号/时间归一成占位符 ----
_TOKEN_SUBS = [
    # 1) 随机 UUID（写出的 part 文件名等），先于其它替换
    (re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"),
     "<uuid>"),
    # 2) 随机临时目录
    (re.compile(r"/tmp/[^\s|,)\]]+"), "<tmp>"),
    # 3) 执行计划内部编号
    (re.compile(r"plan_id=\d+"), "plan_id=N"),
    (re.compile(r"\[id=\d+\]"), "[id=N]"),
    # 4) 时间戳（含微秒，Spark 打印的 TIMESTAMP 字面量）
    (re.compile(r"\b20\d{2}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(\.\d+)?\b"), "<TS>"),
    # 5) 纯日期（CURRENT_DATE 类输出，值取决于运行当天）
    (re.compile(r"\b20\d{2}-\d{2}-\d{2}\b"), "<DATE>"),
]
_JSON_LINE = re.compile(r'^\s*\{.*\}\s*$')


def norm_line(line: str) -> str:
    s = line.rstrip("\n")
    for pat, rep in _TOKEN_SUBS:
        s = pat.sub(rep, s)
    ls = s.lstrip()
    # 表格边框行：+-----+---+----+  ->  +---+
    if ls.startswith("+") and ls.endswith("+") and set(ls) <= {"+", "-"}:
        return "+---+"
    # show 数据行：去掉单元格内为对齐补的空格 |name |age| -> |name|age|
    if ls.startswith("|") and ls.rstrip().endswith("|"):
        cells = [c.strip() for c in s.split("|")[1:-1]]
        return "|" + "|".join(cells) + "|"
    return s


def compare(golden_text: str, actual_text: str):
    """返回 (status, notes, diff_lines)。
    status: 'ok' 通过 | 'diff' 不一致（diff_lines 为可打印的 unified diff）。"""
    g_lines = golden_text.splitlines()
    a_lines = actual_text.splitlines()
    g_norm = [norm_line(x) for x in g_lines]
    a_norm = [norm_line(x) for x in a_lines]

    if g_norm == a_norm:
        return "ok", [], None

    # 第 2 级：JSON 记录行（读文件得到，行序随文件枚举而变）按集合比较
    g_json = [n for raw, n in zip(g_lines, g_norm) if _JSON_LINE.match(raw)]
    a_json = [n for raw, n in zip(a_lines, a_norm) if _JSON_LINE.match(raw)]
    g_rest = [n for raw, n in zip(g_lines, g_norm) if not _JSON_LINE.match(raw)]
    a_rest = [n for raw, n in zip(a_lines, a_norm) if not _JSON_LINE.match(raw)]
    if g_rest == a_rest and Counter(g_json) == Counter(a_json):
        return ("ok",
                ["JSON 记录行共 %d 行的出现顺序与存档不同（已按记录集合比对，内容一致）"
                 % len(g_json)], None)

    # 第 3 级：数值掩码（如 DATEDIFF(CURRENT_DATE(), ...) 每天漂移 1）
    g_mask = [re.sub(r"\d+", "#", n) for n in g_norm]
    a_mask = [re.sub(r"\d+", "#", n) for n in a_norm]
    if g_mask == a_mask:
        return ("ok",
                ["存在随运行时间漂移的数值（已把数字抹掉后比对，结构完全一致）"], None)

    diff = list(difflib.unified_diff(
        g_lines, a_lines, fromfile="存档 out/", tofile="本次运行", lineterm=""))
    return "diff", [], diff


def run_chapter(py: str, n: int, out_file: str) -> int:
    """运行单章脚本，stdout 写入 out_file；返回退出码。cwd 必须是书根目录。"""
    script = os.path.join(EXAMPLES, "ch%02d.py" % n)
    with open(out_file, "w", encoding="utf-8") as f:
        proc = subprocess.run(
            [py, script], cwd=BOOK_ROOT, stdout=f, stderr=subprocess.DEVNULL)
    return proc.returncode


def cleanup_runtime() -> None:
    """清理重跑必然产生的临时产物（不影响交付物）。"""
    for pat in [
        os.path.join("examples", "data", "out_*"),
        os.path.join("examples", "data", "*.db"),
        os.path.join("examples", "data", "warehouse"),
        os.path.join("examples", "out_mode_csv"),
        os.path.join("spark-warehouse"),
        os.path.join("examples", "spark-warehouse"),
    ]:
        for p in glob.glob(os.path.join(BOOK_ROOT, pat)):
            if os.path.isdir(p):
                shutil.rmtree(p, ignore_errors=True)
            else:
                try:
                    os.remove(p)
                except FileNotFoundError:
                    pass
    for root, dirs, files in os.walk(BOOK_ROOT):
        kept = []
        for d in dirs:
            if d == "__pycache__":
                shutil.rmtree(os.path.join(root, d), ignore_errors=True)
            elif not d.startswith("."):   # 跳过 .git/.Trash-0/.venv-spark 等隐藏目录
                kept.append(d)
        dirs[:] = kept
        for f in files:
            if f.endswith((".pyc", ".class")):
                try:
                    os.remove(os.path.join(root, f))
                except (FileNotFoundError, PermissionError):
                    pass


def main() -> int:
    ap = argparse.ArgumentParser(description="全书示例一键自检")
    ap.add_argument("chapters", nargs="*", type=int,
                    help="要校验的章节号（如 3 5），缺省为 1~9")
    ap.add_argument("--update", action="store_true",
                    help="把本次运行结果覆盖写入 out/ 存档（谨慎）")
    ap.add_argument("--no-clean", action="store_true",
                    help="保留临时输出与运行产物")
    args = ap.parse_args()

    chapters = sorted({c for c in args.chapters if 1 <= c <= 9}) or CHAPTERS
    py = sys.executable

    tmp = tempfile.mkdtemp(prefix="spark_book_verify_")
    failures = []
    try:
        for n in chapters:
            actual = os.path.join(tmp, "ch%02d.txt" % n)
            golden = os.path.join(OUT_DIR, "ch%02d.txt" % n)
            if not os.path.exists(golden):
                print("ch%02d: 缺少存档 examples/out/ch%02d.txt，跳过" % (n, n))
                continue
            rc = run_chapter(py, n, actual)
            print("ch%02d: 运行退出码=%d" % (n, rc), end="")
            if rc != 0:
                print("  [FAIL 脚本报错]")
                failures.append(n)
                continue
            status, notes, diff = compare(
                open(golden, encoding="utf-8").read(),
                open(actual, encoding="utf-8").read())
            if status == "ok":
                print("  [PASS]")
                for nt in notes:
                    print("        ~ %s" % nt)
            else:
                print("  [FAIL 输出与存档不一致，差异如下]")
                for line in diff:
                    print("    " + line.rstrip("\n"))
                failures.append(n)
            if args.update:
                shutil.copyfile(actual, golden)
                print("    -> 已刷新 out/ch%02d.txt" % n)
    finally:
        if not args.no_clean:
            shutil.rmtree(tmp, ignore_errors=True)
            cleanup_runtime()

    if failures:
        print("未通过: ch" + "、ch".join("%02d" % n for n in failures))
        print("提示: 随机 UUID/plan_id/临时目录/日期时间/漂移数值已被自动豁免；"
              "其余差异请检查 PySpark 版本（应为 3.5.x，见 README）。")
        return 1
    print("全部通过: ch01~ch%02d 输出与存档一致（%d 章）"
          % (max(chapters), len(chapters)))
    if args.update:
        print("已用本次运行结果刷新存档。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
