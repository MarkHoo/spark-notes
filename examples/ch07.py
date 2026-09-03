# -*- coding: utf-8 -*-
"""第7章示例：读写外部数据源（验证脚本）"""
import sys, os, shutil, json
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import new_session, make_employees, make_orders

BASE = "examples/data"
for d in ["out_emp_csv", "out_emp_json", "out_emp_parquet", "out_orders_parquet",
          "out_mode_csv", "spark-warehouse"]:
    shutil.rmtree(os.path.join(BASE, d), ignore_errors=True)

from pyspark.sql import SparkSession
spark = (SparkSession.builder.master("local[2]").appName("ch07")
         .config("spark.ui.enabled", "false")
         .config("spark.jars", "examples/lib/sqlite-jdbc.jar")  # JDBC 驱动
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

emp = make_employees(spark)
orders = make_orders(spark)

def list_files(root):
    files = []
    for dirpath, _, names in os.walk(root):
        for n in sorted(names):
            rel = os.path.relpath(os.path.join(dirpath, n), root)
            files.append(rel)
    return files

print("== 7.3.1 CSV 写出 ==")
(emp.select("emp_id", "name", "dept", "salary")
    .write.mode("overwrite").option("header", True)
    .csv("examples/data/out_emp_csv"))
print("写出后的文件:", list_files("examples/data/out_emp_csv"))
print()

print("== 7.3.2 CSV 读回（并自动推断类型） ==")
df_back = (spark.read.option("header", True).option("inferSchema", True)
           .csv("examples/data/out_emp_csv"))
df_back.printSchema()
df_back.show(3, truncate=False)
print()

print("== 7.3.3 读 TSV（自定义分隔符） ==")
(emp.select("emp_id", "name", "city")
    .coalesce(1)  # 单文件：预览行序稳定可复现（同 7.4.1 的说明）
    .write.mode("overwrite").option("header", True).option("sep", "\t")
    .csv("examples/data/out_emp_tsv"))
print("TSV 文件内容预览:")
with open(os.path.join("examples/data/out_emp_tsv",
                       [f for f in os.listdir("examples/data/out_emp_tsv")
                        if f.startswith("part")][0])) as f:
    for _ in range(3):
        print("  ", f.readline().rstrip())
tsv = (spark.read.option("header", True).option("sep", "\t")
       .csv("examples/data/out_emp_tsv"))
print("TSV 读回行数:", tsv.count())
print()

print("== 7.4.1 JSON 写出 ==")
(orders.select("order_id", "user_id", "product", "amount")
    .coalesce(1)  # 合并成单文件：行序稳定，便于示例输出可复现（生产上小文件是话题，见第 8 章）
    .write.mode("overwrite").json("examples/data/out_emp_json"))
print("JSON 文件内容前 2 行:")
with open(os.path.join("examples/data/out_emp_json",
                       [f for f in os.listdir("examples/data/out_emp_json")
                        if f.startswith("part")][0])) as f:
    for _ in range(2):
        print("  ", f.readline().rstrip())
print()

print("== 7.4.2 读多行 JSON（multiline） ==")
multi_json = os.path.join(BASE, "multi_users.json")
with open(multi_json, "w") as f:
    json.dump([
        {"id": 1, "name": "Alice", "tags": ["vip", "老客"]},
        {"id": 2, "name": "Bob", "tags": ["新客"]},
    ], f, ensure_ascii=False, indent=2)
(spark.read.option("multiline", True).json(multi_json)).printSchema()
(spark.read.option("multiline", True).json(multi_json)).show(truncate=False)
print()

print("== 7.5.1 Parquet 写出并按部门分区 ==")
(emp.write.mode("overwrite")
    .partitionBy("dept")
    .parquet("examples/data/out_emp_parquet"))
print("目录结构:")
for rel in sorted(list_files("examples/data/out_emp_parquet")):
    print("  ", rel)
print()

print("== 7.5.2 读回 Parquet：分区列自动出现 ==")
pq = spark.read.parquet("examples/data/out_emp_parquet")
pq.printSchema()
print()

print("== 7.5.3 Parquet 读回时可以直接按分区列过滤 ==")
print("技术部人数:", spark.read.parquet("examples/data/out_emp_parquet")
      .filter("dept = '技术部'").count())
print()

print("== 7.6.1 saveAsTable 保存成'表' ==")
emp.write.mode("overwrite").saveAsTable("emp_tab")
print("spark.table 读取行数:", spark.table("emp_tab").count())
for t in spark.catalog.listTables():
    if t.name == "emp_tab":
        print("表类型:", t.tableType)
print()

print("== 7.6.2 用 SQL 直接查表 ==")
spark.sql("SELECT dept, COUNT(*) AS cnt FROM emp_tab GROUP BY dept").show(truncate=False)
print()

print("== 7.7.1 用 Python 造一张'业务库表'（模拟 MySQL） ==")
db_path = "/tmp/io_book.db"
if os.path.exists(db_path):
    os.remove(db_path)
import sqlite3
con = sqlite3.connect(db_path)
con.execute("CREATE TABLE emp_src (emp_id INTEGER, name TEXT, dept TEXT, salary INTEGER)")
con.executemany(
    "INSERT INTO emp_src VALUES (?,?,?,?)",
    [(1, "Alice", "tech", 12000), (2, "Bob", "sales", 9000),
     (3, "Cindy", "tech", 15000), (4, "David", "ops", 11000)],
)
con.commit()
con.close()
print("已在", db_path, "建好 emp_src 表（4 行）")
print()

print("== 7.7.2 JDBC 读取业务库 ==")
jdbc_opt = dict(
    url="jdbc:sqlite:/tmp/io_book.db",
    dbtable="emp_src",
    driver="org.sqlite.JDBC",
)
df_jdbc = (spark.read.format("jdbc")
           .options(**jdbc_opt).load())
df_jdbc.printSchema()
df_jdbc.show(truncate=False)
print()

print("== 7.7.3 JDBC 写入业务库 ==")
(emp.select("emp_id", "name", "dept", "salary")
    .limit(3)
    .write.format("jdbc")
    .options(**{**jdbc_opt, "dbtable": "emp_dst"})
    .mode("overwrite").save())
print("emp_dst 表当前行数:",
      spark.read.format("jdbc").options(**{**jdbc_opt, "dbtable": "emp_dst"}).load().count())
print()

print("== 7.8.1 写出模式对比 overwrite vs append ==")
base_rows = [(1, "a"), (2, "b")]
add_rows = [(3, "c")]
def read_count(path):
    return (spark.read.option("header", True).csv(path)).count()

spark.createDataFrame(base_rows, ["id", "v"]).write.mode("overwrite") \
     .option("header", True).csv("examples/data/out_mode_csv")
print("第一次 overwrite 后行数:", read_count("examples/data/out_mode_csv"))
spark.createDataFrame(add_rows, ["id", "v"]).write.mode("append") \
     .option("header", True).csv("examples/data/out_mode_csv")
print("append 1 行后行数:", read_count("examples/data/out_mode_csv"))
spark.createDataFrame(base_rows, ["id", "v"]).write.mode("overwrite") \
     .option("header", True).csv("examples/data/out_mode_csv")
print("再次 overwrite 后行数:", read_count("examples/data/out_mode_csv"))
print()

print("== 7.8.2 一个目录多个 part 文件也可以整体读 ==")
print("读回 out_mode_csv 目录（多个 part 文件）行数:",
      read_count("examples/data/out_mode_csv"))
print()

# 清理：删除生成的表与临时输出
spark.sql("DROP TABLE IF EXISTS emp_tab")
for d in ["out_emp_csv", "out_emp_tsv", "out_emp_json", "out_emp_parquet",
          "out_mode_csv"]:
    shutil.rmtree(os.path.join(BASE, d), ignore_errors=True)
os.remove(multi_json)
print("示例产生的临时文件已清理")

spark.stop()
