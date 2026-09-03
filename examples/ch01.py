# -*- coding: utf-8 -*-
"""第1章示例：验证脚本（与书中代码一致）"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (SparkSession.builder
         .master("local[2]")
         .appName("spark-sql-book-ch01")
         .config("spark.ui.enabled", "false")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

print("== 示例1.1 第一个 SparkSession ==")
print("Spark 版本:", spark.version)
print("默认 shuffle 分区数:", spark.conf.get("spark.sql.shuffle.partitions"))
print()

print("== 示例1.2 从内存创建 DataFrame ==")
data = [("Alice", 25, "北京"), ("Bob", 30, "上海"), ("Cindy", 28, "广州")]
df = spark.createDataFrame(data, ["name", "age", "city"])
df.printSchema()
df.show(truncate=False)
print()

print("== 示例1.3 注册临时视图并用 SQL 查询 ==")
df.createOrReplaceTempView("people")
spark.sql("SELECT name, age FROM people WHERE age > 26 ORDER BY age DESC").show(truncate=False)
print()

print("== 示例1.4 DataFrame API 与 SQL 等价写法 ==")
df.select("name", "age").filter(F.col("age") > 26).orderBy(F.desc("age")).show(truncate=False)
print()

print("== 示例1.5 查看执行计划 ==")
spark.sql("SELECT city, count(*) AS cnt FROM people GROUP BY city").explain()
print()

print("== 示例1.6 读文件并自动推断 Schema ==")
import tempfile, os
csv_dir = tempfile.mkdtemp(prefix="ch01_")
csv_path = os.path.join(csv_dir, "people.csv")
with open(csv_path, "w") as f:
    f.write("name,age,city\n")
    f.write("David,35,深圳\n")
    f.write("Eva,22,北京\n")
    f.write("Frank,40,杭州\n")
df2 = spark.read.option("header", True).csv(csv_path)
df2.printSchema()
df2.show(truncate=False)
print("临时目录:", csv_dir)
print()

print("== 示例1.7 查看临时视图列表 ==")
print(spark.catalog.listTables())
print()

spark.stop()
