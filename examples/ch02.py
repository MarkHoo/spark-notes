# -*- coding: utf-8 -*-
"""第2章示例：DataFrame 核心操作（验证脚本）"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import new_session, make_employees
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = new_session("ch02")

print("== 2.2.1 从元组列表创建，显式指定列名 ==")
df1 = spark.createDataFrame(
    [("Alice", 25), ("Bob", 30)],
    ["name", "age"],
)
df1.show()
print()

print("== 2.2.2 从字典列表创建 ==")
df2 = spark.createDataFrame([
    {"name": "Alice", "age": 25},
    {"name": "Bob", "age": 30},
])
df2.printSchema()
df2.show()
print()

print("== 2.2.3 用 SQL 的 VALUES 子句创建 ==")
df3 = spark.sql("""
    SELECT 1 AS id, 'Alice' AS name
    UNION ALL
    SELECT 2 AS id, 'Bob' AS name
""")
df3.show()
print()

print("== 2.2.4 读 CSV 并自动推断类型 ==")
df4 = (spark.read
       .option("header", True)
       .option("inferSchema", True)
       .csv("examples/data/people.csv"))
df4.printSchema()
print()

print("== 2.3.1 show / count / first / head ==")
emp = make_employees(spark)
emp.show(5)                      # 默认最多显示 20 行
print("总行数:", emp.count())
print("第一行:", emp.first())
print("前 2 行:", emp.head(2))
print()

print("== 2.3.2 describe 统计信息 ==")
emp.select("salary", "age").describe().show()
print()

print("== 2.3.3 查看列名列表 ==")
print("列名:", emp.columns)
print()

print("== 2.4.1 select 选择列 ==")
emp.select("name", "dept").show(5, truncate=False)
print()

print("== 2.4.2 selectExpr 写表达式 ==")
emp.selectExpr("name", "salary / 10000 AS salary_wan").show(5, truncate=False)
print()

print("== 2.4.3 withColumn 新增/替换列 ==")
emp.withColumn("salary_raise", F.col("salary") * 1.1).show(5, truncate=False)
print()

print("== 2.4.4 用表达式函数 F.expr 新增列 ==")
emp.withColumn("age_group",
               F.expr("CASE WHEN age < 30 THEN '青年' "
                      "WHEN age < 35 THEN '中年' ELSE '资深' END")) \
   .select("name", "age", "age_group").show(truncate=False)
print()

print("== 2.4.5 重命名列 withColumnRenamed ==")
print(emp.withColumnRenamed("salary", "monthly_salary").columns)
print()

print("== 2.4.6 删除列 drop ==")
print(emp.drop("hire_date").columns)
print()

print("== 2.4.7 类型转换 cast ==")
emp.select("salary", F.col("salary").cast("double").alias("salary_d")) \
   .printSchema()
print()

print("== 2.5.1 过滤 ==")
emp.filter(F.col("dept") == "技术部").show(truncate=False)
print()

print("== 2.5.2 多个条件组合 ==")
emp.filter((F.col("dept") == "技术部") & (F.col("salary") >= 20000)) \
   .select("name", "dept", "salary").show(truncate=False)
print()

print("== 2.5.3 OR 条件 ==")
emp.where((F.col("city") == "北京") | (F.col("dept") == "财务部")) \
   .select("name", "dept", "city").show(truncate=False)
print()

print("== 2.5.4 isin 条件 ==")
emp.filter(F.col("dept").isin("技术部", "产品部")) \
   .select("name", "dept").show(truncate=False)
print()

print("== 2.5.5 like 模糊匹配 ==")
emp.filter(F.col("name").like("张%")).select("name").show(truncate=False)
print()

print("== 2.5.6 between 范围 ==")
emp.filter(F.col("salary").between(15000, 25000)) \
   .select("name", "salary").show(truncate=False)
print()

print("== 2.5.7 字符串 startswith 判断 ==")
emp.filter(F.col("city").startswith("上")).select("name", "city").show(truncate=False)
print()

print("== 2.6 排序 orderBy 多列 ==")
emp.orderBy(F.desc("salary"), F.asc("age")) \
   .select("name", "salary", "age").show(truncate=False)
print()

print("== 2.7.1 distinct 全字段去重 ==")
df_dup = spark.createDataFrame(
    [("Alice", 25), ("Alice", 25), ("Bob", 30), ("Bob", 31)],
    ["name", "age"],
)
print("去重前行数:", df_dup.count(), "-> 去重后行数:", df_dup.distinct().count())
print()

print("== 2.7.2 dropDuplicates 按指定列去重 ==")
df_dup2 = spark.createDataFrame(
    [("Alice", 25, "A001"), ("Alice", 26, "A002"), ("Bob", 30, "B001")],
    ["name", "age", "card"],
)
df_dup2.dropDuplicates(["name"]).show(truncate=False)
print()

print("== 2.8.1 空值过滤 isNull ==")
df_null = spark.createDataFrame(
    [(1, "Alice", None), (2, "Bob", 5000), (3, "Cindy", None)],
    ["id", "name", "bonus"],
)
df_null.filter(F.col("bonus").isNull()).show(truncate=False)
print("空值行数:", df_null.filter(F.col("bonus").isNull()).count())
print("非空行数:", df_null.filter(F.col("bonus").isNotNull()).count())
print()

print("== 2.8.2 删除含空值的行 dropna ==")
df_null.dropna().show(truncate=False)
print()

print("== 2.8.3 空值填充 fillna ==")
df_null.fillna(0).show(truncate=False)
print()

print("== 2.8.4 用 coalesce 填充（SQL 通用） ==")
df_null.select("id", F.coalesce("bonus", F.lit(0)).alias("bonus")) \
       .show(truncate=False)
print()

print("== 2.9.1 groupBy 基本聚合 ==")
emp.groupBy("dept").count().orderBy("dept").show(truncate=False)
print()

print("== 2.9.2 多个聚合函数 agg ==")
emp.groupBy("dept").agg(
    F.count("*").alias("人数"),
    F.sum("salary").alias("总薪资"),
    F.avg("salary").alias("平均薪资"),
    F.max("salary").alias("最高薪资"),
    F.min("salary").alias("最低薪资"),
).orderBy("dept").show(truncate=False)
print()

print("== 2.9.3 先过滤再分组 ==")
emp.filter(F.col("dept") != "市场部") \
   .groupBy("dept").agg(F.round(F.avg("salary"), 1).alias("avg_salary")) \
   .orderBy(F.desc("avg_salary")).show(truncate=False)
print()

print("== 2.2.5 显式指定 Schema（方式一：StructType 对象） ==")
schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("city", StringType(), True),
])
df5 = (spark.read.option("header", True)
       .schema(schema)
       .csv("examples/data/people.csv"))
df5.printSchema()
df5.show(truncate=False)
print()

print("-- 方式二：DDL 字符串（写起来更短）--")
df6 = (spark.read.option("header", True)
       .schema("name STRING, age INT, city STRING")
       .csv("examples/data/people.csv"))
df6.printSchema()
print()

print("== 2.8.5 NULL 的三值逻辑 ==")
df_null3 = spark.createDataFrame(
    [(1, "Alice", 8000), (2, "Bob", None), (3, None, 6000), (4, None, 7000)],
    ["id", "name", "bonus"],
)
print("-- 演示表（bonus / name 里各有 NULL）--")
df_null3.orderBy("id").show(truncate=False)
print()

print("-- 坑：等号比较永远查不到 NULL（NULL 参与比较，结果既不是真也不是假，而是'未知'）--")
df_null3.filter("bonus = NULL").show(truncate=False)
print()

print("-- 正确姿势：用 isNull / isNotNull 显式判断 --")
df_null3.filter(F.col("bonus").isNull()).select("id", "name").show(truncate=False)
print()

print("-- 聚合时 NULL 被自动忽略：avg/sum/count(列) 都跳过 NULL，count(*) 不跳 --")
df_null3.agg(
    F.avg("bonus").alias("avg(bonus)"),
    F.sum("bonus").alias("sum(bonus)"),
    F.count("bonus").alias("count(bonus)"),
    F.count("*").alias("count(*)"),
).show(truncate=False)
print()

print("-- 去重时 NULL 被视为'同一个值'；countDistinct 同样忽略 NULL --")
print("按 name 去重前总行数:", df_null3.count())
print("按 name 去重后行数:", df_null3.dropDuplicates(["name"]).count())
print("countDistinct(name):", df_null3.select(F.countDistinct("name")).collect()[0][0])
print()

spark.stop()
