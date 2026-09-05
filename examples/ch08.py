# -*- coding: utf-8 -*-
"""第8章示例：性能调优实践（验证脚本）"""
import sys, os, shutil
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import new_session, make_employees, make_departments, make_orders
from pyspark.sql import functions as F

spark = new_session("ch08")

emp = make_employees(spark)
dept = make_departments(spark)
orders = make_orders(spark)
emp.createOrReplaceTempView("employees")
dept.createOrReplaceTempView("departments")
orders.createOrReplaceTempView("orders")

print("== 8.2.1 默认关键配置 ==")
for k in ["spark.sql.adaptive.enabled",
          "spark.sql.autoBroadcastJoinThreshold",
          "spark.sql.shuffle.partitions"]:
    print(f"{k} = {spark.conf.get(k)}")
print()

print("== 8.2.2 explain 读执行计划：GROUP BY ==")
spark.sql("SELECT dept, COUNT(*) FROM employees GROUP BY dept").explain()
print()

print("== 8.2.3 explain 读执行计划：JOIN ==")
spark.sql("""
    SELECT e.name, d.manager
    FROM employees e JOIN departments d ON e.dept = d.dept
""").explain()
print()

print("== 8.3.1 分区数由 spark.sql.shuffle.partitions 控制 ==")
spark.conf.set("spark.sql.adaptive.enabled", "false")   # 先关掉 AQE，看清 shuffle 分区
spark.conf.set("spark.sql.shuffle.partitions", "4")
g = orders.groupBy("category").count()
g.count()   # 触发执行
print("shuffle.partitions=4 时，GROUP BY 结果分区数:", g.rdd.getNumPartitions())
spark.conf.set("spark.sql.shuffle.partitions", "8")
g8 = orders.groupBy("category").count()
g8.count()
print("shuffle.partitions=8 时，GROUP BY 结果分区数:", g8.rdd.getNumPartitions())
spark.conf.set("spark.sql.shuffle.partitions", "200")
spark.conf.set("spark.sql.adaptive.enabled", "true")
print()

print("== 8.4.1 AQE 开启时，小结果自动收敛分区 ==")
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "200")
ga = orders.groupBy("category").count()
ga.count()
print("AQE 开启(200 分区设定)后 GROUP BY 实际分区数:", ga.rdd.getNumPartitions())
spark.conf.set("spark.sql.adaptive.enabled", "false")
gb = orders.groupBy("category").count()
gb.count()
print("AQE 关闭(200 分区设定)后 GROUP BY 实际分区数:", gb.rdd.getNumPartitions())
spark.conf.set("spark.sql.adaptive.enabled", "true")
print()

print("== 8.5.1 默认 join 走 SortMergeJoin：内存表无统计信息，不会自动广播 ==")
print("     （尽管默认阈值 autoBroadcastJoinThreshold=10MB）")
spark.sql("""
    SELECT e.name, d.manager
    FROM employees e JOIN departments d ON e.dept = d.dept
""").explain()
print()

print("== 8.5.2 把自动广播阈值设为 -1（关闭）后，仍是 SortMergeJoin ==")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
spark.sql("""
    SELECT e.name, d.manager
    FROM employees e JOIN departments d ON e.dept = d.dept
""").explain()
print()

print("== 8.5.3 手动 hint：/*+ BROADCAST(d) */ 强制广播 ==")
spark.sql("""
    SELECT /*+ BROADCAST(d) */ e.name, d.manager
    FROM employees e JOIN departments d ON e.dept = d.dept
""").explain()
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")
print()

print("== 8.6.1 缓存：cache 后用 explain 看 InMemoryTableScan ==")
cached = emp.filter(F.col("dept") == "技术部").cache()
print("存储级别:", cached.storageLevel)
cached.count()   # 第一次真正计算并放入缓存
cached.explain()
print("是否已缓存:", cached.is_cached)
cached.unpersist()
print("解除缓存后是否还缓存:", cached.is_cached)
print()

print("== 8.7.1 输出小文件：coalesce(1) 控制 part 文件数 ==")
out_dir = "examples/data/out_coalesce"
shutil.rmtree(out_dir, ignore_errors=True)
(orders.groupBy("category").count()
       .coalesce(1)
       .write.mode("overwrite").csv(out_dir))
part_files = [f for f in os.listdir(out_dir) if f.startswith("part")]
print("coalesce(1) 写出的 part 文件数:", len(part_files))
shutil.rmtree(out_dir, ignore_errors=True)
print()

print("== 8.8.1 数据倾斜：两阶段加盐聚合结果与普通聚合一致 ==")
spark.sql("""
    CREATE OR REPLACE TEMP VIEW sales AS
    SELECT * FROM (
        SELECT 1 AS dept, 100 AS amount UNION ALL SELECT 1, 100
        UNION ALL SELECT 1, 100 UNION ALL SELECT 1, 100
        UNION ALL SELECT 1, 100 UNION ALL SELECT 1, 100
        UNION ALL SELECT 2, 10   UNION ALL SELECT 2, 10
        UNION ALL SELECT 3, 20   UNION ALL SELECT 3, 20
    )
""")
naive = spark.sql("SELECT dept, SUM(amount) AS s FROM sales GROUP BY dept ORDER BY dept")
naive.show(truncate=False)

# 两阶段：先按 (dept, 随机盐) 聚合，再按 dept 聚合
salted = (spark.table("sales")
          .withColumn("salt", (F.rand() * 3).cast("int")))
stage1 = salted.groupBy("dept", "salt").agg(F.sum("amount").alias("part"))
stage2 = (stage1.groupBy("dept")
          .agg(F.sum("part").alias("s"))
          .orderBy("dept"))
stage2.show(truncate=False)
print("两阶段聚合与普通聚合结果一致:", 
      [tuple(r) for r in naive.collect()] == [tuple(r) for r in stage2.collect()])
print()

print("== 8.8.2 处理倾斜的另一种思路：先聚合再 join（下推） ==")
print("略，思路见正文。")
print()

spark.stop()
