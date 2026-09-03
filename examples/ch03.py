# -*- coding: utf-8 -*-
"""第3章示例：SQL 查询与临时视图（验证脚本）"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import new_session, make_employees, make_departments
from pyspark.sql import functions as F

spark = new_session("ch03")

emp = make_employees(spark)
dept = make_departments(spark)
emp.createOrReplaceTempView("employees")
dept.createOrReplaceTempView("departments")

print("== 3.2.1 注册视图后用 spark.sql 查询 ==")
spark.sql("""
    SELECT name, dept, salary
    FROM employees
    WHERE salary > 20000
    ORDER BY salary DESC
""").show(truncate=False)
print()

print("== 3.2.2 SQL 与 DataFrame API 同源 ==")
print("SQL  总行数:", spark.sql("SELECT * FROM employees").count())
print("API  总行数:", emp.count())
print()

print("== 3.2.3 从视图中取回 DataFrame：spark.table ==")
t = spark.table("employees")
print("spark.table 类型:", type(t).__name__)
print()

print("== 3.3.1 别名 AS ==")
spark.sql("SELECT name AS `姓名`, salary AS `月薪` FROM employees LIMIT 3").show(truncate=False)
print()

print("== 3.3.2 DISTINCT 去重 ==")
spark.sql("SELECT DISTINCT city FROM employees").show(truncate=False)
print()

print("== 3.3.3 GROUP BY + HAVING ==")
spark.sql("""
    SELECT dept, COUNT(*) AS cnt, ROUND(AVG(salary), 0) AS avg_salary
    FROM employees
    GROUP BY dept
    HAVING COUNT(*) >= 2
    ORDER BY cnt DESC
""").show(truncate=False)
print()

print("== 3.3.4 各子句书写顺序与执行顺序 ==")
spark.sql("""
    SELECT dept, COUNT(*) AS cnt
    FROM employees
    WHERE salary >= 15000
    GROUP BY dept
    HAVING COUNT(*) >= 2
    ORDER BY cnt DESC
    LIMIT 2
""").show(truncate=False)
print()

print("== 3.4.1 用 CTE 计算'高于本部门平均薪资' ==")
spark.sql("""
    WITH dept_avg AS (
        SELECT dept, AVG(salary) AS avg_sal
        FROM employees
        GROUP BY dept
    )
    SELECT e.name, e.dept, e.salary, ROUND(d.avg_sal, 0) AS dept_avg_salary
    FROM employees e
    JOIN dept_avg d ON e.dept = d.dept
    WHERE e.salary > d.avg_sal
    ORDER BY e.dept, e.salary DESC
""").show(truncate=False)
print()

print("== 3.4.2 CTE 一次写多条 ==")
spark.sql("""
    WITH cnt AS (SELECT dept, COUNT(*) AS n FROM employees GROUP BY dept)
    SELECT dept, n
    FROM cnt
    WHERE n = (SELECT MAX(n) FROM cnt)
""").show(truncate=False)
print()

print("== 3.5.1 IN 子查询 ==")
spark.sql("""
    SELECT name, dept
    FROM employees
    WHERE dept IN (SELECT dept FROM departments WHERE budget >= 200000)
    ORDER BY dept
""").show(truncate=False)
print()

print("== 3.5.2 EXISTS 子查询 ==")
spark.sql("""
    SELECT dept, manager
    FROM departments d
    WHERE EXISTS (SELECT 1 FROM employees e WHERE e.dept = d.dept)
""").show(truncate=False)
print()

print("== 3.6.1 全局临时视图 ==")
emp.createGlobalTempView("g_employees")
spark.sql("SELECT name, dept FROM global_temp.g_employees LIMIT 3").show(truncate=False)
print()

print("== 3.6.2 查看当前库与全部表 ==")
print("当前数据库:", spark.catalog.currentDatabase())
for t in spark.catalog.listTables():
    print("表:", t.name, "| 类型:", t.tableType)
print()

print("== 3.7.1 关键字字段名需要反引号 ==")
spark.sql("""
    CREATE OR REPLACE TEMP VIEW v_special AS
    SELECT dept AS `group`, name AS `select`
    FROM employees
""")
spark.sql("SELECT `group`, count(*) AS n FROM v_special GROUP BY `group`").show(truncate=False)
print()

print("== 3.7.2 UNION ALL 与 UNION ==")
spark.sql("""
    SELECT 'A' AS tag, dept FROM employees WHERE dept = '技术部'
    UNION ALL
    SELECT 'B' AS tag, dept FROM employees WHERE dept = '产品部'
""").show(truncate=False)
print()

print("== 3.8.1 演示数据：一张 6 行的销售表 ==")
spark.sql("""
    CREATE OR REPLACE TEMP VIEW sales AS
    SELECT * FROM VALUES
        ('华东', 'A', 100), ('华东', 'B', 200), ('华东', 'A', 50),
        ('华北', 'A', 300), ('华北', 'B', 400), ('华南', 'C', 500)
        AS t(region, product, amount)
""")
spark.table("sales").show(truncate=False)
print()

print("== 3.8.1 ROLLUP：小计逐层向上，末尾一行是总计 ==")
spark.sql("""
    SELECT region, product, SUM(amount) AS total
    FROM sales
    GROUP BY ROLLUP(region, product)
    ORDER BY region NULLS LAST, product NULLS LAST
""").show(truncate=False)
print()

print("-- GROUPING() 区分'真 NULL'与'小计占位 NULL' --")
spark.sql("""
    SELECT region, product, SUM(amount) AS total,
           GROUPING(region) AS grp_region, GROUPING(product) AS grp_product
    FROM sales
    GROUP BY ROLLUP(region, product)
    ORDER BY region NULLS LAST, product NULLS LAST
""").show(truncate=False)
print()

print("== 3.8.2 CUBE：所有维度组合都出一份小计 ==")
spark.sql("""
    SELECT region, product, SUM(amount) AS total
    FROM sales
    GROUP BY CUBE(region, product)
    ORDER BY region NULLS LAST, product NULLS LAST
""").show(truncate=False)
print()

print("== 3.8.3 GROUPING SETS：只要自己指定的那几种组合 ==")
spark.sql("""
    SELECT region, product, SUM(amount) AS total
    FROM sales
    GROUP BY GROUPING SETS ((region), (region, product))
    ORDER BY region NULLS LAST, product NULLS LAST
""").show(truncate=False)
print()

print("== 3.8.4 行转列 pivot：把 product 摊平成多列 ==")
spark.table("sales").groupBy("region") \
    .pivot("product") \
    .agg(F.sum("amount")) \
    .orderBy("region") \
    .show(truncate=False)
print()

spark.stop()
