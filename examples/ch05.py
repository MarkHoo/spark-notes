# -*- coding: utf-8 -*-
"""第5章示例：表连接 Join 详解（验证脚本）"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import new_session, make_employees, make_departments, make_orders

spark = new_session("ch05")

emp = make_employees(spark)
dept = make_departments(spark)
orders = make_orders(spark)
emp.createOrReplaceTempView("employees")
dept.createOrReplaceTempView("departments")
orders.createOrReplaceTempView("orders")

# 扩充员工表：多一个"行政部"员工（部门表里没有），多一个 dept=NULL 的员工
spark.sql("""
    CREATE OR REPLACE TEMP VIEW emp_full AS
    SELECT * FROM employees
    UNION ALL SELECT 11, '钱进', '行政部', 14000, 25, '北京', '2024-01-05'
    UNION ALL SELECT 12, '周舟', NULL, 16000, 26, '上海', '2024-02-01'
""")

print("== 5.2.1 SQL：INNER JOIN 员工↔部门 ==")
spark.sql("""
    SELECT e.emp_id, e.name, e.dept, d.manager, d.budget
    FROM employees e
    JOIN departments d ON e.dept = d.dept
    ORDER BY e.emp_id
""").show(truncate=False)
print("行数:", spark.sql("""
    SELECT e.emp_id FROM employees e
    JOIN departments d ON e.dept = d.dept
""").count())
print()

print("== 5.2.2 DataFrame API 等价写法 ==")
emp.join(dept, on="dept", how="inner") \
   .select("emp_id", "name", "dept", "manager", "budget") \
   .orderBy("emp_id").show(truncate=False)
print()

print("== 5.3 LEFT JOIN（含'行政部'与'无部门'员工） ==")
spark.sql("""
    SELECT e.emp_id, e.name, e.dept, d.manager
    FROM emp_full e
    LEFT JOIN departments d ON e.dept = d.dept
    ORDER BY e.emp_id
""").show(truncate=False)
print("其中经理为 NULL 的行数:",
      spark.sql("""
        SELECT e.name FROM emp_full e
        LEFT JOIN departments d ON e.dept = d.dept
        WHERE d.manager IS NULL
      """).count())
print()

print("== 5.4 RIGHT JOIN ==")
spark.sql("""
    SELECT d.dept, d.manager, e.name
    FROM emp_full e
    RIGHT JOIN departments d ON e.dept = d.dept
    ORDER BY d.dept
""").show(truncate=False)
print()

print("== 5.5 FULL OUTER JOIN ==")
spark.sql("""
    SELECT COALESCE(e.dept, d.dept) AS dept, e.name, d.manager
    FROM emp_full e
    FULL OUTER JOIN departments d ON e.dept = d.dept
    ORDER BY dept, e.name
""").show(truncate=False)
print()

print("== 5.6.1 LEFT SEMI JOIN：找出'在部门表里有对应部门'的员工 ==")
spark.sql("""
    SELECT e.name, e.dept
    FROM emp_full e
    LEFT SEMI JOIN departments d ON e.dept = d.dept
    ORDER BY e.emp_id
""").show(truncate=False)
print()

print("== 5.6.2 LEFT ANTI JOIN：找出'部门表里没有对应部门'的员工 ==")
spark.sql("""
    SELECT e.name, e.dept
    FROM emp_full e
    LEFT ANTI JOIN departments d ON e.dept = d.dept
    ORDER BY e.emp_id
""").show(truncate=False)
print()

print("== 5.7.1 连接键含 NULL 的行为 ==")
print("emp_full 总行数:", spark.table("emp_full").count())
print("INNER 后行数:",
      spark.sql("SELECT e.name FROM emp_full e JOIN departments d ON e.dept = d.dept").count())
print("LEFT  后行数:",
      spark.sql("SELECT e.name FROM emp_full e LEFT JOIN departments d ON e.dept = d.dept").count())
print()

print("== 5.7.2 连接键类型不一致 ==")
spark.sql("CREATE OR REPLACE TEMP VIEW codes AS SELECT 1 AS id, 'A' AS c UNION ALL SELECT 2 AS id, 'B' AS c")
spark.sql("CREATE OR REPLACE TEMP VIEW names AS SELECT '1' AS id, 'one' AS nm UNION ALL SELECT 'X' AS id, 'eks' AS nm")
print("-- 情况1：直接 join 不报错，但类型不一致的行匹配不上（X 丢失） --")
spark.sql("""
    SELECT a.id, a.c, b.nm
    FROM codes a JOIN names b ON a.id = b.id
    ORDER BY a.id
""").show(truncate=False)
print("-- 情况2：CAST 非数字串在默认（非 ANSI）模式下返回 NULL --")
spark.sql("SELECT CAST('X' AS INT) AS v").show(truncate=False)
print("-- 情况3：开启 ANSI 模式后，同样的 CAST 直接报错 --")
spark.conf.set("spark.sql.ansi.enabled", "true")
try:
    spark.sql("SELECT CAST('X' AS INT) AS v").show()
except Exception as ex:
    msg = str(ex)
    print("报错类型:", type(ex).__name__)
    print(msg.splitlines()[0])
print()
spark.conf.set("spark.sql.ansi.enabled", "false")
print("-- 修正写法：显式 CAST 统一类型，一眼能看出两边都是 INT --")
spark.sql("""
    SELECT a.id, a.c, b.nm
    FROM codes a JOIN names b ON a.id = CAST(b.id AS INT)
    ORDER BY a.id
""").show(truncate=False)
print()

print("== 5.7.3 重复连接键导致行数膨胀 ==")
spark.sql("""
    CREATE OR REPLACE TEMP VIEW dept_tag AS
    SELECT '技术部' AS dept, '薪资高' AS tag
    UNION ALL SELECT '技术部', '人数多'
    UNION ALL SELECT '产品部', '用户多'
    UNION ALL SELECT '市场部', '活动多'
""")
spark.sql("""
    SELECT e.dept, COUNT(*) AS cnt
    FROM employees e
    JOIN dept_tag t ON e.dept = t.dept
    GROUP BY e.dept
    ORDER BY e.dept
""").show(truncate=False)
print("原 employees 10 行，join 后总行数:",
      spark.sql("SELECT e.name FROM employees e JOIN dept_tag t ON e.dept = t.dept").count())
print()

print("== 5.8 三表关联：订单×员工×部门 ==")
spark.sql("""
    SELECT o.order_id, o.product, e.name AS user_name, d.manager
    FROM orders o
    JOIN employees e ON o.user_id = e.emp_id
    JOIN departments d ON e.dept = d.dept
    ORDER BY o.order_id
""").show(truncate=False)
print()

spark.stop()
