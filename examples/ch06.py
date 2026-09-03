# -*- coding: utf-8 -*-
"""第6章示例：窗口函数实战（验证脚本）"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import new_session, make_employees, make_orders
from pyspark.sql import functions as F

spark = new_session("ch06")

emp = make_employees(spark)
orders = make_orders(spark)
emp.createOrReplaceTempView("employees")
orders.createOrReplaceTempView("orders")

# 成绩表：班级内有并列分数，用于演示三个排名函数的差异
spark.sql("""
    CREATE OR REPLACE TEMP VIEW scores AS
    SELECT 'A' AS class, '小明' AS name, 90 AS score
    UNION ALL SELECT 'A', '小红', 90
    UNION ALL SELECT 'A', '小刚', 80
    UNION ALL SELECT 'A', '小强', 80
    UNION ALL SELECT 'B', '阿丽', 85
    UNION ALL SELECT 'B', '阿飞', 70
""")

print("== 6.2.1 窗口函数入门：部门内按薪资排名 ==")
spark.sql("""
    SELECT name, dept, salary,
           ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rn
    FROM employees
    ORDER BY dept, rn
""").show(truncate=False)
print()

print("== 6.3.1 三个排名函数对比 ==")
spark.sql("""
    SELECT class, name, score,
           ROW_NUMBER() OVER (PARTITION BY class ORDER BY score DESC) AS rn,
           RANK()       OVER (PARTITION BY class ORDER BY score DESC) AS rk,
           DENSE_RANK() OVER (PARTITION BY class ORDER BY score DESC) AS drk
    FROM scores
    ORDER BY class, score DESC, name
""").show(truncate=False)
print()

print("== 6.4.1 经典：每个部门薪资 Top 2 ==")
spark.sql("""
    SELECT name, dept, salary, rn
    FROM (
        SELECT name, dept, salary,
               ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rn
        FROM employees
    ) t
    WHERE rn <= 2
    ORDER BY dept, rn
""").show(truncate=False)
print()

print("== 6.4.2 经典：每个用户最近一笔订单（按时间去重取最新） ==")
spark.sql("""
    SELECT user_id, order_id, product, amount, order_date
    FROM (
        SELECT user_id, order_id, product, amount, order_date,
               ROW_NUMBER() OVER (PARTITION BY user_id
                                  ORDER BY order_date DESC, order_id DESC) AS rn
        FROM orders
    ) t
    WHERE rn = 1
    ORDER BY user_id
""").show(truncate=False)
print()

print("== 6.5.1 LAG：每笔订单相对上一笔的变化 ==")
spark.sql("""
    SELECT user_id, order_date, amount,
           LAG(amount, 1) OVER (PARTITION BY user_id ORDER BY order_date) AS prev_amount
    FROM orders
    ORDER BY user_id, order_date
""").show(truncate=False)
print()

print("== 6.6.1 每日销售额 + 累计值 ==")
spark.sql("""
    WITH daily AS (
        SELECT order_date, SUM(amount) AS day_amt
        FROM orders
        WHERE status = '已支付'
        GROUP BY order_date
    )
    SELECT order_date, day_amt,
           SUM(day_amt) OVER (ORDER BY order_date
                              ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_amt
    FROM daily
    ORDER BY order_date
""").show(truncate=False)
print()

print("== 6.6.2 3 日移动平均 ==")
spark.sql("""
    WITH daily AS (
        SELECT order_date, SUM(amount) AS day_amt
        FROM orders
        WHERE status = '已支付'
        GROUP BY order_date
    )
    SELECT order_date, day_amt,
           ROUND(AVG(day_amt) OVER (ORDER BY order_date
                                    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS ma3
    FROM daily
    ORDER BY order_date
""").show(truncate=False)
print()

print("== 6.6.3 部门内薪资占比 ==")
spark.sql("""
    SELECT name, dept, salary,
           ROUND(salary / SUM(salary) OVER (PARTITION BY dept), 4) AS dept_share
    FROM employees
    ORDER BY dept, salary DESC
""").show(truncate=False)
print()

print("== 6.7.1 NTILE 分桶：按薪资分 4 档 ==")
spark.sql("""
    SELECT name, salary,
           NTILE(4) OVER (ORDER BY salary DESC) AS quartile
    FROM employees
    ORDER BY salary DESC
""").show(truncate=False)
print()

spark.stop()
