# -*- coding: utf-8 -*-
"""第4章示例：常用内置函数精讲（验证脚本）"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import new_session, make_employees, make_orders
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

spark = new_session("ch04")

emp = make_employees(spark)
orders = make_orders(spark)
emp.createOrReplaceTempView("employees")
orders.createOrReplaceTempView("orders")

# ---------- 4.2 数值函数 ----------
print("== 4.2.1 数值函数一网打尽 ==")
spark.sql("""
    SELECT ROUND(3.14159, 2) AS r_round,
           ROUND(2.5)        AS r_round25,
           BROUND(2.5)       AS r_bround25,
           FLOOR(3.9)        AS r_floor,
           CEIL(3.1)         AS r_ceil,
           ABS(-5)           AS r_abs,
           POWER(2, 10)      AS r_power,
           SQRT(2)           AS r_sqrt
""").show(truncate=False)
print()

print("== 4.2.2 用 round 处理金额（订单 9 折） ==")
orders.selectExpr("order_id", "amount", "ROUND(amount * 0.9, 2) AS discount") \
      .show(5, truncate=False)
print()

# ---------- 4.3 字符串函数 ----------
print("== 4.3.1 字符串函数一网打尽 ==")
spark.sql("""
    SELECT CONCAT('Spark', 'SQL')            AS s_concat,
           CONCAT_WS('-', '2026', '09', '01') AS s_ws,
           LENGTH('Spark SQL')               AS s_len,
           UPPER('hello')                    AS s_upper,
           LOWER('HELLO')                    AS s_lower,
           TRIM('  hi  ')                    AS s_trim,
           LPAD('7', 3, '0')                 AS s_lpad,
           SUBSTRING('Spark SQL', 1, 5)      AS s_sub,
           INSTR('Spark SQL', 'SQL')         AS s_pos,
           REPLACE('hello world', 'world', 'spark') AS s_replace
""").show(truncate=False, vertical=False)
print()

print("== 4.3.2 split 与数组 ==")
spark.sql("SELECT SPLIT('a,b,c', ',') AS arr").show(truncate=False)
print()

print("== 4.3.3 正则：regexp_extract / regexp_replace ==")
spark.sql("""
    SELECT 'order_2026_001' AS raw_str,
           REGEXP_EXTRACT('order_2026_001', '_(\\\\d+)', 1) AS ext_num,
           REGEXP_REPLACE('id:123', '\\\\d+', '*')          AS replaced
""").show(truncate=False)
print()

print("== 4.3.4 业务例子：从邮箱地址提取域名 ==")
email_df = spark.createDataFrame(
    [("Alice", "alice@company.com"), ("Bob", "bob@sub.example.com")],
    ["name", "email"],
)
email_df.createOrReplaceTempView("emails")
spark.sql("""
    SELECT name, email,
           REGEXP_EXTRACT(email, '@(.+)$', 1) AS domain
    FROM emails
""").show(truncate=False)
print()

print("== 4.3.5 业务例子：订单日期补零为规范格式 ==")
spark.sql("SELECT CONCAT_WS('-', '2026', LPAD('6', 2, '0'), LPAD('1', 2, '0')) AS dt") \
      .show(truncate=False)
print()

# ---------- 4.4 日期时间函数 ----------
print("== 4.4.1 类型转换 to_date ==")
spark.sql("""
    SELECT TO_DATE('2026-06-01')                       AS d1,
           TO_DATE('20260601', 'yyyyMMdd')             AS d2,
           TO_DATE('2026/06/01', 'yyyy/MM/dd')         AS d3
""").show(truncate=False)
print()

print("== 4.4.2 取当前日期/时间 ==")
spark.sql("SELECT CURRENT_DATE() AS today, CURRENT_TIMESTAMP() AS now").show(truncate=False)
print()

print("== 4.4.3 日期加减与间隔 ==")
spark.sql("""
    SELECT DATE_ADD(TO_DATE('2026-06-01'), 7)     AS plus7,
           DATE_SUB(TO_DATE('2026-06-15'), 7)     AS minus7,
           ADD_MONTHS(TO_DATE('2026-06-01'), 3)   AS plus3m,
           LAST_DAY(TO_DATE('2026-02-01'))        AS feb_end,
           DATEDIFF(TO_DATE('2026-06-15'), TO_DATE('2026-06-01')) AS days_gap,
           MONTHS_BETWEEN(TO_DATE('2026-06-15'), TO_DATE('2026-03-01')) AS month_gap
""").show(truncate=False)
print()

print("== 4.4.4 提取与格式化 ==")
spark.sql("""
    SELECT TO_DATE('2026-06-15') AS d,
           YEAR(TO_DATE('2026-06-15'))   AS y,
           MONTH(TO_DATE('2026-06-15'))  AS m,
           DAY(TO_DATE('2026-06-15'))    AS day,
           DAYOFWEEK(TO_DATE('2026-06-15')) AS dow,
           WEEKOFYEAR(TO_DATE('2026-06-15')) AS woy,
           DATE_FORMAT(TO_DATE('2026-06-15'), 'yyyy-MM') AS ym
""").show(truncate=False)
print()

print("== 4.4.5 业务例子：入职年份分布 ==")
spark.sql("""
    SELECT YEAR(TO_DATE(hire_date)) AS hire_year, COUNT(*) AS cnt
    FROM employees
    GROUP BY hire_year
    ORDER BY hire_year
""").show(truncate=False)
print()

print("== 4.4.6 业务例子：计算每单距今多少天 ==")
spark.sql("""
    SELECT order_id, order_date,
           DATEDIFF(CURRENT_DATE(), TO_DATE(order_date)) AS days_ago
    FROM orders
    ORDER BY order_id
    LIMIT 5
""").show(truncate=False)
print()

# ---------- 4.5 条件与空值函数 ----------
print("== 4.5.1 IF 函数 ==")
spark.sql("SELECT IF(salary >= 30000, '高薪', '普通') AS level FROM employees") \
      .groupBy("level").count().show(truncate=False)
print()

print("== 4.5.2 CASE WHEN ==")
spark.sql("""
    SELECT name, salary,
           CASE
               WHEN salary >= 30000 THEN '高'
               WHEN salary >= 20000 THEN '中'
               ELSE '低'
           END AS level
    FROM employees
    ORDER BY salary DESC
""").show(truncate=False)
print()

print("== 4.5.3 NULLIF ==")
spark.sql("SELECT NULLIF(5, 5) AS a, NULLIF(5, 3) AS b").show(truncate=False)
print()

print("== 4.5.4 订单状态打标 ==")
spark.sql("""
    SELECT order_id, amount, status,
           CASE WHEN status = '已支付' THEN '计入营收'
                WHEN status = '待支付' THEN '待确认'
                ELSE '不计入'
           END AS biz_flag
    FROM orders
    ORDER BY order_id
    LIMIT 6
""").show(truncate=False)
print()

# ---------- 4.6 聚合函数补充 ----------
print("== 4.6.1 COUNT DISTINCT / APPROX_COUNT_DISTINCT ==")
spark.sql("""
    SELECT COUNT(DISTINCT city)            AS exact_cnt,
           APPROX_COUNT_DISTINCT(city)     AS approx_cnt
    FROM employees
""").show(truncate=False)
print()

print("== 4.6.2 collect_list / collect_set ==")
spark.sql("""
    SELECT dept,
           COLLECT_LIST(name) AS names,
           COLLECT_SET(city)  AS cities
    FROM employees
    GROUP BY dept
    ORDER BY dept
""").show(truncate=False)
print()

print("== 4.6.3 业务例子：每个订单日有多少用户下单（按天去重） ==")
spark.sql("""
    SELECT order_date, COUNT(DISTINCT user_id) AS uv
    FROM orders
    GROUP BY order_date
    ORDER BY order_date
""").show(truncate=False)
print()

# ---------- 4.7 数组与炸裂 ----------
print("== 4.7.1 explode 一行变多行 ==")
spark.sql("""
    WITH t AS (SELECT 1 AS id, '苹果,香蕉,橘子' AS fruits)
    SELECT id, EXPLODE(SPLIT(fruits, ',')) AS fruit
    FROM t
""").show(truncate=False)
print()

print("== 4.7.2 size / array_contains ==")
spark.sql("""
    SELECT SPLIT('苹果,香蕉,橘子', ',') AS arr,
           SIZE(SPLIT('苹果,香蕉,橘子', ',')) AS n,
           ARRAY_CONTAINS(SPLIT('苹果,香蕉,橘子', ','), '香蕉') AS has_banana
""").show(truncate=False)
print()

print("== 4.7.3 业务例子：品类打标（一次性多标签） ==")
spark.sql("""
    SELECT order_id, category,
           SPLIT(CASE category
                     WHEN '手机数码' THEN '电子,数码'
                     WHEN '电脑办公' THEN '电子,办公'
                     WHEN '家用电器' THEN '家电,生活'
                     ELSE '其他'
                 END, ',') AS tags
    FROM orders
    LIMIT 5
""").show(truncate=False)
print()

# ---------- 4.8 UDF ----------
print("== 4.8.1 注册普通 Python UDF ==")
def salary_level(salary):
    if salary is None:
        return "未知"
    if salary >= 30000:
        return "高"
    if salary >= 20000:
        return "中"
    return "低"

spark.udf.register("salary_level", salary_level, StringType())
spark.sql("""
    SELECT name, salary, salary_level(salary) AS level
    FROM employees
    ORDER BY salary DESC
    LIMIT 6
""").show(truncate=False)
print()

print("== 4.4.7 时区：同一个时刻，不同显示 ==")
print("-- 当前会话时区 --")
spark.sql("SELECT current_timezone() AS session_tz").show(truncate=False)
print()

print("-- to_utc_timestamp / from_utc_timestamp：本地时间 <-> UTC --")
spark.sql("""
    SELECT to_utc_timestamp('2026-06-01 14:00:00', 'Asia/Shanghai') AS bj2utc,
           from_utc_timestamp('2026-06-01 06:00:00', 'Asia/Shanghai') AS utc2bj
""").show(truncate=False)
print()

print("-- 同一时刻，会话时区不同，show 出来的'钟点'就不同 --")
spark.conf.set("spark.sql.session.timeZone", "UTC")
moment = spark.sql("SELECT to_utc_timestamp('2026-06-01 22:00:00', 'Asia/Shanghai') AS t")
spark.conf.set("spark.sql.session.timeZone", "Asia/Shanghai")
print("会话时区 Asia/Shanghai:")
moment.show(truncate=False)
spark.conf.set("spark.sql.session.timeZone", "Asia/Tokyo")
print("会话时区 Asia/Tokyo:")
moment.show(truncate=False)
spark.conf.set("spark.sql.session.timeZone", "UTC")
print("会话时区 UTC:")
moment.show(truncate=False)
print()

print("== 4.7.4 Map：键值对集合 ==")
spark.sql("""
    SELECT map('技术部', 4, '产品部', 3) AS m,
           map('技术部', 4, '产品部', 3)['技术部'] AS tech_cnt,
           map_keys(map('技术部', 4, '产品部', 3)) AS ks
""").show(truncate=False)
print()

print("== 4.7.5 Struct：把多列打包成一个对象 ==")
spark.table("employees").select(
    F.struct(F.col("name"), F.col("dept"), F.col("salary")).alias("s")
).limit(2).show(truncate=False)
print("-- 用 s.字段 取回 --")
spark.table("employees").select(
    F.struct(F.col("name"), F.col("dept"), F.col("salary")).alias("s")
).selectExpr("s.name AS name", "s.salary AS salary").limit(2).show(truncate=False)
print()

print("== 4.7.6 JSON：文本 <-> 结构化 ==")
df_json = (spark.table("employees")
           .select(F.to_json(F.struct("name", "dept", "salary")).alias("json"))
           .limit(3))
df_json.show(truncate=False)
print("-- from_json：解析成 struct 后照常用字段 --")
df_json.select(F.from_json(F.col("json"), "struct<name STRING, dept STRING, salary INT>").alias("s")) \
    .selectExpr("s.name", "s.dept", "s.salary").show(truncate=False)
print("-- get_json_object：不建 schema，直接按路径提字段 --")
df_json.selectExpr("get_json_object(json, '$.salary') AS salary").show(truncate=False)
print()

spark.stop()
