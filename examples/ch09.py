# -*- coding: utf-8 -*-
"""第9章示例：综合实战——电商订单分析日任务（验证脚本）"""
import os, shutil, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

RAW_DIR = "examples/data/raw_orders"
WH_DIR = "examples/data/warehouse/ods_orders"
REPORT_PATH = "examples/data/reports/daily_report.csv"
for p in [RAW_DIR, WH_DIR, "examples/data/reports"]:
    shutil.rmtree(p, ignore_errors=True)
os.makedirs(RAW_DIR, exist_ok=True)

# ---------- 第一步：上游每日同步来的原始订单文件 ----------
raw_text = """order_id,user_id,product,category,amount,order_date,status
1001,1,iPhone 15,手机数码,5999.0,2026-06-01,已支付
1002,2,机械键盘,电脑办公,399.0,2026-06-01,已支付
1003,3,运动鞋,服饰运动,499.0,2026-06-02,已退款
1004,1,蓝牙耳机,手机数码,899.0,2026-06-02,已支付
1005,4,显示器 27寸,电脑办公,1599.0,2026-06-03,已支付
1006,2,无线鼠标,电脑办公,129.0,2026-06-03,待支付
1007,5,连衣裙,服饰运动,329.0,2026-06-04,已支付
1008,1,平板电脑,手机数码,2999.0,2026-06-04,已支付
1009,6,咖啡机,家用电器,799.0,2026-06-05,已支付
1010,3,跑鞋,服饰运动,699.0,2026-06-06,已支付
1011,7,空气炸锅,家用电器,459.0,2026-06-07,已退款
1012,2,笔记本支架,电脑办公,199.0,2026-06-08,已支付
1013,1,空气净化器,家用电器,1299.0,2026-06-09,已支付
1014,8,电饭煲,家用电器,329.0,2026-06-10,已支付
1015,9,手办,玩具乐器,199.0,2026-06-10,已支付
1016,1,加湿器,家用电器,99.0,2026-06-11,待支付
1001,1,iPhone 15,手机数码,5999.0,2026-06-01,已支付
1017,10,台灯,家居日用,,2026-06-11,已支付
"""
with open(os.path.join(RAW_DIR, "orders_20260601.csv"), "w") as f:
    f.write(raw_text)
print("== 步骤1 原始文件已就绪 ==")
print("原始记录数(含 1 条重复、1 条金额为空):", raw_text.strip().count("\n"))
print()

spark = (SparkSession.builder.master("local[2]").appName("ch09")
         .config("spark.ui.enabled", "false").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

# ---------- 第二步：读取并清洗 ----------
raw = (spark.read.option("header", True).option("inferSchema", True)
       .csv(RAW_DIR))
print("== 步骤2 读入原始订单 ==")
print("读入行数:", raw.count())
raw.printSchema()
raw.filter("amount IS NULL OR order_date IS NULL").show(truncate=False)
print()

# 2.1 去掉金额/日期为空的行
clean1 = raw.na.drop(subset=["amount", "order_date"])
print("去空值后行数:", clean1.count())
# 2.2 按 order_id 去重
clean2 = clean1.dropDuplicates(["order_id"])
print("去重后行数:", clean2.count())
# 2.3 统一类型与口径：状态去空格、金额保留 2 位
clean = (clean2
         .withColumn("amount", F.col("amount").cast("decimal(10,2)"))
         .withColumn("status", F.trim("status"))
         .withColumn("order_date", F.to_date("order_date")))
clean.createOrReplaceTempView("ods_orders")
print("清洗后 Schema:")
clean.printSchema()
print()

# ---------- 第三步：落成 ODS 层（Parquet，按天分区） ----------
print("== 步骤3 写出 ODS 层 Parquet（按 order_date 分区） ==")
(clean.write.mode("overwrite")
      .partitionBy("order_date")
      .parquet(WH_DIR))
dirs = sorted(d for d in os.listdir(WH_DIR) if d.startswith("order_date="))
print("分区目录:", dirs)
print()

# ---------- 第四步：每日经营日报 ----------
print("== 步骤4 每日经营指标（仅统计已支付） ==")
daily = spark.sql("""
    SELECT order_date,
           COUNT(DISTINCT user_id) AS uv,          -- 下单用户数
           COUNT(*)                AS order_cnt,   -- 订单数
           ROUND(SUM(amount), 2)   AS gmv          -- 销售额
    FROM ods_orders
    WHERE status = '已支付'
    GROUP BY order_date
    ORDER BY order_date
""")
daily.show(truncate=False)
print()

# ---------- 第五步：品类销售排行 Top3 ----------
print("== 步骤5 品类销售额排行 Top3 ==")
top3 = spark.sql("""
    SELECT category, ROUND(SUM(amount), 2) AS gmv
    FROM ods_orders
    WHERE status = '已支付'
    GROUP BY category
    ORDER BY gmv DESC
    LIMIT 3
""")
top3.show(truncate=False)
print()

# ---------- 第六步：复购分析 ----------
print("== 步骤6 复购用户（已支付订单数 >= 2） ==")
rebuy = spark.sql("""
    SELECT user_id, COUNT(*) AS paid_cnt, ROUND(SUM(amount), 2) AS total
    FROM ods_orders
    WHERE status = '已支付'
    GROUP BY user_id
    HAVING COUNT(*) >= 2
    ORDER BY paid_cnt DESC, user_id
""")
rebuy.show(truncate=False)
print("已支付订单用户总数:",
      spark.sql("SELECT COUNT(DISTINCT user_id) FROM ods_orders WHERE status='已支付'").collect()[0][0])
print("复购用户数:", rebuy.count())
print()

# ---------- 第七步：报表落地（单文件 CSV 给运营） ----------
print("== 步骤7 写出日报 CSV（coalesce(1) 保证单文件） ==")
(daily.repartition(1)
     .write.mode("overwrite").option("header", True)
     .csv(REPORT_PATH))
out_file = os.path.join(REPORT_PATH,
                        [f for f in os.listdir(REPORT_PATH) if f.startswith("part")][0])
with open(out_file) as f:
    for line in f.readlines()[:3]:
        print(" ", line.rstrip())
print()

print("== 步骤8 全流程演练完毕，清理演示产物 ==")
for p in [RAW_DIR, WH_DIR, "examples/data/reports"]:
    shutil.rmtree(p, ignore_errors=True)
print("已清理: raw_orders / warehouse / reports")
print()

print("== 9.8.1 三天的人员维度日快照 ==")
spark.sql("""
    CREATE OR REPLACE TEMP VIEW snap AS
    SELECT * FROM VALUES
        (1001, '王强', '技术部', DATE '2026-06-30'),
        (1002, '李娜', '产品部', DATE '2026-06-30'),
        (1001, '王强', '技术部', DATE '2026-07-01'),
        (1002, '李娜', '技术部', DATE '2026-07-01'),
        (1003, '赵磊', '财务部', DATE '2026-07-01'),
        (1001, '王强', '技术部', DATE '2026-07-02'),
        (1002, '李娜', '技术部', DATE '2026-07-02'),
        (1003, '赵磊', '财务部', DATE '2026-07-02')
        AS t(emp_id, name, dept, d)
""")
spark.table("snap").orderBy("emp_id", "d").show(truncate=False)
print()

print("-- 用 LAG 看每个人昨天的部门：值变了就是'变化起点' --")
spark.sql("""
    SELECT emp_id, name, dept, d,
           LAG(dept) OVER (PARTITION BY emp_id ORDER BY d) AS prev_dept
    FROM snap
    ORDER BY emp_id, d
""").show(truncate=False)
print()

print("== 9.8.2 拉链合并：只留变化起点，用 LEAD 算每条链的闭链日 ==")
spark.sql("""
    WITH starts AS (
        SELECT emp_id, name, dept, d AS start_date,
               LAG(dept) OVER (PARTITION BY emp_id ORDER BY d) AS prev_dept
        FROM snap
    ),
    seg AS (
        SELECT emp_id, name, dept, start_date
        FROM starts
        WHERE prev_dept IS NULL OR prev_dept <> dept
    )
    SELECT emp_id, name, dept, start_date,
           COALESCE(DATE_ADD(LEAD(start_date) OVER (PARTITION BY emp_id ORDER BY start_date), -1),
                    DATE '9999-12-31') AS end_date,
           LEAD(start_date) OVER (PARTITION BY emp_id ORDER BY start_date) IS NULL AS is_current
    FROM seg
    ORDER BY emp_id, start_date
""").show(truncate=False)
print()

spark.stop()
