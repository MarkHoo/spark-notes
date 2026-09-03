# -*- coding: utf-8 -*-
"""全书共用示例数据集：employees / departments / orders"""
from pyspark.sql import SparkSession


def new_session(app_name="spark-sql-book"):
    spark = (SparkSession.builder
             .master("local[2]")
             .appName(app_name)
             .config("spark.ui.enabled", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def make_employees(spark):
    """员工表：10 名员工"""
    rows = [
        (1,  "张伟", "技术部", 25000, 32, "北京", "2021-03-01"),
        (2,  "李娜", "产品部", 20000, 28, "上海", "2020-06-15"),
        (3,  "王强", "技术部", 30000, 35, "深圳", "2019-01-10"),
        (4,  "刘洋", "市场部", 15000, 26, "北京", "2022-09-01"),
        (5,  "陈静", "产品部", 22000, 30, "杭州", "2020-06-15"),
        (6,  "赵磊", "技术部", 18000, 29, "广州", "2023-02-20"),
        (7,  "孙丽", "市场部", 12000, 24, "上海", "2023-07-11"),
        (8,  "周明", "技术部", 35000, 38, "北京", "2018-05-06"),
        (9,  "吴昊", "财务部", 16000, 33, "深圳", "2021-11-30"),
        (10, "郑爽", "产品部", 19000, 27, "北京", "2022-04-18"),
    ]
    schema = ("emp_id INT, name STRING, dept STRING, salary INT, "
              "age INT, city STRING, hire_date STRING")
    return spark.createDataFrame(rows, schema)


def make_departments(spark):
    """部门表：技术部/产品部/市场部/财务部有员工；人事部没有员工"""
    rows = [
        ("技术部", "周明", 500000),
        ("产品部", "李娜", 300000),
        ("市场部", "孙丽", 200000),
        ("财务部", "吴昊", 150000),
        ("人事部", "何平", 100000),
    ]
    return spark.createDataFrame(rows, "dept STRING, manager STRING, budget INT")


def make_orders(spark):
    """订单表：用于第 5/6/9 章的关联、窗口函数与聚合练习"""
    rows = [
        (1001, 1,  "iPhone 15",     "手机数码", 5999.0, "2026-06-01", "已支付"),
        (1002, 2,  "机械键盘",      "电脑办公",  399.0, "2026-06-01", "已支付"),
        (1003, 3,  "运动鞋",        "服饰运动",  499.0, "2026-06-02", "已退款"),
        (1004, 1,  "蓝牙耳机",      "手机数码",  899.0, "2026-06-02", "已支付"),
        (1005, 4,  "显示器 27寸",   "电脑办公", 1599.0, "2026-06-03", "已支付"),
        (1006, 2,  "无线鼠标",      "电脑办公",  129.0, "2026-06-03", "待支付"),
        (1007, 5,  "连衣裙",        "服饰运动",  329.0, "2026-06-04", "已支付"),
        (1008, 1,  "平板电脑",      "手机数码", 2999.0, "2026-06-04", "已支付"),
        (1009, 6,  "咖啡机",        "家用电器",  799.0, "2026-06-05", "已支付"),
        (1010, 3,  "跑鞋",          "服饰运动",  699.0, "2026-06-06", "已支付"),
        (1011, 7,  "空气炸锅",      "家用电器",  459.0, "2026-06-07", "已退款"),
        (1012, 2,  "笔记本支架",    "电脑办公",  199.0, "2026-06-08", "已支付"),
    ]
    schema = ("order_id INT, user_id INT, product STRING, category STRING, "
              "amount DOUBLE, order_date STRING, status STRING")
    return spark.createDataFrame(rows, schema)
