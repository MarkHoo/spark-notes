# 第 2 章 DataFrame：Spark SQL 的"编程之手"

> 本章目标：掌握 DataFrame 的创建与增删改查（选列、过滤、排序、去重、空值处理、分组聚合）。
> 这些操作占日常工作代码量的 90%，是后面所有章节的地基。
> 全部示例已在 Spark 3.5.4 上运行验证，数据来自本书共用的"员工表"。

## 2.1 DataFrame 是什么？一张"会分布式计算的表"

第 1 章我们把 DataFrame 叫做"临时表"。更准确地说：

> **DataFrame 是一张分布式的、带列名和类型（Schema）的"表"，里面的数据散落在集群的多台机器（或本机的多个分区）上，但对使用者来说就像一张普通的表。**

它有四个让你越用越爽的特性：

1. **有 Schema（表结构）**：每一列叫什么、什么类型，一目了然（`printSchema()`）；
2. **有类型**：对 `int` 列做加法、对 `string` 列做字符串函数，类型不对会报错而不是悄悄出错；
3. **惰性求值（Lazy Evaluation）**：写 `select`、`filter` 只是"记下要做什么"，不会真的去算；只有遇到"动作"（如 `show()`、`count()`、`save()`）才会真正触发计算。Spark 借此把一连串操作合并优化，一次跑完；
4. **与 SQL 完全互通**：注册成临时视图就能用 SQL 查，API 和 SQL 是同一套引擎。

这一章，我们把对它的"增删改查"全部过一遍。先准备数据：全书后续章节都会使用 `examples/common.py` 里的员工表 `employees`（10 名员工），代码开头先把它"装进来"：

```python
# 每个示例脚本开头都要先做的事：建会话 + 取员工表
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (SparkSession.builder
         .master("local[2]")
         .appName("example")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

# 员工表来自本书配套的 examples/common.py
emp = spark.createDataFrame([
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
], ["emp_id", "name", "dept", "salary", "age", "city", "hire_date"])
```

---

## 2.2 四种创建 DataFrame 的方式

### 2.2.1 从元组列表创建（最常用）

```python
df1 = spark.createDataFrame(
    [("Alice", 25), ("Bob", 30)],
    ["name", "age"],          # 第二个参数：列名列表
)
df1.show()
```

输出：

```
+-----+---+
| name|age|
+-----+---+
|Alice| 25|
|  Bob| 30|
+-----+---+
```

注意 `25` 被推断成了 `long`。如果想精确控制类型，可以传一个 DDL 风格的类型字符串，例如：

```python
df = spark.createDataFrame(
    [(1, "Alice", 25)],
    "id INT, name STRING, age INT",   # 每个字段写"列名 + 类型"
)
df.printSchema()
```

```
root
 |-- id: integer (nullable = true)
 |-- name: string (nullable = true)
 |-- age: integer (nullable = true)
```

### 2.2.2 从字典列表创建（字段更直观）

```python
df2 = spark.createDataFrame([
    {"name": "Alice", "age": 25},
    {"name": "Bob", "age": 30},
])
df2.printSchema()
df2.show()
```

输出：

```
root
 |-- age: long (nullable = true)
 |-- name: string (nullable = true)

+---+-----+
|age| name|
+---+-----+
| 25|Alice|
| 30|  Bob|
+---+-----+
```

字典的 key 成为列名。注意**列的顺序未必等于字典书写顺序**（这里 `age` 排到了 `name` 前面）：PySpark 会按 key 排序后确定列序，因此结果稳定可复现，但和你在字典里的书写顺序不一定一致。需要精确控制列顺序时，优先用元组列表 + 列名。

### 2.2.3 用纯 SQL 创建（调试 SQL 片段神器）

不需要任何文件，`SELECT ... UNION ALL ...` 就能造出一张表——**调试 SQL 片段时极其好用**：

```python
df3 = spark.sql("""
    SELECT 1 AS id, 'Alice' AS name
    UNION ALL
    SELECT 2 AS id, 'Bob' AS name
""")
df3.show()
```

输出：

```
+---+-----+
| id| name|
+---+-----+
|  1|Alice|
|  2|  Bob|
+---+-----+
```

### 2.2.4 从文件读取（第 7 章主角，先感受一下）

```python
df4 = (spark.read
       .option("header", True)
       .option("inferSchema", True)   # 让 Spark 推断每列类型
       .csv("examples/data/people.csv"))
df4.printSchema()
```

输出：

```
root
 |-- name: string (nullable = true)
 |-- age: integer (nullable = true)
 |-- city: string (nullable = true)
```

加了 `inferSchema` 后，`age` 被正确识别为 `integer`。文件读写的完整细节在第 7 章。

### 2.2.5 显式指定 Schema：不让 Spark 猜

`inferSchema` 很方便，但**生产代码里更推荐显式指定 Schema**。原因有两个：

1. 推断会出错：CSV 里某列前几行恰好为空、是 `"001"` 这样的编号、或是 `"2026/06/01"` 这样的日期格式，推断结果往往不是你想要的；
2. 显式指定后，**列名、顺序、类型都是"契约"**——数据源哪天悄悄变了结构，程序会直接报错而不是静默算错。这比"结果不对"好排查一万倍。

写法有两种，效果完全等价：

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("name", StringType(), True),   # True = 允许 NULL
    StructField("age", IntegerType(), True),
    StructField("city", StringType(), True),
])
df5 = (spark.read.option("header", True)
       .schema(schema)
       .csv("examples/data/people.csv"))
df5.printSchema()
df5.show(truncate=False)
```

输出：

```
root
 |-- name: string (nullable = true)
 |-- age: integer (nullable = true)
 |-- city: string (nullable = true)

+-----+---+----+
|name |age|city|
+-----+---+----+
|David|35 |深圳|
|Eva  |22 |北京|
|Frank|40 |杭州|
+-----+---+----+
```

第二种写法用 **DDL 字符串**，短得多，日常最常用：

```python
df6 = (spark.read.option("header", True)
       .schema("name STRING, age INT, city STRING")
       .csv("examples/data/people.csv"))
df6.printSchema()
```

输出：

```
root
 |-- name: string (nullable = true)
 |-- age: integer (nullable = true)
 |-- city: string (nullable = true)
```

两种方式结果一样。记住常用类型的关键字即可：`STRING / INT / BIGINT / DOUBLE / DECIMAL(10,2) / DATE / TIMESTAMP / BOOLEAN`。读文件时若担心文件里有坏行，还可以 `.option("mode", "FAILFAST")`（遇到类型不匹配直接报错）或 `"PERMISSIVE"`（坏行放进 `_corrupt_record` 列）。

> 顺手记一条：DataFrame 的 `printSchema()` 是**排查类型问题第一工具**——"这列怎么是 string 不是 int？"先 `printSchema()`，八成是没写 Schema 或没开 `inferSchema`。

---

## 2.3 查看数据：先"看看"再动手

拿到一张表，第一件事永远是"看看里面有什么"。

```python
emp.show(5)            # 显示 5 行（默认 20 行）
print("总行数:", emp.count())
print("第一行:", emp.first())
print("前 2 行:", emp.head(2))
```

输出：

```
+------+----+------+------+---+----+----------+
|emp_id|name|  dept|salary|age|city| hire_date|
+------+----+------+------+---+----+----------+
|     1|张伟|技术部| 25000| 32|北京|2021-03-01|
|     2|李娜|产品部| 20000| 28|上海|2020-06-15|
|     3|王强|技术部| 30000| 35|深圳|2019-01-10|
|     4|刘洋|市场部| 15000| 26|北京|2022-09-01|
|     5|陈静|产品部| 22000| 30|杭州|2020-06-15|
+------+----+------+------+---+----+----------+
only showing top 5 rows

总行数: 10
第一行: Row(emp_id=1, name='张伟', dept='技术部', salary=25000, age=32, city='北京', hire_date='2021-03-01')
前 2 行: [Row(emp_id=1, name='张伟', dept='技术部', salary=25000, age=32, city='北京', hire_date='2021-03-01'), Row(emp_id=2, name='李娜', dept='产品部', salary=20000, age=28, city='上海', hire_date='2020-06-15')]
```

数值列做统计概览，用 `describe()`：

```python
emp.select("salary", "age").describe().show()
```

输出：

```
+-------+-----------------+-----------------+
|summary|           salary|              age|
+-------+-----------------+-----------------+
|  count|               10|               10|
|   mean|          21200.0|             30.2|
| stddev|7067.924416372572|4.315347288715269|
|    min|            12000|               24|
|    max|            35000|               38|
+-------+-----------------+-----------------+
```

`describe()` 给出每列的**行数、均值、标准差、最小、最大**，是排查数据异常（突然出现负数、超大值、全空）的第一利器。

想拿列名清单？`emp.columns` 返回一个 Python 列表，常用来配合循环批量处理列：

```python
print("列名:", emp.columns)
```

输出：

```
列名: ['emp_id', 'name', 'dept', 'salary', 'age', 'city', 'hire_date']
```

> **实用技巧**：`show(n, truncate=False)` 显示完整内容不被截断；`emp.limit(10).collect()` 在调试大表时能"只取几行到驱动端看看"。

---

## 2.4 选列、加列、删列、改列

### 2.4.1 `select`：挑出需要的列

```python
emp.select("name", "dept").show(5, truncate=False)
```

输出：

```
+----+------+
|name|dept  |
+----+------+
|张伟|技术部|
|李娜|产品部|
|王强|技术部|
|刘洋|市场部|
|陈静|产品部|
+----+------+
```

`select` 是日常最频繁的操作——**先缩小列，再算再看**，能让下游更轻、输出更清爽。

### 2.4.2 `selectExpr`：把 SQL 表达式直接写进括号

不想写完整 SQL、又想用表达式？`selectExpr` 接受**标准 SQL 表达式字符串**：

```python
emp.selectExpr("name", "salary / 10000 AS salary_wan").show(5, truncate=False)
```

输出：

```
+----+----------+
|name|salary_wan|
+----+----------+
|张伟|2.5       |
|李娜|2.0       |
|王强|3.0       |
|刘洋|1.5       |
|陈静|2.2       |
+----+----------+
```

`selectExpr` 是"用 API 写 SQL 逻辑"的桥梁——你会 SQL，就会用它。

### 2.4.3 `withColumn`：新增一列（或覆盖旧列）

```python
emp.withColumn("salary_raise", F.col("salary") * 1.1).show(5, truncate=False)
```

输出：

```
+------+----+------+------+---+----+----------+------------------+
|emp_id|name|dept  |salary|age|city|hire_date |salary_raise      |
+------+----+------+------+---+----+----------+------------------+
|1     |张伟|技术部|25000 |32 |北京|2021-03-01|27500.000000000004|
|2     |李娜|产品部|20000 |28 |上海|2020-06-15|22000.0           |
|3     |王强|技术部|30000 |35 |深圳|2019-01-10|33000.0           |
|4     |刘洋|市场部|15000 |26 |北京|2022-09-01|16500.0           |
|5     |陈静|产品部|22000 |30 |杭州|2020-06-15|24200.000000000004|
+------+----+------+------+---+----+----------+------------------+
```

两个要点：

- `F.col("salary")` 表示"引用 salary 这一列"，后面跟运算表达式；
- 看第一行结果 `27500.000000000004`——这是**浮点数的经典误差**：`25000 × 1.1` 在二进制浮点里并不精确等于 27500。所以**凡是涉及钱的字段，要么用 `decimal` 类型，要么最后用 `round()` 收尾**，第 4 章还会细讲。

### 2.4.4 `withColumn` + 条件表达式：给数据"分桶打标签"

新增"年龄段"这类逻辑列，最常用 `F.expr`（在字符串里写标准 SQL 的 CASE WHEN）：

```python
emp.withColumn("age_group",
               F.expr("CASE WHEN age < 30 THEN '青年' "
                      "WHEN age < 35 THEN '中年' ELSE '资深' END")) \
   .select("name", "age", "age_group").show(truncate=False)
```

输出：

```
+----+---+---------+
|name|age|age_group|
+----+---+---------+
|张伟|32 |中年     |
|李娜|28 |青年     |
|王强|35 |资深     |
|刘洋|26 |青年     |
|陈静|30 |中年     |
|赵磊|29 |青年     |
|孙丽|24 |青年     |
|周明|38 |资深     |
|吴昊|33 |中年     |
|郑爽|27 |青年     |
+----+---+---------+
```

年龄分桶、金额分层、状态归类……这类"打标签"逻辑，CASE WHEN 是最通用的解法，在 SQL 里写法完全一样。

### 2.4.5 重命名列：`withColumnRenamed`

```python
emp.withColumnRenamed("salary", "monthly_salary").columns
# ['emp_id', 'name', 'dept', 'monthly_salary', 'age', 'city', 'hire_date']
```

在"上游改名字、下游统一口径"的 ETL 中，重命名列是家常便饭。

### 2.4.6 删除列：`drop`

```python
emp.drop("hire_date").columns
# ['emp_id', 'name', 'dept', 'salary', 'age', 'city']
```

写数据前去掉调试用的临时列、不需要透传的敏感列，都用它。

### 2.4.7 类型转换：`cast`

```python
emp.select("salary", F.col("salary").cast("double").alias("salary_d")).printSchema()
```

输出：

```
root
 |-- salary: integer (nullable = true)
 |-- salary_d: double (nullable = true)
```

`cast("double")`、`cast("decimal(10,2)")`、`cast("date")`……都是高频率操作。**类型不对是 Spark SQL 报错的第一大来源**，养成"改类型先 `cast`"的习惯能少踩很多坑。

---

## 2.5 过滤：`filter` / `where`

过滤是 ETL 的第一步：**先把脏的、不要的行扔掉**。`filter` 和 `where` 完全等价，按个人习惯使用即可。

### 2.5.1 单条件：等于

```python
emp.filter(F.col("dept") == "技术部").show(truncate=False)
```

输出：

```
+------+----+------+------+---+----+----------+
|emp_id|name|dept  |salary|age|city|hire_date |
+------+----+------+------+---+----+----------+
|1     |张伟|技术部|25000 |32 |北京|2021-03-01|
|3     |王强|技术部|30000 |35 |深圳|2019-01-10|
|6     |赵磊|技术部|18000 |29 |广州|2023-02-20|
|8     |周明|技术部|35000 |38 |北京|2018-05-06|
+------+----+------+------+---+----+----------+
```

### 2.5.2 多条件"且"：用 `&`，并且每个条件都要加括号

```python
emp.filter((F.col("dept") == "技术部") & (F.col("salary") >= 20000)) \
   .select("name", "dept", "salary").show(truncate=False)
```

输出：

```
+----+------+------+
|name|dept  |salary|
+----+------+------+
|张伟|技术部|25000 |
|王强|技术部|30000 |
|周明|技术部|35000 |
+----+------+------+
```

> **新手最容易犯的错误**：在 Python 里写 `(条件1) and (条件2)`。Spark 的 Column 对象不能用 Python 的 `and`，必须用 **`&`（且）**、**`|`（或）**、**`~`（非）**，并且每个条件都要**加括号**。

### 2.5.3 多条件"或"：用 `|`

```python
emp.where((F.col("city") == "北京") | (F.col("dept") == "财务部")) \
   .select("name", "dept", "city").show(truncate=False)
```

输出：

```
+----+------+----+
|name|dept  |city|
+----+------+----+
|张伟|技术部|北京|
|刘洋|市场部|北京|
|周明|技术部|北京|
|吴昊|财务部|深圳|
|郑爽|产品部|北京|
+----+------+----+
```

### 2.5.4 `isin`：值在集合里（SQL 的 IN）

```python
emp.filter(F.col("dept").isin("技术部", "产品部")) \
   .select("name", "dept").show(truncate=False)
```

输出：

```
+----+------+
|name|dept  |
+----+------+
|张伟|技术部|
|李娜|产品部|
|王强|技术部|
|陈静|产品部|
|赵磊|技术部|
|周明|技术部|
|郑爽|产品部|
+----+------+
```

### 2.5.5 `like`：模糊匹配

```python
emp.filter(F.col("name").like("张%")).select("name").show(truncate=False)
```

输出：

```
+----+
|name|
+----+
|张伟|
+----+
```

SQL 里写 `WHERE name LIKE '张%'` 完全等价。Spark 的 `like` 只支持 `%`（任意多个字符）和 `_`（单个字符），与 MySQL 一致。

### 2.5.6 `between`：闭区间范围

```python
emp.filter(F.col("salary").between(15000, 25000)) \
   .select("name", "salary").show(truncate=False)
```

输出：

```
+----+------+
|name|salary|
+----+------+
|张伟|25000 |
|李娜|20000 |
|刘洋|15000 |
|陈静|22000 |
|赵磊|18000 |
|吴昊|16000 |
|郑爽|19000 |
+----+------+
```

注意 `between` 是**闭区间**（包含两端），等价于 `salary >= 15000 AND salary <= 25000`。

### 2.5.7 字符串前缀判断：`startswith`

```python
emp.filter(F.col("city").startswith("上")).select("name", "city").show(truncate=False)
```

输出：

```
+----+----+
|name|city|
+----+----+
|李娜|上海|
|孙丽|上海|
+----+----+
```

同类还有 `endswith("...")` 和 `contains("...")`，都是高频率过滤手段。

> **习惯建议**：先 `filter` 再 `select`、先过滤再聚合，让"无用行"尽早被丢弃。虽然 Catalyst 优化器有时会自动下推谓词，但**先过滤是好习惯**，尤其数据量大时。

---

## 2.6 排序：`orderBy` / `sort`

```python
emp.orderBy(F.desc("salary"), F.asc("age")) \
   .select("name", "salary", "age").show(truncate=False)
```

输出：

```
+----+------+---+
|name|salary|age|
+----+------+---+
|周明|35000 |38 |
|王强|30000 |35 |
|张伟|25000 |32 |
|陈静|22000 |30 |
|李娜|20000 |28 |
|郑爽|19000 |27 |
|赵磊|18000 |29 |
|吴昊|16000 |33 |
|刘洋|15000 |26 |
|孙丽|12000 |24 |
+----+------+---+
```

要点：

- 多列排序：**先写的列优先**。这里先按薪资降序，薪资相同再按年龄升序（示例中暂无薪资相同的两人）；
- `F.desc("salary")` / `F.asc("age")` 是标准写法，也可以写 `emp.salary.desc()`；
- 大表全排序是昂贵的操作（需要 shuffle），尽量在**聚合之后、结果集较小时**再做排序。

---

## 2.7 去重：`distinct` 与 `dropDuplicates`

### 2.7.1 `distinct()`：按所有列去重

```python
df_dup = spark.createDataFrame(
    [("Alice", 25), ("Alice", 25), ("Bob", 30), ("Bob", 31)],
    ["name", "age"],
)
print("去重前行数:", df_dup.count(), "-> 去重后行数:", df_dup.distinct().count())
```

输出：

```
去重前行数: 4 -> 去重后行数: 3
```

`("Alice", 25)` 出现两次只保留一次；`("Bob", 30)` 和 `("Bob", 31)` 姓名相同但整行不同，不会被去重。

### 2.7.2 `dropDuplicates([列])`：只按指定列去重（工作中更常用）

日志、同步数据里经常"同一个人有多条记录"，想每人只留一条时：

```python
df_dup2 = spark.createDataFrame(
    [("Alice", 25, "A001"), ("Alice", 26, "A002"), ("Bob", 30, "B001")],
    ["name", "age", "card"],
)
df_dup2.dropDuplicates(["name"]).show(truncate=False)
```

输出：

```
+-----+---+----+
|name |age|card|
+-----+---+----+
|Alice|25 |A001|
|Bob  |30 |B001|
+-----+---+----+
```

两个 Alice（age、card 都不同）因为 `name` 相同被合并成一行。**注意：保留哪一行是不确定的**（分布式下没有"天然顺序"）。如果想去重时保留最新一条（如按时间戳取最大），要用窗口函数——第 6 章会给标准解法。

---

## 2.8 空值处理：`isNull` / `dropna` / `fillna` / `coalesce`

空值（`NULL`）是数据仓库里最常见的"脏数据"。先造一张带空值的表：

```python
df_null = spark.createDataFrame(
    [(1, "Alice", None), (2, "Bob", 5000), (3, "Cindy", None)],
    ["id", "name", "bonus"],
)
```

### 2.8.1 找出空值：`isNull()` / `isNotNull()`

```python
df_null.filter(F.col("bonus").isNull()).show(truncate=False)
print("空值行数:", df_null.filter(F.col("bonus").isNull()).count())
print("非空行数:", df_null.filter(F.col("bonus").isNotNull()).count())
```

输出：

```
+---+-----+-----+
|id |name |bonus|
+---+-----+-----+
|1  |Alice|NULL |
|3  |Cindy|NULL |
+---+-----+-----+

空值行数: 2
非空行数: 1
```

> **大坑预警**：NULL 不等于任何值，所以 `bonus == None`、`bonus != 5000` 都**查不出空值**，必须用 `isNull()` / `isNotNull()`。SQL 里同理：`WHERE bonus IS NULL`，而不是 `WHERE bonus = NULL`。

### 2.8.2 丢弃空值：`dropna()`

```python
df_null.dropna().show(truncate=False)
```

输出：

```
+---+----+-----+
|id |name|bonus|
+---+----+-----+
|2  |Bob |5000 |
+---+----+-----+
```

`dropna()` 默认删除**任意一列有 NULL** 的行。可以精细控制：`dropna(how="all")` 只删整行全为 NULL 的；`dropna(subset=["bonus"])` 只看某几列。**弃用要谨慎**：直接丢行可能丢掉有效信息，除非业务上明确"缺失即无效"。

### 2.8.3 填充空值：`fillna(值)`

```python
df_null.fillna(0).show(truncate=False)
```

输出：

```
+---+-----+-----+
|id |name |bonus|
+---+-----+-----+
|1  |Alice|0    |
|2  |Bob  |5000 |
|3  |Cindy|0    |
+---+-----+-----+
```

数值列填 0；字符串列可以填 `"未知"`。也支持按列填：`fillna({"bonus": 0, "city": "未知"})`。

### 2.8.4 `coalesce`：多个列里取第一个非空（SQL 通用）

```python
df_null.select("id", F.coalesce("bonus", F.lit(0)).alias("bonus")) \
       .show(truncate=False)
```

输出：

```
+---+-----+
|id |bonus|
+---+-----+
|1  |0    |
|2  |5000 |
|3  |0    |
+---+-----+
```

`coalesce(a, b, 0)` 的含义是"取 a，a 为空取 b，还为空取 0"。它是**跨表、跨字段兜底**的万能写法，SQL 里 `COALESCE` 同样适用。真实场景：某天业务上线了新字段，老数据都是 NULL，统计时就可以 `COALESCE(new_field, old_field)`。

### 2.8.5 NULL 的三值逻辑：为什么 `WHERE bonus = NULL` 查不到

空值处理单独讲一节，是因为 NULL 是整个 SQL 世界**最容易踩、最隐蔽**的坑。核心一句话：

> SQL 的判断结果不是两种，是**三种**：`TRUE` / `FALSE` / `UNKNOWN`（未知）。**任何算术、任何比较，只要碰到 NULL，结果就是 UNKNOWN**，而 `WHERE` 只保留结果为 `TRUE` 的行。

用一个演示表看透它：

```python
df_null3 = spark.createDataFrame(
    [(1, "Alice", 8000), (2, "Bob", None), (3, None, 6000), (4, None, 7000)],
    ["id", "name", "bonus"],
)
df_null3.orderBy("id").show(truncate=False)
```

输出：

```
+---+-----+-----+
|id |name |bonus|
+---+-----+-----+
|1  |Alice|8000 |
|2  |Bob  |NULL |
|3  |NULL |6000 |
|4  |NULL |7000 |
+---+-----+-----+
```

**坑 1：等号永远查不到 NULL。** 下面这条查询看起来"找 bonus 为空的"，结果一行都没有——因为 `bonus = NULL` 的结果是 UNKNOWN 而不是 TRUE：

```python
df_null3.filter("bonus = NULL").show(truncate=False)
```

输出：

```
+---+----+-----+
|id |name|bonus|
+---+----+-----+
+---+----+-----+
```

正确姿势是**显式判断**：`isNull()` / `isNotNull()`（SQL 里是 `IS NULL` / `IS NOT NULL`）：

```python
df_null3.filter(F.col("bonus").isNull()).select("id", "name").show(truncate=False)
```

输出：

```
+---+----+
|id |name|
+---+----+
|2  |Bob |
+---+----+
```

同理：`WHERE name != 'Alice'` 也**不会**包含 `name` 为 NULL 的行——"NULL 不等于 Alice"是 UNKNOWN。想连空值一起排除，要写成 `WHERE name IS NULL OR name != 'Alice'`。

**坑 2：聚合函数自动忽略 NULL。** `avg` / `sum` / `max` 等只统计非空值，`count(列)` 也只数非空的，只有 `count(*)` 是数行数：

```python
df_null3.agg(
    F.avg("bonus").alias("avg(bonus)"),
    F.sum("bonus").alias("sum(bonus)"),
    F.count("bonus").alias("count(bonus)"),
    F.count("*").alias("count(*)"),
).show(truncate=False)
```

输出：

```
+----------+----------+------------+--------+
|avg(bonus)|sum(bonus)|count(bonus)|count(*)|
+----------+----------+------------+--------+
|7000.0    |21000     |3           |4       |
+----------+----------+------------+--------+
```

`bonus` 有 3 个非空值（8000/6000/7000），所以 `avg = 7000`、`count(bonus) = 3`，而表一共 4 行。**业务含义要想清楚**：`avg` 得到的是"有奖金的人的平均奖金"；如果 NULL 表示"应发 0"，你得先 `fillna(0)` 再算。

**坑 3：去重时，NULL 被视为"同一个值"。** `dropDuplicates(["name"])` 会把两行 `name` 为 NULL 的当成重复；`countDistinct` 同样把 NULL 排除在外：

```python
print("按 name 去重前总行数:", df_null3.count())
print("按 name 去重后行数:", df_null3.dropDuplicates(["name"]).count())
print("countDistinct(name):", df_null3.select(F.countDistinct("name")).collect()[0][0])
```

输出：

```
按 name 去重前总行数: 4
按 name 去重后行数: 3
countDistinct(name): 2
```

（去重后剩 `Alice` / `Bob` / 一行 NULL；`countDistinct` 只数到 2 个非空名字。）

> **排错心法**：发现"结果少了行 / 平均数不对 / 去重后数量不对"时，先别怀疑代码逻辑——把相关列 `filter(isNull)` 拉出来看一眼，NULL 往往是真凶。

---

## 2.9 分组聚合：`groupBy` + `agg`

统计"每个部门多少人、多少薪资"这类问题，是 Spark SQL 的日常核心。

### 2.9.1 最简单：`groupBy(...).count()`

```python
emp.groupBy("dept").count().orderBy("dept").show(truncate=False)
```

输出：

```
+------+-----+
|dept  |count|
+------+-----+
|产品部|3    |
|市场部|2    |
|技术部|4    |
|财务部|1    |
+------+-----+
```

### 2.9.2 多指标：`groupBy(...).agg(...)`

用 `agg` 一次算出多个统计量，每个都要用 `F.函数(列).alias("列名")`：

```python
emp.groupBy("dept").agg(
    F.count("*").alias("人数"),
    F.sum("salary").alias("总薪资"),
    F.avg("salary").alias("平均薪资"),
    F.max("salary").alias("最高薪资"),
    F.min("salary").alias("最低薪资"),
).orderBy("dept").show(truncate=False)
```

输出：

```
+------+----+------+------------------+--------+--------+
|dept  |人数|总薪资|平均薪资          |最高薪资|最低薪资|
+------+----+------+------------------+--------+--------+
|产品部|3   |61000 |20333.333333333332|22000   |19000   |
|市场部|2   |27000 |13500.0           |15000   |12000   |
|技术部|4   |108000|27000.0           |35000   |18000   |
|财务部|1   |16000 |16000.0           |16000   |16000   |
+------+----+------+------------------+--------+--------+
```

观察：平均薪资出现 `20333.333333333332` 的长尾小数，这是正常的浮点行为。**展示给业务时通常要取整**。

### 2.9.3 组合套路：过滤 → 分组 → 聚合 → 排序

工作中真实的统计常常是"只要某部分数据，按维度分组算指标，再按指标排序"：

```python
emp.filter(F.col("dept") != "市场部") \
   .groupBy("dept") \
   .agg(F.round(F.avg("salary"), 1).alias("avg_salary")) \
   .orderBy(F.desc("avg_salary")) \
   .show(truncate=False)
```

输出：

```
+------+----------+
|dept  |avg_salary|
+------+----------+
|技术部|27000.0   |
|产品部|20333.3   |
|财务部|16000.0   |
+------+----------+
```

注意顺序：**先 `filter` 缩小范围，再 `groupBy` 分组，`agg` 里用 `round` 收尾，最后 `orderBy` 排序**。SQL 等价写法：

```sql
SELECT dept, ROUND(AVG(salary), 1) AS avg_salary
FROM employees
WHERE dept != '市场部'
GROUP BY dept
ORDER BY avg_salary DESC
```

到这里，SQL 和 DataFrame API 的对应关系应该已经"对上号"了。

---

## 2.10 本章小结与练习

**本章核心技能清单（建议逐条自查）：**

| 能力 | 方法 |
| --- | --- |
| 建 DataFrame | `createDataFrame` / `spark.sql` / `spark.read` |
| 看数据 | `show` / `printSchema` / `count` / `describe` / `columns` |
| 选列加列 | `select` / `selectExpr` / `withColumn` / `drop` / `withColumnRenamed` / `cast` |
| 过滤 | `filter` + `&` `|` `~`、`isin`、`like`、`between`、`isNull` |
| 排序 | `orderBy(F.desc(...), F.asc(...))` |
| 去重 | `distinct` / `dropDuplicates([列])` |
| 空值 | `isNull` / `dropna` / `fillna` / `coalesce` |
| 显式 Schema | `.schema(StructType(...))` 或 `.schema("列名 类型,...")` |
| NULL 三值逻辑 | 比较碰到 NULL 结果"未知"；判空用 `IS NULL`，别用等号；聚合/去重自动忽略 NULL |
| 聚合 | `groupBy(...).agg(count/sum/avg/max/min)` |

**练习建议：**

1. 用员工表查出"薪资高于各部门平均薪资的员工"（提示：先算部门均值，再 join，下章就能完整做出来）；
2. 给员工表加一列 `salary_level`：薪资 ≥ 30000 为"高"，15000~30000 为"中"，否则为"低"；
3. 统计每个城市的人数与平均年龄，按平均年龄降序。

下一章我们打通"SQL 与 DataFrame"的最后一公里：临时视图、`spark.sql` 的完整用法，以及两种写法的混用技巧。
