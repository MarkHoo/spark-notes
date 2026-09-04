# 第 1 章 认识 Spark SQL：它能为你做什么

> 本章目标：用最短的时间搞清楚三件事——Spark SQL 到底是什么、为什么值得学、怎么在本地把它跑起来。
> 读完本章，你将拥有一个可以运行全书所有示例的环境，并完成第一个 Spark SQL 程序。

## 1.1 Spark SQL 是什么？先打个比方

很多工程师第一次接触 Spark SQL 时都有同样的困惑："我明明会 SQL，为什么要再学一个 Spark SQL？它和 MySQL 里的 SQL 有什么区别？"

先记住一句话：

> **Spark SQL = 跑在分布式集群上的 SQL 引擎 + 一套统一的编程接口（DataFrame）。**

你可以把它想象成"加了超大内存、多台机器一起干活"的数据库：

| 对比项 | 传统数据库（如 MySQL） | Spark SQL |
| --- | --- | --- |
| 处理的数据量 | 单机，受磁盘和内存限制 | 分布式，可扩展到成百上千台机器 |
| 数据存在哪 | 数据库内部 | 分布式文件系统（HDFS/S3）、各种数据仓库 |
| 数据格式 | 只认自己的存储格式 | 几乎什么都认：CSV/JSON/Parquet/ORC/Delta…… |
| 语法 | 标准 SQL | **完全兼容标准 SQL 的大部分语法** |
| 最擅长的事 | 在线事务（OLTP），增删改查 | 海量数据批处理分析（OLAP），只读分析为主 |

换句话说：**如果你已经会 SQL，你已经会了一半的 Spark SQL。** 剩下的另一半是学会"用编程接口（DataFrame）写 SQL 一样的逻辑"，以及理解"分布式的思维"。

Spark SQL 的另一层身份是**整个 Spark 生态的统一入口**。无论你用 Python、Scala、Java、R，还是直接写 SQL，最终都会翻译成同一套执行计划。你写的 DataFrame 代码，和写的 SQL，在 Spark 内部**是等价**的（第 1.5 节会亲眼看到）。

## 1.2 实际工作中，Spark SQL 用来做什么？

结合真实的日常工作，Spark SQL 最常出现在这几个场景：

1. **离线 ETL（数据清洗与加工）**
   从上游（业务库同步、日志文件、消息队列落盘）读取原始数据，经过过滤、去重、格式转换、多表关联，写出到下游数据仓库。这是 80% 的日常工作量。

2. **数据仓库分层建设**
   在 Hive 数仓的 ODS → DWD → DWS → ADS 分层中，每一层几乎都在用 Spark SQL 做"读上一层 → 变换 → 写下一层"。

3. **报表与指标计算**
   日活、留存、GMV、漏斗转化……这类"按时间/按维度做聚合"的统计，是 Spark SQL 的舒适区。

4. **临时取数与分析**
   接一个"帮我查一下最近 30 天订单量 TOP10 的商品"的需求，在 Spark 上开个 `spark-sql` 或者跑一段脚本即可。

本书将围绕这些场景展开，**只讲工作中真正高频用到的部分**：DataFrame 核心操作（第 2 章）、SQL 查询（第 3 章）、内置函数（第 4 章）、多表关联（第 5 章）、窗口函数（第 6 章）、读写数据源（第 7 章）、性能调优（第 8 章），最后用一个贯穿全书的实战案例收尾（第 9 章）。

## 1.3 环境准备：20 分钟跑通本地 Spark

学习阶段不需要集群，一台电脑即可。要求也很简单：

- **Java 8 / 11 / 17**（Spark 3.5.x 官方支持；新装机器推荐 17）；**Java 21 实测可跑**（本书已在 17 与 21 上验证 PASS，CI 验证环境为 Java 21）
- **Python 3.8+**（如果你用 PySpark，本书示例默认 Python）

安装步骤（以 PySpark 为例，这是上手最快的组合）：

```bash
# 1. 确认 java 可用（输出类似 openjdk version "17.0.x" 或 "21.0.x"）
java -version

# 2. 安装 pyspark（与生产环境主流版本保持一致，例如 3.5.4）
pip install pyspark==3.5.4
```

> **避坑提示**：如果你的 Python 环境是系统管理的（PEP 668），pip 直接安装会被拒绝。解决办法是创建虚拟环境，例如：
> ```bash
> python3 -m venv .venv-spark
> .venv-spark/bin/pip install pyspark==3.5.4
> ```

装好后先花 10 秒确认环境真的通了：

```bash
python -c "import pyspark; print(pyspark.__version__)"   # 应打印你安装的版本（如 3.5.4）
```

如果这一步或下面的第一个程序报错，九成是下表三种情况之一：

| 报错现场 | 原因 | 解决 |
| --- | --- | --- |
| `Java gateway process exited` | 没装 Java，或 `JAVA_HOME` 指向了 JRE 而非 JDK | 先 `java -version` 确认；安装 JDK 并把 `JAVA_HOME` 指到 JDK 目录 |
| `ModuleNotFoundError: No module named 'pyspark'` | PySpark 装到了另一个 Python 解释器 | 确认 `pip install` 与运行用的是同一个 `python`；用虚拟环境就始终走 `.venv-spark/bin/python` |
| 打印结果中文乱码 | 终端编码不是 UTF-8 | 把终端切到 UTF-8（本书示例 `show()` 均带 `truncate=False`） |

排错通过后，新建任意一个 Python 文件，写下全书第一个程序：

## 1.4 第一个 Spark SQL 程序

```python
from pyspark.sql import SparkSession

# 创建 SparkSession：Spark 程序的"总开关"
spark = (SparkSession.builder
         .master("local[2]")          # 本地模式，用 2 个 CPU 核
         .appName("my-first-app")     # 应用名称，便于在 UI 中识别
         .getOrCreate())

# 一行数据列表 + 字段名 -> DataFrame（后面章节会详细讲）
data = [("Alice", 25, "北京"), ("Bob", 30, "上海"), ("Cindy", 28, "广州")]
df = spark.createDataFrame(data, ["name", "age", "city"])

# 把 DataFrame 注册成一张"临时表"，就可以用 SQL 查了
df.createOrReplaceTempView("people")

# 用纯 SQL 查询！
result = spark.sql("SELECT name, age FROM people WHERE age > 26")
result.show()
```

输出：

```
+-----+---+
|name |age|
+-----+---+
|Bob  |30 |
|Cindy|28 |
+-----+---+
```

这段代码演示了 Spark SQL 最核心的"三板斧"，也是全书反复出现的套路：

1. **建会话**：`SparkSession` 是所有操作的起点；
2. **造数据 / 读数据**：得到一张 `DataFrame`；
3. **算数据**：注册成临时视图后用 `spark.sql(...)` 写 SQL，或者直接用 DataFrame API 写。

## 1.5 数据也能"建"出来：从内存创建 DataFrame

日常工作中，DataFrame 大多来自读文件或读表，但为了调试和写示例，**从内存创建 DataFrame** 是最常用的手段，它不需要任何外部依赖：

```python
data = [("Alice", 25, "北京"), ("Bob", 30, "上海"), ("Cindy", 28, "广州")]
df = spark.createDataFrame(data, ["name", "age", "city"])

df.printSchema()   # 查看"表结构"
df.show()          # 查看数据内容
```

输出：

```
root
 |-- name: string (nullable = true)
 |-- age: long (nullable = true)
 |-- city: string (nullable = true)

+-----+---+----+
|name |age|city|
+-----+---+----+
|Alice|25 |北京|
|Bob  |30 |上海|
|Cindy|28 |广州|
+-----+---+----+
```

两个观察要点：

- `printSchema()` 打印的就是**表的 Schema（结构）**，告诉你每个字段叫什么、什么类型。以后你会经常靠它确认"读进来的数据长什么样"。
- 数字 `25` 被自动推断成了 `long` 类型。**类型是 Spark SQL 一切行为的基础**，后面讲函数、讲 join 时你都会和"字段类型不匹配"打交道。

## 1.6 同一种逻辑，两种写法：SQL vs DataFrame API

Spark SQL 最友好的地方在于：**同一条查询，你可以用 SQL 写，也可以用 DataFrame API（面向对象的方法调用）写**，两者结果完全一致。

```python
# 写法一：SQL（先注册临时视图）
df.createOrReplaceTempView("people")
spark.sql("SELECT name, age FROM people WHERE age > 26 ORDER BY age DESC").show()

# 写法二：DataFrame API
from pyspark.sql import functions as F
df.select("name", "age") \
  .filter(F.col("age") > 26) \
  .orderBy(F.desc("age")) \
  .show()
```

两种写法的输出一模一样：

```
+-----+---+
|name |age|
+-----+---+
|Bob  |30 |
|Cindy|28 |
+-----+---+
```

学习建议：**先把 SQL 写熟，再对照着学 DataFrame API**。因为 SQL 是你已有的知识，而 API 是"工作的手"。看懂了等价关系，之后遇到复杂逻辑就可以在两种写法间自由切换。

> 一个实用的等价关系速查（后续章节会逐一展开）：
>
> | SQL | DataFrame API |
> | --- | --- |
> | `SELECT a, b` | `.select("a", "b")` |
> | `WHERE` | `.filter(...)` / `.where(...)` |
> | `GROUP BY` | `.groupBy(...).agg(...)` |
> | `ORDER BY` | `.orderBy(...)` / `.sort(...)` |
> | `JOIN` | `.join(...)` |
> | `LIMIT` | `.limit(...)` |

## 1.7 眼见为实：看 SQL 是怎么执行的（执行计划）

Spark SQL 之所以快，是因为它把逻辑翻译成了一份**经过优化器调优的执行计划**。想查看一条 SQL 会被怎么执行，用 `explain()`：

```python
spark.sql("SELECT city, count(*) AS cnt FROM people GROUP BY city").explain()
```

输出：

```
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- HashAggregate(keys=[city#2], functions=[count(1)])
   +- Exchange hashpartitioning(city#2, 200), ENSURE_REQUIREMENTS, [plan_id=62]
      +- HashAggregate(keys=[city#2], functions=[partial_count(1)])
         +- Project [city#2]
            +- Scan ExistingRDD[name#0,age#1L,city#2]
```

现在看不懂没关系，先建立两个印象：

- 最底下的 `Scan ExistingRDD` 是**数据从哪读**；`Project` 是**选哪些列**；`HashAggregate` 是**按什么聚合**；`Exchange ... hashpartitioning(city, 200)` 是**按 city 字段做哈希分发到 200 个分区做聚合**。
- `200` 来自 Spark 的默认 `spark.sql.shuffle.partitions`，也就是**发生聚合/join 时产生的分区数**。它直接决定 shuffle 的并发度，第 8 章调优会专门讲它。

遇到"SQL 跑得慢""结果不对"时，`explain()` 是你第一个要用的诊断工具。

如果你用的是交互式环境（Jupyter、`pyspark` 命令行等），Spark 还自带一个可视化监控页：浏览器打开 `http://localhost:4040`，能按 Job / Stage 看到每步耗时与数据量，比盯着日志直观。第 8 章讲调优时我们还会用到它。

## 1.8 从文件读数据：自动推断 Schema

真实场景的第一步往往是"把一个文件读进来"。Spark 支持几十种格式，最基础的 CSV 读法如下：

```python
df2 = (spark.read
       .option("header", True)   # 第一行是表头
       .csv("examples/data/people.csv"))

df2.printSchema()
df2.show()
```

输出：

```
root
 |-- name: string (nullable = true)
 |-- age: string (nullable = true)
 |-- city: string (nullable = true)

+-----+---+----+
|name |age|city|
+-----+---+----+
|David|35 |深圳|
|Eva  |22 |北京|
|Frank|40 |杭州|
+-----+---+----+
```

请注意一个**非常容易踩的坑**：CSV 文件里的 `35` 读进来变成了 `string` 而不是数字！因为 CSV 本身没有类型信息，Spark 默认**全部按字符串读**。解决办法是用 `option("inferSchema", True)` 让 Spark 自动推断类型，或者更推荐的做法——**显式指定 Schema**（第 7 章会详细演示）。这正是"类型意识"在 Spark 中的第一课。

## 1.9 本章小结与练习

本章要点：

1. Spark SQL 是"分布式 SQL 引擎 + 统一编程接口（DataFrame）"，**会 SQL 就赢在起跑线上**；
2. 学习路径：建会话 → 读/造数据 → 用 SQL 或 DataFrame API 计算；
3. SQL 与 DataFrame API 是等价的，推荐以 SQL 为纲、API 为用；
4. `explain()` 能查看执行计划，是排查慢查询的第一工具；
5. 从 CSV 等无类型文件读数据时，**类型默认是 string**，需要显式指定或推断；
6. 环境类报错九成可归为三类：Java 缺失、解释器装错、编码不对——先查 1.3 节的排错表。

**练习建议：**

- 把示例里的数据改成你自己的三行，加一个"城市在北京的人"的查询；
- 试试 `spark.sql("SELECT city, avg(age) FROM people GROUP BY city")` 看看聚合怎么写。

下一章，我们将正式学习 DataFrame——理解它之后，你会发现 Spark SQL 的"内力"都在这里。
