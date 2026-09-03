# 第 3 章 用 SQL 查数据：临时视图与查询子句

> 本章目标：打通"DataFrame ↔ SQL"的最后一公里，并系统掌握工作中最高频的 SQL 子句：
> WHERE、GROUP BY、HAVING、ORDER BY、LIMIT、子查询、CTE。
> 全部示例已在 Spark 3.5.4 上验证。

## 3.1 为什么既有 API 又要 SQL？

第 2 章你已经见识了 DataFrame API 的强大。那为什么还要花一整章讲 SQL？

因为真实工作中，**SQL 是团队协作的"通用语言"**：

- 需求方（运营、产品）给的往往就是一段 SQL 或一句"帮我统计……"的话，你直接把它翻译成 SQL 最不容易错；
- 数仓同学维护的常常是几百行的 SQL 任务，你需要**看得懂、改得动**；
- 在 Spark 里写 SQL，和写 DataFrame API，**优化器会翻译成同一套执行计划**，性能上没有差别。

所以正确的姿势是：**把 SQL 当"需求语言"，把 DataFrame 当"工程语言"**，两者熟练切换。

## 3.2 临时视图：把 DataFrame 变成"能查的表"

### 3.2.1 三步套路：建视图 → 写 SQL → 看结果

先把员工表和部门表注册成临时视图（数据来自配套的 `examples/common.py`；`spark` 为第 2 章开头建好的会话）：

```python
import sys
sys.path.insert(0, "examples")                  # 让 Python 能找到配套数据模块
from common import make_employees, make_departments

emp = make_employees(spark)                     # 员工表（10 人）
dept = make_departments(spark)                  # 部门表（5 个部门）
emp.createOrReplaceTempView("employees")        # 注册视图
dept.createOrReplaceTempView("departments")
```

有了视图，就可以用 SQL 查了。比如找出薪资最高的几位：

```python
spark.sql("""
    SELECT name, dept, salary
    FROM employees
    WHERE salary > 20000
    ORDER BY salary DESC
""").show(truncate=False)
```

输出：

```
+----+------+------+
|name|dept  |salary|
+----+------+------+
|周明|技术部|35000 |
|王强|技术部|30000 |
|张伟|技术部|25000 |
|陈静|产品部|22000 |
+----+------+------+
```

`createOrReplaceTempView` 的意思是：**创建（或覆盖同名）临时视图**。临时视图只在当前 SparkSession 内有效、不落盘，程序退出即消失——非常适合 ETL 中间过程。

> 方法名里三个词的含义：
> - `Temp`：临时，进程结束就没了；
> - `View`：视图，只是一个"引用"，**不复制数据**；
> - `OrReplace`：同名则覆盖，重复运行时不会报"表已存在"。

### 3.2.2 SQL 查的和 API 算的是同一份数据

```python
print("SQL  总行数:", spark.sql("SELECT * FROM employees").count())
print("API  总行数:", emp.count())
```

输出：

```
SQL  总行数: 10
API  总行数: 10
```

SQL 和 DataFrame 操作的是同一个逻辑表，你可以在两者间**任意反复横跳**：先用 API 清洗，注册视图后用 SQL 聚合，再取回 DataFrame 继续加工。

### 3.2.3 反向操作：`spark.table` 把视图取回 DataFrame

```python
t = spark.table("employees")     # 按名字取回一张表/视图
print("spark.table 类型:", type(t).__name__)
```

输出：

```
spark.table 类型: DataFrame
```

`spark.table("xxx")` 在 ETL 中非常常用：数仓里已经建好了一张 Hive/Spark 表，你只要 `spark.table("库名.表名")` 就能拿到 DataFrame，然后随便用 API 加工。这也解释了为什么很多 Spark 任务的代码是"表 → DataFrame → 表"的循环。

## 3.3 查询子句逐个击破

假设你已经执行了 3.2.1 的注册语句，下面每个例子都可以直接在 `spark.sql(...)` 里运行。

### 3.3.1 列别名：`AS`

```python
spark.sql("SELECT name AS `姓名`, salary AS `月薪` FROM employees LIMIT 3").show(truncate=False)
```

输出：

```
+----+-----+
|姓名|月薪 |
+----+-----+
|张伟|25000|
|李娜|20000|
|王强|30000|
+----+-----+
```

两个注意点：

- `AS` 可以省略（`SELECT name 姓名`），但**建议显式写**，可读性更好；
- **中文别名必须用反引号包起来**（`` `姓名` ``）。不加反引号会直接报语法错误（已验证：`Syntax error at or near '姓'`）。事实上，任何**非字母数字、或以数字开头、或撞了关键字**的列名，都要用反引号。

### 3.3.2 `DISTINCT`：去重

```python
spark.sql("SELECT DISTINCT city FROM employees").show(truncate=False)
```

输出：

```
+----+
|city|
+----+
|杭州|
|深圳|
|上海|
|北京|
|广州|
+----+
```

员工分布在 5 个城市，`DISTINCT` 后只剩 5 个去重值。**注意：输出顺序不保证**——没有 `ORDER BY` 时，分布式引擎的行序是不确定的，不要依赖它。

### 3.3.3 `GROUP BY` + `HAVING`：分组后过滤

`WHERE` 在分组**之前**过滤行，`HAVING` 在分组**之后**过滤组。想找"人数 ≥ 2 的部门"：

```python
spark.sql("""
    SELECT dept, COUNT(*) AS cnt, ROUND(AVG(salary), 0) AS avg_salary
    FROM employees
    GROUP BY dept
    HAVING COUNT(*) >= 2
    ORDER BY cnt DESC
""").show(truncate=False)
```

输出：

```
+------+---+----------+
|dept  |cnt|avg_salary|
+------+---+----------+
|技术部|4  |27000.0   |
|产品部|3  |20333.0   |
|市场部|2  |13500.0   |
+------+---+----------+
```

只有 1 人的财务部被 `HAVING` 过滤掉了。**记住**：`HAVING` 里可以写聚合函数（`COUNT(*) >= 2`），`WHERE` 里不行——因为执行 `WHERE` 时分组还没发生。

### 3.3.4 完整查询：子句顺序

一条 SELECT 各子句有严格的**书写顺序**和**执行顺序**：

```python
spark.sql("""
    SELECT dept, COUNT(*) AS cnt
    FROM employees          -- 1. 取哪张表
    WHERE salary >= 15000   -- 2. 先按行过滤
    GROUP BY dept           -- 3. 再分组
    HAVING COUNT(*) >= 2    -- 4. 过滤组
    ORDER BY cnt DESC       -- 5. 排序
    LIMIT 2                 -- 6. 取前 N 行
""").show(truncate=False)
```

输出：

```
+------+---+
|dept  |cnt|
+------+---+
|技术部|4  |
|产品部|3  |
+------+---+
```

逻辑过程是：先筛选出薪资 ≥ 15000 的行 → 按部门分组数人头 → 只留 ≥ 2 人的部门 → 按人数降序 → 取前 2。**建议把这张"子句执行顺序图"背下来**，排查 SQL 结果不对时，顺着它走一遍就知道问题出在哪一步。

## 3.4 CTE：把复杂查询拆成"命名步骤"

写大查询时，全部塞在一个 SELECT 里会又长又乱。CTE（Common Table Expression，公共表表达式，`WITH ... AS` 语法）允许你**先把中间结果命名**，再在后面引用。它是复杂 SQL 的"函数封装"，日常写数仓 SQL 几乎天天用。

### 3.4.1 经典场景：找出"高于本部门平均薪资"的员工

```python
spark.sql("""
    WITH dept_avg AS (                    -- 第 1 步：每个部门的平均薪资
        SELECT dept, AVG(salary) AS avg_sal
        FROM employees
        GROUP BY dept
    )
    SELECT e.name, e.dept, e.salary, ROUND(d.avg_sal, 0) AS dept_avg_salary
    FROM employees e                      -- 第 2 步：员工表 join 部门均值
    JOIN dept_avg d ON e.dept = d.dept
    WHERE e.salary > d.avg_sal            -- 第 3 步：过滤出高于均值的人
    ORDER BY e.dept, e.salary DESC
""").show(truncate=False)
```

输出：

```
+----+------+------+---------------+
|name|dept  |salary|dept_avg_salary|
+----+------+------+---------------+
|陈静|产品部|22000 |20333.0        |
|刘洋|市场部|15000 |13500.0        |
|周明|技术部|35000 |27000.0        |
|王强|技术部|30000 |27000.0        |
+----+------+------+---------------+
```

这个需求用嵌套子查询也能写，但 CTE 的写法**像流水线一样直白**：先算什么、再算什么，一目了然。第 5 章会专门讲 JOIN 的各种细节。

### 3.4.2 CTE 可以被引用多次

CTE 不只是语法糖，它还能让你"算一次、用多次"。找人数最多的部门：

```python
spark.sql("""
    WITH cnt AS (SELECT dept, COUNT(*) AS n FROM employees GROUP BY dept)
    SELECT dept, n
    FROM cnt
    WHERE n = (SELECT MAX(n) FROM cnt)   -- cnt 被引用了两次
""").show(truncate=False)
```

输出：

```
+------+---+
|dept  |n  |
+------+---+
|技术部|4  |
+------+---+
```

## 3.5 子查询：嵌套的查询

子查询就是"查询里套查询"，用来表达"先算出一个集合，再基于它做判断"。Spark 支持**任何地方**嵌套查询。

### 3.5.1 `IN` 子查询：某列的值落在另一个查询结果里

"找出在预算 ≥ 20 万的部门里工作的员工"——先查出哪些部门符合条件，再看员工在不在其中：

```python
spark.sql("""
    SELECT name, dept
    FROM employees
    WHERE dept IN (SELECT dept FROM departments WHERE budget >= 200000)
    ORDER BY dept
""").show(truncate=False)
```

输出：

```
+----+------+
|name|dept  |
+----+------+
|李娜|产品部|
|陈静|产品部|
|郑爽|产品部|
|刘洋|市场部|
|孙丽|市场部|
|张伟|技术部|
|王强|技术部|
|赵磊|技术部|
|周明|技术部|
+----+------+
```

预算 ≥ 20 万的部门是技术部（50 万）、产品部（30 万）、市场部（20 万），财务部被排除——所以 9 名员工入选，只有财务部的吴昊不在。

### 3.5.2 `EXISTS` 子查询：有没有关联行

"找出有员工的部门"：

```python
spark.sql("""
    SELECT dept, manager
    FROM departments d
    WHERE EXISTS (SELECT 1 FROM employees e WHERE e.dept = d.dept)
""").show(truncate=False)
```

输出：

```
+------+-------+
|dept  |manager|
+------+-------+
|产品部|李娜   |
|市场部|孙丽   |
|技术部|周明   |
|财务部|吴昊   |
+------+-------+
```

`EXISTS` 只看"有没有"，不管有多少。这里人事部（经理何平）因为没有员工而被过滤掉了。工作中 `IN` 和 `EXISTS` 大部分场景可互换，语义差别：`IN` 是"值匹配"，`EXISTS` 是"存在关联行"。数据量大、子查询结果小时，`EXISTS` 往往更快。

## 3.6 三种"视图"：临时、全局临时、持久表

| 种类 | 创建方式 | 作用范围 | 数据落地 | 何时用 |
| --- | --- | --- | --- | --- |
| 临时视图 | `createOrReplaceTempView` | 当前会话 | 否 | 程序内部中间表 |
| 全局临时视图 | `createGlobalTempView` | 跨会话（同一应用） | 否 | 多会话共享（少见） |
| 持久表 | `df.write.saveAsTable(...)` | 跨应用 | 是 | 结果给下游用（第 7 章详述） |

全局临时视图挂在特殊的 `global_temp` 库下，查询时**必须带库名前缀**：

```python
emp.createGlobalTempView("g_employees")
spark.sql("SELECT name, dept FROM global_temp.g_employees LIMIT 3").show(truncate=False)
```

输出：

```
+----+------+
|name|dept  |
+----+------+
|张伟|技术部|
|李娜|产品部|
|王强|技术部|
+----+------+
```

查看当前库里有哪些表（临时视图也会列出来）：

```python
print("当前数据库:", spark.catalog.currentDatabase())
for t in spark.catalog.listTables():
    print("表:", t.name, "| 类型:", t.tableType)
```

输出：

```
当前数据库: default
表: departments | 类型: TEMPORARY
表: employees | 类型: TEMPORARY
```

注意上面只列出了 default 库下的 `employees`、`departments`，**没有** `g_employees`——它住在 `global_temp` 库下。这个细节可以帮助你理解视图的"命名空间"概念。

## 3.7 容易踩的语法细节

### 3.7.1 保留字和特殊字符：一律加反引号

`group`、`select`、`date`、`order` 等都是 Spark 的保留字，直接当列名会报错，必须用反引号：

```python
# 建一张列名撞了保留字的临时视图
spark.sql("""
    CREATE OR REPLACE TEMP VIEW v_special AS
    SELECT dept AS `group`, name AS `select`
    FROM employees
""")

# 使用的时候同样要加反引号
spark.sql("SELECT `group`, count(*) AS n FROM v_special GROUP BY `group`") \
      .show(truncate=False)
```

输出：

```
+------+---+
|group |n  |
+------+---+
|技术部|4  |
|市场部|2  |
|产品部|3  |
|财务部|1  |
+------+---+
```

**实用建议**：上游表字段尽量用 `snake_case`（如 `order_id`），天然规避这个问题；万一字段名不规范，就记住"反引号万能"。

### 3.7.2 `UNION ALL` 保留重复行，`UNION` 去重

把两个查询结果**纵向拼接**时用 UNION：

```python
spark.sql("""
    SELECT 'A' AS tag, dept FROM employees WHERE dept = '技术部'
    UNION ALL                       -- ALL：保留全部重复行
    SELECT 'B' AS tag, dept FROM employees WHERE dept = '产品部'
""").show(truncate=False)
```

输出：

```
+---+------+
|tag|dept  |
+---+------+
|A  |技术部|
|A  |技术部|
|A  |技术部|
|A  |技术部|
|B  |产品部|
|B  |产品部|
|B  |产品部|
+---+------+
```

- `UNION ALL`：不检查重复，直接拼接，**性能好**，绝大多数场景用它；
- `UNION`：会做全字段去重（隐式 DISTINCT），有额外开销，除非确实需要去重否则别用；
- 拼接的两个查询**列数必须相同**，类型最好一致。

## 3.8 多维分析：ROLLUP / CUBE / GROUPING SETS 与 pivot

到目前为止，`GROUP BY` 输出的都是"按某种组合汇总后的明细"。但报表常要"**既要明细、又要小计、还要总计**"——此时一行行 `UNION` 太蠢，SQL 提供了三种多维分组语法，DataFrame 侧则是 `pivot` 做行列转换。

先建一张 6 行的销售演示表（华东/华北/华南卖 A/B/C 三类产品）：

```python
spark.sql("""
    CREATE OR REPLACE TEMP VIEW sales AS
    SELECT * FROM VALUES
        ('华东', 'A', 100), ('华东', 'B', 200), ('华东', 'A', 50),
        ('华北', 'A', 300), ('华北', 'B', 400), ('华南', 'C', 500)
        AS t(region, product, amount)
""")
spark.table("sales").show(truncate=False)
```

输出：

```
+------+-------+------+
|region|product|amount|
+------+-------+------+
|华东  |A      |100   |
|华东  |B      |200   |
|华东  |A      |50    |
|华北  |A      |300   |
|华北  |B      |400   |
|华南  |C      |500   |
+------+-------+------+
```

### 3.8.1 ROLLUP：小计逐层向上，末尾是总计

`GROUP BY ROLLUP(region, product)` = 先按 `region` 出小计，再在里面按 `region, product` 出明细，最后给一行**全表总计**：

```python
spark.sql("""
    SELECT region, product, SUM(amount) AS total
    FROM sales
    GROUP BY ROLLUP(region, product)
    ORDER BY region NULLS LAST, product NULLS LAST
""").show(truncate=False)
```

输出：

```
+------+-------+-----+
|region|product|total|
+------+-------+-----+
|华东  |A      |150  |
|华东  |B      |200  |
|华东  |NULL   |350  |
|华北  |A      |300  |
|华北  |B      |400  |
|华北  |NULL   |700  |
|华南  |C      |500  |
|华南  |NULL   |500  |
|NULL  |NULL   |1550 |
+------+-------+-----+
```

`product` 为 NULL 的行是**region 小计**（华东 350 = 150 + 200），最后 `(NULL, NULL)` 是**全表总计**。维度越多，"金字塔"层数越多：`ROLLUP(年, 月, 日)` 会依次给出年小计、年月小计、年月日明细、总计。

> 警觉点：这里 NULL 有歧义——它既可能是"小计占位"，也可能是数据里真的为 NULL。用 `GROUPING()` 函数可以分辨：某列取值 1 表示"这一行是按该列做了汇总"，0 表示该值是真实值：

```python
spark.sql("""
    SELECT region, product, SUM(amount) AS total,
           GROUPING(region) AS grp_region, GROUPING(product) AS grp_product
    FROM sales
    GROUP BY ROLLUP(region, product)
    ORDER BY region NULLS LAST, product NULLS LAST
""").show(truncate=False)
```

输出：

```
+------+-------+-----+----------+-----------+
|region|product|total|grp_region|grp_product|
+------+-------+-----+----------+-----------+
|华东  |A      |150  |0         |0          |
|华东  |B      |200  |0         |0          |
|华东  |NULL   |350  |0         |1          |
|华北  |A      |300  |0         |0          |
|华北  |B      |400  |0         |0          |
|华北  |NULL   |700  |0         |1          |
|华南  |C      |500  |0         |0          |
|华南  |NULL   |500  |0         |1          |
|NULL  |NULL   |1550 |1         |1          |
+------+-------+-----+----------+-----------+
```

最后一行的 `grp_region = 1` 明确告诉你"这是全表总计"，而不是某个 region 真的叫 NULL。

### 3.8.2 CUBE：所有维度组合都出一份小计

`CUBE(a, b)` 对**每种维度组合**都生成一行小计：`(a, b)` 明细 + 按 `a` + 按 `b` + 总计，共 `2^n` 类分组。n 越大组合爆炸越快，一般别超过 3 个维度：

```python
spark.sql("""
    SELECT region, product, SUM(amount) AS total
    FROM sales
    GROUP BY CUBE(region, product)
    ORDER BY region NULLS LAST, product NULLS LAST
""").show(truncate=False)
```

输出：

```
+------+-------+-----+
|region|product|total|
+------+-------+-----+
|华东  |A      |150  |
|华东  |B      |200  |
|华东  |NULL   |350  |
|华北  |A      |300  |
|华北  |B      |400  |
|华北  |NULL   |700  |
|华南  |C      |500  |
|华南  |NULL   |500  |
|NULL  |A      |450  |
|NULL  |B      |600  |
|NULL  |C      |500  |
|NULL  |NULL   |1550 |
+------+-------+-----+
```

对比 ROLLUP：CUBE 多出了**按 product 的小计**（`(NULL, A) = 450` 表示"所有地区卖 A 的总和"），而 ROLLUP 不会出这一层。

### 3.8.3 GROUPING SETS：只要我指定的那几种组合

如果既不想像 CUBE 那样全组合、又想要不止一种粒度，用 `GROUPING SETS` 精确列出分组条件。下面只要"按 region 小计 + 按 region,product 明细"两类（**没有总计**）：

```python
spark.sql("""
    SELECT region, product, SUM(amount) AS total
    FROM sales
    GROUP BY GROUPING SETS ((region), (region, product))
    ORDER BY region NULLS LAST, product NULLS LAST
""").show(truncate=False)
```

输出：

```
+------+-------+-----+
|region|product|total|
+------+-------+-----+
|华东  |A      |150  |
|华东  |B      |200  |
|华东  |NULL   |350  |
|华北  |A      |300  |
|华北  |B      |400  |
|华北  |NULL   |700  |
|华南  |C      |500  |
|华南  |NULL   |500  |
+------+-------+-----+
```

一个实际收益：原本"日报 + 月报"要跑两个查询、两个结果集拼接；现在一个 `GROUPING SETS ((day), (month))` 一次出两种粒度的结果。

### 3.8.4 行转列：pivot

把**某个维度的取值变成列**，就是透视/行转列。DataFrame 的 `groupBy(...).pivot(...)` 一步到位：按 region 分组，product 的每种取值（A/B/C）摊成列，单元格填 `sum(amount)`：

```python
spark.table("sales").groupBy("region") \
    .pivot("product") \
    .agg(F.sum("amount")) \
    .orderBy("region") \
    .show(truncate=False)
```

输出：

```
+------+----+----+----+
|region|A   |B   |C   |
+------+----+----+----+
|华东  |150 |200 |NULL|
|华北  |300 |400 |NULL|
|华南  |NULL|NULL|500 |
+------+----+----+----+
```

没卖过某产品就是 NULL（华南没有 A/B），想填 0 就 `.na.fill(0)`。这是"**宽表**"，报表透视表就是这个形态。

> **决策建议**：`ROLLUP`/`CUBE`/`GROUPING SETS` 是 SQL 侧"多粒度汇总"，一次查询出明细+小计+总计，适合数仓报表；`pivot` 是"行转列"重塑形态，适合给 BI/Excel 喂宽表。两者互补，别混为一谈。

## 3.9 本章小结与练习

**核心要点：**

1. `createOrReplaceTempView` + `spark.sql()` 是"API 加工 + SQL 分析"混合开发的核心套路；
2. 子句执行顺序：`FROM → WHERE → GROUP BY → HAVING → SELECT → ORDER BY → LIMIT`；
3. `WHERE` 过滤行、`HAVING` 过滤组，别用反了；
4. 复杂查询优先用 `WITH ... AS`（CTE）分步拆解；
5. `IN`/`EXISTS` 子查询解决"基于集合做判断"的需求；
6. 特殊列名（中文、保留字、带空格）一律反引号包裹；
7. 竖着拼表用 `UNION ALL`（能去重需求才用 `UNION`）；
8. 多粒度汇总用 `ROLLUP` / `CUBE` / `GROUPING SETS`，行转列透视用 `pivot`。

**练习建议：**

1. 用员工表统计"每个城市的平均薪资"，只要平均薪资 ≥ 20000 的城市，按平均薪资降序；
2. 找出"薪资高于全公司平均水平"的员工（提示：`WHERE salary > (SELECT AVG(salary) FROM employees)`）；
3. 用 CTE 把练习 2 改写一遍，体会两种写法的差异。

下一章是本册的"高频函数词典"：字符串、数值、日期、条件、聚合——工作中 80% 的加工都在用它们。
