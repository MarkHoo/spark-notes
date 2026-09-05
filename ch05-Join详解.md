# 第 5 章 多表关联 JOIN：数仓里的"灵魂操作"

> 本章目标：搞懂 Spark 支持的 7 种 JOIN，掌握每种的适用场景，
> 并亲手踩一遍 JOIN 最常见的四个坑（NULL 键、类型不一致、重复键膨胀、列名冲突）。
> 全部示例已在 Spark 3.5.4 上验证。

## 5.1 先理解 JOIN 在干什么

第 3 章你已经用过一个 JOIN：把员工表和部门表"对上"。
JOIN 的直观理解是**把两张表按某个共同字段（连接键）"拼"起来**——像把两个 Excel 表用 VLOOKUP 关联一样，只是它在分布式引擎上完成，且能处理海量数据。

本章沿用两张大表，并加了两个"麻烦员工"：

- `employees`：10 名正式员工（都在部门表里有对应部门）；
- `emp_full`：12 行 = 正式员工 + **钱进（行政部，部门表里没有）** + **周舟（dept 为 NULL）**；
- `departments`：5 个部门（含**没有任何员工的人事部**）。

> **读输出前的约定**：下面表格里的"匹配上"，指的是连接键（`dept`）在两表中存在相等的值。请带着"哪些行能对上、哪些对不上"的疑问看每一张表。

准备数据（在 `employees`/`departments` 两个视图基础上，扩充两张"麻烦表"）：

```python
from common import make_employees, make_departments, make_orders

emp = make_employees(spark)
dept = make_departments(spark)
orders = make_orders(spark)
emp.createOrReplaceTempView("employees")
dept.createOrReplaceTempView("departments")
orders.createOrReplaceTempView("orders")

# emp_full：正式员工 + 钱进（行政部，部门表里没有）+ 周舟（dept 为 NULL）
spark.sql("""
    CREATE OR REPLACE TEMP VIEW emp_full AS
    SELECT * FROM employees
    UNION ALL SELECT 11, '钱进', '行政部', 14000, 25, '北京', '2024-01-05'
    UNION ALL SELECT 12, '周舟', NULL, 16000, 26, '上海', '2024-02-01'
""")
```

## 5.2 INNER JOIN：只要"两边都对得上"的

最常用、最安全。**只保留两张表都匹配成功的行**。

```python
spark.sql("""
    SELECT e.emp_id, e.name, e.dept, d.manager, d.budget
    FROM employees e
    JOIN departments d ON e.dept = d.dept      -- INNER JOIN 可简写为 JOIN
    ORDER BY e.emp_id
""").show(truncate=False)
```

输出：

```
+------+----+------+-------+------+
|emp_id|name|dept  |manager|budget|
+------+----+------+-------+------+
|1     |张伟|技术部|周明   |500000|
|2     |李娜|产品部|李娜   |300000|
|3     |王强|技术部|周明   |500000|
|4     |刘洋|市场部|孙丽   |200000|
|5     |陈静|产品部|李娜   |300000|
|6     |赵磊|技术部|周明   |500000|
|7     |孙丽|市场部|孙丽   |200000|
|8     |周明|技术部|周明   |500000|
|9     |吴昊|财务部|吴昊   |150000|
|10    |郑爽|产品部|李娜   |300000|
+------+----+------+-------+------+
```

10 名员工全部匹配成功（因为他们的部门都在部门表里）。DataFrame API 写法完全等价：

```python
emp.join(dept, on="dept", how="inner") \
   .select("emp_id", "name", "dept", "manager", "budget") \
   .orderBy("emp_id").show(truncate=False)
```

> 当两表**只有连接键同名**时（都叫 `dept`），API 里可以用 `on="dept"` 简写；若同名列不止一个，必须写完整条件 `emp.dept == dept.dept`。

## 5.3 LEFT JOIN：左表全保留，右表没匹配上就填空

**左表的每一行都保留**；右表匹配不上的列用 NULL 填充。这是 ETL 里用得最多的 JOIN 类型之一（"以左表为准，能补的信息尽量补"）。

用 `emp_full` 来 join，钱进（行政部）和周舟（dept=NULL）在部门表里都找不到对应部门：

```python
spark.sql("""
    SELECT e.emp_id, e.name, e.dept, d.manager
    FROM emp_full e
    LEFT JOIN departments d ON e.dept = d.dept
    ORDER BY e.emp_id
""").show(truncate=False)
```

输出：

```
+------+----+------+-------+
|emp_id|name|dept  |manager|
+------+----+------+-------+
|1     |张伟|技术部|周明   |
|2     |李娜|产品部|李娜   |
|3     |王强|技术部|周明   |
|4     |刘洋|市场部|孙丽   |
|5     |陈静|产品部|李娜   |
|6     |赵磊|技术部|周明   |
|7     |孙丽|市场部|孙丽   |
|8     |周明|技术部|周明   |
|9     |吴昊|财务部|吴昊   |
|10    |郑爽|产品部|李娜   |
|11    |钱进|行政部|NULL   |
|12    |周舟|NULL  |NULL   |
+------+----+------+-------+
```

12 行全部保留，钱进和周舟的 `manager` 是 NULL——**这正是"left join 之后用 `WHERE d.xxx IS NULL` 找出没匹配上的行"的来源**。比如找出"没有部门信息的员工"：

```sql
SELECT e.name FROM emp_full e
LEFT JOIN departments d ON e.dept = d.dept
WHERE d.manager IS NULL          -- 2 行：钱进、周舟
```

## 5.4 RIGHT JOIN：右表全保留

`RIGHT JOIN` 与 `LEFT JOIN` 只是"以谁为准"不同，可写作 `LEFT JOIN` 的镜像。把两个表换个方向写即可，不重复贴代码，直接看效果——**右表（部门）全保留，没有员工的部门（人事部）员工列是 NULL**：

```python
spark.sql("""
    SELECT d.dept, d.manager, e.name
    FROM emp_full e
    RIGHT JOIN departments d ON e.dept = d.dept
    ORDER BY d.dept, e.emp_id   -- 部门内按员工编号排序，保证输出顺序稳定
""").show(truncate=False)
```

输出：

```
+------+-------+----+
|dept  |manager|name|
+------+-------+----+
|产品部|李娜   |李娜|
|产品部|李娜   |陈静|
|产品部|李娜   |郑爽|
|人事部|何平   |NULL|
|市场部|孙丽   |刘洋|
|市场部|孙丽   |孙丽|
|技术部|周明   |张伟|
|技术部|周明   |王强|
|技术部|周明   |赵磊|
|技术部|周明   |周明|
|财务部|吴昊   |吴昊|
+------+-------+----+
```

看到人事部了吗？它是"有部门、没员工"的例子——`e.name` 为 NULL。**工作中为了让口径统一，绝大多数场景只用 LEFT JOIN**（并约定"左表是主表"），RIGHT JOIN 偶尔出现在别人写的 SQL 里，看得懂即可。

## 5.5 FULL OUTER JOIN：两边都要，谁也别漏

**左右两表的行全部保留**，各自没匹配上的部分用 NULL 填充。常用于"两边对账"（比如：找出只在 A 表、只在 B 表、两表都有的数据）：

```python
spark.sql("""
    SELECT COALESCE(e.dept, d.dept) AS dept, e.name, d.manager
    FROM emp_full e
    FULL OUTER JOIN departments d ON e.dept = d.dept
    ORDER BY dept, e.name
""").show(truncate=False)
```

输出：

```
+------+----+-------+
|dept  |name|manager|
+------+----+-------+
|NULL  |周舟|NULL   |
|产品部|李娜|李娜   |
|产品部|郑爽|李娜   |
|产品部|陈静|李娜   |
|人事部|NULL|何平   |
|市场部|刘洋|孙丽   |
|市场部|孙丽|孙丽   |
|技术部|周明|周明   |
|技术部|张伟|周明   |
|技术部|王强|周明   |
|技术部|赵磊|周明   |
|行政部|钱进|NULL   |
|财务部|吴昊|吴昊   |
+------+----+-------+
```

- 周舟（dept 为 NULL）和钱进（行政部）来自左表、没有部门信息；
- 人事部来自右表、没有员工信息；
- `COALESCE(e.dept, d.dept)` 把两边的 dept 合二为一，否则会出现 `e.dept`、`d.dept` 两个名字相同的列，后续引用必须写全限定名。

## 5.6 两个"过滤型"JOIN：SEMI 与 ANTI（不取右表任何列）

`LEFT SEMI JOIN` 和 `LEFT ANTI JOIN` 不是为了"取右表的列"，而是**纯粹拿右表当过滤器**——类似 `IN`/`NOT IN` 子查询，但语义更清晰、性能更好。

### 5.6.1 LEFT SEMI JOIN：只要"右表里有对应"的左表行

```python
spark.sql("""
    SELECT e.name, e.dept
    FROM emp_full e
    LEFT SEMI JOIN departments d ON e.dept = d.dept
    ORDER BY e.emp_id
""").show(truncate=False)
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
|赵磊|技术部|
|孙丽|市场部|
|周明|技术部|
|吴昊|财务部|
|郑爽|产品部|
+----+------+
```

10 名"有部门归属"的员工被选中，钱进、周舟被过滤掉。注意结果**只有左表的列**——这是 SEMI JOIN 与 INNER JOIN 的本质区别。**若在 SELECT 里引用右表列（如 `d.*`、`d.manager`），会直接报 `UNRESOLVED_COLUMN` 错误**（右表在 SEMI JOIN 中只充当过滤器，其列不可输出，与 INNER JOIN 能取两侧列不同）。

### 5.6.2 LEFT ANTI JOIN：只要"右表里没有对应"的左表行

```python
spark.sql("""
    SELECT e.name, e.dept
    FROM emp_full e
    LEFT ANTI JOIN departments d ON e.dept = d.dept
    ORDER BY e.emp_id
""").show(truncate=False)
```

输出：

```
+----+------+
|name|dept  |
+----+------+
|钱进|行政部|
|周舟|NULL  |
+----+------+
```

**ANTI JOIN 是找"孤儿数据"的标准答案**：比如"下了单但用户表里没有的用户""有消费记录但不在黑名单库的账号"。语义等价写法是 `WHERE NOT EXISTS (...)`。**注意不要用 `NOT IN` 替代**：当被比较的键含 NULL 时，`NOT IN` 会因三值逻辑把"NULL 是否在集合里"判为 UNKNOWN 而静默漏掉这些行（本页 `emp_full` 里 dept 为 NULL 的周舟就是一个例子：`ANTI JOIN` 能返回他，`NOT IN` 会丢掉他）——这正是 5.7 坑 1 的延伸，务必用 ANTI JOIN 或 `NOT EXISTS`。

> 七种 JOIN 已经见完，做个小结。记一个口诀：**Inner 两边都要，Left 左表为王，Right 右表为王，Full 谁都不丢，Semi 左表当 filter，Anti 找孤儿**。实际工作中，LEFT + INNER 占 90%，其余看懂即可。

## 5.7 JOIN 的四个经典大坑

这一节是本章的**精华中的精华**——以下四个问题，是 Spark SQL 任务结果"悄悄不对"的最常见原因，且都不报错。

### 坑 1：连接键里有 NULL，行悄悄消失

`NULL = NULL` 的结果是 UNKNOWN（既不是 TRUE 也不是 FALSE），**NULL 永远匹配不上任何东西**。

验证（`emp_full` 12 行，其中周舟 dept=NULL）：

```python
print("emp_full 总行数:", spark.table("emp_full").count())
# INNER JOIN 之后
print("INNER 后行数:", spark.sql(
    "SELECT e.name FROM emp_full e JOIN departments d ON e.dept = d.dept").count())
# LEFT JOIN 之后
print("LEFT  后行数:", spark.sql(
    "SELECT e.name FROM emp_full e LEFT JOIN departments d ON e.dept = d.dept").count())
```

输出：

```
emp_full 总行数: 12
INNER 后行数: 10
LEFT  后行数: 12
```

12 行 INNER 后只剩 10——**dept 为 NULL 的周舟（以及行政部的钱进）在 INNER JOIN 里被静默丢弃**。所以：如果发现 join 后行数变少，先查连接键有没有 NULL。

### 坑 2：连接键类型不一致，靠"隐式转换"悄悄带偏

先造两张"同名但类型不同"的表（`codes.id` 是 INT，`names.id` 是 STRING，其中一行 `'X'` 无法转成数字）：

```sql
CREATE OR REPLACE TEMP VIEW codes AS
SELECT 1 AS id, 'A' AS c UNION ALL SELECT 2 AS id, 'B' AS c;
CREATE OR REPLACE TEMP VIEW names AS
SELECT '1' AS id, 'one' AS nm UNION ALL SELECT 'X' AS id, 'eks' AS nm;
```

先看一个真实且反直觉的现象：`id`（INT）与 `id`（STRING）直接 join，**竟然不报错**：

```python
spark.sql("""
    SELECT a.id, a.c, b.nm
    FROM codes a JOIN names b ON a.id = b.id   -- a.id 是 INT，b.id 是 STRING
    ORDER BY a.id
""").show(truncate=False)
```

输出：

```
+---+---+---+
|id |c  |nm |
+---+---+---+
|1  |A  |one|
+---+---+---+
```

`codes` 有两行（1、2），`names` 有两行（'1'、'X'），结果只有 1 行：Spark 把两边做了隐式类型统一，但**值为 `'X'` 的那行因为无法转成数字而匹配失败，且完全不报错**。行数悄悄少了，你还以为 join 没问题。

再叠加一个默认行为：在 Spark 默认配置（非 ANSI）下，`CAST('X' AS INT)` 不报错，返回 NULL：

```python
spark.sql("SELECT CAST('X' AS INT) AS v").show(truncate=False)
```

输出：

```
+----+
|v   |
+----+
|NULL|
+----+
```

把 `spark.sql.ansi.enabled` 打开后，同样的转换立刻报错：

```python
spark.conf.set("spark.sql.ansi.enabled", "true")
spark.sql("SELECT CAST('X' AS INT) AS v").show()   # 报错！
```

报错信息：

```
[CAST_INVALID_INPUT] The value 'X' of the type "STRING" cannot be cast to "INT" because it is malformed.
Correct the value as per the syntax, or change its target type.
Use `try_cast` to tolerate malformed input and return NULL instead.
If necessary set "spark.sql.ansi.enabled" to "false" to bypass this error.
```

**正确的做法不是靠隐式转换，而是显式统一类型**：

```python
spark.sql("""
    SELECT a.id, a.c, b.nm
    FROM codes a JOIN names b ON a.id = CAST(b.id AS INT)
    ORDER BY a.id
""").show(truncate=False)
```

输出：

```
+---+---+---+
|id |c  |nm |
+---+---+---+
|1  |A  |one|
+---+---+---+
```

> **本章最重要的建议**：join/union 的连接键，**写代码时就显式 CAST 成同一类型**。一方面杜绝隐式转换带来的意外，另一方面代码一眼能看出"两边的键类型一致"。另外建议在正式集群开启 `spark.sql.ansi.enabled=true`，让"脏数据"尽早以报错形式暴露，而不是静默变成 NULL（第 8 章调优配置会再提）。

### 坑 3：右表连接键有重复 → 行数膨胀（笛卡尔放大）

维度表里同一把"钥匙"出现多行，join 后左表会被**复制多份**。构造一个带重复标签的 `dept_tag`（技术部有 2 个标签）：

```sql
CREATE OR REPLACE TEMP VIEW dept_tag AS
SELECT '技术部' AS dept, '薪资高' AS tag
UNION ALL SELECT '技术部', '人数多'
UNION ALL SELECT '产品部', '用户多'
UNION ALL SELECT '市场部', '活动多'
```

然后看 join 后的行数变化：

```python
spark.sql("""
    SELECT e.dept, COUNT(*) AS cnt
    FROM employees e
    JOIN dept_tag t ON e.dept = t.dept
    GROUP BY e.dept
    ORDER BY e.dept
""").show(truncate=False)

print("原 employees 10 行，join 后总行数:",
      spark.sql("SELECT e.name FROM employees e JOIN dept_tag t ON e.dept = t.dept").count())
```

输出：

```
+------+---+
|dept  |cnt|
+------+---+
|产品部|3  |
|市场部|2  |
|技术部|8  |
+------+---+

原 employees 10 行，join 后总行数: 13
```

技术部有 4 名员工 × 2 个标签 = 8 行。**原来 10 行 join 后变成 13 行**——这就是"膨胀"。膨胀后做聚合，指标会**翻倍错乱**（比如 SUM 被重复计算）。排查要点：

1. join 前先检查**右表的连接键是否唯一**：`SELECT 键, COUNT(*) FROM 右表 GROUP BY 键 HAVING COUNT(*) > 1`；
2. 维度表（字典表）若存在"一维多标签"，先按业务需要 `COLLECT_LIST` 聚成一行再 join，或拆成多张一对一表。

### 坑 4：两表有同名列 → 引用歧义

当两张表除了连接键外还有同名列（比如都有 `name`），直接写 `SELECT name` 会报 `Reference 'name' is ambiguous`。解决方案：

- 给表起别名（`e`、`d`），字段一律写 `e.name`、`d.name`；
- 只想要某一侧的列，用 `e.*` 或 `d.*`；
- 最终结果列想合并/重命名，用 `COALESCE`、`AS`。

本书所有 JOIN 示例都遵循"**表别名 + 列全限定**"的写法，这是数仓 SQL 的行业惯例，请直接养成习惯。

## 5.8 三表关联：把套路串起来

真实 ETL 很少只 join 一张表。经典场景：订单 → 用户 → 部门，把三个信息串成一张明细：

```python
spark.sql("""
    SELECT o.order_id, o.product, e.name AS user_name, d.manager
    FROM orders o
    JOIN employees e  ON o.user_id = e.emp_id      -- 订单 → 用户
    JOIN departments d ON e.dept = d.dept          -- 用户 → 部门
    ORDER BY o.order_id
""").show(truncate=False)
```

输出：

```
+--------+-----------+---------+-------+
|order_id|product    |user_name|manager|
+--------+-----------+---------+-------+
|1001    |iPhone 15  |张伟     |周明   |
|1002    |机械键盘   |李娜     |李娜   |
|1003    |运动鞋     |王强     |周明   |
|1004    |蓝牙耳机   |张伟     |周明   |
|1005    |显示器 27寸|刘洋     |孙丽   |
|1006    |无线鼠标   |李娜     |李娜   |
|1007    |连衣裙     |陈静     |李娜   |
|1008    |平板电脑   |张伟     |周明   |
|1009    |咖啡机     |赵磊     |周明   |
|1010    |跑鞋       |王强     |周明   |
|1011    |空气炸锅   |孙丽     |孙丽   |
|1012    |笔记本支架 |李娜     |李娜   |
+--------+-----------+---------+-------+
```

三表（及以上）join 的唯一技巧是：**从左到右，一次只 join 一张，把每步的结果想清楚**。别试图一口吃成胖子。

## 5.9 本章小结与练习

**JOIN 类型速查：**

| JOIN 类型 | 保留哪些行 | 最常用场景 |
| --- | --- | --- |
| INNER | 两边都匹配 | 取交集、关联明细 |
| LEFT | 左表全部 + 右表匹配 | 以左表为准补信息（90% 场景） |
| RIGHT | 右表全部 + 左表匹配 | 看懂别人代码即可 |
| FULL OUTER | 两边全部 | 对账、找差异 |
| LEFT SEMI | 左表里"右表有对应"的行 | 替代 IN 子查询做过滤 |
| LEFT ANTI | 左表里"右表没对应"的行 | 找孤儿数据、替代 NOT IN |

**四个大坑与对策：**

1. NULL 键 → 匹配不上、行数变少；先 `WHERE 键 IS NULL` 排查；
2. 类型不一致 → 隐式转换静默丢数据；**连接键显式 CAST 统一类型**；
3. 重复键 → 行数膨胀、聚合翻倍；join 前查右表键的唯一性；
4. 同名列 → 歧义报错；**表别名 + 列全限定**。

**练习建议：**

1. 找出"从来没有下过单的员工"（提示：员工表 `LEFT ANTI JOIN` 订单表）；
2. 统计每个部门名下订单的总金额与订单数（提示：orders join employees 后再 group by dept）；
3. 用 FULL OUTER JOIN 找出"订单表里有、用户表里没有"和"用户表里有、订单表里没有"的两类数据（对账场景）。

下一章讲窗口函数——分组内排序、TopN、同比环比，这些"既要分组又想要组内每一行"的需求都靠它。
