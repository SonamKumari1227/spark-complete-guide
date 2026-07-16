# Spark Interview Questions (Basic → Advanced)

## SparkContext vs SparkSession

### Q1. What is SparkContext?

SparkContext is the entry point to Spark's core functionality.

It is responsible for:

- Connecting to the Cluster Manager
- Allocating Resources
- Creating RDDs
- Scheduling Jobs
- Managing Executors

Example:

```python
from pyspark import SparkContext

sc = SparkContext("local", "MyApp")
```

Historically, SparkContext was the primary entry point before Spark 2.x.

---

### Q2. What is SparkSession?

SparkSession is the unified entry point introduced in Spark 2.0.

It combines the functionality of:

- SparkContext
- SQLContext
- HiveContext
- StreamingContext (partially)

Example:

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("MyApp")
    .getOrCreate()
)
```

Today, almost all Spark applications start with SparkSession.

---

### Q3. Difference Between SparkContext and SparkSession

| SparkContext | SparkSession |
|-------------|-------------|
| Introduced before Spark 2.0 | Introduced in Spark 2.0 |
| Used mainly for RDD operations | Used for DataFrame, SQL, and RDD operations |
| Lower-level API | Higher-level API |
| Cannot directly execute SQL queries | Supports SQL queries |
| Accessed using `sc` | Accessed using `spark` |

---

### Q4. Can SparkSession Access SparkContext?

Yes.

```python
spark.sparkContext
```

Example:

```python
sc = spark.sparkContext
```

SparkSession internally contains a SparkContext.

---

### Q5. Which One Should You Use?

For modern Data Engineering workloads:

```text
SparkSession
```

For legacy RDD-based applications:

```text
SparkContext
```

---

### Q6. Show a Query Difference Between SparkContext and SparkSession

#### Using SparkContext (RDD)

```python
rdd = sc.parallelize([
    ("IT", 1000),
    ("HR", 2000),
    ("IT", 3000)
])

result = (
    rdd
    .filter(lambda x: x[0] == "IT")
    .map(lambda x: x[1])
    .sum()
)

print(result)
```

Output:

```text
4000
```

---

#### Using SparkSession (DataFrame)

```python
from pyspark.sql.functions import col, sum

df = spark.createDataFrame([
    ("IT", 1000),
    ("HR", 2000),
    ("IT", 3000)
], ["department", "salary"])

result = (
    df
    .filter(col("department") == "IT")
    .agg(sum("salary"))
)

result.show()
```

Output:

```text
+-----------+
|sum(salary)|
+-----------+
|4000       |
+-----------+
```

---

#### Using Spark SQL

```python
df.createOrReplaceTempView("employees")

spark.sql("""
SELECT SUM(salary)
FROM employees
WHERE department = 'IT'
""").show()
```

Output:

```text
4000
```

---

### Interview Answer

Today SparkSession is preferred because it supports:

- DataFrames
- Spark SQL
- Structured APIs
- Catalyst Optimizer
- Tungsten Engine

while SparkContext is primarily used for low-level RDD operations.

---

# Architecture & Execution

### Q7. What happens when you execute a Spark job?

```text
Driver
  ↓
Logical Plan
  ↓
DAG
  ↓
Stages
  ↓
Tasks
  ↓
Executors
```

---

### Q8. Difference Between Job, Stage and Task

| Component | Description |
|------------|------------|
| Job | Created when an Action is executed |
| Stage | Group of tasks separated by shuffle boundaries |
| Task | Smallest execution unit processed by an executor |

---

### Q9. When is a Job Created?

A Job is created whenever an Action is executed.

Examples:

```python
df.count()
df.collect()
df.show()
df.write.parquet(...)
```

---

### Q10. What Creates a New Stage?

A new stage is generally created when Spark encounters a shuffle operation.

Examples:

```python
groupBy()
join()
distinct()
repartition()
```

---

# DAG

### Q11. What is a DAG?

DAG stands for:

```text
Directed Acyclic Graph
```

Spark builds a DAG of transformations before execution.

---

### Q12. When is DAG Created?

DAG is created during action execution.

Example:

```python
df.filter(...)
  .groupBy(...)
```

No DAG execution yet.

```python
df.count()
```

Now Spark builds the DAG and starts execution.

---

### Q13. Can Multiple Stages Feed Into One Stage?

Yes.

Example:

```text
Stage 1 ──┐
          │
          ▼
       Stage 3
          ▲
          │
Stage 2 ──┘
```

Common in joins.

---

# Partitioning

### Q14. What is a Partition?

The smallest unit of data distribution in Spark.

---

### Q15. How Many Tasks Are Created?

For a stage:

```text
Number of Tasks
        =
Number of Partitions
```

---

### Q16. Difference Between repartition() and coalesce()

| repartition() | coalesce() |
|-------------|-------------|
| Full Shuffle | Usually No Shuffle |
| Increase Partitions | Cannot Increase |
| Reduce Partitions | Reduce Partitions |
| Expensive | More Efficient |

---

### Q17. What is Partition Pruning?

Spark reads only the required partition directories instead of scanning the entire dataset.

Example:

```text
year=2024
year=2025
year=2026
```

Query:

```python
df.filter(col("year") == 2025)
```

Spark reads only:

```text
year=2025
```

---

### Q18. What is Data Skew?

Uneven distribution of data across partitions.

Example:

```text
Partition 1 → 100 MB
Partition 2 → 90 MB
Partition 3 → 95 MB
Partition 4 → 10 GB
```

The largest partition becomes the bottleneck.

---

# Catalyst Optimizer

### Q19. What is Catalyst?

Spark's query optimization engine.

---

### Q20. What Optimizations Does Catalyst Perform?

- Predicate Pushdown
- Column Pruning
- Constant Folding
- Join Optimization
- Filter Reordering

---

### Q21. Does Catalyst Work on RDDs?

No.

Catalyst works with:

- DataFrames
- Datasets
- Spark SQL

---

# Tungsten

### Q22. What is Tungsten?

Spark's physical execution engine.

---

### Q23. Major Tungsten Optimizations?

- Off-Heap Memory
- Binary Storage Format
- Whole-Stage Code Generation
- Cache-Aware Processing

---

### Q24. Do We Need To Enable Tungsten?

No.

It is built into Spark.

---

# Caching

### Q25. Difference Between cache() and persist()

```python
df.cache()
```

uses Spark's default persistence strategy.

```python
df.persist(StorageLevel.MEMORY_ONLY)
```

allows custom storage levels.

---

### Q26. Is cache() Lazy?

Yes.

The cache is populated only after an action.

---

### Q27. How Do You Remove Cached Data?

```python
df.unpersist()
```

---

# Shuffle

### Q28. What is a Shuffle?

Redistribution of data across partitions.

---

### Q29. Why Is Shuffle Expensive?

Because it involves:

- Disk I/O
- Network Transfer
- Serialization
- Sorting

---

### Q30. Which Operations Cause Shuffle?

```python
groupBy()
join()
distinct()
repartition()
orderBy()
```

---

# Broadcast Join

### Q31. What is a Broadcast Join?

A join strategy where Spark sends a small table to every executor.

---

### Q32. How Do You Force a Broadcast Join?

```python
from pyspark.sql.functions import broadcast

large_df.join(
    broadcast(small_df),
    "id"
)
```

---

### Q33. How Do You Verify a Broadcast Join?

```python
df.explain(True)
```

Look for:

```text
BroadcastHashJoin
```

---

# Performance Optimization

### Q34. How Would You Optimize a Slow Spark Job?

Typical approach:

1. Check Spark UI
2. Identify Skew
3. Reduce Shuffles
4. Use Broadcast Joins
5. Apply Partition Pruning
6. Cache Reused DataFrames
7. Tune Partition Count

---

### Q35. Why Are DataFrames Faster Than RDDs?

Because DataFrames benefit from:

```text
Catalyst Optimizer
        +
Tungsten Engine
```

while RDDs bypass many of these optimizations.

---

# Senior-Level Questions

### Q36. Why Doesn't Adding More Executors Always Improve Performance?

Because bottlenecks are often caused by:

- Data Skew
- Shuffle Overhead
- Poor Partitioning

rather than lack of resources.

---

### Q37. What Is Adaptive Query Execution (AQE)?

AQE allows Spark to optimize execution plans at runtime.

Capabilities:

- Dynamic Broadcast Joins
- Skew Handling
- Shuffle Partition Optimization

---

### Q38. What Is the Most Common Performance Issue in Spark?

In real-world ETL pipelines:

```text
Data Skew
```

and

```text
Excessive Shuffle Operations
```

are among the most common causes of poor performance.

---

# Ultimate Interview Summary

```text
Job
 ↓
Stage
 ↓
Task

1 Partition = 1 Task

Shuffle
 ↓
New Stage

Catalyst
 ↓
Optimizes Query

Tungsten
 ↓
Executes Query Efficiently

Broadcast Join
 ↓
Avoid Shuffle

Partition Pruning
 ↓
Reduce I/O

AQE
 ↓
Optimize At Runtime

DataFrame
 ↓
Catalyst + Tungsten

RDD
 ↓
Direct Execution
```