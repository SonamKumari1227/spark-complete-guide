# Partitioning in Apache Spark

## Introduction

Partitioning is one of the most fundamental concepts in Spark.

Spark achieves parallel processing by dividing data into smaller chunks called **Partitions**. These partitions are distributed across executors, allowing multiple tasks to process data simultaneously.

Without partitioning, Spark would not be able to scale beyond a single machine.

```text
Dataset
   │
   ▼
Partitions
   │
   ▼
Tasks
   │
   ▼
Executors
```

---

## What is a Partition?

A Partition is the smallest unit of data distribution in Spark.

Example:

```text
Dataset
│
├── Partition 1
├── Partition 2
├── Partition 3
└── Partition 4
```

Each partition contains a subset of the data.

Spark processes partitions independently and in parallel.

---

## Why Are Partitions Important?

Partitions determine:

- Parallelism
- Task Count
- Resource Utilization
- Execution Time

Remember:

```text
1 Partition = 1 Task
```

For a given Stage:

```text
Number of Partitions
          =
Number of Tasks
```

---

## Partition → Task → Executor

```text
Partition 1 → Task 1 → Executor 1
Partition 2 → Task 2 → Executor 2
Partition 3 → Task 3 → Executor 1
Partition 4 → Task 4 → Executor 3
```

Spark creates one task per partition and schedules those tasks on available executors.

---

# How Are Partitions Created?

## 1. Source-Based Partitioning

When reading data, Spark creates partitions automatically.

Example:

```python
df = spark.read.parquet("/data/sales")
```

Spark determines partitioning based on:

- File Size
- Number of Files
- Configuration Settings

---

## 2. Shuffle-Based Partitioning

Shuffle operations create new partitions.

Examples:

```python
groupBy()
join()
distinct()
repartition()
```

After a shuffle, Spark redistributes data and generates new partitions.

---

# Checking Number of Partitions

### DataFrame

```python
df.rdd.getNumPartitions()
```

### RDD

```python
rdd.getNumPartitions()
```

Example:

```python
print(df.rdd.getNumPartitions())
```

Output:

```text
200
```

---

# Types of Partitioning

## Hash Partitioning

Rows are distributed based on the hash value of a column.

Example:

```python
df.repartition("customer_id")
```

Conceptually:

```text
hash(customer_id) % number_of_partitions
```

Rows with the same key tend to end up in the same partition.

Commonly used during:

- Joins
- Aggregations
- Shuffles

---

## Range Partitioning

Rows are distributed based on value ranges.

Example:

```python
df.repartitionByRange(4, "salary")
```

Conceptually:

```text
0 - 10000
10001 - 50000
50001 - 100000
100001+
```

Useful for:

- Sorting
- Ordered Processing
- Large Analytical Queries

---

# Repartition

Used to increase or redistribute partitions.

Example:

```python
df.repartition(100)
```

Characteristics:

- Full Shuffle
- Even Data Distribution
- Expensive Operation

```text
All Data
    ↓
Shuffle
    ↓
100 New Partitions
```

---

# Coalesce

Used primarily to reduce partitions.

Example:

```python
df.coalesce(10)
```

Characteristics:

- Minimizes Data Movement
- Usually No Full Shuffle
- More Efficient than Repartition when reducing partitions

---

# Repartition vs Coalesce

| Feature | repartition() | coalesce() |
|----------|----------|----------|
| Shuffle | Yes | Usually No |
| Increase Partitions | Yes | No |
| Reduce Partitions | Yes | Yes |
| Performance | More Expensive | More Efficient |

---

# Partition Pruning

One of the most important Spark optimizations.

Suppose data is stored as:

```text
sales/
│
├── year=2024/
├── year=2025/
└── year=2026/
```

Query:

```python
df.filter(col("year") == 2025)
```

Spark reads:

```text
year=2025
```

only.

Instead of scanning:

```text
year=2024
year=2025
year=2026
```

Benefits:

- Less I/O
- Faster Queries
- Lower Resource Consumption

---

# Small Files Problem

A common Data Engineering challenge.

Suppose:

```text
10,000 Files
```

Each file:

```text
1 MB
```

Spark may create thousands of tiny partitions.

Result:

```text
Too Many Tasks
Scheduler Overhead
Poor Performance
```

This is known as the **Small Files Problem**.

---

# Data Skew

Data Skew occurs when partitions contain uneven amounts of data.

Example:

```text
Partition 1 → 10 MB
Partition 2 → 12 MB
Partition 3 → 15 MB
Partition 4 → 8 GB
```

Execution cannot finish until the largest partition completes.

Symptoms:

- Long-running Tasks
- Executor Imbalance
- Slow Jobs

Data skew is one of the most common causes of Spark performance issues.

---

# How Many Partitions Should You Use?

There is no universal answer.

A common guideline:

```text
Total Executor Cores × 2–4
```

Example:

```text
20 Executor Cores

Recommended:
40–80 Partitions
```

The optimal number depends on:

- Data Volume
- Cluster Size
- Workload Type
- Shuffle Behavior

---

# Important Interview Facts

### What is a Partition?

Smallest unit of data distribution in Spark.

---

### What creates Tasks?

```text
1 Partition = 1 Task
```

---

### Does repartition() create a shuffle?

Yes.

---

### Does coalesce() create a shuffle?

Usually no when reducing partitions.

---

### What is Partition Pruning?

Reading only relevant partitions during query execution.

---

### What is Data Skew?

Uneven data distribution across partitions.

---

### Why are partitions important?

They determine:

- Parallelism
- Task Count
- Resource Utilization

---

# Key Takeaways

- Partitions are the foundation of Spark's parallel processing model.
- One partition corresponds to one task.
- More partitions generally increase parallelism, but too many partitions create scheduling overhead.
- `repartition()` performs a full shuffle and redistributes data evenly.
- `coalesce()` is more efficient when reducing partitions.
- Partition Pruning can significantly reduce I/O.
- Data Skew and Small Files are common partition-related performance challenges.
- Choosing an appropriate partition strategy is one of the most important Spark optimization techniques for Data Engineers.