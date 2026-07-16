# Caching in Apache Spark

## Introduction

By default, Spark follows a **lazy evaluation** model.

When an action is executed, Spark recomputes all required transformations from the source data unless the intermediate result has been stored.

Caching allows Spark to store intermediate results in memory (or disk), avoiding repeated computation and improving performance.

---

## Why Do We Need Caching?

Consider the following pipeline:

```python
sales_df = (
    spark.read.parquet("/sales")
         .filter(col("amount") > 1000)
         .groupBy("city")
         .sum("amount")
)
```

Now suppose we execute:

```python
sales_df.show()

sales_df.count()

sales_df.write.parquet("/output")
```

Without caching:

```text
Read Data
    ↓
Filter
    ↓
GroupBy
    ↓
Aggregate
```

is executed three separate times.

Spark recomputes the entire DAG for every action.

---

## Using Cache

```python
sales_df.cache()
```

Now:

```python
sales_df.show()

sales_df.count()

sales_df.write.parquet("/output")
```

Execution:

```text
First Action
    ↓
Compute Data
    ↓
Store In Cache

Subsequent Actions
    ↓
Read From Cache
```

Spark avoids recomputation.

---

# How Cache Works

When you call:

```python
df.cache()
```

Spark does **not** immediately cache the data.

Remember:

```text
Spark Uses Lazy Evaluation
```

The cache is populated only when an action is executed.

Example:

```python
df.cache()
```

Nothing happens yet.

---

```python
df.count()
```

Now Spark:

```text
Compute Data
      ↓
Store In Cache
      ↓
Return Result
```

---

# cache() vs persist()

## cache()

Shortcut for:

```python
df.persist(StorageLevel.MEMORY_AND_DISK)
```

It stores data in memory and spills to disk if memory is insufficient.

---

## persist()

Allows explicit control over the storage strategy.

Example:

```python
from pyspark import StorageLevel

df.persist(StorageLevel.MEMORY_ONLY)
```

---

# Common Storage Levels

### MEMORY_ONLY

```python
df.persist(StorageLevel.MEMORY_ONLY)
```

Characteristics:

- Fastest Access
- Data Stored Only In Memory
- Recomputed If Evicted

---

### MEMORY_AND_DISK

```python
df.persist(StorageLevel.MEMORY_AND_DISK)
```

Characteristics:

- Memory First
- Spill To Disk When Needed
- Most Common Choice

---

### DISK_ONLY

```python
df.persist(StorageLevel.DISK_ONLY)
```

Characteristics:

- Lower Memory Usage
- Slower Reads

---

# When Should You Cache?

Cache when the same DataFrame is reused multiple times.

Example:

```python
df.cache()

df.count()
df.show()
df.write.parquet(...)
```

Good candidate for caching.

---

## Expensive Transformations

Example:

```python
df = (
    spark.read.parquet(...)
         .join(...)
         .groupBy(...)
         .agg(...)
)
```

If reused multiple times:

```python
df.cache()
```

can significantly reduce execution time.

---

# When Should You NOT Cache?

## Single Use DataFrame

```python
df = spark.read.parquet(...)

df.write.parquet(...)
```

Caching provides no benefit.

---

## Very Large DataFrames

If the dataset exceeds available memory:

```text
Large Memory Pressure
      ↓
Frequent Evictions
      ↓
Reduced Performance
```

Caching may hurt performance instead of improving it.

---

# Cache vs Checkpoint

### Cache

Stores data for performance improvement.

```text
Goal:
Faster Reuse
```

---

### Checkpoint

Stores data to break lineage.

```text
Goal:
Fault Recovery
```

Example:

```python
df.checkpoint()
```

Checkpoint writes data to reliable storage.

---

# How to Verify Caching?

## Spark UI

Navigate to:

```text
Storage Tab
```

You can see:

- Cached DataFrames
- Memory Usage
- Storage Level

---

## Programmatically

```python
df.is_cached
```

Output:

```text
True
```

or

```text
False
```

---

# Unpersisting Data

Cached data occupies cluster memory.

When no longer needed:

```python
df.unpersist()
```

This releases memory resources.

---

# Common Mistakes

### Forgetting an Action

```python
df.cache()
```

does not cache immediately.

You need:

```python
df.count()
```

or another action.

---

### Caching Everything

Bad practice:

```python
df1.cache()
df2.cache()
df3.cache()
df4.cache()
```

Excessive caching can increase memory pressure and reduce overall performance.

---

### Not Unpersisting

Unused cached datasets continue consuming executor memory.

Always remove caches that are no longer required.

---

# Spark UI Perspective

Without Cache:

```text
Action 1 → Recompute DAG
Action 2 → Recompute DAG
Action 3 → Recompute DAG
```

---

With Cache:

```text
Action 1 → Compute + Cache
Action 2 → Read Cache
Action 3 → Read Cache
```

---

# Important Interview Questions

### Is cache() eagerly executed?

No.

Caching follows Spark's lazy evaluation model.

The cache is populated only after an action.

---

### What is the difference between cache() and persist()?

```text
cache()
```

uses Spark's default storage level.

```text
persist()
```

allows custom storage levels.

---

### Does caching improve fault tolerance?

No.

Caching is primarily a performance optimization.

Lost cached partitions can be recomputed using lineage.

---

### When should caching be used?

When the same DataFrame is reused multiple times.

---

### How do you remove cached data?

```python
df.unpersist()
```

---

# Key Takeaways

- Spark recomputes transformations for every action unless results are cached.
- Caching stores intermediate results and reduces recomputation.
- `cache()` is a shortcut for Spark's default persistence strategy.
- `persist()` provides explicit control over storage levels.
- Cache only frequently reused DataFrames.
- Excessive caching can increase memory pressure.
- Use `unpersist()` when cached data is no longer needed.
- Verify cached datasets through Spark UI's Storage tab.