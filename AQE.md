# Adaptive Query Execution (AQE) in Apache Spark

## Introduction

One of the biggest challenges in distributed data processing is that Spark creates an execution plan **before the job actually runs**.

At planning time, Spark makes decisions based on:

- Table statistics
- Metadata
- Estimated partition sizes
- Cost calculations

The problem is that estimates are not always accurate.

For example:

```text
Expected Data Size  : 100 GB
Actual Data Size    : 15 GB
```

Or:

```text
Expected Partitions : Evenly Distributed
Actual Partitions   : Highly Skewed
```

In such situations, Spark may choose a suboptimal execution plan.

To solve this problem, Spark introduced **Adaptive Query Execution (AQE)**.

---

# What is AQE?

Adaptive Query Execution (AQE) is a query optimization framework introduced in Spark 3.0.

AQE allows Spark to:

> Re-optimize a query plan during runtime based on actual execution statistics.

Instead of relying entirely on estimates generated during planning, Spark can adjust its strategy after seeing real data.

---

# Why AQE Was Introduced

Before AQE:

```text
Logical Plan
      ↓
Physical Plan
      ↓
Execution
```

Once the physical plan was created, Spark followed it until completion.

Even if Spark discovered better optimization opportunities during execution, it could not change the plan.

---

With AQE:

```text
Logical Plan
      ↓
Initial Physical Plan
      ↓
Execution Starts
      ↓
Collect Runtime Statistics
      ↓
Re-optimize Plan
      ↓
Continue Execution
```

Spark becomes smarter during execution.

---

# How AQE Works

When a query starts executing:

1. Spark generates an initial physical plan.
2. Stages begin execution.
3. Runtime statistics are collected.
4. AQE analyzes the collected information.
5. Spark modifies the execution plan if beneficial.

```text
Query
  ↓
Initial Plan
  ↓
Execute Stage
  ↓
Collect Metrics
  ↓
AQE Optimization
  ↓
Updated Plan
  ↓
Continue Execution
```

---

# Problems AQE Solves

AQE primarily addresses three major performance issues:

1. Small Partitions
2. Inefficient Join Strategies
3. Data Skew

---

# 1. Coalescing Small Shuffle Partitions

## The Problem

Suppose a shuffle operation creates:

```text
200 Partitions
```

But each partition contains only:

```text
5 MB
```

Spark must launch:

```text
200 Tasks
```

even though very little data exists.

This creates unnecessary scheduling overhead.

---

## Without AQE

```text
200 Partitions
      ↓
200 Tasks
```

Many tasks finish almost instantly but still consume cluster resources.

---

## With AQE

Spark detects:

```text
Partition Size = Very Small
```

and merges partitions automatically.

Example:

```text
200 Partitions
      ↓
20 Partitions
```

Result:

```text
20 Tasks
```

instead of:

```text
200 Tasks
```

This reduces scheduling overhead and improves execution efficiency.

---

# 2. Dynamic Join Optimization

## The Problem

During planning, Spark may choose a Sort Merge Join.

Example:

```text
Customers Table = 5 GB
Orders Table    = 3 GB
```

Spark chooses:

```text
Sort Merge Join
```

because both tables appear large.

---

## Runtime Reality

During execution Spark discovers:

```text
Customers Table = 10 MB
Orders Table    = 3 GB
```

Now a Broadcast Join would be much faster.

---

## Without AQE

Spark continues with:

```text
Sort Merge Join
```

because the plan cannot change.

---

## With AQE

Spark dynamically switches:

```text
Sort Merge Join
       ↓
Broadcast Hash Join
```

This eliminates unnecessary shuffling.

---

## Example

Without AQE:

```text
Large Shuffle
      ↓
Sort Merge Join
```

With AQE:

```text
Broadcast Small Table
      ↓
Broadcast Hash Join
```

which is usually much faster.

---

# 3. Handling Data Skew

## The Problem

One of the most common Spark performance issues is skewed data.

Example:

```text
Customer A = 1 Row
Customer B = 2 Rows
Customer C = 5 Rows
Customer D = 10 Million Rows
```

Partitions become uneven.

```text
Partition 1 = 50 MB
Partition 2 = 45 MB
Partition 3 = 48 MB
Partition 4 = 8 GB
```

One executor becomes overloaded.

---

## Without AQE

```text
Task 1 → 10 sec
Task 2 → 12 sec
Task 3 → 11 sec
Task 4 → 20 min
```

The entire job waits for Task 4.

This is called a **straggler task**.

---

## With AQE

Spark detects the skewed partition and splits it.

```text
8 GB Partition
      ↓
2 GB
2 GB
2 GB
2 GB
```

Now multiple executors can process the data.

```text
Executor 1 → 2 GB
Executor 2 → 2 GB
Executor 3 → 2 GB
Executor 4 → 2 GB
```

Execution becomes more balanced.

---

# AQE Architecture

```text
Query
  ↓
Logical Plan
  ↓
Physical Plan
  ↓
Stage Execution
  ↓
Runtime Statistics
  ↓
Adaptive Query Execution
  ↓
Updated Physical Plan
  ↓
Remaining Stages
```

The key idea is that Spark can modify the physical plan after execution has already started.

---

# Enabling AQE

AQE is enabled by default in modern Spark versions.

You can verify:

```python
spark.conf.get(
    "spark.sql.adaptive.enabled"
)
```

---

## Enable AQE

```python
spark.conf.set(
    "spark.sql.adaptive.enabled",
    "true"
)
```

---

# Important AQE Configurations

## Enable AQE

```python
spark.conf.set(
    "spark.sql.adaptive.enabled",
    "true"
)
```

---

## Enable Skew Join Optimization

```python
spark.conf.set(
    "spark.sql.adaptive.skewJoin.enabled",
    "true"
)
```

---

## Enable Partition Coalescing

```python
spark.conf.set(
    "spark.sql.adaptive.coalescePartitions.enabled",
    "true"
)
```

---

## Broadcast Join Threshold

```python
spark.conf.set(
    "spark.sql.autoBroadcastJoinThreshold",
    "10MB"
)
```

Controls when Spark considers broadcasting a table.

---

# Viewing AQE in Action

Use:

```python
df.explain("formatted")
```

or

```python
df.explain(True)
```

When AQE is active, you may see:

```text
AdaptiveSparkPlan
```

in the execution plan.

Example:

```text
AdaptiveSparkPlan
 +- BroadcastHashJoin
```

or

```text
AdaptiveSparkPlan
 +- SortMergeJoin
```

---

# AQE and Databricks

AQE is heavily used in Databricks workloads.

Typical benefits include:

- Faster joins
- Reduced shuffle overhead
- Better cluster utilization
- Lower execution cost
- Reduced runtime for skewed datasets

This is one of the reasons modern Databricks workloads often perform significantly better than older Spark jobs.

---

# Interview Questions

## What is AQE?

Adaptive Query Execution is a Spark optimization framework that allows Spark to modify execution plans during runtime using actual execution statistics.

---

## What Problems Does AQE Solve?

AQE mainly solves:

- Small shuffle partitions
- Inefficient join selection
- Data skew

---

## Can AQE Change Join Strategies?

Yes.

Spark can dynamically switch:

```text
Sort Merge Join
       ↓
Broadcast Hash Join
```

if runtime statistics indicate that broadcasting is more efficient.

---

## Why is AQE Important?

Traditional Spark optimization happens before execution begins.

AQE allows Spark to optimize queries during execution, making decisions based on real data rather than estimates.

---

# Key Takeaways

- AQE stands for Adaptive Query Execution.
- Introduced in Spark 3.0.
- Re-optimizes execution plans during runtime.
- Uses actual execution statistics instead of estimates.
- Automatically handles:
  - Small shuffle partitions
  - Dynamic join selection
  - Data skew
- Enabled by default in modern Spark versions.
- Plays a major role in improving Spark SQL and DataFrame performance.

In modern Spark applications, AQE is one of the most important optimization features and is heavily relied upon in production-grade Data Engineering workloads.