# Catalyst Optimizer

## Introduction

One of the biggest reasons Spark DataFrames outperform RDDs is the **Catalyst Optimizer**.

Instead of immediately executing a query, Spark first analyzes the entire computation, builds an execution plan, and applies a series of optimizations before generating the final execution strategy.

This allows Spark to eliminate unnecessary work, reduce data movement, and improve overall performance.

---

## Why Does Spark Need an Optimizer?

Consider the following query:

```python
df.filter(col("salary") > 50000) \
  .select("name", "salary")
```

A naive execution engine might:

1. Read all columns.
2. Load all rows.
3. Apply filter.
4. Select required columns.

Spark takes a different approach.

Before execution, Catalyst analyzes the query and determines the most efficient execution strategy.

---

## Catalyst Optimization Flow

Every DataFrame query passes through multiple phases:

```text
User Code
    ↓
Logical Plan
    ↓
Optimized Logical Plan
    ↓
Physical Plan
    ↓
Execution
```

---

## Step 1: Logical Plan

The Logical Plan describes:

```text
What needs to be done
```

Example:

```python
df.filter(col("salary") > 50000) \
  .select("name", "salary")
```

Logical Plan:

```text
Read Data
    ↓
Filter salary > 50000
    ↓
Select name, salary
```

At this stage Spark is not concerned with execution.

---

## Step 2: Optimized Logical Plan

Catalyst applies optimization rules.

Example:

```python
df.select("name", "salary") \
  .filter(col("salary") > 50000)
```

Catalyst may rewrite it as:

```python
df.filter(col("salary") > 50000) \
  .select("name", "salary")
```

to reduce the amount of data processed.

---

## Step 3: Physical Plan

The Physical Plan describes:

```text
How Spark will execute the query
```

Example:

```text
File Scan
    ↓
Filter
    ↓
Project
```

Spark may also choose different execution strategies depending on the workload.

For example:

```text
Broadcast Hash Join
```

or

```text
Sort Merge Join
```

---

# Common Optimizations Performed by Catalyst

## Predicate Pushdown

Moves filters closer to the data source.

Example:

```python
df.filter(col("country") == "UK")
```

Instead of reading all records, Spark attempts to read only the required data.

Benefits:

- Less Disk I/O
- Less Memory Usage
- Faster Execution

---

## Column Pruning

Reads only required columns.

Example:

```python
df.select("name")
```

Instead of reading:

```text
id
name
salary
department
country
email
```

Spark reads:

```text
name
```

only.

Benefits:

- Reduced I/O
- Reduced Memory Usage

---

## Constant Folding

Evaluates constant expressions during optimization.

Example:

```python
df.filter(col("salary") > (1000 * 12))
```

Catalyst converts:

```text
1000 * 12
```

into:

```text
12000
```

before execution.

---

## Filter Reordering

Catalyst may rearrange filters to reduce processing costs.

Example:

```python
df.filter(...)
  .filter(...)
```

Spark can optimize the order of evaluation.

---

## Join Strategy Selection

Catalyst chooses the most efficient join method.

Common strategies:

```text
Broadcast Hash Join
Sort Merge Join
Shuffle Hash Join
```

Example:

If one table is small:

```python
broadcast(dim_df)
```

Spark can avoid a costly shuffle.

---

## Projection Pruning

Removes unnecessary columns from intermediate execution steps.

This reduces:

- Memory Usage
- Serialization Cost
- Shuffle Volume

---

# How to View Catalyst Decisions

Use:

```python
df.explain()
```

or

```python
df.explain(True)
```

or

```python
df.explain("formatted")
```

These commands show:

```text
Logical Plan
Optimized Logical Plan
Physical Plan
```

and are frequently used during Spark tuning.

---

# What Catalyst Cannot Optimize

Catalyst works best with DataFrame and SQL APIs.

Example:

```python
df.filter(col("salary") > 50000)
```

Catalyst understands:

- Columns
- Data Types
- Expressions

---

However, Catalyst cannot fully optimize arbitrary Python code.

Example:

```python
rdd.filter(lambda x: expensive_custom_logic(x))
```

Spark treats this as a black box.

This is one reason DataFrames generally outperform RDDs.

---

# DataFrame vs RDD

### DataFrame

```text
Query
   ↓
Catalyst
   ↓
Optimized Plan
   ↓
Execution
```

### RDD

```text
Transformation
      ↓
Direct Execution
```

No Catalyst Optimization.

---

# Spark UI and Catalyst

When investigating performance issues:

1. Check the Physical Plan.
2. Verify filters are pushed down.
3. Look for unnecessary shuffles.
4. Inspect join strategies.
5. Confirm column pruning is occurring.

Catalyst often explains why two logically equivalent queries have different execution times.

---

# Important Interview Facts

### Does Catalyst work with RDDs?

No.

Catalyst only optimizes:

- DataFrames
- Datasets
- Spark SQL

---

### What is Predicate Pushdown?

Moving filters closer to the data source.

---

### What is Column Pruning?

Reading only required columns.

---

### What is Constant Folding?

Evaluating constant expressions during optimization.

---

### How do you inspect Catalyst's execution plan?

```python
df.explain(True)
```

---

### Why are DataFrames faster than RDDs?

Because DataFrames benefit from:

- Catalyst Optimizer
- Tungsten Execution Engine

---

# Key Takeaways

- Catalyst is Spark's query optimization engine.
- It converts Logical Plans into optimized execution plans.
- Common optimizations include Predicate Pushdown, Column Pruning, Constant Folding, and Join Optimization.
- Catalyst works with DataFrames, Datasets, and Spark SQL.
- RDDs bypass Catalyst and therefore receive fewer optimizations.
- Understanding execution plans is essential for Spark performance tuning.