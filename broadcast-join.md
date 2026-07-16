# Broadcast Joins in Apache Spark

## Introduction

Joins are among the most expensive operations in Spark because they often require a **shuffle**, where data is redistributed across the cluster.

However, when one of the datasets is significantly smaller than the other, Spark can avoid this expensive shuffle by using a **Broadcast Join**.

Instead of moving both datasets across the network, Spark broadcasts the smaller dataset to every executor and performs the join locally.

```text
Large Table
      │
      ▼
Executor

Small Table
      │
      ▼
Broadcast To All Executors

Result:
Local Join
(No Shuffle on Large Table)
```

Broadcast Joins are one of the most important optimization techniques used in Data Engineering workloads, especially when joining large fact tables with small dimension tables.

---

# Why Are Regular Joins Expensive?

Consider two datasets:

```text
Sales Table      : 500 Million Rows
Customers Table  : 50 Million Rows
```

Normal join execution:

```python
sales_df.join(customers_df, "customer_id")
```

Spark typically performs:

```text
Shuffle Sales Data
        +
Shuffle Customer Data
        ↓
Join
```

This results in:

- Network Transfer
- Shuffle Read/Write
- Additional Stages
- Increased Execution Time

---

# What is a Broadcast Join?

A Broadcast Join sends a small dataset to every executor running the large dataset.

Instead of:

```text
Move Both Tables
```

Spark performs:

```text
Move Small Table Only
```

Conceptually:

```text
Executor 1
├── Sales Partition
└── Customer Table (Broadcasted)

Executor 2
├── Sales Partition
└── Customer Table (Broadcasted)

Executor 3
├── Sales Partition
└── Customer Table (Broadcasted)
```

Each executor can now perform the join locally.

---

# Example

Suppose:

```text
sales_df      → 500 Million Rows
country_df    → 200 Rows
```

Joining:

```python
sales_df.join(country_df, "country_code")
```

without broadcasting would still trigger a shuffle.

Instead:

```python
from pyspark.sql.functions import broadcast

sales_df.join(
    broadcast(country_df),
    "country_code"
)
```

Spark sends the 200-row table to every executor.

Result:

```text
No Large Shuffle
Faster Join
Lower Network Cost
```

---

# Broadcast Hash Join

The most common implementation of a Broadcast Join is:

```text
Broadcast Hash Join
```

Execution flow:

```text
Small Table
      │
      ▼
Broadcast
      │
      ▼
Build Hash Map
      │
      ▼
Join Locally On Each Executor
```

Because the small table is already available in memory, lookups are extremely fast.

---

# How Spark Decides Automatically

Spark can automatically choose a Broadcast Join if a table is below a configurable threshold.

Default configuration:

```python
spark.conf.get(
    "spark.sql.autoBroadcastJoinThreshold"
)
```

Default value is typically:

```text
10 MB
```

If Spark estimates a table is smaller than this threshold, it may automatically broadcast it.

---

# Verifying a Broadcast Join

Use:

```python
df.explain("formatted")
```

or

```python
df.explain(True)
```

Look for:

```text
BroadcastHashJoin
```

Example:

```text
BroadcastHashJoin
:- Scan Sales
+- BroadcastExchange
```

This confirms Spark is using a Broadcast Join.

---

# Broadcast Join vs Shuffle Join

## Shuffle Join

```text
Large Table
      │
      ▼
Shuffle

Small Table
      │
      ▼
Shuffle

      ▼
     Join
```

Characteristics:

- High Network Cost
- Additional Shuffle Stage
- Slower Execution

---

## Broadcast Join

```text
Small Table
      │
      ▼
Broadcast

Large Table
      │
      ▼
Local Join
```

Characteristics:

- No Large Shuffle
- Faster Execution
- Reduced Network Traffic

---

# Real-World Data Engineering Example

Consider:

```text
transactions_df
```

containing:

```text
2 Billion Rows
```

and:

```text
country_lookup_df
```

containing:

```text
250 Rows
```

A Broadcast Join is ideal:

```python
transactions_df.join(
    broadcast(country_lookup_df),
    "country_code"
)
```

Typical use cases:

- Country Codes
- Product Categories
- Currency Mappings
- Status Lookup Tables
- Reference Data
- Small Dimension Tables

---

# When Should You Use Broadcast Joins?

Broadcast when:

```text
One Table Is Small
```

and

```text
Another Table Is Large
```

Typical pattern:

```text
Fact Table
      +
Dimension Table
```

Examples:

```text
Orders + Countries
Sales + Product Categories
Transactions + Currency Codes
```

---

# When Should You NOT Broadcast?

## Large Tables

Bad example:

```text
Sales → 500 Million Rows
Customers → 100 Million Rows
```

Broadcasting a large table can:

- Exhaust Executor Memory
- Cause OOM Errors
- Slow Down Execution

---

## Multiple Large Joins

Broadcasting several large tables simultaneously can create memory pressure across the cluster.

---

# Broadcast Join and AQE

Modern Spark versions support:

```python
spark.conf.set(
    "spark.sql.adaptive.enabled",
    "true"
)
```

Adaptive Query Execution (AQE) can automatically convert a planned shuffle join into a broadcast join if runtime statistics indicate a table is small enough.

This is known as:

```text
Dynamic Join Optimization
```

---

# Broadcast Join and Data Skew

Broadcast joins are also a common technique for reducing skew-related join problems.

Instead of:

```text
Shuffle Large Table A
Shuffle Large Table B
```

Spark performs:

```text
Broadcast Small Table
Join Locally
```

which eliminates one side of the shuffle.

---

# Common Interview Questions

## What is a Broadcast Join?

A join strategy where Spark sends a small dataset to every executor and performs the join locally, avoiding a large shuffle.

---

## Why is a Broadcast Join faster?

Because it eliminates expensive network shuffling of large datasets.

---

## How do you force a Broadcast Join?

```python
from pyspark.sql.functions import broadcast

large_df.join(
    broadcast(small_df),
    "id"
)
```

---

## How do you verify a Broadcast Join?

```python
df.explain(True)
```

Look for:

```text
BroadcastHashJoin
```

---

## Does Spark automatically choose Broadcast Joins?

Yes.

If the table size is below:

```text
spark.sql.autoBroadcastJoinThreshold
```

Spark may automatically use a Broadcast Join.

---

## Can Broadcast Joins help with Data Skew?

Yes.

Because they avoid shuffling the smaller dataset.

---

# Key Takeaways

- Broadcast Joins are one of the most effective Spark join optimizations.
- Spark sends the small table to every executor instead of shuffling both datasets.
- The most common implementation is a **Broadcast Hash Join**.
- Broadcast Joins reduce network traffic, shuffle cost, and execution time.
- They are ideal for joining large fact tables with small lookup or dimension tables.
- Verify Broadcast Joins using `explain()` and look for `BroadcastHashJoin`.
- Avoid broadcasting large tables, as this can cause memory pressure and executor failures.
- AQE can automatically convert eligible joins into Broadcast Joins at runtime.