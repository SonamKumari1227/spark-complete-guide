# Tungsten Engine

## Introduction

The **Tungsten Engine** is Spark's physical execution engine responsible for improving CPU efficiency, memory management, and overall query performance.

If Catalyst Optimizer decides **what** Spark should execute, Tungsten determines **how** that execution is performed as efficiently as possible.

```text
Catalyst Optimizer
        ↓
Creates Optimized Plan
        ↓
Tungsten Engine
        ↓
Executes Efficiently
```

Introduced in Spark 1.4, Project Tungsten was designed to overcome bottlenecks caused by:

- Excessive JVM Object Creation
- Garbage Collection Overhead
- Inefficient Memory Usage
- CPU Inefficiencies

Today, Tungsten is one of the main reasons DataFrames significantly outperform traditional RDD workloads.

---

# Is Tungsten Built-In?

Yes.

Tungsten is built directly into Spark.

As a Spark developer, you do **not** explicitly enable or implement Tungsten.

When you use:

```python
df.filter(...)
  .groupBy(...)
  .agg(...)
```

Spark automatically uses:

```text
Catalyst Optimizer
        +
Tungsten Engine
```

behind the scenes.

For most workloads:

```text
DataFrame
Dataset
Spark SQL
```

automatically benefit from Tungsten optimizations.

---

# Where Does Tungsten Fit?

Spark execution flow:

```text
User Code
      ↓
Logical Plan
      ↓
Catalyst Optimizer
      ↓
Physical Plan
      ↓
Tungsten Engine
      ↓
Tasks Executed by Executors
```

Catalyst generates the plan.

Tungsten executes the plan efficiently.

---

# Why Was Tungsten Needed?

Before Tungsten, Spark relied heavily on JVM objects.

Consider:

```text
Employee
├── id
├── name
└── salary
```

Each row could create multiple JVM objects.

Millions of rows resulted in:

- High Memory Usage
- Excessive Garbage Collection
- CPU Overhead

As data volumes increased, performance degraded significantly.

Tungsten was introduced to reduce these costs.

---

# Key Optimizations Provided by Tungsten

## 1. Off-Heap Memory Management

Traditionally:

```text
Data
 ↓
JVM Heap
```

Problem:

- Large Heap Usage
- Frequent Garbage Collection
- Memory Fragmentation

Tungsten can store data outside the JVM heap:

```text
Data
 ↓
Off-Heap Memory
```

Benefits:

- Less GC Pressure
- Better Memory Utilization
- Faster Processing

---

## 2. Binary Memory Format

Instead of storing data as complex JVM objects:

```text
Employee Object
├── id
├── name
└── salary
```

Tungsten stores data in a compact binary format.

Example:

```text
[Binary Data]
```

Benefits:

- Less Memory Usage
- Better CPU Cache Efficiency
- Faster Serialization

---

## 3. Cache-Friendly Processing

Modern CPUs are optimized for sequential memory access.

Tungsten organizes data in memory to improve:

- CPU Cache Utilization
- Data Locality
- Processing Speed

Result:

```text
Fewer CPU Cache Misses
      ↓
Better Performance
```

---

## 4. Whole-Stage Code Generation

One of Tungsten's most important optimizations.

Instead of executing many operators separately:

```text
Filter
 ↓
Project
 ↓
Aggregate
```

Spark generates optimized Java bytecode that combines multiple operations.

Conceptually:

```text
Multiple Operators
        ↓
Single Optimized Execution Function
```

Benefits:

- Reduced Function Call Overhead
- Better CPU Utilization
- Faster Execution

---

# Example

Query:

```python
df.filter(col("salary") > 50000) \
  .select("name", "salary")
```

Without optimization:

```text
Read
 ↓
Filter
 ↓
Store Intermediate Result
 ↓
Select
```

With Whole-Stage Code Generation:

```text
Single Generated Function
        ↓
Read + Filter + Select
```

Less overhead.

Faster execution.

---

# Tungsten and DataFrames

DataFrames automatically benefit from:

- Catalyst Optimization
- Tungsten Execution

```text
DataFrame
     ↓
Catalyst
     ↓
Tungsten
     ↓
Execution
```

This is one reason DataFrames outperform RDDs.

---

# Tungsten and RDDs

RDDs do not fully benefit from Tungsten.

Example:

```python
rdd.map(...)
   .filter(...)
```

Spark treats user-defined functions as black boxes.

As a result:

```text
Limited Catalyst Optimization
Limited Tungsten Benefits
```

This is one reason Spark recommends DataFrames for most ETL workloads.

---

# How to Verify Tungsten Usage

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
WholeStageCodegen
```

Example:

```text
*(1) Filter
*(1) Project
```

The `*` often indicates Whole-Stage Code Generation is being used.

---

# Tungsten vs Catalyst

Many beginners confuse these two components.

| Component | Responsibility |
|------------|------------|
| Catalyst | Query Optimization |
| Tungsten | Query Execution |
| Catalyst | Decides What To Execute |
| Tungsten | Decides How To Execute Efficiently |

Think of them as:

```text
Catalyst
     ↓
Creates Best Plan

Tungsten
     ↓
Executes Best Plan
```

---

# Common Interview Questions

## Is Tungsten a separate framework?

No.

It is built into Spark.

---

## Do we need to enable Tungsten?

No.

Spark automatically uses it.

---

## Does Tungsten work with DataFrames?

Yes.

DataFrames and Spark SQL heavily benefit from Tungsten.

---

## Does Tungsten improve RDD performance?

Not to the same extent.

RDDs bypass many Catalyst and Tungsten optimizations.

---

## What are the major Tungsten optimizations?

- Off-Heap Memory Management
- Binary Memory Format
- Cache-Aware Processing
- Whole-Stage Code Generation

---

## Why are DataFrames faster than RDDs?

Because DataFrames benefit from:

```text
Catalyst Optimizer
        +
Tungsten Engine
```

while RDDs largely bypass these optimizations.

---

# What Should a Data Engineer Remember?

### Most Important Facts

```text
Catalyst = Optimization Engine

Tungsten = Execution Engine
```

```text
DataFrame
    ↓
Catalyst
    ↓
Tungsten
    ↓
Execution
```

```text
Whole-Stage Code Generation
```

is one of the biggest performance improvements provided by Tungsten.

```text
Off-Heap Memory
```

helps reduce garbage collection overhead.

```text
Binary Memory Storage
```

improves memory efficiency and CPU performance.

And most importantly:

```text
You do not implement Tungsten.

You automatically benefit from it when using
DataFrames, Datasets, and Spark SQL.
```