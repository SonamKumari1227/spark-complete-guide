# RDD, DataFrame, and Dataset in Apache Spark

## Introduction

When learning Spark, one of the most common questions is:

> Why does Spark provide multiple APIs to represent data?

If Spark can process data using RDDs, why were DataFrames introduced? And if DataFrames already exist, why do Datasets exist?

The answer lies in the evolution of Spark itself.

As Spark matured, developers needed:

- Better performance
- Easier APIs
- Query optimization
- Stronger type safety
- Reduced development complexity

To address these requirements, Spark introduced three major abstractions:

```text
RDD
 ↓
DataFrame
 ↓
Dataset
```

Each abstraction was designed to solve the limitations of the previous one.

Understanding this evolution is important because modern Data Engineering workloads are built almost entirely on DataFrames, while many of Spark's core concepts still originate from RDDs.

---

# Evolution of Spark APIs

## The Early Days of Spark

When Spark was initially released, the primary abstraction was the **RDD (Resilient Distributed Dataset)**.

RDDs provided developers with complete control over distributed data processing.

While powerful, RDD-based applications had several drawbacks:

- Verbose code
- No schema awareness
- No query optimization
- Lower performance for analytical workloads

As Spark adoption increased, these limitations became more visible.

To solve this problem, Spark introduced **DataFrames**.

---

## The Introduction of DataFrames

DataFrames brought a familiar tabular structure to Spark.

Instead of dealing with low-level distributed objects, developers could work with:

- Rows
- Columns
- Schemas

More importantly, Spark could finally understand the structure of the data being processed.

This enabled Spark's **Catalyst Optimizer** and **Tungsten Execution Engine**, leading to major performance improvements.

---

## The Introduction of Datasets

While DataFrames improved performance, they sacrificed compile-time type safety.

To bridge the gap between RDDs and DataFrames, Spark introduced **Datasets**.

Datasets combine:

```text
RDD Type Safety
        +
DataFrame Optimization
```

Datasets are primarily used in Scala and Java and are not available in PySpark.

---

# What is an RDD?

RDD stands for:

**Resilient Distributed Dataset**

It is the fundamental distributed data structure in Spark.

An RDD is:

- Immutable
- Distributed
- Partitioned
- Fault Tolerant

RDDs represent a collection of objects distributed across multiple nodes in a Spark cluster.

Example:

```python
rdd = spark.sparkContext.parallelize(
    [1, 2, 3, 4, 5]
)
```

---

# Breaking Down the Name

## Resilient

RDDs can recover lost partitions using lineage information.

Spark does not need to replicate every intermediate dataset.

Instead, it remembers how the data was created.

Example:

```text
Source Data
      ↓
Map
      ↓
Filter
      ↓
RDD
```

If a partition is lost, Spark rebuilds it using the lineage graph.

---

## Distributed

RDD data is distributed across multiple partitions.

```text
RDD
├── Partition 1
├── Partition 2
├── Partition 3
└── Partition 4
```

Each partition can be processed independently.

This enables parallel processing across multiple executors.

---

## Dataset

An RDD is essentially a collection of distributed records.

Example:

```python
rdd = spark.sparkContext.parallelize([
    ("Jonh", 100000),
    ("Tom", 120000)
])
```

---

# Characteristics of RDDs

## Immutable

RDDs cannot be modified after creation.

Every transformation creates a new RDD.

```python
rdd1 = spark.sparkContext.parallelize([1, 2, 3])

rdd2 = rdd1.map(lambda x: x * 2)
```

The original RDD remains unchanged.

---

## Partitioned

RDDs are divided into partitions.

For a given stage:

```text
1 Partition = 1 Task
```

Example:

```text
4 Partitions
=
4 Tasks
```

Tasks are distributed to executors for execution.

---

## Fault Tolerant

RDDs maintain lineage information.

```text
RDD A
  ↓
Map
  ↓
Filter
  ↓
RDD B
```

If part of RDD B is lost, Spark can recompute it using RDD A.

---

# Limitations of RDDs

Although RDDs are powerful, they have several limitations.

## No Schema Awareness

Consider the following RDD:

```python
rdd = spark.sparkContext.parallelize([
    (1, "Sonam", 100000)
])
```

Spark only sees:

```text
(1, "Sonam", 100000)
```

It does not know:

```text
id = 1
name = Sonam
salary = 100000
```

The meaning of each field exists only in the developer's code.

---

## No Query Optimization

RDD operations are treated as black-box transformations.

Example:

```python
rdd.filter(lambda x: x[2] > 50000)
```

Spark cannot optimize the lambda function because it does not understand its intent.

---

## Less Efficient for Analytics

Analytical workloads involving:

- Filtering
- Aggregations
- Joins
- SQL Queries

typically perform better with DataFrames.

---

# What is a DataFrame?

A DataFrame is a distributed collection of data organized into named columns.

Conceptually, it is similar to:

- SQL Tables
- Pandas DataFrames

Example:

```python
df = spark.createDataFrame(
    [(1, "Sonam", 100000)],
    ["id", "name", "salary"]
)
```

Output:

| id | name | salary |
|----|------|---------|
| 1 | Sonam | 100000 |

---

# Why DataFrames Were Introduced

The biggest limitation of RDDs was that Spark had no understanding of the data structure.

DataFrames solved this problem by introducing schemas.

Example:

```python
df.printSchema()
```

Output:

```text
root
 |-- id: integer
 |-- name: string
 |-- salary: integer
```

Spark now understands:

- Column names
- Data types
- Relationships between columns

This allows Spark to optimize execution.

---

# Internal Representation of a DataFrame

RDD:

```text
Partition
│
└── (1, "Sonam", 100000)
```

DataFrame:

```text
Partition
│
└── Row(
       id=1,
       name='Sonam',
       salary=100000
   )
```

Although both are distributed as partitions, the DataFrame carries schema metadata.

This additional information enables optimization.

---

# Catalyst Optimizer

One of the most important components of Spark SQL.

When Spark receives:

```python
df.filter(col("salary") > 50000)
```

it does not execute immediately.

Instead, Spark creates:

```text
Logical Plan
      ↓
Optimized Logical Plan
      ↓
Physical Plan
      ↓
Execution
```

Catalyst can perform:

- Predicate Pushdown
- Constant Folding
- Column Pruning
- Join Reordering

without requiring any changes to user code.

---

# Tungsten Execution Engine

DataFrames also benefit from Tungsten.

Tungsten improves:

- CPU Utilization
- Memory Management
- Binary Processing
- Garbage Collection Efficiency

This is another major reason why DataFrames outperform RDDs.

---

# Advantages of DataFrames

## Schema Awareness

Spark understands the structure of the data.

---

## Query Optimization

Catalyst automatically optimizes execution plans.

---

## Better Performance

DataFrames are typically faster than equivalent RDD operations.

---

## SQL Support

DataFrames integrate directly with Spark SQL.

```python
df.createOrReplaceTempView("employees")

spark.sql("""
SELECT *
FROM employees
WHERE salary > 50000
""")
```

---

# What is a Dataset?

A Dataset is a strongly typed distributed collection of objects.

Datasets combine:

```text
RDD Type Safety
        +
DataFrame Optimization
```

Datasets are available only in:

- Scala
- Java

They are not supported in PySpark.

---

# Example Dataset

Scala Example:

```scala
case class Employee(
    id: Int,
    name: String,
    salary: Double
)

val ds = spark.read
              .json("employees.json")
              .as[Employee]
```

Unlike DataFrames, the compiler understands the Employee object.

---

# Advantages of Datasets

## Compile-Time Type Safety

Errors can be detected during compilation rather than runtime.

---

## Catalyst Optimization

Datasets still benefit from Spark's optimizer.

---

## Strongly Typed APIs

Developers can work with domain-specific objects.

---

# Why Datasets Are Rare in PySpark

PySpark is built on Python, which is dynamically typed.

As a result, Spark cannot provide Dataset-style compile-time guarantees.

Therefore, PySpark users primarily work with:

```text
RDD
DataFrame
```

and most production pipelines use DataFrames.

---

# RDD vs DataFrame vs Dataset

| Feature | RDD | DataFrame | Dataset |
|----------|----------|----------|----------|
| Schema Support | ❌ No | ✅ Yes | ✅ Yes |
| Type Safe | ✅ Yes | ❌ No | ✅ Yes |
| Catalyst Optimizer | ❌ No | ✅ Yes | ✅ Yes |
| Tungsten Engine | ❌ No | ✅ Yes | ✅ Yes |
| SQL Support | ❌ No | ✅ Yes | ✅ Yes |
| Performance | Low | High | High |
| Ease of Use | Medium | High | Medium |
| PySpark Support | ✅ Yes | ✅ Yes | ❌ No |

---

# Which One Should Data Engineers Use?

## Use DataFrames for Most Workloads

DataFrames are the industry standard for:

- ETL Pipelines
- Data Warehousing
- Data Lakes
- Delta Lake Processing
- Spark SQL
- Analytics

If you're working with Databricks, Azure Synapse, EMR, or modern Spark-based platforms, most of your daily work will involve DataFrames.

---

## Use RDDs Only When Necessary

RDDs are useful when:

- Working with unstructured data
- Implementing custom distributed algorithms
- Needing low-level control over partitions

For most modern workloads, DataFrames are preferred.

---

## Use Datasets in Scala/Java Projects

Datasets are most valuable when:

- Type safety is important
- Working in Scala
- Building enterprise JVM applications

---

# Industry Perspective

Today, the majority of Spark workloads use DataFrames.

A simplified view of real-world adoption looks like:

```text
DataFrame  → 95%
RDD        → Rare
Dataset    → Mostly Scala/Java Teams
```

While RDDs remain the foundation of Spark, DataFrames have become the default choice for modern Data Engineering because they offer better performance, simpler APIs, and powerful query optimization capabilities.