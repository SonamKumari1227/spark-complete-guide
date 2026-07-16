# Jobs, Stages, and Tasks

## Introduction

When an Action is triggered, Spark converts the execution plan into a hierarchy of:

```text
Job → Stage → Task
```

Understanding this hierarchy is essential for:

- Spark UI Analysis
- Performance Tuning
- Shuffle Optimization
- Troubleshooting Failed Jobs

---

## Job

A Job is the highest execution unit in Spark.

A new Job is created whenever an Action is executed.

Example:

```python
df.count()
```

```python
df.show()
```

```python
df.write.parquet("/output")
```

Each Action creates a separate Job.

### Key Point

```text
1 Action = 1 Job
```

---

## Stage

A Stage is a collection of Tasks that can execute together without requiring data redistribution.

Stages are separated by:

```text
Shuffle Boundaries
```

Example:

```python
df.filter(...)
  .groupBy(...)
  .count()
```

Execution:

```text
Stage 1
Read
Filter

===== Shuffle =====

Stage 2
GroupBy
Count
```

### Expression

```text
Number of Shuffles + 1
        =
Number of Stages
```
> Does `number of shuffles + 1 = number of stages` always hold?

The best answer is:

> No. It's a useful approximation for simple linear DAGs, but stage creation depends on Spark's dependency graph, shuffle boundaries, join strategies, and execution plan. Complex DAGs can have multiple parent stages converging into a single stage, so there is no universal formula.

(Not always true for complex DAGs, but useful in most interview scenarios.)

---

## Task

A Task is the smallest unit of execution in Spark.

Tasks are executed by Executors.

For every partition in a Stage, Spark creates one Task.

### Expression

```text
Number of Partitions
          =
Number of Tasks
```

Example:

```text
200 Partitions
      =
200 Tasks
```

---

## Partition → Task → Executor

```text
Partition 1 → Task 1 → Executor 1
Partition 2 → Task 2 → Executor 2
Partition 3 → Task 3 → Executor 1
```

### Key Rule

```text
1 Partition = 1 Task
```

An Executor can execute multiple Tasks during its lifetime.

---

## Job, Stage, Task Hierarchy

```text
Job
│
├── Stage 1
│     ├── Task 1
│     ├── Task 2
│     └── Task 3
│
└── Stage 2
      ├── Task 4
      ├── Task 5
      └── Task 6
```

---

## Narrow vs Wide Transformation

### Narrow Transformation

Examples:

```python
filter()
select()
map()
withColumn()
```

Characteristics:

- No Shuffle
- Same Stage
- Faster Execution

---

### Wide Transformation

Examples:

```python
groupBy()
join()
distinct()
orderBy()
repartition()
```

Characteristics:

- Shuffle Required
- New Stage Created
- Network Data Movement

---

## Who Creates What?

| Component | Created By |
|------------|------------|
| Job | Driver |
| Stage | DAG Scheduler |
| Task | Task Scheduler |
| Execution | Executors |

---

## Important Interview Facts

### Does every Action create a Job?

Yes.

```text
1 Action = 1 Job
```

---

### What creates a new Stage?

A Shuffle operation.

---

### What creates Tasks?

Partitions.

```text
1 Partition = 1 Task
```

---

### Can a Stage have multiple parent Stages?

Yes.

Commonly seen in:

- Join Operations
- Complex DAGs
- Multi-source ETL Pipelines

Example:

```text
Stage 1 ──┐
          ▼
       Stage 3
          ▲
Stage 2 ──┘
```

---

### Can a Task span multiple Partitions?

No.

```text
1 Task processes exactly 1 Partition
```

---

### Can an Executor execute multiple Tasks?

Yes.

The number of concurrently running Tasks depends on the number of Executor Cores.

Example:

```text
Executor Cores = 4

Maximum Concurrent Tasks = 4
```

---

## Spark UI Mapping

| Spark UI Tab | Represents |
|--------------|------------|
| Jobs | Actions Executed |
| Stages | DAG Breakdown |
| Tasks | Partition-Level Execution |
| Executors | JVM Processes Running Tasks |

---

## Key Takeaways

- Action triggers a Job.
- Shuffle creates a new Stage.
- Partitions create Tasks.
- One Partition corresponds to one Task.
- Executors execute Tasks.
- Driver manages Jobs and Stages.
- Understanding Job → Stage → Task hierarchy is critical for Spark optimization and debugging.