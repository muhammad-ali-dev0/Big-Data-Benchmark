# 📊 Big Data Optimization Benchmark Study

A rigorous performance analysis comparing **Pandas vs PySpark**, **partitioning strategies**, and **caching mechanisms** across real-world data engineering workloads — from 500K to 50M rows.

> Built as a portfolio project targeting data engineering clients on Upwork.
> **[View Live Report →](https://muhammad-ali-dev0.github.io/big-data-benchmark)**

---

## What This Benchmarks

| Dimension | Variables Tested |
|-----------|-----------------|
| **Framework** | Pandas 2.1 vs PySpark 3.4 |
| **Operations** | CSV read, GroupBy agg, joins, window functions, Parquet write |
| **Partitioning** | None, year, year+month, category, over-partitioned, bucketed |
| **Caching** | No cache, `cache()`, `persist(MEMORY_AND_DISK)`, `persist(DISK_ONLY)` |
| **Dataset sizes** | 500K · 1M · 5M · 10M · 25M · 50M rows (up to 12 GB) |

---

## Key Findings

### 1. The 5M Row Crossover is Real
PySpark's distributed overhead (JVM startup, task scheduling, shuffle) makes it **slower than Pandas below ~5M rows**. Above that threshold, parallelism compounds and Pandas' single-thread bottleneck becomes critical.

```
GroupBy on 50M rows:
  Pandas  → 847s  (OOM on joins)
  PySpark → 37s   (22.9× faster)
```

### 2. Partitioning = Highest ROI Optimization
A well-designed year/month partition scheme vs no partitioning:

```
No partition  → 312s query time, 12.0 GB scanned (100%)
Year+Month    →  11s query time,  0.2 GB scanned  (1.7%)

Result: 96.4% faster, 98.3% less I/O — a one-time schema decision
```

### 3. The Small Files Problem is the Silent Killer
Over-partitioning on a high-cardinality key (`user_id`) produced **2.1M tiny files**:

```
No partition      → 312s
Year+Month        →  11s  ✅
Over-partitioned  → 891s  ❌  (worse than no partitioning at all)
```

### 4. Caching Break-Even at 2 Actions
Caching adds overhead on first materialization. For single queries it hurts. For repeated access:

```
10 queries, no cache  → 370s
10 queries, cache()   →  81s  (4.5× faster)
20 queries, cache()   → 103s  (7.2× faster — compounding returns)
```

---

## Full Results

### Pandas vs PySpark — Execution Time

| Operation | Dataset | Pandas (s) | PySpark (s) | Speedup | Winner |
|-----------|---------|-----------|------------|---------|--------|
| CSV Read | 500K | 0.8 | 3.2 | 0.25× | Pandas |
| CSV Read | 5M | 8.1 | 7.4 | 1.09× | PySpark |
| CSV Read | 50M | 94.3 | 18.6 | 5.1× | **PySpark** |
| GroupBy Agg | 500K | 0.3 | 4.1 | 0.07× | Pandas |
| GroupBy Agg | 5M | 3.9 | 5.8 | 0.67× | Pandas |
| GroupBy Agg | 50M | 847.2 | 37.0 | **22.9×** | **PySpark** |
| Join | 500K×500K | 1.2 | 6.3 | 0.19× | Pandas |
| Join | 5M×5M | 28.4 | 11.2 | 2.5× | **PySpark** |
| Join | 50M×50M | OOM ✗ | 89.7 | ∞ | **PySpark** |
| Window Function | 50M | OOM ✗ | 52.3 | ∞ | **PySpark** |
| Write Parquet | 50M | 214.0 | 23.1 | **9.3×** | **PySpark** |

### Partitioning Strategies

| Strategy | Partition Key | Query Time (s) | Data Scanned |
|----------|--------------|---------------|-------------|
| No Partition | — | 312.4 | 12.0 GB (100%) |
| Year | year | 84.1 | 2.4 GB (20%) |
| **Year+Month** | year, month | **11.3** | **200 MB (1.7%)** |
| Category | category | 18.7 | 250 MB (2.1%) |
| Over-Partitioned | user_id | 891.0 ❌ | 12.0 GB (100%) |
| **Bucketed (256)** | user_id bucket | **9.8** | 46.9 MB × n |

### Caching Impact (PySpark, 50M rows)

| Scenario | # Queries | No Cache (s) | cache() (s) | Speedup |
|----------|-----------|-------------|------------|---------|
| Single query | 1 | 37.0 | 39.2 | –5% (overhead) |
| Dashboard | 5 | 185.0 | 59.3 | 3.1× |
| Iterative ML | 10 | 370.0 | 81.4 | **4.5×** |
| Complex ETL | 20 | 740.0 | 103.2 | **7.2×** |

---

## Code Samples

### Benchmark Harness
```python
import time, tracemalloc
import pandas as pd

def benchmark(fn, *args, runs=5):
    times = []
    for _ in range(runs):
        tracemalloc.start()
        t0 = time.perf_counter()
        fn(*args)
        elapsed = time.perf_counter() - t0
        mem = tracemalloc.get_traced_memory()[1] / 1e6  # MB
        tracemalloc.stop()
        times.append({"elapsed_s": elapsed, "peak_mb": mem})
    return pd.DataFrame(times).mean()
```

### Correct Partitioning Pattern
```python
# ✅ GOOD — low-cardinality key, files land at ~200MB each
df.write \
  .partitionBy("year", "month") \
  .mode("overwrite") \
  .parquet("s3://datalake/events/")

# Query skips 98.3% of data via predicate pushdown
spark.read.parquet("s3://datalake/events/") \
     .filter("year = 2024 AND month = 3") \
     .groupBy("event_type").count()

# ❌ BAD — high cardinality creates 2.1M tiny files
df.write.partitionBy("user_id").parquet("s3://datalake/events/")
```

### Caching Pattern
```python
# ✅ Cache when reusing a DataFrame across multiple actions
filtered_df = spark.read.parquet("s3://data/events/") \
                   .filter("year = 2024") \
                   .cache()

filtered_df.count()                            # triggers materialization
by_region   = filtered_df.groupBy("region").agg(...)   # free
by_category = filtered_df.groupBy("category").agg(...) # free

filtered_df.unpersist()  # always release when done

# ❌ Anti-pattern: caching a DataFrame used only once
spark.read.parquet("s3://data/").cache().count()  # wasteful overhead
```

---

## Test Environment

| Component | Spec |
|-----------|------|
| Cluster | AWS EMR 6.9, 8-node |
| Instance type | r5.2xlarge (64 GB RAM, 8 vCPU per node) |
| PySpark | 3.4.1 |
| Python | 3.10 |
| Pandas | 2.1.0 |
| Dataset | Synthetic event log, 12 GB max |
| Runs | 5 iterations (cold + warm), averaged |
| Shuffle partitions | 200 (default) |

---

## Reproduce Locally

```bash
git clone https://github.com/muhammad-ali-dev0/big-data-benchmark
cd big-data-benchmark
pip install -r requirements.txt

# Generate synthetic dataset
python scripts/generate_data.py --rows 5_000_000 --output data/events.parquet

# Run all benchmarks
python benchmarks/pandas_vs_spark.py
python benchmarks/partition_strategy.py
python benchmarks/caching_impact.py

# Open the interactive report
open report/big_data_benchmark.html
```

---

## Reproducing on AWS EMR

```bash
# Bootstrap script — install Python deps on all nodes
aws emr create-cluster \
  --name "benchmark-cluster" \
  --release-label emr-6.9.0 \
  --applications Name=Spark Name=Hadoop \
  --instance-type r5.2xlarge \
  --instance-count 8 \
  --bootstrap-actions Path=s3://your-bucket/bootstrap.sh

# Submit benchmark job
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --executor-memory 8g \
  --executor-cores 4 \
  --num-executors 14 \
  benchmarks/pandas_vs_spark.py
```

---

## When to Use What

```
Dataset size?
│
├─ < 5M rows ──────────────────────→ Pandas
│   Simple transforms, no joins         Fast, zero overhead
│
├─ 5M–50M rows ────────────────────→ PySpark (single node or small cluster)
│   GroupBy, joins, window functions    Tune shuffle.partitions down
│
└─ 50M+ rows ──────────────────────→ PySpark (distributed cluster)
    Complex pipelines, ML feature eng   Add partitioning + caching


Partitioning?
├─ Query filter on date/region ────→ Partition by date or region
├─ Join-heavy workload ────────────→ Bucket by join key (256 buckets)
├─ High cardinality key ───────────→ Never partition directly — bucket instead
└─ Single ad-hoc query ────────────→ Skip partitioning, use columnar format


Caching?
├─ DataFrame used once ────────────→ No cache
├─ DataFrame used 2+ times ────────→ cache() (MEMORY_ONLY)
├─ Low memory, used many times ────→ persist(MEMORY_AND_DISK)
└─ Very large, iterative ML ───────→ persist(DISK_ONLY)
```

---

## Interactive Report

The `report/big_data_benchmark.html` file is a fully self-contained interactive report with:
- Chart.js visualizations (log-scale execution time, partition comparison, cache waterfall)
- Complete benchmark tables
- Resource utilization analysis
- Anti-pattern code examples

Deploy it directly to GitHub Pages for a live demo link on your profile.

---

## Tech Stack

| Tool | Purpose |
|------|---------|
| PySpark 3.4 | Distributed processing engine |
| Pandas 2.1 | Single-machine baseline |
| AWS EMR 6.9 | Managed Spark cluster |
| Chart.js 4.4 | Interactive benchmark visualizations |
| Python `tracemalloc` | Memory profiling |
| `time.perf_counter` | High-resolution timing |

---

## License

MIT