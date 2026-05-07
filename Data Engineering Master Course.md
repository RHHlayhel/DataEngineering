# Complete Data Engineering Course — Master Reference

> **A complete, annotated, and enriched course notes** — every concept, every code snippet, every best practice, organized into one definitive reference document.

Author: **Raed Hlayhel**  

---

## Table of Contents

1. [Course Introduction & Software Overview](#1-course-introduction--software-overview)
2. [What is Big Data?](#2-what-is-big-data)
3. [Hadoop Architecture](#3-hadoop-architecture)
4. [Hadoop Components Deep Dive](#4-hadoop-components-deep-dive)
5. [GCP & Hadoop Practical Use](#5-gcp--hadoop-practical-use)
6. [MapReduce](#6-mapreduce)
7. [YARN Components](#7-yarn-components)
8. [Apache Spark — Introduction](#8-apache-spark--introduction)
9. [Spark Core API — RDD](#9-spark-core-api--rdd)
10. [Spark DataFrame](#10-spark-dataframe)
11. [Spark SQL](#11-spark-sql)
12. [Caching in Spark](#12-caching-in-spark)
13. [Spark Architecture](#13-spark-architecture)
14. [Apache Hive](#14-apache-hive)
15. [Apache Kafka](#15-apache-kafka)
16. [Docker & Containers](#16-docker--containers)
17. [Apache Airflow](#17-apache-airflow)
18. [Databricks](#18-databricks)
19. [Final Project — Azure End-to-End Pipeline](#19-final-project--azure-end-to-end-pipeline)
20. [SQL — Code Reference](#20-sql--code-reference)
21. [Python — Code Reference](#21-python--code-reference)
22. [PySpark — Code Reference](#22-pyspark--code-reference)
23. [The Role of a Data Engineer — Best Practices](#23-the-role-of-a-data-engineer--best-practices)
24. [How the Tools Connect — The Big Picture](#24-how-the-tools-connect--the-big-picture)
25. [Alternatives to Every Tool in This Stack](#25-alternatives-to-every-tool-in-this-stack)
26. [Course Summary](#26-course-summary)
27. [Suggested Projects to Build](#27-suggested-projects-to-build)

---

## 1. Course Introduction & Software Overview

**Key software covered in this course:**

- **Google Cloud Platform (GCP)** — Cloud environment for datasets. Free trial: 3 months / $400 credits.
- **Hadoop** — HDFS for storage, MapReduce for processing, YARN for resource management.
- **Apache Spark** — The most important processing framework. Written in PySpark, SparkSQL, with caching, joins, and optimization.
- **Hive** — Data warehousing tool. Write SQL-like queries (HiveQL) over distributed data.
- **Kafka** — Streaming data platform. Also used with Spark Streaming.
- **Docker** — Container platform. Sets up workflows and cloud-based environments.
- **Airflow** — Workflow orchestration (via Astronomer/AstroCloud).
- **Databricks** — Managed Spark platform on the cloud.
- **Azure** — Cloud platform. Free trial: 1 month / $200 credits. Azure Synapse for analytics.
- **MongoDB** — NoSQL document store.
- **MySQL** — Relational database for SQL workbenches. Download: [MySQL Installer](https://dev.mysql.com/downloads/installer/)

> **Rule:** No local storage — all data lives in the cloud.

---

## 2. What is Big Data?

### The Problem with Traditional Systems

Traditional RDBMS are designed for small, structured data. As data grows larger, operations become slow or prone to failure, and storage becomes costly. **Big Data** = datasets too large, fast, or complex for traditional databases to process efficiently. The explosion is driven by the internet era: IoT, Social Media, mobile apps, and real-time systems.

### The 5 V's of Big Data

| V | Definition | Example |
|---|---|---|
| **Volume** | The sheer size of data generated | Petabytes of logs at Google per day |
| **Velocity** | The speed at which data is generated and must be processed | Real-time bank fraud detection |
| **Variety** | Different formats and sources of data | SQL tables, JSON APIs, video files, sensor readings |
| **Veracity** | Quality and reliability of the data | Forms with negative ages, duplicate records |
| **Value** | Data must provide actionable insights | Predicting customer churn to reduce it |

**Processing speeds by velocity:**
- **Real-time**: Bank transactions, live video feeds (processed in milliseconds)
- **Near real-time**: Website analytics, processed every 2–5 minutes
- **Batch processing**: Monthly billing statements, end-of-day reports

**Variety of data types:**
1. **Structured** — rows and columns (SQL tables)
2. **Semi-structured** — JSON, XML, CSV
3. **Unstructured** — video, audio, emails, sensor data

### Distributed Systems

- **Monolithic systems** — one machine. Upgradable but has physical limits; upgrading past a point gives diminishing returns at increasing cost.
- **Distributed systems** — multiple machines (nodes) in a cluster, each contributing storage and compute. Easier and cheaper to scale horizontally.

**All good Big Data systems are based on distributed architectures.**

### Properties of a Good Big Data System

1. **Scalability** — adds capacity without performance degradation (horizontal scaling)
2. **Reliability & Fault Tolerance** — continues working even when components fail
3. **Cost Effectiveness** — balances performance and infrastructure cost
4. **Security & Privacy** — data protected from unauthorized access

### On-Premise vs Cloud

| Aspect | On-Premise | Cloud |
|---|---|---|
| **Cost** | High upfront (hardware, space) | Pay-as-you-go |
| **Scaling** | Manual, expensive | Automatic, elastic |
| **Security** | Full control | Shared responsibility |
| **Maintenance** | Your team | Provider's team |

**Cloud types:** Public (AWS, Azure, GCP), Private (intra-company), Hybrid (private + public), Community (shared by organizations with common concerns).

### Database vs. Data Warehouse vs. Data Lake

| | Database | Data Warehouse | Data Lake |
|---|---|---|---|
| **Purpose** | Day-to-day operations | Business analytics | Raw storage for all data |
| **Data type** | Structured only | Structured + aggregated | All types |
| **Optimized for** | Writing (OLTP) | Reading (OLAP) | Flexibility |
| **Cost** | High per GB | Medium | Very low |
| **Example** | MySQL, PostgreSQL | Snowflake, Redshift | HDFS, S3 |

- **OLTP** — optimized for frequent, small reads/writes (e.g., bank transactions)
- **OLAP** — optimized for complex queries over large historical data (e.g., quarterly revenue reports)

### ETL vs. ELT

**ETL (Extract → Transform → Load):** Raw data extracted, cleaned in a staging area, then loaded into a warehouse as business-ready data.
- Tools: Informatica, Talend, Fivetran, Apache NiFi, dbt
- Use Cases: Financial reporting, data warehouses (Snowflake, Redshift, BigQuery)

**ELT (Extract → Load → Transform):** Raw data loaded into a data lake first, then transformed on-demand for each use case.
- Tools: Apache Spark, Snowflake, BigQuery, Databricks, dbt Cloud
- Use Cases: Data science experiments, machine learning, data lakes (S3, ADLS)

### Roles: Data Engineer vs. Data Analyst

**Data Engineer** — Builds and maintains the infrastructure and pipelines that collect, store, and process data at scale.
Skills: Spark, Hadoop, Kafka, Docker, Kubernetes, AWS/GCP/Azure, SQL, Python, Airflow, dbt.

**Data Analyst** — Translates data into business insights through exploration, visualization, and reporting.
Skills: SQL, Python/R, Tableau/Power BI, Excel, statistics, BigQuery/Snowflake.

> **Key distinction:** Data Engineers ask *"How do we store and process petabytes reliably?"* while Data Analysts ask *"What does this data tell us about our business?"*

---

## 3. Hadoop Architecture

### What is Hadoop?

In the early 2000s, Google developed **GFS** (Google File System) for storage and **GMR** (Google MapReduce) for processing. Inspired by these, the open-source **Hadoop** was created — providing massive data storage and fast parallel processing.

### Core Properties of Hadoop

| Property | Description |
|---|---|
| **Scalability** | Scales horizontally — add more nodes as needed |
| **Fault Tolerance** | Maintains multiple replicas; survives individual machine failures |
| **Distributed Processing** | Processes data where it lives — no unnecessary data movement |
| **Cost Effectiveness** | Runs on commodity (inexpensive) hardware |
| **Open Source** | Free to use and modify (Apache license) |

### Hadoop Ecosystem Components

**Core trio:**
1. **HDFS** (Hadoop Distributed File System) — distributed storage, inspired by GFS
2. **MapReduce** (MR) — distributed processing; divides data into batches, processes in parallel
3. **YARN** (Yet Another Resource Negotiator) — since Hadoop 2.0; decouples resource management from execution

**Additional components:**

| Component | Purpose | Status |
|---|---|---|
| **Hive** | SQL-like interface (HiveQL) over MapReduce/Spark | Active |
| **Pig** | High-level scripting language (Pig Latin) over MR | Legacy |
| **Sqoop** | Import/export between Hadoop and RDBMS | Legacy |
| **Oozie** | XML-based workflow scheduling | Replaced by Airflow |
| **HBase** | Real-time NoSQL columnar store on HDFS | Active |
| **Mahout** | ML libraries on Hadoop | **Deprecated** → replaced by Spark MLlib |
| **Flume** | Real-time log/event streaming to Hadoop | Active |
| **Zookeeper** | Distributed coordination and consistency | Active |
| **Spark** | Fast in-memory distributed processing engine | **De facto standard** |

**Operational categories:**
- **Storage**: HDFS, HBase
- **Processing**: MapReduce, Pig, Hive, Spark
- **Data Ingestion**: Flume, Sqoop
- **Co-ordination**: Zookeeper
- **Workflow Management**: Oozie (→ now Airflow)

> Hadoop is a **loosely coupled framework**: components can be removed without breaking the rest. Modern data engineers prioritize Spark over MapReduce.

### HDFS (Hadoop Distributed File System)

**Key terminology:**

| Term | Definition |
|---|---|
| **File System (FS)** | Layer between OS and hardware. Linux=ext4, Windows=NTFS, macOS=APFS, Hadoop=HDFS |
| **Block** | Smallest storage unit in a FS. **HDFS default = 128 MB** |
| **Distributed FS** | Shares storage across multiple machines in a cluster |
| **Cluster** | Group of nodes (machines) working together |
| **Daemon Process** | A background process running without user intervention |
| **Metadata** | Data about data (file name, location, block list, permissions) |
| **Replication** | Copying data to multiple nodes for fault tolerance |

**HDFS Architecture — Master-Worker:**
- **NameNode** (Master) — the "librarian": stores all metadata, tracks where every block lives, coordinates all operations
- **DataNodes** (Workers) — the "shelves": store the actual data blocks; send heartbeats to NameNode every 10 seconds

> Commands run in a **local file system** only affect the master node. To communicate with the full cluster, use **HDFS commands**.

**How the NameNode stores a file:**
1. NameNode receives write request; divides file into 128 MB blocks
2. Assigns each block to different DataNodes (rack-aware)
3. Creates metadata mapping: which blocks are in which DataNodes
4. DataNodes store blocks and acknowledge

**Blocks in HDFS:**
- Smaller blocks → more partitions → more parallelism + more metadata overhead
- Larger blocks → fewer partitions → less parallelism + less metadata
- Balance these two factors when choosing block size

**Replication Factor** (default: 3 = 1 original + 2 copies):
- Replicas distributed across different DataNodes AND different racks
- Example with File F1 (3 blocks), 3 DataNodes, RF=2:
  - Node 1: F1-P1, F1-P2
  - Node 2: F1-P2, F1-P3
  - Node 3: F1-P3, F1-P1

**Rack Awareness:** Ensures replicas are placed on different racks (not just different DataNodes) for maximum fault tolerance.

---

## 4. Hadoop Components Deep Dive

### DataNode Failure Handling

**Temporary Failure** (network outage, software crash, maintenance):
1. NameNode detects missing heartbeats (3+ consecutive = dead)
2. Re-replicates blocks to other healthy DataNodes; updates metadata
3. When node recovers: excess replicas deleted, metadata updated

**Permanent Failure** (hardware failure, disk corruption, decommissioning):
1. NameNode marks node dead after ~10 minutes of no heartbeats
2. All blocks permanently re-replicated
3. If the failed node recovers later → **NOT trusted**; data assumed corrupted; must rejoin as a fresh empty DataNode

| Aspect | Temporary | Permanent |
|---|---|---|
| **Detection** | 10–30 seconds | ~10 minutes |
| **Node recovery** | Resumes with original data | Must rejoin empty |
| **Trust assumption** | Data still valid | Data assumed corrupted |

### NameNode Failure Handling

**Secondary NameNode (SNN):** NOT a failover mechanism — a **checkpointing helper**.
- NameNode maintains an **fsimage** (complete namespace snapshot) and **edit logs** (all changes since last snapshot)
- On restart, applying all edit logs to fsimage can take a long time → cluster is down during this time
- SNN periodically merges fsimage + edit logs → creates a new clean fsimage → uploads back to NameNode → dramatically reduces restart time

**Standby NameNode:** A **true hot backup** — real failover mechanism:
- Maintains a live synchronized copy of the namespace in memory via **JournalNodes** (shared edit log storage)
- Can seamlessly take over with no data loss and minimum downtime

**Zookeeper & Failover Controller:**
- Monitors both active and standby NameNode
- When active fails → activates standby NameNode
- Ensures only **one NameNode is active at a time** (split-brain prevention)

### HDFS Write Operation

1. Client contacts NameNode → gets prioritized DataNode list (rack-aware)
2. Client writes to first DataNode in the **pipeline**
3. First DataNode forwards data to second simultaneously (pipelined replication)
4. Acknowledgments flow backward: 3rd → 2nd → 1st → client
5. Client writes next block only after receiving full acknowledgment
6. After all blocks: NameNode creates final metadata

Rack placement: Replica 1 = same node as client; Replica 2 = different rack; Replica 3 = same rack as 2, different node.

### HDFS Read Operation

1. Client contacts NameNode → gets block locations ordered by proximity (same node → same rack → other racks)
2. Client reads from nearest DataNode
3. If DataNode fails during read → client switches to next replica automatically
4. Checksums detect corrupt blocks; reported to NameNode for re-replication

> Unlike writes, **reads are asynchronous** — no waiting for all replicas.

### Hadoop High Availability Architecture

Full HA uses: **Active NameNode** + **Standby NameNode** + **JournalNodes** (quorum-based shared edit logs) + **Zookeeper + Failover Controller** (automatic detection and execution).

---

## 5. GCP & Hadoop Practical Use

### Best Practices for GCP

- Create few clusters to conserve credits
- If running out of memory: reload cluster, close Jupyter kernels
- **Always STOP the cluster when done practicing**

### Essential Linux Commands

```bash
# Navigation and inspection
pwd              # print working directory
whoami           # see logged-in user
clear            # clear terminal

# File and directory listing
ls               # list files
ls -l            # detailed listing (permissions, size, date)
ls -ltr -h       # long listing, sorted by time, human-readable sizes
ls -a            # show hidden files
ls -R "name"     # recursive listing

# Permission format:
# drwxr-xr-x  => d=directory, rwx=owner, r-x=group, r-x=others
# r=4(read), w=2(write), x=1(execute)
chmod 444 file   # read-only for all
chmod 777 file   # full access for all
chmod 744 file   # full for owner, read-only for others

cd               # change directory
cd ~             # go to home directory
cd ..            # go up one level

mkdir foldername   # create folder
touch filename     # create empty file
mv file1 file2 dest/  # move files; without location = RENAME
rm file            # delete file
rm -r folder       # delete folder recursively
rm -f file         # force delete (use with caution)
cp file dest/      # copy file
cp -p file dest/   # copy preserving metadata
cp -r folder copy/ # recursive folder copy

head file          # first 10 lines
head -n 1 file     # first line only
tail file          # last 10 lines
cat file           # entire file content

vi file            # open vi editor (i=insert, :wq=save+quit, :q!=quit without saving)

grep "pattern" file       # search file for pattern
grep -c "pattern" file    # count occurrences
```

### HDFS Commands

```bash
# Linux commands act LOCALLY on the master node
# Use HDFS commands to communicate with the full cluster

hadoop fs -put localfile /hdfs/path/      # local -> HDFS
hadoop fs -get /hdfs/path/file localfile  # HDFS -> local
hadoop fs -ls /path/                      # list HDFS directory
hadoop fs -mv /hdfs/source /hdfs/dest     # move within HDFS
hadoop fs -cat /hdfs/path/file            # view HDFS file content
hadoop fs -rm /hdfs/path/file             # delete HDFS file
hadoop fs -mkdir /hdfs/path/              # create HDFS directory
hadoop fs -du -h /path/                   # check HDFS disk usage
```

---

## 6. MapReduce

### Introduction

MapReduce (MR) **was** the primary big data processing framework and the conceptual basis for Spark. Understanding it explains why Spark was built and what it improved.

**Problems with MapReduce:**
- **Slow**: Heavily disk-dependent — writes intermediate results to disk after every stage
- **Complex**: Java-only API, hard to write and maintain
- **Batch only**: No support for streaming or real-time data
- **Rigid structure**: Fixed map-shuffle-reduce pattern; no intermediate steps
- **No interactive mode**: No real-time monitoring

### MapReduce Execution Pipeline

MR sends **code to the data** instead of data to the code.

```
Input Data (HDFS)
       |
  Input Splits (~128MB each — logical divisions)
       |
  Record Reader (assigns byte-offset key to each record)
       |
  Mapper (processes each split in parallel, emits key-value pairs)
       |
  [Optional] Combiner (mini-reducer: aggregates LOCALLY before shuffle)
       |
  Shuffle & Sort (groups all values with the same key, routes to same reducer)
       |
  Reducer (aggregates grouped key-value pairs into final result)
       |
  Output (written back to HDFS)
```

**Key concepts:**
- **One CPU core = One mapper task** (one input split)
- **Number of mappers ≈ Number of HDFS blocks**
- If splits exceed available CPUs: processing happens in batches (longer runtime)
- **Record Reader key** (byte-offset) is usually discarded inside the mapper — it's for tracking only

**The Combiner (optional but important optimization):**
- "Mini-reducer" that runs locally on each mapper's output BEFORE network transfer
- Sends `(key, 3)` instead of `(key,1)(key,1)(key,1)` → dramatically reduces shuffle network traffic
- **Only works for commutative and associative operations**: sum, count, min, max
- **Does NOT work for**: average, median (can't average the averages)
- Hadoop does not guarantee the combiner runs — it's a hint, not a contract

**Output file naming:**
- `part-m-00000` — output with zero reducers
- `part-r-00000` — output with reducers

### Practical Reducer Configurations

**Zero Reducers:** Filter, format conversion, or transformation. Output = mapper output.

**One Reducer (default):** Works but bottlenecks on large data.

**Multiple Reducers:** Enables parallel aggregation. Must ensure the same key always goes to the same reducer (e.g., a–k → reducer 1, l–z → reducer 2).

---

## 7. YARN Components

### Introduction

YARN (Yet Another Resource Negotiator) is Hadoop's resource management layer. It decouples resource management from application execution, allowing Spark, MapReduce, and Hive to share the same cluster.

**Alternatives to YARN:** Apache Mesos, Kubernetes, Docker Swarm.

### YARN Architecture Components

| Component | Role | Analogy |
|---|---|---|
| **Resource Manager** | Master daemon; allocates resources cluster-wide | Head office |
| **Scheduler** (inside RM) | Schedules jobs across the cluster | Head office scheduler |
| **Application Manager** (inside RM) | Manages application lifecycle | Head office manager |
| **Node Manager** | Worker-side daemon; reports resource usage, manages containers | Construction site foreman |
| **Application Master** | One per job; negotiates resources, monitors task execution | Project manager |
| **Container** | Physical resource allocation (RAM + CPU cores) for a task | Worker team |

### YARN Execution Flow

```
1. Job Submission -> Client submits to Application Manager -> gets Job ID
2. Application Master is launched and initialized
3. AM requests containers from Resource Manager
4. RM allocates containers on NodeManagers
5. Tasks run inside containers; AM monitors progress, restarts failed tasks
6. Job Completion -> AM notifies RM -> RM frees containers
```

> Monitor in-progress jobs: **YARN Resource Manager web interface** (typically `http://master-node:8088`).

---

## 8. Apache Spark — Introduction

> **SPARK = THE MOST IMPORTANT TOPIC** in this course. You will almost certainly be asked about Spark in interviews.

### Why Spark Was Created

| Problem in MR | Spark's Solution |
|---|---|
| Disk-dependent (slow) | **In-memory processing** (10–100x faster) |
| Java-only, complex | APIs in Python, Scala, Java, R |
| Batch processing only | Supports **batch AND real-time streaming** |
| Rigid map-reduce structure | **Flexible DAG-based execution** |
| No interactive mode | **Interactive shell + Spark UI** |

### Spark Characteristics

1. **In-memory processing** — data stays in RAM throughout the pipeline; disk I/O only at start (read) and end (write)
2. **Ease of use** — high-level APIs for Python (PySpark), Scala, Java, R, SQL
3. **Unified framework** — handles batch, streaming, ML, and graph processing in one system
4. **Speed** — 10–100x faster than MapReduce for most workloads
5. **Scalability** — runs on a single laptop or a 10,000-node cluster
6. **Fault Tolerance** — tracks data lineage (DAG); can recompute lost partitions

### Spark Ecosystem (Bottom to Top)

```
+-----------------------------------------------------------+
| 5. Programming: Python (PySpark) | Scala | Java | R       |
+-----------------------------------------------------------+
| 4. Libraries: Spark SQL | Streaming | MLlib | GraphX      |
+-----------------------------------------------------------+
| 3. Engine: Spark Core (RDD-based distributed execution)   |
+-----------------------------------------------------------+
| 2. Management: YARN | Mesos | K8s | Spark Standalone      |
+-----------------------------------------------------------+
| 1. Storage: HDFS | S3 | NoSQL | SQL | Local FS            |
+-----------------------------------------------------------+
```

### Transformations vs. Actions

**Transformations** — lazy; build the execution plan (DAG):
- `map()`, `filter()`, `flatMap()`, `reduceByKey()`, `groupByKey()`, `distinct()`, `repartition(n)`, `coalesce(n)`

**Actions** — eager; trigger actual computation:
- `collect()`, `count()`, `show()`, `first()`, `take(n)`, `saveAsTextFile()`, `countByValue()`

**Analogy:** Transformations = writing a recipe (no cooking). Actions = actually cooking (everything executes at once).

**Why lazy evaluation?** Spark optimizes the full pipeline before running anything — skip unnecessary steps, reorder operations, minimize data movement.

> **Always stop your session when done:** `spark.stop()`

### Where to Run Spark

1. **Google Dataproc** (GCP) — managed cluster with JupyterLab, YARN UI, History Server
2. **Databricks** (free Community Edition) — managed Spark platform
3. **Google Colab** — free, single-node cluster
4. **AWS EMR** — managed Spark on AWS
5. **Local mode** — good for learning and small testing

### Spark Common Q&A

**Q: Do I need Hadoop to use Spark?** No. Spark runs standalone. But understanding Hadoop helps understand Spark deeply.

**Q: What storage systems can Spark work with?** HDFS, S3 (AWS), ADLS Gen2 (Azure), GCS (GCP), Cassandra, Kafka, JDBC databases.

**Q: Is Spark free?** Yes — open source. Some providers charge for managed services.

**Q: How is Spark different from Hive?** Hive is a data warehousing tool (SQL interface). Spark is a general-purpose processing engine. They work together via Hive metastore.

---

## 9. Spark Core API — RDD

### What is an RDD?

**RDD** = **Resilient Distributed Dataset** — the fundamental data structure of Spark.

| Letter | Meaning |
|---|---|
| **R** — Resilient | Fault-tolerant through DAG lineage; lost partitions can be recomputed |
| **D** — Distributed | Data is partitioned and spread across multiple nodes |
| **D** — Dataset | A collection of data records |

**Properties:**
1. **Immutable** — once created, cannot be modified; transformations create new RDDs
2. **Lazy Execution** — no computation until an Action is triggered
3. **Partitioned** — data is split across executors for parallel processing
4. **Low-level control** — maximum control, but more verbose

> RDDs require a **SparkContext**: `sc = spark.sparkContext`

### How Spark Reads and Partitions Data

```
Input Source (HDFS/S3/local)
        |
  InputFormat (TextInputFormat, ParquetInputFormat, etc.)
        |
  Splits into Partitions (1 partition ~ 1 HDFS block ~ 128MB)
        |
  Data Locality (Spark tries to run tasks on the node where data lives)
        |
  Loaded into Memory as RDD Partitions
```

**Partition defaults:**
- `defaultParallelism` — total CPU cores. Used for transformations like `reduceByKey`.
- `defaultMinPartitions` — `min(defaultParallelism, 2)` — minimum partitions when reading files.

### Narrow vs. Wide Transformations

**Narrow** — each input partition maps to exactly **one** output partition; no network movement.
- Examples: `map()`, `filter()`, `flatMap()`
- Cost: **cheap and fast**; stays within the same stage

**Wide** — records with the same key may be on different nodes; data must be **shuffled across the network**.
- Examples: `reduceByKey()`, `groupByKey()`, `join()`, `repartition()`
- Cost: **expensive** — network I/O, serialization, new stage boundary

**Optimization rules:**
1. Use **fewer wide transformations**
2. **Filter early** so less data travels across the network
3. Use **`reduceByKey` instead of `groupByKey`** to aggregate locally first

### `reduceByKey()` vs. `groupByKey()`

| | `reduceByKey()` | `groupByKey()` |
|---|---|---|
| **How** | Aggregates locally first, then shuffles results | Shuffles ALL values first, then groups |
| **Network** | Low | High — risk of OOM errors |
| **Output** | `(key, aggregated_value)` | `(key, [list of all values])` |
| **When** | Sum, count, min, max | Average, median — when full list needed |

> **Rule: Always prefer `reduceByKey()`** unless you specifically need the full list.

### Jobs, Stages, and Tasks

```
Job     -> triggered by each Action call
  Stage -> separated by each Wide Transformation (shuffle boundary)
    Task  -> one per partition
```

- Number of Jobs = number of Actions
- Number of Stages = number of wide transformations + 1
- Number of Tasks per Stage = number of partitions in that stage

**Why this matters:** Spark UI lets you diagnose skewed partitions, idle cores, expensive shuffles, and disk spills — without it you're guessing.

### Partition Management

```python
rdd.getNumPartitions()

rdd_more = rdd.repartition(16)  # increase: full shuffle; use when cluster underutilized
rdd_less = rdd.coalesce(4)      # decrease: no full shuffle; use after filter or before save
```

**General guideline: target 2–4 partitions per CPU core.**

### Higher-Level APIs

**DataFrames** — structured RDDs with named columns and schema. The **Catalyst optimizer** automatically optimizes operations.

**Spark SQL** — query DataFrames using `spark.sql("SELECT ...")`. Same Catalyst optimizer underneath.

**Note:** No Datasets in PySpark — Dataset API requires compile-time type safety (Java/Scala only). In Python, DataFrames are the highest-level abstraction.

---

## 10. Spark DataFrame

### What is a Spark DataFrame?

A Spark DataFrame is a **distributed collection of data organized into named columns** — like a relational table, but distributed across a cluster. Built on top of RDDs with added schema information.

**Key differences from Pandas:**
- **Distributed** — data spans multiple nodes
- **Lazy** — operations not executed until an Action is triggered
- **Catalyst-optimized** — Spark automatically optimizes the execution plan

**Advantages over raw RDDs:** Named columns, schema enforcement, Catalyst optimization, multi-format support (CSV, JSON, Parquet, ORC, Avro, Delta).

### Reading Data: Lazy or Eager?

| Option | Behavior |
|---|---|
| `header=True` only | Spark reads the first line eagerly to get column names |
| `inferSchema=True` | Scans the **entire dataset** to detect types — 2 jobs upfront |
| `schema=explicit` | **Fully lazy** — no upfront scanning |

**Best practice: always define schema explicitly in production.**

```python
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType, DateType

# Defining schema explicitly (StructType approach)
schema = StructType([
    StructField("customer_id", IntegerType(), True),  # True = nullable
    StructField("name",        StringType(), True),
    StructField("city",        StringType(), True),
    StructField("state",       StringType(), True),
    StructField("country",     StringType(), True),
    StructField("registration_date", DateType(), True),
    StructField("is_active",   BooleanType(), True),
])

# Using the explicit schema (full laziness preserved)
df = spark.read.csv("data.csv", schema=schema, header=True)

# Alternative: DDL string schema (more concise)
schema_ddl = "customer_id INT, name STRING, city STRING, state STRING, country STRING, registration_date DATE, is_active BOOLEAN"
df = spark.read.csv("data.csv", schema=schema_ddl, header=True)
```

### Problems with `inferSchema=True`

1. **Performance cost** — scans the full dataset twice (once for headers, once for types)
2. **Type mismatches** — `"001"` may be inferred as `StringType()` instead of `IntegerType()`
3. **Date inference fails** — `"2024-01-15"` is almost always inferred as `StringType()`
4. **Inconsistency across runs** — new data may cause different inference
5. **Nullable inference** — columns are marked nullable by default; no non-null enforcement

### Read Modes for Corrupt Data

```python
# PERMISSIVE (default) — parse what's possible, set corrupt fields to null
df = spark.read.option("mode", "PERMISSIVE") \
               .option("columnNameOfCorruptRecord", "_corrupt_record") \
               .csv("data.csv", header=True)
# Best for production: captures bad records without crashing the pipeline

# DROPMALFORMED — silently drop any row that cannot be parsed
df = spark.read.option("mode", "DROPMALFORMED").csv("data.csv", header=True)
# RISKY — silent data loss; bad records disappear with no trace

# FAILFAST — immediately raise an exception on the first malformed record
df = spark.read.option("mode", "FAILFAST").csv("data.csv", header=True)
# Use when: strict data integrity required; any bad record = blocker

# IMPORTANT: "null" as a string is NOT treated as null by Spark
# The string "null" in a StringType column is a valid string, not a missing value
from pyspark.sql.functions import col, when, lit
df = df.withColumn("name", when(col("name") == "null", lit(None)).otherwise(col("name")))
# Or more concisely:
df = df.replace("null", None)  # replaces string "null" with actual null
```

> **Production recommendation:** Use `PERMISSIVE` with `_corrupt_record` column — captures bad data without stopping the pipeline.

> **Type incompatibility:** When a forced schema type is incompatible with actual data, Spark silently produces nulls (nullable column) or null rows (non-nullable). Spark does NOT throw an error. Always validate your data after reading.

### Write Operations

```python
# Basic write (an Action — triggers computation)
df.write \
  .format("csv") \
  .option("header", "true") \
  .option("delimiter", "|") \
  .mode("overwrite") \           # also: "append", "ignore", "error"
  .save("/path/to/output/")

# Write to single file — coalesce is cheaper than repartition (no full shuffle)
df.coalesce(1).write.format("csv").option("header", "true").save("/path/output")

# repartition(1) redistributes all data across the network first — more expensive
df.repartition(1).write.format("csv").option("header", "true").save("/path/output")
# Rule: always use coalesce(1) when reducing to a single output file
```

Reference: [Spark DataFrameWriter API](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.html)

### Common DataFrame Operations

```python
# Schema and overview
df.columns                          # list of column names
df.printSchema()                    # schema with types and nullability
df.describe().show()                # basic stats (count, mean, stddev, min, max)
df.count()                          # total row count

# Selecting and filtering
df.select("col1", "col2").show()    # select specific columns
df.filter(df.age > 20).show()       # filter rows by condition
df.where(df.name == "Alice").show() # alias for filter()

# Sorting
df.orderBy("age").show()            # ascending
df.orderBy(df.age.desc()).show()    # descending

# Removing duplicates
df.distinct().show()

# Adding and modifying columns
df = df.withColumn("new_age", df.age + 5)   # add or replace column
df = df.withColumnRenamed("age", "years")   # rename column
df = df.drop("age")                         # drop column

# Aggregation
df.groupBy("city").count().show()
df.groupBy("city").agg({"salary": "avg"}).show()
df.agg({"salary": "avg"}).show()            # global average (no grouping)

# Handling missing values
df.na.drop("all")                           # drop rows where ALL values are null
df.na.drop("any")                           # drop rows with ANY null
df.na.drop(subset=["name", "city"])         # drop rows with nulls in specified columns
df.na.fill({"salary": 0, "city": "Unknown"})  # fill nulls with specified values
df.fillna(0)                                # fill all nulls with 0
```

Reference: [Spark DataFrame API](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html)

### Handling Data Types

```python
from pyspark.sql.functions import (
    col, upper, lower, length, trim, split, concat, lit, when,
    to_date, to_timestamp, year, month, dayofmonth, hour, minute, second,
    dayofweek, dayofyear, datediff, current_date
)
from pyspark.sql.types import IntegerType, StringType, DoubleType, DateType

# Casting a column type
df = df.withColumn("id", col("id").cast(IntegerType()))

# String operations
df = df.withColumn("name_upper", upper(df.name))
df = df.withColumn("name_lower", lower(df.name))
df = df.withColumn("name_trim",  trim(df.name))
df.filter(df.city.startswith("B")).show()

# Fill NaN/null values
df = df.fillna({"amount": 0})

# Date parsing — read as string first, then parse
# Dates not in YYYY-MM-DD format must be read as strings and converted
df = df.withColumn("parsed_date_iso", to_date(df.date_iso, "yyyy-MM-dd"))
df = df.withColumn("parsed_date_dmy", to_date(df.date_dmy, "dd/MM/yyyy"))
df = df.withColumn("parsed_date_mdy", to_date(df.date_mdy, "MM/dd/yyyy"))

# Timestamp operations
df = df.withColumn("parsed_ts", to_timestamp(df.timestamp, "yyyy-MM-dd HH:mm:ss"))
df = df.withColumn("year",   year(df.parsed_ts)) \
       .withColumn("month",  month(df.parsed_ts)) \
       .withColumn("day",    dayofmonth(df.parsed_ts)) \
       .withColumn("hour",   hour(df.parsed_ts)) \
       .withColumn("minute", minute(df.parsed_ts)) \
       .withColumn("second", second(df.parsed_ts)) \
       .withColumn("dow",    dayofweek(df.parsed_ts))   # Sunday=1, Saturday=7

# Date difference
df = df.withColumn("days_diff", datediff(df.end_date, df.start_date))
```

Reference: [Spark SQL Functions](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html)

### DataFrame Joins

```python
from pyspark.sql.functions import broadcast

# Inner Join — only matching rows from both tables
customers.join(orders, on="customer_id", how="inner")

# Left Join — all rows from left, nulls where no match on right
customers.join(orders, on="customer_id", how="left")

# Right Join — all rows from right, nulls where no match on left
customers.join(orders, on="customer_id", how="right")

# Full Outer Join — all rows from both, nulls where no match
customers.join(orders, on="customer_id", how="outer")

# Left Anti Join — rows from left with NO match on right (unmatched records)
customers.join(orders, on="customer_id", how="left_anti")

# Left Semi Join — rows from left that DO have a match (no right columns returned)
customers.join(orders, on="customer_id", how="left_semi")

# Broadcast Join — efficient when one table is small (avoids full shuffle)
# Spark sends the small table to all partitions rather than shuffling both
customers.join(broadcast(orders), on="customer_id", how="inner")
# Rule of thumb: broadcast tables smaller than ~10MB
```

---

## 11. Spark SQL

### What is Spark SQL?

Spark SQL is a module for structured data processing that provides a SQL interface on top of Spark DataFrames. SQL queries and DataFrame operations are interchangeable — both go through the **Catalyst optimizer** and produce identical results.

**Catalyst Optimizer pipeline:**
1. **Parsing** — tokenizes SQL into an Abstract Syntax Tree (AST)
2. **Analysis** — validates AST against the schema
3. **Logical plan optimization** — reorders joins, pushes filters down, prunes unused columns
4. **Physical plan generation** — selects the most efficient execution strategy

**Important:** Spark SQL is **not a database**. There is no persistent storage engine behind it. It is purely a processing layer — think of it as using SQL as a computation tool, not a storage system.

### Temporary and Persistent Views

```python
# Temporary view (session-scoped — gone when session ends)
df.createOrReplaceTempView("customers")
spark.sql("SELECT city, COUNT(*) FROM customers GROUP BY city").show()
spark.sql("SHOW TABLES").show()

# Global temporary view (application-scoped; accessed via global_temp prefix)
df.createOrReplaceGlobalTempView("customers_global")
spark.sql("SHOW TABLES IN global_temp").show()
spark.sql("SELECT * FROM global_temp.customers_global").show()
spark.sql("DROP TABLE global_temp.customers_global")
```

### Table Types

| Table Type | Scope | Lives Until | Use Case |
|---|---|---|---|
| Temporary View | Current session | Session ends | Intermediate transformations |
| Global Temp View | Current application | Application stops | Sharing across sessions in same app |
| Persistent Table (managed) | Forever | Manually dropped | Production; Spark manages data AND metadata |
| Persistent Table (external) | Forever | Manually dropped | Production; you manage data location |

```python
# Persistent tables require enableHiveSupport()
spark = SparkSession.builder \
    .appName("Table Demo") \
    .enableHiveSupport() \
    .getOrCreate()

# Managed table — Spark owns the data; DROP deletes data permanently
df.write.mode("overwrite").saveAsTable("customers_managed")

# External/unmanaged table — you own the data; DROP only removes metadata
spark.sql("""
    CREATE EXTERNAL TABLE customers_external
    USING csv
    LOCATION '/path/to/existing/data'
""")

# Inspect table metadata (shows Location, Type: MANAGED or EXTERNAL)
spark.sql("DESCRIBE EXTENDED customers_managed").show(truncate=False)
```

> **Production rule:** Always use **external tables** when data must be shared, retained independently, or accessed by multiple tools. This protects against accidental data loss from `DROP TABLE`.

### Data Persistence in Spark SQL

```python
# In-memory caching (persists for the session only)
spark.catalog.cacheTable("customers")
df.cache()
spark.catalog.isCached("customers")      # True or False
spark.catalog.uncacheTable("customers")

# Permanent persistence — write explicitly
df.write.parquet("hdfs:///user/output/result")
df.write.csv("hdfs:///user/output/result")
df.write.saveAsTable("customers_final")  # Hive table (survives session)
```

---

## 12. Caching in Spark

### What is Caching?

Caching stores a DataFrame or RDD in memory so it doesn't need to be recomputed from scratch each time it is used. Since Spark's lazy evaluation **recomputes the full lineage on every action**, caching is critical when the same expensive DataFrame is used multiple times.

```python
df = spark.read.csv("data.csv", schema=schema, header=True)

# Without caching — every action recomputes from scratch
df.count()  # reads and computes
df.show()   # reads and computes again

# With caching — computed once, stored in memory
df.cache()  # mark for caching (does not compute yet)
df.count()  # first action: computes AND stores in cache
df.show()   # reads from cache — much faster

# Always unpersist when done to free memory
df.unpersist()
```

**Important:** Spark's laziness means it caches only the slices of data actually used — if you only need the top 5 rows, only the first relevant partition gets cached.

### Storage Levels

```python
from pyspark import StorageLevel

# DataFrames — default is MEMORY_AND_DISK
df.cache()                                          # shorthand for MEMORY_AND_DISK

# Explicit storage levels via persist()
df.persist(StorageLevel.MEMORY_ONLY)                # fast but evicts if full -> recomputes
df.persist(StorageLevel.MEMORY_AND_DISK)            # same as cache() in PySpark
df.persist(StorageLevel.DISK_ONLY)                  # disk only, no memory
df.persist(StorageLevel.MEMORY_ONLY_SER)            # serialized: less memory, slower access
df.persist(StorageLevel.MEMORY_AND_DISK_SER)        # serialized + disk fallback

# RDDs — default is MEMORY_ONLY (different from DataFrames!)
rdd.cache()                                         # MEMORY_ONLY — recomputes if evicted
rdd.persist(StorageLevel.MEMORY_AND_DISK)           # safer for large RDDs
rdd.unpersist()
```

### Caching Tables

```python
df.createOrReplaceTempView("customers")

# Eager caching — computes and caches immediately
spark.sql("CACHE TABLE customers")

# Lazy caching — defers until first action
spark.sql("CACHE LAZY TABLE customers")

# Via catalog API
spark.catalog.cacheTable("customers")
spark.catalog.isCached("customers")         # True or False
spark.catalog.uncacheTable("customers")
spark.catalog.clearCache()                  # clear all caches
```

### Cache vs. Persist

`cache()` = shorthand for `persist(StorageLevel.MEMORY_AND_DISK)`.  
`persist()` = `cache()` with full control over storage level, serialization, and replication.

### Serialization

Serialization converts data objects into a compact byte stream; deserialization reverses this.

| | Not Serialized (default) | Serialized (`_SER` levels) |
|---|---|---|
| **Memory usage** | High (object overhead) | Low (compact bytes) |
| **Access speed** | Fast | Slower (CPU overhead) |
| **Best when** | Memory is abundant | Memory is scarce |

> Use serialized levels when working with large datasets — the reduced memory footprint can improve overall performance by reducing garbage collection pressure.

### Caching Best Practices

| Question | Answer |
|---|---|
| **When to cache?** | Data used repeatedly; expensive to recompute (large joins, aggregations) |
| **When not to cache?** | Single-use DataFrames; very large datasets that would overwhelm memory |
| **Where is cached data?** | Executor memory (local disk on overflow); visible in Spark UI → Storage tab |
| **Is cache persistent?** | No — lost when session ends |
| **Can caching hurt performance?** | Yes — caching large datasets increases memory pressure and can slow the cluster |

### Q&A About Caching

- **Memory vs. Disk caching?** If insufficient memory, Spark drops least-used partitions or spills to **local disk** (not HDFS) for speed.
- **Why local disk and not HDFS?** Faster access, temporary nature of cache, no need for fault tolerance for ephemeral data.
- **When to unpersist?** When done with the DataFrame — free memory for other operations.
- **Best Practice:** Cache medium-sized datasets that are used repeatedly. Avoid caching if memory pressure is a concern.

---

## 13. Spark Architecture

### Run Modes

| Mode | Description | Use Case |
|---|---|---|
| **Standalone** | Spark's built-in cluster manager | Small-scale or testing |
| **YARN** | Integrates with Hadoop YARN | Large-scale production |
| **Mesos** | Apache Mesos cluster manager | Multi-framework clusters |
| **Kubernetes (K8s)** | Container-based cluster manager | Modern cloud deployments |

### Spark Architecture Components

```
+--------------------------------------------------+
|  Driver Program                                  |
|  +----------------+                              |
|  | SparkContext   |  <- entry point              |
|  +----------------+                              |
|  - Creates DAG from user code                   |
|  - Schedules and manages execution              |
|  - Collects results from executors              |
+------------------+-------------------------------+
                   |
                   v
+--------------------------------------------------+
|  Cluster Manager (YARN / Mesos / K8s)            |
|  - Manages resource allocation                   |
|  - Launches executors on worker nodes            |
+------------------+-------------------------------+
                   |
                   v
+--------------------------------------------------+
|  Worker Nodes                                    |
|  +------------------------------------------+   |
|  |  Executor (container)                    |   |
|  |  - Executes tasks assigned by driver     |   |
|  |  - Stores intermediate results in RAM    |   |
|  |  - 1 core = 1 task                       |   |
|  +------------------------------------------+   |
+--------------------------------------------------+
```

**Key terminology:**
- **Driver Program**: Entry point; analogous to `main()`. Creates SparkContext, builds DAG, schedules tasks.
- **SparkContext**: Entry point for Spark functionality; connects driver to cluster.
- **Cluster Manager**: Allocates resources (YARN, Mesos, K8s, Standalone).
- **Executors**: Worker processes on nodes. Execute tasks, store cached data in memory.
- **Tasks**: Smallest unit of work; one task = one partition. One core runs one task at a time.

**Typical job execution flow:**
1. Driver submits application to cluster manager
2. Driver constructs DAG from transformations
3. DAG Scheduler breaks DAG into stages
4. Task Scheduler assigns tasks to executors
5. Executors process tasks in parallel
6. Intermediate results stored in memory
7. Final results returned to driver
8. History Server records job execution details

### Spark Standalone Architecture

Spark controls its own resources without YARN. Verified by checking the SparkSession's **Master** field — it shows `local[*]` (star = all cores).

- **Master**: Central coordination. Registers applications, allocates resources, schedules tasks, monitors worker health.
- **Workers**: Receive tasks from master; launch executors; report progress.
- **Executors**: Run the actual Spark code; one core = one task.
- **Spark History Server**: Records completed and running jobs.

### Spark on YARN Architecture

The most common production setup. YARN provides robust resource management for multi-tenant clusters.

- **Application Master (AM)**: One AM per Spark application, launched in a YARN container. Negotiates resources with YARN's ResourceManager.
- **Executors**: Launched as YARN containers on NodeManagers.

**Standalone vs YARN:**
- Standalone: Spark has full control — simpler but less suitable for shared clusters.
- YARN: robust resource management, ideal for production-grade multi-application environments.

### Deployment Modes

```
Mode:       Driver Location     Executor Location   Use Case
------------------------------------------------------------------
Local       Your machine        Your machine        Learning, testing
Client      Your machine        Cluster             Interactive dev (JupyterLab)
Cluster     Inside cluster      Cluster             Production
```

**Recommended workflow:** `Local -> Client -> Cluster`
- Test logic in local mode
- Debug output line-by-line in client mode (JupyterLab)
- Deploy production jobs in cluster mode for availability and fault tolerance

---

## 14. Apache Hive

### Introduction

Apache Hive is a **data warehousing system built on top of Hadoop**. It allows users to query large datasets stored in HDFS using a SQL-like language called **HiveQL (HQL)**, without needing Java MapReduce code.

Originally developed at Facebook in 2010. Hive translates HQL queries into MapReduce, Tez, or Spark jobs that run on the Hadoop cluster.

**Hive's superpower:** Enables SQL-fluent analysts and engineers to work with petabytes of data without Java knowledge.

### Hive Q&A

**Q: Is Hive a database like MySQL or PostgreSQL?**
No. Hive is a **data warehouse tool and metastore**. It stores metadata (schema, table definitions, partition locations) and provides a query interface over data stored in HDFS. It does not store data itself.

**Q: Is Hive a replacement for Hadoop?**
No — it is built *on top of* Hadoop and uses its components. Hive is a translator: it handles SQL-like code instead of requiring Java MapReduce.

**Q: Do Hive queries run instantly like MySQL?**
No. HQL is converted to distributed jobs (MR/Tez/Spark) and run across the cluster. Latency is much higher than a traditional RDBMS.

**Q: Can Hive do row-level transactions like MySQL?**
Technically yes, but it is not designed for this. Hive excels at bulk processing of large volumes, not small incremental row changes.

**Q: Does Hive only work with HDFS?**
No. Hive can also work with S3, Azure Data Lake (ADLS), and Google Cloud Storage (GCS).

**Q: How does Hive store tables?**
Hive is a metastore. It stores the schema/metadata of tables, while actual data lives in HDFS (or other distributed storage).

### Hive Commands

```bash
# Check Hive version
hive --version

# Access Hive configuration (from SSH terminal)
vi /etc/hive/conf/hive-site.xml   # username, password, and properties

# Start Beeline (recommended Hive client)
beeline

# Connect Beeline to HiveServer2 (run inside Beeline)
!connect jdbc:hive2://localhost:10000/default

# Exit Hive or Beeline
!quit
# or !exit or Ctrl+C

# IMPORTANT: Semicolon (;) is required at the end of each Hive command
# Without it, Hive considers the next line a continuation
```

**Inside Hive/Beeline (HQL commands):**

```sql
-- Set execution engine
SET hive.execution.engine = spark;  -- options: spark, tez (default), mr
SET hive.execution.engine;          -- check current engine

-- Database management
SHOW DATABASES;
CREATE DATABASE databasename;
USE database_name;

-- Create a table
CREATE TABLE customers (
    customer_id INT,
    name        STRING,
    city        STRING,
    state       STRING,
    country     STRING,
    registration_date STRING,
    is_active   BOOLEAN
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- Insert data
INSERT INTO customers VALUES (1, 'Alice', 'New York', 'NY', 'USA', '2020-01-22', TRUE);

-- Query data
SELECT * FROM customers LIMIT 10;
SELECT state, COUNT(*) AS num_customers FROM customers GROUP BY state;
```

**Accessing data and metadata:**
```bash
# Check data stored in HDFS (run in Hadoop terminal)
hadoop fs -ls /user/hive/warehouse/

# Check metastore directory (run in Beeline)
SET hive.metastore.warehouse.dir;
```

### Hive Architecture

**Layers (client to storage):**

**1. Client Layer** — CLI (legacy), Beeline (recommended), JDBC/ODBC (BI tools), Web UI

**2. HiveServer2** — gateway; accepts connections, manages sessions, handles authentication

**3. Driver (core processing brain):**
- **Compiler**: Parses HQL into an Abstract Syntax Tree (AST)
- **Optimizer**: Rewrites the plan (predicate pushdown, partition pruning, join reordering)
- **Executor**: Submits the final plan to the execution engine

**4. Execution Engine** — historically MapReduce; now **Tez** or **Spark** (much faster)

**5. Metastore** — the catalog:
- Stores all table definitions, schemas, partitions, and file locations
- Backed by MySQL or PostgreSQL (production), or Derby (development)
- This is what Spark connects to when you call `.enableHiveSupport()`

**6. HDFS / Cloud Storage** — where actual data lives (ORC, Parquet, Avro formats)

### Hive Query Flow

```
1. User submits HQL query (via CLI, Beeline, or JDBC)
2. HiveServer2 -> authenticates and forwards to Driver
3. Parser -> tokenizes query -> builds Abstract Syntax Tree (AST)
4. Semantic Analyzer -> validates AST against metastore
   (do these tables and columns exist? are types compatible?)
5. Logical Plan -> AST becomes a tree of logical operators
6. Optimizer -> rewrites for efficiency
7. Physical Plan + Execution -> converted to MR/Tez/Spark jobs that read from HDFS
8. Results -> collected and returned to client via HiveServer2
```

> **Metastore is consulted at step 4** — before any data is touched. **HDFS is only accessed at step 7** when actual computation begins.

### DerbyDB — The Default Development Metastore

Derby (Apache Derby) is a lightweight Java relational database that backs the Hive metastore by default in development environments.

```
Hive Metastore -> needs a relational DB -> Derby (development default)
                                        -> MySQL (production)
                                        -> PostgreSQL (production)
```

**Derby limitations (why it's dev-only):**
- Only **one connection at a time** — unusable in multi-user or cluster environments
- Metadata stored **locally** in a `metastore_db` folder — not shared across machines
- **Not persistent** across environments — deleting the folder destroys all table definitions

**Switching to MySQL in production:**
```xml
<!-- hive-site.xml -->
<property>
  <name>javax.jdo.option.ConnectionURL</name>
  <value>jdbc:mysql://localhost/hive_metastore</value>
</property>
<property>
  <name>javax.jdo.option.ConnectionDriverName</name>
  <value>com.mysql.jdbc.Driver</value>
</property>
<property>
  <name>javax.jdo.option.ConnectionUserName</name>
  <value>hive</value>
</property>
```

> In **Spark with `enableHiveSupport()`**, if no external metastore is configured, Spark also defaults to Derby — which is why persistent tables work locally without setting up MySQL. Fine for learning; never for production.

---

## 15. Apache Kafka

### Introduction

Apache Kafka is a **distributed event streaming platform** used to handle high-throughput, real-time data streams. It enables applications to publish, store, process, and subscribe to streams of records efficiently.

**Before Kafka**, traditional systems (databases, message queues, batch ETL) were high-latency, not scalable, and not real-time — like a newspaper (updated once daily) vs. a live news feed (continuous updates).

**Kafka solved these with:**
1. **Message retention** — persistent storage of messages (configurable duration)
2. **Scalability** — horizontal scaling via partitions
3. **High throughput** — handles millions of events per second
4. **Fault tolerance** — automatic replication across brokers
5. **Real-time processing** — continuous rather than batch
6. **Low latency** — millisecond-level message delivery

**Kafka use cases:** E-commerce (order events), Finance (transactions), IoT (sensor data), Social Media (activity feeds), Log management, Real-time analytics.

### Kafka Architecture

**Core components:**

| Component | Definition | Analogy |
|---|---|---|
| **Broker** | A server in the Kafka cluster. Stores and manages messages; handles replication. | Post office in a city |
| **Topic** | A named category/channel where messages are stored | Mailbox category |
| **Partition** | Sub-division of a topic for parallelism and scalability | Individual mail slots |
| **Offset** | A unique sequential ID for each message within a partition | Tracking number |
| **Producer** | Sends messages to topics | Sender |
| **Consumer** | Reads messages from topics | Recipient |
| **Consumer Group** | Multiple consumers sharing a topic; each reads from separate partitions | Delivery team |

**Partitioning strategies:**
1. **Round-Robin** — messages distributed evenly across all partitions
2. **Key-based** — messages with the same key always go to the same partition (via consistent hashing). Guarantees ordering per key.

**Consumer group rules:**
- Each partition is consumed by **only one consumer** within a group
- If fewer consumers than partitions: some consumers read from multiple partitions
- If more consumers than partitions: excess consumers sit idle
- Different consumer groups can each read the **entire topic** independently at their own pace

### Offsets

An offset is a unique ID assigned to each message within a partition. Consumers track their position using offsets. Each consumer group maintains its own offset per partition independently — two groups can read the same topic at completely different paces.

### Replication and Fault Tolerance

Kafka replicates each partition across multiple brokers for fault tolerance.

- **Replication factor**: controls how many copies of each partition exist (e.g., RF=3 → 1 leader + 2 followers)
- **Leader**: the broker handling all reads and writes for a partition
- **Follower/Replica**: silently replicates the leader's data in the background
- **ISR (In-Sync Replicas)**: followers that are fully caught up; only ISR replicas can become leaders
- **Automatic failover**: if the leader goes down, Kafka instantly promotes the highest ISR replica — no manual intervention

**Trade-off:** Higher replication factor = more fault tolerance + more storage and network overhead. Standard production setting: **RF=3** (tolerates loss of up to 2 brokers).

### How to Use Kafka

**Deployment options:**
1. **Local setup** — not recommended (requires Zookeeper and many dependencies)
2. **Confluent Cloud** — created by Kafka's founders; easy setup, free tier available
3. **AWS MSK** — managed Kafka on AWS
4. **GCP PubSub** — Google's managed messaging service
5. **Azure Event Hubs** — Microsoft's managed Kafka-compatible service
6. **Confluent Cloud on Azure** — third-party managed Kafka on Azure

### Kafka on Confluent — Practical Setup

**Data hierarchy:** Environment → Cluster → Topic → Messages

**Creating a Topic:**
- Choose number of partitions (default: 6)
- Set retention policy (default: delete after 1 week)
- Set max message size (default: 2 MB)
- Define a data contract (schema) for message validation

**Kafka callbacks, poll, and flush:**
- **Callback**: function called when a message delivery is acknowledged (success or failure)
- **poll()**: triggers background network activity; must be called regularly to process delivery reports
- **flush()**: waits until all outstanding messages are delivered to Kafka brokers

### Kafka CLI Commands

```bash
# Login and environment management
confluent login
confluent environment list
confluent environment use env-rwr0r7               # switch by ID
confluent environment create env_name

# Cluster management
confluent kafka cluster list
confluent kafka cluster create my-cluster \
    --region us-east-1 --cloud aws
confluent kafka cluster use cluster_ID

# Topic management
confluent kafka topic list
confluent kafka topic create topic_name --partitions 2

# API key management
confluent kafka api-key create --resource topic_ID  # SAVE the key and secret
confluent api-key use API-Key

# Producing messages
confluent kafka topic produce customer_transactions --from-beginning
# After running, type messages:
{"key": 123, "value": "customer_event"}

# Consuming messages
confluent kafka topic consume customer_transactions --from-beginning
```

---

## 16. Docker & Containers

### Introduction to Containers

A **container** is a way to package an application together with all its dependencies, configurations, libraries, and environment variables into a single portable unit. The application runs identically regardless of where the container is deployed — solving the classic "it works on my machine" problem.

**Why containers matter in data engineering:**
- Homogenize workflows across teams — everyone runs the exact same environment
- Eliminate version conflicts — Python 3.9, pandas 1.5.3, exactly, on every machine
- Simplify deployment — package a pipeline once, run it anywhere
- Enable reproducibility — data science experiments can be replicated exactly

### Docker Images vs. Containers

**Image** = a static, read-only blueprint (the recipe). Contains: base OS, runtime, libraries, application code. Does nothing on its own.

**Container** = the live, running instance created from an image (the actual meal). Has its own filesystem, processes, and network.

```
Docker Image   ->  recipe       (static, stored on disk, shareable via Docker Hub)
Docker Container -> actual meal  (running instance, has state, consumes resources)
```

**Image layers:** Docker images are built in layers. Each `Dockerfile` instruction creates a new layer. Layers are cached and reused — if two images share the same base layer, it is stored only once.

```
Layer 4: Your application code
Layer 3: Python libraries (pandas, pyspark, etc.)
Layer 2: Python runtime
Layer 1: Base Linux OS  <- shared across many images; only stored once
```

### Docker vs. Virtual Machines

**OS Layer Structure:**
```
Layer 2: Applications  (browsers, Python, Spark, etc.)
Layer 1: Kernel        (communicates between software and hardware)
Layer 0: Hardware      (CPU, RAM, disk)
```

```
Docker  ->  virtualizes the APPLICATION LAYER only
            uses the HOST machine's kernel directly
            result: lightweight, fast startup (seconds)

VM      ->  virtualizes BOTH the application layer AND the kernel
            has its own complete OS kernel inside
            result: heavy, slow startup (minutes)
```

| Aspect | Docker | Virtual Machine |
|---|---|---|
| **Startup time** | Seconds | Minutes |
| **Size** | MBs | GBs |
| **Resource usage** | Minimal (shares kernel) | Heavy (full OS inside) |
| **Isolation** | Process-level | Complete OS-level |
| **Portability** | High | Medium |
| **Compatibility** | Depends on host kernel | Fully self-contained |

**When to use Docker:** Fast, lightweight, portable environments; deploying microservices or data pipelines; resource efficiency matters.

**When to use VMs:** Full OS isolation for security; running a completely different OS; hard compatibility requirements across host machines.

### Writing a Dockerfile

```dockerfile
FROM python:3.9              # Base image to start from (check Docker Hub for versions)
WORKDIR /app                 # Set working directory inside the container
COPY . /app                  # Copy all files from your local folder into the container
RUN pip install -r requirements.txt  # Install dependencies (creates a new layer)
EXPOSE 8080                  # Tell Docker which port the app listens on
CMD ["python", "app.py"]     # Default command to run when the container starts
```

### Essential Docker Commands

```bash
# Building images
docker build -t my-app:1.0 .           # build from Dockerfile in current directory
docker images                          # list all local images

# Running containers
docker run -p 5000:5000 my-app:1.0     # run container, map host:container ports
docker run -d -p 80:80 my-app:1.0      # run in detached mode (background)
# -p host_port:container_port
#    container_port: fixed by the app (defined by EXPOSE in Dockerfile)
#    host_port: port on your local machine to access the container

# Managing containers
docker ps                              # list running containers
docker ps -a                           # list all containers (including stopped)
docker stop container_ID               # stop a running container
docker rm container_ID                 # remove a stopped container
docker logs container_ID               # view container logs

# Pulling from Docker Hub
docker pull docker/getting-started
docker run -d -p 80:80 docker/getting-started
```

### Pushing to Docker Hub

```bash
docker login                                  # authenticate
docker tag my-app:1.0 yourusername/my-app:1.0 # tag in Hub format
docker push yourusername/my-app:1.0           # push to Docker Hub
docker pull yourusername/my-app:1.0           # anyone can now pull it
docker run -d -p 8080:80 yourusername/my-app:1.0
```

> **Security note:** Docker Hub repositories are **public by default**. Create a **private repository** before pushing sensitive code.

### Docker Compose

Docker Compose defines and runs **multiple containers simultaneously** using a single `docker-compose.yml` file. Critical in data engineering where you often need Spark, a database, Kafka, and an API all running together.

```yaml
# docker-compose.yml
version: '3'

services:
  app:                                  # Container 1 — your Python app
    build: .
    ports:
      - "8080:8080"
    depends_on:
      - database                        # wait for database to start first
    environment:
      - DB_HOST=database                # containers communicate by service name
      - DB_PORT=5432

  database:                             # Container 2 — PostgreSQL
    image: postgres:13
    ports:
      - "5432:5432"
    environment:
      - POSTGRES_USER=admin
      - POSTGRES_PASSWORD=password
      - POSTGRES_DB=mydb
    volumes:
      - db_data:/var/lib/postgresql/data  # persist data outside container

  kafka:                                # Container 3 — Kafka broker
    image: confluentinc/cp-kafka:7.0.0
    ports:
      - "9092:9092"
    environment:
      - KAFKA_BROKER_ID=1
      - KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181

  zookeeper:                            # Container 4 — required by Kafka
    image: confluentinc/cp-zookeeper:7.0.0
    ports:
      - "2181:2181"

volumes:
  db_data:                              # named volume — data persists across restarts
```

```bash
# Docker Compose commands
docker-compose up                       # start all containers
docker-compose up -d                    # start in background (detached)
docker-compose up --build               # rebuild images before starting
docker-compose down                     # stop and remove all containers
docker-compose down -v                  # also remove volumes (deletes persisted data)
docker-compose ps                       # list running services
docker-compose logs                     # view logs from all services
docker-compose logs app                 # view logs from a specific service
docker-compose exec app bash            # open terminal inside a running container
docker-compose restart app              # restart a specific service
```

**Key Docker Compose concepts:**

- **Services** — each service is one container. Services communicate by service name as hostname (the `app` container reaches `database:5432`).
- **Volumes** — persist data outside container lifecycle:
  ```yaml
  volumes:
    - ./local/path:/container/path      # bind mount — maps a local folder
    - db_data:/var/lib/postgresql/data  # named volume — managed by Docker
  ```
- **Networks** — all services in a `docker-compose.yml` are on the same default network automatically.
- **depends_on** — controls startup order. Note: only waits for the container to start, not for the service inside to be ready. Use health checks for service readiness.

---

## 17. Apache Airflow

### Introduction

Apache Airflow is an **open-source workflow orchestration platform** used to programmatically author, schedule, and monitor pipelines. It lets you define complex workflows as code (Python) and manage their execution reliably.

**The key insight:** Airflow is not a data processor itself. It is the **conductor** that tells other tools (Spark, Hive, Python scripts, databases) when to run, in what order, with what dependencies — and alerts you if something goes wrong.

### The Life Cycle of a Data Project

```
1. Business Understanding   -> Define KPIs, success metrics, problem scope
         |
2. Data Acquisition         -> Locate sources, assess quality, ingest data
         |                    [AIRFLOW starts here: schedules ingestion]
3. Data Exploration (EDA)   -> Understand distributions, missing values, patterns
         |
4. Data Preparation         -> Clean, transform, encode, scale, feature engineer
         |                    [AIRFLOW orchestrates transformation steps]
5. Modeling                 -> Train, tune, validate ML/statistical models
         |                    [AIRFLOW can trigger retraining]
6. Evaluation               -> Assess vs. business goals, not just technical metrics
         |
7. Deployment               -> Package, containerize (Docker), integrate into systems
         |                    [AIRFLOW manages the full pipeline]
8. Monitoring               -> Watch for data drift, concept drift, performance drop
         |                    [AIRFLOW schedules monitoring jobs]
         +---> (cycle restarts)
```

> **The most time-consuming phase in practice:** Data Preparation — typically 60–80% of total project time.

**Setting up a data project:**
1. **Requirements gathering** — business analysts + domain experts + data scientists define objectives
2. **Data availability assessment** — identify data sources, formats, completeness
3. **Data engineering phase** — build and maintain the pipeline (ingestion, transformation, storage, quality)
4. **Data science phase** — exploration, modeling, analysis on clean, reliable data
5. **Deployment and monitoring** — model to production, continuously monitored

### Airflow Core Concepts

**DAG (Directed Acyclic Graph)** — the central concept. A DAG defines your pipeline as a graph where:
- Each **node** is a task (a unit of work)
- Each **edge** defines the dependency between tasks
- "Acyclic" means no circular dependencies

**Airflow Components:**

| Component | Role |
|---|---|
| **DAG files** | Python files defining your pipeline — tasks, dependencies, schedule |
| **Webserver** | Browser-based UI for monitoring, triggering, and managing DAGs |
| **Scheduler** | Continuously parses DAG files, checks schedules, submits tasks to executors |
| **Executor** | Decides HOW tasks run: SequentialExecutor (dev), LocalExecutor (single machine), CeleryExecutor (distributed), KubernetesExecutor (pods) |
| **Workers** | Processes that actually execute task code |
| **Metadata DB** | PostgreSQL/MySQL (prod) or SQLite (local) — stores DAG state, run history, logs |

### Basic Airflow DAG

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def say_hello():
    print("Hello from Airflow!")

def train_model():
    print("Training the model...")

def evaluate_model():
    print("Evaluating the model...")

with DAG(
    dag_id='my_first_dag',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',    # also: '@weekly', '@hourly', cron syntax
    catchup=False                  # don't backfill for past missed runs
) as dag:

    hello    = PythonOperator(task_id='say_hello',    python_callable=say_hello)
    train    = PythonOperator(task_id='train_model',  python_callable=train_model)
    evaluate = PythonOperator(task_id='eval_model',   python_callable=evaluate_model)

    # Set execution order — >> means "then run"
    hello >> train >> evaluate
```

### XCom — Passing Data Between Tasks

```python
# Traditional API — explicit xcom_push and xcom_pull
def start_number(**context):
    context["ti"].xcom_push(key='current_value', value=10)  # push to XCom

def adding(**context):
    current_value = context["ti"].xcom_pull(
        key='current_value', task_ids='start_task')         # pull from XCom
    new_value = current_value + 5
    context["ti"].xcom_push(key='current_value', value=new_value)

with DAG('math_dag', start_date=datetime(2024, 1, 1),
         schedule_interval='@daily', catchup=False) as dag:
    task_1 = PythonOperator(task_id='start_task', python_callable=start_number)
    task_2 = PythonOperator(task_id='add_task',   python_callable=adding)
    task_1 >> task_2
```

### TaskFlow API (Modern Airflow 2.0+ — Recommended)

TaskFlow uses Python decorators. Return values automatically flow as XCom between tasks. Dependencies are inferred from data flow — no explicit `>>` needed.

```python
from airflow.decorators import dag, task
from datetime import datetime

@dag(dag_id='math_dag_taskflow', start_date=datetime(2024, 1, 1),
     schedule_interval='@daily', catchup=False)
def math_pipeline():

    @task
    def start_number():
        print("Starting with 10")
        return 10                      # return auto-pushed to XCom

    @task
    def adding(current_value):         # input auto-pulled from XCom
        new_value = current_value + 5
        print(f"After adding 5: {new_value}")
        return new_value

    @task
    def multiplying(current_value):
        new_value = current_value * 2
        print(f"After multiplying by 2: {new_value}")
        return new_value

    @task
    def subtracting(current_value):
        new_value = current_value - 3
        print(f"After subtracting 3: {new_value}")
        return new_value

    @task
    def squaring(current_value):
        new_value = current_value ** 2
        print(f"After squaring: {new_value}")
        return new_value

    # Dependencies inferred automatically from function call chain
    value = start_number()
    value = adding(value)
    value = multiplying(value)
    value = subtracting(value)
    value = squaring(value)

math_pipeline()
```

**TaskFlow vs. Traditional API:**

| Aspect | Traditional | TaskFlow (Recommended) |
|---|---|---|
| Task definition | `PythonOperator(...)` | `@task` decorator |
| Data passing | Explicit `xcom_push` / `xcom_pull` | Return values flow automatically |
| Dependencies | Explicit `>>` operators | Inferred from function call chain |
| Readability | Verbose | Clean and Pythonic |

### ETL with Airflow — API to PostgreSQL

```python
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import requests

@dag(dag_id='etl_api_to_postgres', start_date=datetime(2024, 1, 1),
     schedule_interval='@daily', catchup=False)
def etl_pipeline():

    @task
    def extract():
        """Step 1: Pull data from a REST API."""
        response = requests.get("https://api.example.com/data")
        response.raise_for_status()   # raise error if API call fails
        data = response.json()
        print(f"Extracted {len(data)} records")
        return data

    @task
    def transform(raw_data):
        """Step 2: Clean and structure the data."""
        return [{
            "id":         record["id"],
            "name":       record["name"].strip().lower(),  # clean whitespace + case
            "value":      float(record["value"]),          # ensure numeric type
            "created_at": record["timestamp"][:10]         # extract date from timestamp
        } for record in raw_data]

    @task
    def load(transformed_data):
        """Step 3: Load into PostgreSQL (connection defined in Airflow UI)."""
        hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS api_data (
                id VARCHAR PRIMARY KEY, name VARCHAR, value FLOAT, created_at DATE)
        """)
        for record in transformed_data:
            cursor.execute("""
                INSERT INTO api_data (id, name, value, created_at)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE
                SET name=EXCLUDED.name, value=EXCLUDED.value, created_at=EXCLUDED.created_at
            """, (record["id"], record["name"], record["value"], record["created_at"]))
        conn.commit()
        cursor.close()

    raw    = extract()
    clean  = transform(raw)
    load(clean)

etl_pipeline()
```

**Setting up the PostgreSQL connection in Airflow UI:**
1. Go to **Admin → Connections** → Click **Add a new record**
2. Fill in: `Conn Id: postgres_default`, `Conn Type: Postgres`, `Host: localhost`, `Schema: dbname`, `Login: user`, `Password: pass`, `Port: 5432`

### Why Airflow Over a Plain Python Script?

| Feature | Plain Script | Airflow |
|---|---|---|
| **Scheduling** | Manual/cron | Automatic, configurable |
| **Retries** | Manual | Automatic with configurable retry logic |
| **Observability** | None | Full UI with task-level logs |
| **Backfilling** | Manual | Automatic for missed runs |
| **Failure handling** | Script crashes | Task-level failure isolation |
| **Notifications** | None | Email/Slack alerts on failure |

### Using Airflow with Astronomer

[Astronomer](https://www.astronomer.io/) is **Airflow as a Service** — handles all infrastructure so you focus only on writing DAGs.

```bash
# Install Astro CLI
brew install astro           # macOS
winget install -e --id Astronomer.Astro  # Windows

astro version                # verify installation

# Initialize project
astro dev init
# Creates: dags/, plugins/, include/, requirements.txt, Dockerfile

# Run locally (uses Docker)
astro dev start
# Airflow UI: http://localhost:8080  (default: admin/admin)

astro dev stop               # stop local environment

# Deploy to Astronomer Cloud
astro login
astro deploy
```

**To access Airflow UI in Docker Desktop:** Click the arrow next to the Airflow container in Docker Desktop.

### ETL on AWS — Astronomer + S3 + RDS

```python
@dag(dag_id='etl_api_to_aws', start_date=datetime(2024, 1, 1),
     schedule_interval='@daily', catchup=False, tags=['etl', 'aws', 'production'])
def etl_pipeline():

    @task
    def extract():
        response = requests.get("https://api.example.com/data")
        response.raise_for_status()
        return response.json()

    @task
    def store_raw_to_s3(raw_data):
        """Save raw data to S3 Bronze layer for auditability."""
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        hook = S3Hook(aws_conn_id='aws_default')
        hook.load_string(
            string_data=json.dumps(raw_data),
            key=f"raw/data_{datetime.now().strftime('%Y%m%d')}.json",
            bucket_name="your-etl-bucket",
            replace=True
        )

    @task
    def transform(raw_data):
        return [{
            "id":         r["id"],
            "name":       r["name"].strip().lower(),
            "value":      float(r["value"]),
            "created_at": r["timestamp"][:10]
        } for r in raw_data]

    @task
    def store_transformed_to_s3(transformed_data):
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        hook = S3Hook(aws_conn_id='aws_default')
        hook.load_string(
            string_data=json.dumps(transformed_data),
            key=f"transformed/data_{datetime.now().strftime('%Y%m%d')}.json",
            bucket_name="your-etl-bucket", replace=True
        )

    @task
    def load_to_rds(transformed_data):
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        hook = PostgresHook(postgres_conn_id='postgres_rds')
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute("""CREATE TABLE IF NOT EXISTS api_data (
            id VARCHAR PRIMARY KEY, name VARCHAR, value FLOAT, created_at DATE)""")
        for r in transformed_data:
            cursor.execute("""INSERT INTO api_data VALUES (%s,%s,%s,%s)
                ON CONFLICT (id) DO UPDATE SET name=EXCLUDED.name, value=EXCLUDED.value""",
                (r["id"], r["name"], r["value"], r["created_at"]))
        conn.commit()
        cursor.close()

    # Wire tasks — raw is stored AND transformed in parallel after extraction
    raw_data = extract()
    store_raw_to_s3(raw_data)
    transformed = transform(raw_data)
    store_transformed_to_s3(transformed)
    load_to_rds(transformed)

etl_pipeline()
```

**Why store on S3 as an intermediate layer?**
- Raw data preserved — if transform logic has bugs, reprocess without hitting the API again
- Audit trail — daily snapshot of what came from the API
- Decoupling — transform and load steps can be rerun independently
- Cost — S3 storage is cheap; RDS is not. Only clean, final data goes into RDS

> This pattern (raw → S3 → transformed → S3 → database) is essentially the **Bronze/Silver/Gold Medallion Architecture** implemented as an Airflow DAG on AWS.


---

## 18. Databricks

### Introduction

Databricks is a **cloud-based, managed data analytics platform built on Apache Spark**. It provides an interactive workspace for data engineers, data scientists, and analysts to process large-scale data without managing infrastructure.

### Why Databricks?

| Feature | Benefit |
|---|---|
| **No infrastructure management** | Cluster provisioning, scaling, and recovery are automatic |
| **Optimized Spark Runtime** | Proprietary runtime with performance improvements over open-source Spark |
| **Unified platform** | Data ingestion, processing, notebooks, ML tracking — all in one workspace |
| **Delta Lake** | ACID transactions, data versioning, and time travel on top of Spark |
| **Collaborative notebooks** | Multiple team members, multiple languages (Python/SQL/Scala/R) |
| **Native MLflow** | Full ML lifecycle from tracking to deployment, built-in |
| **Cloud native** | Runs on AWS, GCP, and Azure; integrates with their native storage and security |

### Databricks Architecture — Two Layers

```
Databricks Account (Control Plane)          Your Cloud Account (Data Plane)
------------------------------------        ------------------------------------
Web UI                          -------->  Clusters (compute)
Cluster manager                 -------->  S3 / GCS / ADLS (storage)
Job scheduler                   -------->  Delta Lake (table layer)
Notebook storage                            DBFS (file system abstraction)
Unity Catalog metadata
```

**Control Plane** (managed by Databricks in their cloud account):
- Web UI, cluster manager, job scheduler, notebook storage, Unity Catalog metadata
- Never touches your actual data

**Data Plane** (lives entirely in your cloud account):
- Clusters (virtual machines executing Spark code)
- Your cloud storage (S3/GCS/ADLS) — your data never leaves your account
- Delta Lake, DBFS

**Why this separation matters:**
1. **Security** — your data stays in your cloud account; Databricks manages your environment without ever seeing the data
2. **Flexibility** — existing cloud security policies, networking rules, and compliance controls apply directly

### Computation and Data Storage

**Cluster types:**
- **All-Purpose Clusters** — persistent, for interactive notebook development
- **Job Clusters** — spin up for a scheduled job and terminate when finished (more cost-efficient for production)

**Data storage:** Databricks does not store data — it reads/writes to your cloud storage. Data layer: **Delta Lake** on top of S3/GCS/ADLS. Access layer: **DBFS** (file path abstraction). Governance layer: **Unity Catalog**.

### Databricks vs. Open-Source Spark

| Aspect | Open-Source Spark | Databricks |
|---|---|---|
| **Cluster management** | Manual provisioning | Automatic, with auto-scaling |
| **Spark runtime** | Standard open-source | Optimized proprietary runtime (often faster) |
| **Delta Lake** | Requires separate setup | Default storage format |
| **Notebooks** | Not included | Collaborative notebooks built-in |
| **MLflow** | Separate installation | Built-in, natively integrated |
| **Security** | Your responsibility | Unity Catalog, RBAC, audit logging |

> Open-source Spark is the engine. Databricks is the fully equipped car built around that engine.

### DBFS (Databricks File System)

DBFS is a **distributed file system abstraction layer** that sits on top of your cloud storage. It lets you interact with cloud files using simple file path syntax.

```python
# Instead of AWS S3 path (requires credentials and boto3)
df.write.csv("s3://my-bucket/data/customers.csv")

# DBFS path — authentication handled automatically
df.write.csv("/dbfs/data/customers.csv")
# or
df.write.csv("dbfs:/data/customers.csv")

# Reading a file
df = spark.read.csv("dbfs:/data/customers.csv", header=True)

# Writing Delta format
df.write.format("delta").save("dbfs:/delta/customers")
```

**DBFS file operations via DBUtils:**

```python
# Listing files (like 'ls')
display(dbutils.fs.ls("dbfs:/data/"))

# Moving files
dbutils.fs.mv("dbfs:/data/raw/file.csv", "dbfs:/data/processed/file.csv")

# Deleting files
dbutils.fs.rm("dbfs:/data/old/file.csv")

# Copying files (recursively)
dbutils.fs.cp("dbfs:/data/source/", "dbfs:/data/destination/", recurse=True)

# Creating directories
dbutils.fs.mkdirs("/mnt/new-folder/")

# Secrets — securely retrieve credentials without hardcoding
dbutils.secrets.get(scope="aws", key="s3-access-key")

# Run another notebook from within a notebook
dbutils.notebook.run("other_notebook", timeout_seconds=60)

# Widgets — interactive input parameters
dbutils.widgets.text("date", "2024-01-01", "Run Date")
date = dbutils.widgets.get("date")
```

**DBFS Root vs. Mount Points:**
```python
# DBFS Root — default Databricks-managed storage
# Use for: temporary files, libraries, small non-sensitive assets
df.write.csv("dbfs:/FileStore/my_file.csv")

# Mount Points — connect your own cloud storage bucket to DBFS
dbutils.fs.mount(
    source="s3://your-bucket/data",
    mount_point="/mnt/your-data",
    extra_configs={"fs.s3a.access.key": "...", "fs.s3a.secret.key": "..."}
)
# Once mounted, access like a local path
df = spark.read.csv("/mnt/your-data/customers.csv", header=True)
```

### Unity Catalog — Modern Data Governance

**Why DBFS is being replaced:** DBFS was ungoverned — any user could read/write to shared storage with no access controls, no lineage, no audit trail. Unity Catalog brings centralized governance to all Databricks data assets.

**Three-level namespace:**
```
Catalog
  +-- Schema (Database)
        +-- Table / View / Volume / Function

-- Accessing a table
SELECT * FROM my_catalog.my_schema.customers

-- Accessing a file in a Volume
/Volumes/catalog/schema/volume/filename
```

**DBFS vs. Unity Catalog Volumes (migration):**
```python
# OLD way — DBFS (deprecated)
df.write.csv("dbfs:/data/customers.csv")
df = spark.read.csv("dbfs:/data/customers.csv")
dbutils.fs.ls("dbfs:/data/")

# NEW way — Unity Catalog Volumes (recommended)
df.write.csv("/Volumes/my_catalog/my_schema/my_volume/customers.csv")
df = spark.read.csv("/Volumes/my_catalog/my_schema/my_volume/customers.csv")
dbutils.fs.ls("/Volumes/my_catalog/my_schema/my_volume/")
```

**Creating a Volume (Databricks UI):**
1. Go to **Catalog** in the left sidebar
2. Navigate to your catalog and schema
3. **Create → Volume**
4. Choose: **Managed Volume** (Databricks manages storage) or **External Volume** (your S3/ADLS bucket)

**What Unity Catalog adds over DBFS:**
- Fine-grained access control (row/column level)
- Data lineage tracking
- Audit logging
- Cross-workspace governance
- Compliance (GDPR, HIPAA)

> **For anyone learning Databricks today: Unity Catalog is the only path worth learning.** DBFS is legacy and will continue to be deprecated.

---

## 19. Final Project — Azure End-to-End Pipeline

### Project Overview

A full end-to-end data engineering project using the **Brazilian E-commerce (Olist) dataset**, implementing the complete Medallion Architecture on Azure.

**Architecture:**
```
GitHub (CSV files) ---------------------+
Filess.io (MySQL DB, customers data) ---+
MongoDB (product category translation)--+
                                        |
                                        v
                            Azure Data Factory (ADF)
                            (Ingestion & Orchestration)
                                        |
                                        v
                            ADLS Gen2 Storage
                            +-- Bronze/ (raw data)
                            +-- Silver/ (cleaned)
                            +-- Gold/   (aggregated)
                                        |
                                        v
                            Azure Databricks (Spark)
                            (Transform, Join, Enrich)
                                        |
                                        v
                            Azure Synapse Analytics
                            (Serverless SQL + CETAS)
                                        |
                                        v
                            Power BI (Dashboards)
```

### Tools Used

- **Filess.io** — Free online MySQL storage. Hosts the customers dataset and MongoDB translation CSV. Avoids local storage; makes cloud data transfer seamless.
- **GitHub** — Source for all other datasets except customers.
- **Azure Data Factory (ADF)** — Cloud ETL/ELT orchestration tool. Handles data collection, transformation, and movement between sources and ADLS.
- **ADLS Gen2** — Azure Data Lake Storage Gen 2. Scalable, cost-effective storage with hierarchical namespace (folder organization).
- **Azure Databricks** — Spark-powered analytics platform integrated with Azure.
- **Azure Synapse Analytics** — Microsoft's unified analytics platform combining warehousing and big data processing.

### The Medallion Architecture

```
Bronze -> Raw data as received (CSV files, unchanged)
Silver -> Cleaned, typed, deduplicated (individual DataFrames)
Gold   -> Joined, aggregated, business-ready (analytics-ready datasets)
```

Data flows one-way: Bronze is never overwritten; Silver is derived from Bronze; Gold is derived from Silver.

### Azure Data Factory — Ingestion

**Pipeline flow:**
1. Create a pipeline in ADF (Author → Pipelines → New Pipeline)
2. Use **Copy Data** activity (source → sink)
3. **Source**: HTTP connection to GitHub raw CSV files. Use dynamic content for URL.
4. **Sink**: ADLS Gen2 connection to the Bronze container.
5. **ForEach loop**: Read a JSON file containing all filenames; iterate over it to copy all datasets in one run.
6. **Lookup block**: Reads the JSON file dynamically so the pipeline updates automatically when new files are added.
7. **MySQL source**: Filess.io connection for the customers dataset.

**Best practice for iterative reading:**
```
Lookup Block (reads JSON manifest from GitHub)
         |
ForEach Block (iterates over each item in the JSON)
         |
Copy Data Block (inside ForEach; uses @items().csv_relative_url as source URL)
         |
ADLS Gen2 Bronze Layer
```

**ADLS Gen2 Setup:**
1. Create a **Storage Account** (choose Locally-redundant; enable **Hierarchical Namespace** — required for folders)
2. Create a **Container**
3. Add directories: **Bronze**, **Silver**, **Gold**

### Azure Databricks — Setup and Connection

**Connecting Databricks to ADLS Gen2 (3-step secure setup):**

**Step 1 — Create an Access Connector:**
- Azure Portal → Create Resource → **Access Connector for Azure Databricks**
- Note the Resource ID

**Step 2 — Grant Storage Access:**
- Storage Account → **Access Control (IAM)** → Add Role Assignment
- Role: **Storage Blob Data Contributor** → Managed Identity → your Access Connector

**Step 3 — Create Storage Credential in Unity Catalog:**
- Databricks: Catalog → + → Add storage credential → Authentication: Azure Managed Identity
- Create an External Location pointing to your ADLS path

**Reading files after connection:**
```python
base_path = "abfss://olistdata@olistdatastorage1105.dfs.core.windows.net/"

# Always use Spark DataFrames for large files on ADLS
df = spark.read.csv(
    f"{base_path}Bronze/olist_customers_dataset.csv",
    header=True, inferSchema=True
)

# For small MongoDB collections: pandas is acceptable but convert immediately to Spark
import pymongo, pandas as pd
client = pymongo.MongoClient("connection_string")
mongo_pd_df = pd.DataFrame(list(client.db.collection.find({}, {'_id': 0})))
df_spark = spark.createDataFrame(mongo_pd_df)  # convert to Spark as soon as possible
```

> **Golden Rule:** For large files on ADLS, always read as Spark DataFrames. For small collections (< ~1000 rows), pandas is acceptable but convert to Spark immediately after.

### Data Cleaning in Databricks

```python
def clean_dataframe(df, name):
    """Remove duplicates and rows where all values are null."""
    print(f"Cleaning {name}")
    return df.dropDuplicates().na.drop('all')

# Null date handling strategies
from pyspark.sql.functions import col, when, count, year, month, sum, avg

# Strategy 1: Drop rows with null dates (when date is critical, < 5% nulls)
df = df.dropna(subset=['order_purchase_timestamp'])

# Strategy 2: Fill with a default date placeholder
df = df.fillna({'order_purchase_timestamp': '1900-01-01 00:00:00'})

# Strategy 3: Flag nulls and keep (when nulls carry meaning)
df = df.withColumn('is_date_missing',
    when(col('order_purchase_timestamp').isNull(), True).otherwise(False))

# Recommended decision logic for null dates
null_count = df.filter(col('order_purchase_timestamp').isNull()).count()
total_count = df.count()
null_pct = (null_count / total_count) * 100
print(f"Null percentage: {null_pct:.2f}%")

if null_pct < 5:
    df = df.dropna(subset=['order_purchase_timestamp'])  # safe to drop
else:
    df = df.withColumn('is_date_missing',  # flag for investigation
        when(col('order_purchase_timestamp').isNull(), True).otherwise(False))
```

### Joining Datasets (Olist Project)

```python
from pyspark.sql.functions import broadcast, sum, count

# Start from the center of the schema: olist_orders is the fact table
# Use LEFT JOINs to keep all orders even if related data is missing
full_orders_df = olist_orders_df \
    .join(olist_customers_df,         on='customer_id',         how='left') \
    .join(olist_order_items_df,       on='order_id',            how='left') \
    .join(olist_order_payments_df,    on='order_id',            how='left') \
    .join(olist_order_reviews_df,     on='order_id',            how='left') \
    .join(olist_products_df,          on='product_id',          how='left') \
    .join(olist_sellers_df,           on='seller_id',           how='left') \
    .join(broadcast(product_category_translation_df),
          on='product_category_name', how='left')
    # broadcast() for small tables (< 10MB) — avoids shuffle join

full_orders_df.cache()    # cache since this expensive join will be reused
full_orders_df.count()    # trigger the caching
full_orders_df.show(5)

# WATCH OUT: order_payments and order_reviews both join on order_id
# An order can have MULTIPLE payments and MULTIPLE reviews
# This creates many-to-many -> ROW MULTIPLICATION
# SOLUTION: aggregate before joining
olist_order_payments_agg = olist_order_payments_df \
    .groupBy('order_id') \
    .agg(
        sum('payment_value').alias('total_payment'),
        count('payment_sequential').alias('payment_count')
    )
```

### Data Storage — Medallion Architecture

```python
silver_path = "abfss://olistdata@olistdatastorage1105.dfs.core.windows.net/Silver/"
gold_path   = "abfss://olistdata@olistdatastorage1105.dfs.core.windows.net/Gold/"

# Store Silver layer (cleaned individual DataFrames) — always use Delta
olist_orders_df.write.format("delta").mode("overwrite").save(f"{silver_path}olist_orders/")
olist_customers_df.write.format("delta").mode("overwrite").save(f"{silver_path}olist_customers/")

# Store Gold layer (joined and aggregated)
full_orders_df.write.format("delta").mode("overwrite").save(f"{gold_path}full_orders/")

# Read back from Delta
gold_full_orders = spark.read.format("delta").load(f"{gold_path}full_orders/")

# Delta Time Travel — read previous versions
spark.read.format("delta").option("versionAsOf", 0).load(f"{gold_path}full_orders/")
spark.read.format("delta").option("timestampAsOf", "2024-01-01").load(f"{gold_path}full_orders/")

# Register as Unity Catalog tables (optional — enables SQL access)
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS olist_workspace.silver.olist_orders
    USING DELTA LOCATION '{silver_path}olist_orders/'
""")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS olist_workspace.gold.full_orders
    USING DELTA LOCATION '{gold_path}full_orders/'
""")
```

**Why Delta over CSV or Parquet?**

| Format | Schema | ACID | Versioning | Upserts | Performance |
|---|---|---|---|---|---|
| CSV | No | No | No | No | Slow |
| Parquet | Yes | No | No | No | Fast |
| **Delta** | Yes | **Yes** | **Yes** | **Yes** | Fast + more |

### Visualization in Databricks

```python
# Option 1 — Native display() (easiest, interactive chart builder)
display(full_orders_df.groupBy('customer_state').count().orderBy('count', ascending=False))
# Click chart icon to switch between table/bar/line/pie views

# Option 2 — Matplotlib + Seaborn (custom charts)
import matplotlib.pyplot as plt
import seaborn as sns

# ALWAYS aggregate in Spark first, then convert small result to pandas
orders_by_state = full_orders_df \
    .groupBy('customer_state').count() \
    .orderBy('count', ascending=False).limit(10).toPandas()  # converts ~10 rows

plt.figure(figsize=(12, 6))
sns.barplot(data=orders_by_state, x='customer_state', y='count', palette='viridis')
plt.title('Top 10 States by Number of Orders')
plt.tight_layout()
plt.show()

# Option 3 — Plotly (interactive charts)
import plotly.express as px

spending = full_orders_df \
    .groupBy('product_category_name_english').agg(sum('price').alias('total_revenue')) \
    .orderBy('total_revenue', ascending=False).limit(10).toPandas()

fig = px.bar(spending, x='product_category_name_english', y='total_revenue',
             title='Top 10 Product Categories by Revenue', color='total_revenue')
fig.show()

# GOLDEN RULE: NEVER do this
full_orders_df.toPandas()  # converts millions of rows -> will crash or be very slow

# ALWAYS aggregate in Spark first, convert only the small result
full_orders_df.groupBy('customer_state').count().toPandas()  # converts ~27 rows
```

### Azure Synapse Analytics

Azure Synapse is Microsoft's **unified analytics platform** combining data warehousing, big data processing, and data integration.

**Key capabilities:**
- **Synapse SQL** — massively parallel processing (MPP) data warehouse
- **Synapse Spark** — managed Apache Spark pools
- **Synapse Pipelines** — data integration (like Azure Data Factory)
- **Power BI integration** — built-in dashboards

**Two SQL Pool types:**
```sql
-- 1. Serverless SQL Pool (built-in, free to use)
-- Query files directly on ADLS; pay per TB scanned
SELECT *
FROM OPENROWSET(
    BULK 'https://olistdatastorage1105.dfs.core.windows.net/olistdata/Gold/full_orders/',
    FORMAT = 'DELTA'
) AS orders

-- 2. Dedicated SQL Pool (pre-provisioned)
-- Best for predictable, high-performance workloads
-- IMPORTANT: Pause when not in use (costs money even idle)
-- Manage -> SQL Pools -> hover -> Pause
CREATE TABLE dbo.full_orders (
    order_id      VARCHAR(50),
    customer_state VARCHAR(5),
    payment_value  FLOAT
)
WITH (DISTRIBUTION = HASH(order_id), CLUSTERED COLUMNSTORE INDEX)
```

**Synapse vs. Databricks:**

| | Databricks | Synapse |
|---|---|---|
| **Best for** | Python/Spark heavy, ML, complex transformations | SQL heavy, enterprise warehousing, Power BI |
| **Primary interface** | Notebooks (Python, Scala, SQL, R) | SQL scripts + notebooks |
| **ML integration** | Native MLflow | Limited |
| **Power BI** | Via connector | **Native, built-in** |

> In many production architectures, both are used: Databricks for heavy data engineering and ML work, Synapse as the serving layer for SQL-based analytics and reporting.

### CETAS — Create External Table As Select

CETAS runs a SQL query and saves the results as an external table stored on ADLS — materialized for fast future queries.

```sql
-- Step 1: Create external data source
CREATE EXTERNAL DATA SOURCE olist_gold
WITH (LOCATION = 'https://olistdatastorage1105.dfs.core.windows.net/olistdata/Gold/')

-- Step 2: Create file format
CREATE EXTERNAL FILE FORMAT parquet_format
WITH (FORMAT_TYPE = PARQUET,
      DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec')

-- Step 3: CETAS — compute once, materialize as external table on ADLS
CREATE EXTERNAL TABLE dbo.revenue_by_state
WITH (
    LOCATION    = 'cetas/revenue_by_state/',
    DATA_SOURCE = olist_gold,
    FILE_FORMAT = parquet_format
)
AS
SELECT
    customer_state,
    COUNT(*)           AS total_orders,
    SUM(payment_value) AS total_revenue,
    AVG(payment_value) AS avg_order_value
FROM OPENROWSET(
    BULK 'https://olistdatastorage1105.dfs.core.windows.net/olistdata/Gold/full_orders/',
    FORMAT = 'DELTA'
) AS orders
GROUP BY customer_state
ORDER BY total_revenue DESC

-- Query the result (fast — reads pre-computed Parquet files)
SELECT * FROM dbo.revenue_by_state WHERE total_orders > 1000

-- Refreshing (must drop and recreate)
DROP EXTERNAL TABLE dbo.revenue_by_state
-- then re-run the CREATE EXTERNAL TABLE AS SELECT

-- Additional CETAS examples:
-- Monthly revenue trend
CREATE EXTERNAL TABLE dbo.monthly_revenue
WITH (LOCATION = 'cetas/monthly_revenue/', DATA_SOURCE = olist_gold, FILE_FORMAT = parquet_format)
AS
SELECT
    YEAR(order_purchase_timestamp)  AS order_year,
    MONTH(order_purchase_timestamp) AS order_month,
    COUNT(*)                        AS total_orders,
    SUM(payment_value)              AS total_revenue
FROM OPENROWSET(
    BULK 'https://olistdatastorage1105.dfs.core.windows.net/olistdata/Gold/full_orders/',
    FORMAT = 'DELTA'
) AS orders
GROUP BY YEAR(order_purchase_timestamp), MONTH(order_purchase_timestamp)

-- Customer segmentation
CREATE EXTERNAL TABLE dbo.customer_segments
WITH (LOCATION = 'cetas/customer_segments/', DATA_SOURCE = olist_gold, FILE_FORMAT = parquet_format)
AS
SELECT
    customer_id,
    customer_state,
    COUNT(*)           AS total_orders,
    SUM(payment_value) AS total_spent,
    CASE
        WHEN SUM(payment_value) >= 1000 THEN 'High Value'
        WHEN SUM(payment_value) >= 500  THEN 'Medium Value'
        ELSE                                 'Low Value'
    END AS customer_segment
FROM OPENROWSET(
    BULK 'https://olistdatastorage1105.dfs.core.windows.net/olistdata/Gold/full_orders/',
    FORMAT = 'DELTA'
) AS orders
GROUP BY customer_id, customer_state
```

**CETAS comparison:**

| | OPENROWSET | CETAS | Dedicated Pool |
|---|---|---|---|
| **Cost** | Pay per scan | Pay once, cheap to query | Pay per hour |
| **Speed** | Slow on large | Fast (pre-computed) | Very fast |
| **Setup** | Zero | Minimal | Requires provisioning |
| **Best for** | Ad-hoc exploration | Repeated queries, Power BI | Heavy production |

**Full serving layer pattern:**
```
ADLS Bronze -> (Spark in Databricks) -> ADLS Silver -> (Spark) -> ADLS Gold
                                                                       |
                                          CETAS External Tables (pre-aggregated Parquet)
                                                                       |
                                                          Power BI Dashboards
```

### Synapse Studio Hubs

```
Home     ->  overview and quick starts
Data     ->  browse databases, tables, and storage
Develop  ->  write SQL scripts, notebooks, and data flows
Integrate -> build data pipelines (like Azure Data Factory)
Monitor  ->  track pipeline runs, Spark jobs, SQL requests
Manage   ->  create and manage pools, linked services, credentials
```

**Connecting Synapse to your existing ADLS:**
```
Manage -> Linked Services -> New -> Azure Data Lake Storage Gen2
Name:           olist_adls
Account URL:    https://olistdatastorage1105.dfs.core.windows.net
Authentication: Managed Identity
```

---

## 20. SQL — Code Reference

### Basic Database and Table Operations

```sql
-- Creating and selecting databases
CREATE DATABASE LibraryDB;         -- create a new database
CREATE DATABASE ecommerceDB;
SHOW DATABASES;                    -- list all databases

USE librarydb;                     -- VERY IMPORTANT: activate a database before creating tables

-- Creating a table (without constraints)
CREATE TABLE books (
    BookID          INT,           -- integer column
    Title           VARCHAR(25),   -- variable-length string, max 25 chars
    Author          VARCHAR(25),
    Genre           VARCHAR(25),
    PublicationYear INT
);

SHOW TABLES;                       -- list all tables in the current database
SELECT * FROM books;               -- retrieve all data from a table

-- Inserting data
INSERT INTO books (BookID, Title, Author, Genre, PublicationYear)
VALUES
    (1, 'Twilight', 'KN', 'Romantic', 2020),
    (2, 'HP', 'ALAS', 'Sci-fi', 2018);

-- Deleting tables and databases
DROP TABLE books;
DROP DATABASE ecommercedb;
```

### Data Manipulation (DML)

```sql
CREATE DATABASE companydb;
USE companydb;
CREATE TABLE employees (
    EmployeeID  INT PRIMARY KEY,   -- primary key: prevents duplicate IDs
    Firstname   VARCHAR(25),
    Lastname    VARCHAR(50),
    Email       VARCHAR(100),
    Hiredate    DATE,
    salary      DECIMAL(10, 2)     -- 10 digits total, 2 after the decimal point
);

-- Insert multiple rows
INSERT INTO employees VALUES
    (1, 'John', 'Doe', 'john.doe@example.com', '2020-01-15', 60000.00),
    (2, 'Jane', 'Smith', 'Jane.Smith@example.com', '2019-03-22', 75000.00),
    (3, 'Alice', 'Johnson', 'Alice.Johnson@example.com', '2021-07-30', 50000.00),
    (4, 'Bob', 'Brown', 'Bob.Brown@example.com', '2018-11-12', 65000.00);

-- NULL represents missing or absent data
INSERT INTO employees VALUES (5, 'Krish', NULL, 'krishnaik06@gmail.com', NULL, 55000.00);

-- Querying with NULL conditions
SELECT * FROM employees WHERE lastname IS NULL;
SELECT * FROM employees WHERE lastname OR hiredate IS NULL;

-- Updating records (ALWAYS use WHERE to avoid updating ALL rows)
UPDATE employees SET lastname = 'Naik' WHERE employeeid = '5';
UPDATE employees SET salary = salary + 10000 WHERE employeeid = '5';

-- Deleting specific records
DELETE FROM employees WHERE employeeid = '1';
DELETE FROM employees WHERE salary < 66000;

-- ALTER TABLE
ALTER TABLE employees ADD COLUMN phone_number VARCHAR(25);

ALTER TABLE employees
    ADD COLUMN middlename    VARCHAR(25),
    ADD COLUMN date_of_birth DATE;

ALTER TABLE employees MODIFY COLUMN phone_number VARCHAR(50);
ALTER TABLE employees MODIFY COLUMN salary INT;
ALTER TABLE employees CHANGE COLUMN middlename Middle_Name VARCHAR(25);
ALTER TABLE employees DROP COLUMN phone_number;
```

### Auto-Increment and BETWEEN

```sql
-- auto_increment: auto-assign incrementing IDs
CREATE TABLE customer_info (
    id        INT AUTO_INCREMENT PRIMARY KEY,
    firstname VARCHAR(25),
    lastname  VARCHAR(25),
    salary    INT
);

-- When inserting with auto-increment, omit the ID column
INSERT INTO customer_info (firstname, lastname, salary) VALUES
    ('Raed', 'Hlayhel', 42000),
    ('Eric', 'Trinh', 60000),
    ('Lisa', 'kamen', NULL);

-- BETWEEN for range conditions (inclusive on both ends)
UPDATE customer_info SET salary = 10000 WHERE id BETWEEN 1 AND 4;

-- IN for exact match list
UPDATE customer_info SET salary = 10000 WHERE id IN (1, 4);

-- Describe a table (shows column types and properties)
DESC customer_info;

ALTER TABLE customer_info MODIFY COLUMN salary DECIMAL(10, 2);
```

### Constraints and Keys

```sql
CREATE DATABASE librarydb;
USE librarydb;

-- PRIMARY KEY: uniquely identifies each row; no NULL, no duplicates
CREATE TABLE authors (
    Authorid  INT PRIMARY KEY,
    firstName VARCHAR(25),
    LastName  VARCHAR(25),
    email     VARCHAR(50)
);
-- Inserting with the same Authorid again will cause an error (duplicate primary key)

-- FOREIGN KEY + CHECK + NOT NULL
CREATE TABLE books (
    bookid          INT PRIMARY KEY,
    title           VARCHAR(50) NOT NULL,      -- NOT NULL prevents null in this column
    authorid        INT,
    publicationyear INT CHECK (publicationyear > 0),  -- CHECK enforces a condition
    FOREIGN KEY (authorid) REFERENCES authors(authorid)
    -- FOREIGN KEY links authorid to the primary key of the authors table
    -- Only values that exist in authors.authorid are allowed here
);

-- Adding and dropping constraints after table creation
CREATE TABLE students (id INT NOT NULL, firstname VARCHAR(25), lastname VARCHAR(25) NOT NULL, age INT);
ALTER TABLE students MODIFY age INT PRIMARY KEY NOT NULL;  -- add primary key
ALTER TABLE students DROP PRIMARY KEY;                    -- remove primary key

-- PRIMARY KEY vs UNIQUE KEY:
-- Primary Key: unique + no NULLs + only one per table + clustered index
-- Unique Key:  unique + allows one NULL + multiple per table + non-clustered index

CREATE TABLE person (
    id        INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
    firstname VARCHAR(25),
    lastname  VARCHAR(25) UNIQUE,          -- inline unique constraint
    age       INT NOT NULL,
    UNIQUE(age),                           -- explicit unique
    UNIQUE(firstname, age)                 -- COMPOSITE KEY: combination must be unique
);

-- DEFAULT: provides a fallback value when none is specified during INSERT
CREATE TABLE members (
    id        INT AUTO_INCREMENT PRIMARY KEY,
    firstname VARCHAR(25) NOT NULL,
    lastname  VARCHAR(25) NOT NULL,
    email     VARCHAR(25) UNIQUE,
    salary    INT DEFAULT 22000            -- default salary if not provided
);
INSERT INTO members (firstname, lastname, email) VALUES ('Raed', 'Hayhel', 'Raed@pipo.com');

-- INDEX: speeds up data retrieval on frequently queried columns
CREATE INDEX idx_firstname ON members(firstname);

-- EXPLAIN shows the query optimizer's execution plan
EXPLAIN SELECT * FROM members WHERE firstname = 'Raed';
```

### Views and Joins

```sql
-- VIEW: a virtual table defined by a stored SELECT query
-- Does not store data itself; executes the underlying query each time it is accessed
CREATE VIEW student_info AS
SELECT * FROM students;

SELECT * FROM student_info;  -- use like a regular table
DROP VIEW student_info;      -- remove view (underlying table is unaffected)

-- INNER JOIN: returns only rows where there is a match in BOTH tables
SELECT students.name, courses.course_name
FROM students
INNER JOIN courses ON students.id = courses.student_id;

-- LEFT JOIN: all rows from the left table, plus matching rows from the right
-- NULL appears on the right side where there is no match
SELECT members.firstname, payments.amount, payments.payment_date
FROM members
LEFT JOIN payments ON members.id = payments.member_id;

-- RIGHT JOIN: all rows from the right table; NULL on the left where no match

-- FULL OUTER JOIN: all rows from both tables; NULL on whichever side has no match
-- Note: MySQL does not support FULL OUTER JOIN directly
-- PostgreSQL and SQL Server do. In MySQL, simulate with LEFT JOIN UNION RIGHT JOIN
```

---

## 21. Python — Code Reference

### Python Basics

```python
# Type conversion
age = 28
age_str   = str(age)          # int -> string
age_int   = int("28")         # string -> int
age_float = float("28.5")     # string -> float

# User input
name = input("What is your name? ")
a    = float(input("Enter a number: "))

# If / elif / else
number = int(input("Enter a number: "))
if number > 0:
    print("positive")
elif number < 0:
    print("negative")
elif number == 0:
    print("zero")
else:
    print("invalid")

# For loops with range
# Python starts counting from 0; range() excludes the end value
for i in range(5):          # 0, 1, 2, 3, 4
    print(i)

for i in range(1, 10, 2):   # 1, 3, 5, 7, 9 (start, stop, step)
    print(i)

# break, continue, pass
for i in range(10):
    if i == 5: break        # exit the loop
    if i % 2 == 0: continue # skip even numbers
    pass                    # placeholder; does nothing
```

### Data Structures

```python
#%% LISTS — ordered, mutable collection of any types

my_list = [1, 2, 3, "four", "five"]

# Access
item = my_list[0]                       # index 0 = first element
item = my_list[-1]                      # last element

# Slicing: list[start:end:step]
sliced = my_list[::2]                   # every second element

# Modification
my_list.append("six")                   # add to end
my_list.insert(3, "three")             # insert at index 3
my_list.remove(my_list[2])             # remove by position
my_list.pop()                           # remove last element

# Info
index = my_list.index("four")          # find index of value
count = my_list.count("four")          # count occurrences
len(my_list)                            # number of elements

# Sort and reverse
my_list.sort()                          # sort in-place (homogeneous types only)
my_list.reverse()                       # reverse in-place

# Iteration with index
for index, item in enumerate(my_list):
    print(f"Index: {index}, Item: {item}")

# List comprehension
squares = [x**2 for x in range(5) if x % 2 == 0]  # [0, 4, 16]

# Trick: Remove duplicates
lst = [1, 2, 3, 3, 5, 5, 6]
unique = list(set(lst))     # convert to set (removes duplicates) then back to list

my_list.clear()             # remove all elements


#%% TUPLES — ordered, IMMUTABLE collection

my_tuple = (1, 2, 3, 3.14, "four")    # parentheses, comma-separated
packed   = 1, 2, 3                      # also creates a tuple (packing)

# Access
print(my_tuple[2])                      # index 2 = third element

# Concatenation and multiplication
result   = my_tuple + (15, 20)
repeated = (1, 2, 3) * 3               # (1,2,3,1,2,3,1,2,3)

# Methods
my_tuple.count(3)                       # count occurrences of 3
my_tuple.index(3)                       # find index of first 3

# Unpacking
a, b, c   = (1, "A", 305.254)          # must match count exactly
a, *b, c  = (1, 78, 305, 954, "Fin")  # * collects intermediate values

# Nested tuples
nested = ((1,2,3), ('a','b','c'))
print(nested[0][2])                     # access: row then column
for sub in nested:
    for item in sub:
        print(item, end=" ")


#%% SETS — unordered collection of UNIQUE items

my_set = {1, 2, 3, "four"}             # curly braces; duplicates automatically removed
my_set.add(7)
my_set.remove(3)                        # raises error if absent
my_set.discard(11)                      # no error if absent
my_set.pop()                            # remove arbitrary element (unordered)
my_set.clear()

# Membership test
print(3 in my_set)                      # True/False

# Set operations
set1, set2 = {1,2,3,4,5,6}, {4,5,6,7,8,9}
set1.union(set2)                        # all elements from both
set1.intersection(set2)                 # common elements; {4,5,6}
set1.difference(set2)                   # elements in set1 not in set2; {1,2,3}
set1.symmetric_difference(set2)        # elements not common to both
set1.issubset(set2)                     # True if all set1 elements in set2
set1.issuperset(set2)                   # True if all set2 elements in set1


#%% DICTIONARIES — unordered key:value pairs; keys are unique and immutable

my_dict = {"name": "Alice", "age": 30, "city": "New York"}

# Access
my_dict['name']                         # "Alice"
my_dict['name'][0]                      # "A" (first letter)

# Modification
my_dict["country"] = "USA"             # add new key
my_dict["age"]     = 33                # update existing value
del my_dict["city"]                    # delete key (and its value)

# Methods
my_dict.keys()                          # dict_keys of all keys
my_dict.values()                        # dict_values of all values
my_dict.items()                         # dict_items of (key, value) tuples

# WRONG copy method (creates a reference, not a copy!)
copy_wrong = my_dict    # changes to copy_wrong also affect my_dict!

# Correct copy method
copy_right = my_dict.copy()            # creates an independent copy

# Iteration
for key in my_dict.keys():
    print(key)
for k, v in my_dict.items():
    print(k, v)

# Nested dictionaries
students = {
    'student1': {'name': 'Raed', 'age': 28},
    'student2': {'name': 'Coco', 'age': 31}
}
print(students["student1"]["name"])    # "Raed"

# Merging dictionaries
merged     = {**dict1, **dict2}        # unpack both
merged_new = dict1 | dict2             # Python 3.9+ merge operator
merged_override = {**dict1, **dict2, "a": "override_value"}

# Dictionary comprehension
squares = {x: x**2 for x in range(10) if x % 2 == 0}
counter = {i: lst.count(i) for i in lst}  # frequency counter
```

### Functions

```python
# Basic function
def f_unction(a, b, c, d):
    total = a + b + c + d
    return total, a, b, c, d           # return multiple values as tuple

s, a, b, c, d = f_unction(1, 2, 3, 4)  # unpack return values

# *args — any number of positional arguments
def print_numbers(*args):
    for number in args:
        print(number)

# **kwargs — any number of keyword arguments (key:value pairs)
def print_info(**kwargs):
    for title, value in kwargs.items():
        print(f"{title}: {value}")
print_info(one=1, two="2", three=3)

# Combined *args and **kwargs (positional MUST come before keyword)
def full_function(*args, **kwargs):
    for i in args: print(f"Positional: {i}")
    for j, k in kwargs.items(): print(f"{j}: {k}")

# Recursive function — calls itself
def factorial(n):
    if n == 0: return 1
    else:      return n * factorial(n - 1)   # n! = n * (n-1)!

# Lambda functions — small anonymous functions with one expression
even   = lambda a: a % 2 == 0
square = lambda x: x ** 2

# map() — apply a function to all items in a list
squares = list(map(lambda a: a**2, [1, 2, 3, 4, 5]))  # [1, 4, 9, 16, 25]

# map() with multiple lists
added = list(map(lambda x, y: x + y, [1,2,3], [4,5,6]))  # [5, 7, 9]

# filter() — keep items that satisfy a condition
evens    = list(filter(lambda x: x % 2 == 0, [1,2,3,4,5,6]))
big_even = list(filter(lambda x: x > 5 and x % 2 == 0, range(15)))

# reduce() — cumulatively apply function to reduce to single value
from functools import reduce
product = reduce(lambda x, y: x * y, [1, 2, 3, 4, 5])  # 120

# Decorators — modify function behavior without changing source code
def my_decorator(func):
    def wrapper():
        print("Before function call")
        func()
        print("After function call")
    return wrapper

@my_decorator
def say_hello():
    print("Hello!")

say_hello()
# Output: Before function call -> Hello! -> After function call
```

### File Operations

```python
# Reading entire file
with open("filename.txt", 'r') as file:
    content = file.read()
    print(content)

# Reading line by line
with open("filename.txt", 'r') as file:
    for line in file:
        print(line.strip())             # strip() removes trailing newline

# Writing (overwrites existing content)
with open("example.txt", 'w') as file:
    file.write("Hello World!\n")

# Appending (adds to end of existing file)
lines = ["First\n", "Second\n", "Third\n"]
with open("example.txt", 'a') as file:
    file.writelines(lines)

# Binary files (images, audio, compiled code, etc.)
data = b'\x00\x01\x02\x03\x04'
with open('example.bin', 'wb') as file:
    file.write(data)
with open('example.bin', 'rb') as file:
    content = file.read()
    print(content)

# Write and read in the same operation (w+ mode)
with open('example.txt', 'w+') as file:
    file.write("Hello world\n")
    file.seek(0)                        # move cursor back to start
    content = file.read()
    print(content)

# File path operations (os module)
import os
os.getcwd()                             # get current working directory
os.path.exists("path")                 # check if path exists
os.path.abspath("file.txt")           # get absolute path
os.path.isfile("file.txt")            # True if it's a file
os.path.isdir("folder")               # True if it's a directory
os.mkdir("new_folder")
os.listdir("folder")
os.path.join("parent", "child")       # platform-safe path joining
```

### Object-Oriented Programming

```python
#%% Exception Handling

try:
    number = 1 / 0
except ZeroDivisionError as ex:
    print(ex)

# Multiple exception types
try:
    number = 1 / 2
    a = b                               # NameError: b not defined
except ZeroDivisionError as ex:
    print(ex)
except NameError as ex1:
    print(ex1)
except Exception as ex2:                # catches ANY exception (use last)
    print(ex2)
else:
    print("No errors occurred")         # runs only if no exception was raised
finally:
    print("Always runs")                # runs regardless of exception

# Custom exceptions
class CustomError(Exception):
    pass                                # subclass of built-in Exception

class NegativeNumberError(CustomError):
    def __init__(self, number):
        self.number = number
        super().__init__(f"Negative number error: {number} is not allowed")

def check_positive(number):
    if number < 0:
        raise NegativeNumberError(number)

try:
    check_positive(-5)
except NegativeNumberError as ex:
    print(ex)


#%% Classes and Objects

class Car:
    pass

audi = Car()
print(type(audi))                       # <class '__main__.Car'>

# Constructor (__init__): initializes instance variables when object is created
class Dog:
    def __init__(self, name, age):
        self.name = name                # instance variable
        self.age  = age

dog1 = Dog("Buddy", 3)
print(dog1.name)                        # "Buddy"

# Instance methods
class DogWithMethods:
    def __init__(self, name, age):
        self.name = name
        self.age  = age

    def bark(self):
        print(f"{self.name} says Woof!")

dog1 = DogWithMethods("Buddy", 3)
dog1.bark()
dir(DogWithMethods)                     # list all attributes and methods


#%% Inheritance

class Car:                              # Parent class
    def __init__(self, year, model):
        self.year  = year
        self.model = model

    def start_engine(self):
        print("Engine started")

class ElectricCar(Car):                 # Child class inherits from Car
    def __init__(self, year, model, battery_capacity):
        super().__init__(year, model)  # call parent constructor
        self.battery_capacity = battery_capacity

    def charge_battery(self):
        print("Battery charging")

car1 = ElectricCar('2024', 'Tesla', 90)
car1.start_engine()                    # inherited from Car
car1.charge_battery()                  # defined in ElectricCar

# Multiple inheritance chain
class Animal:
    def speak(self): return "This animal speaks"

class Pet(Animal):
    def __init__(self, name, owner):
        super().__init__()
        self.name  = name
        self.owner = owner

class Dog(Pet):
    def __init__(self, name, owner, age):
        super().__init__(name, owner)
        self.age = age

    def GetInfo(self):
        print(f"{self.name} is {self.age} years old, owned by {self.owner}")

dog1 = Dog('Rex', 'Alice', '3')
dog1.GetInfo()
dog1.speak()                            # inherited from Animal through Pet


#%% Polymorphism

# Method Overriding
class Animal:
    def speak(self): return 'This animal speaks'

class Dog(Animal):
    def speak(self): return 'WOOF'

class SmallDog(Dog):
    def speak(self): return 'woof'

def animal_speak(animal):
    print(animal.speak())

animal_speak(Dog())      # "WOOF"
animal_speak(SmallDog()) # "woof"

# Abstract base classes — define interface without implementation
from abc import ABC, abstractmethod

class Vehicle(ABC):
    @abstractmethod
    def start_engine(self): pass       # must be implemented by all subclasses

class Car(Vehicle):
    def start_engine(self): return "Car engine started"

class Motorcycle(Vehicle):
    def start_engine(self): return "Motorcycle engine started"

print(Car().start_engine())


#%% Encapsulation — controlling access to class attributes

class Person:
    def __init__(self, name, age, gender):
        self.age     = age              # public: accessible everywhere
        self._gender = gender           # protected: convention only
        self.__name  = name            # private: name-mangled; not directly accessible

    def get_name(self):                 # getter
        return self.__name

    def set_name(self, name):           # setter
        self.__name = name

person = Person('Alice', 30, 'Female')
person.set_name('Bob')
print(person.get_name())               # 'Bob'
print(person.age)                      # 30 (public, direct access)


#%% Generators — memory-efficient iterables

# Unlike lists, generators compute values one at a time on demand
def squares(n):
    for i in range(n):
        yield i ** 2

for value in squares(5):
    print(value)                        # 0, 1, 4, 9, 16

# Practical: read large file without loading everything into memory
def read_large_file(file_path):
    with open(file_path, "r") as file:
        for line in file:
            yield line


#%% Magic Methods (Dunder Methods)

class Point:
    def __init__(self, x, y):
        self.x = x
        self.y = y

    def __str__(self):                  # defines print() representation
        return f"Point({self.x}, {self.y})"

    def __add__(self, other):           # defines behavior of + operator
        return Point(self.x + other.x, self.y + other.y)

p1, p2 = Point(2, 3), Point(4, 5)
print(p1 + p2)                          # "Point(6, 8)"
```

### Numpy and Pandas

```python
import numpy as np
import pandas as pd

#%% Numpy

# Creating arrays
arr1 = np.array([1, 2, 3, 4, 5])       # 1D array
arr2 = np.array([[1,2,3],[4,5,6]])      # 2D array (2 rows, 3 columns)
arr3 = np.ones((3, 5))                  # 3x5 matrix of ones
arr4 = np.eye(3)                        # 3x3 identity matrix

# Array attributes
print(arr2.shape)                        # (2, 3)
print(arr2.ndim)                         # 2 (dimensions)
print(arr2.size)                         # 6 (total elements)
print(arr2.dtype)                        # int64

# Vectorized operations (element-wise)
a, b = np.array([1,2,3]), np.array([4,5,6])
print(a + b)    # [5, 7, 9]
print(a * b)    # [4, 10, 18]
print(a / b)    # [0.25, 0.4, 0.5]

# Universal functions
np.sqrt(arr1)   # element-wise square root
np.exp(arr1)    # e^x
np.log(arr1)    # natural log
np.sin(arr1)    # sine

# Array slicing: array[row_slice, col_slice]
arr = np.array([[1,2,3,4],[5,6,7,8],[9,10,11,12]])
print(arr[0][0])                         # 1 (first element)
print(arr[2:, 1:])                       # rows from 2, columns from 1

# Statistical functions
np.mean(arr1); np.std(arr1); np.var(arr1)
np.median(arr1); np.min(arr1); np.max(arr1)

# Normalization example
data = np.array([1,2,3,4,5,6,7,8,9,10])
def normalize(array):
    return (array - np.min(array)) / (np.max(array) - np.min(array))
data_norm = normalize(data)


#%% Pandas

# Series: 1D labeled array
series1 = pd.Series([1, 2, 4, 53])               # integer index
series2 = pd.Series({'a': 1, 'b': 2, 'c': 3})   # dict -> named index

# DataFrame: 2D labeled table
data = {
    'Name': ['Raed', 'Hassan', 'Hlayhel'],
    'Age':  [28, 70, 1000],
    'City': ['Kalamoun', 'Al-Qalamoun', 'NoPlace']
}
df = pd.DataFrame(data)

# Reading files
df = pd.read_csv("path/to/file.csv")

# Basic inspection
df.head(5); df.tail(5); df.dtypes; df.describe()
df.isnull().any()    # which columns have nulls
df.isnull().sum()    # count nulls per column

# Data access
df['Date']           # select column
df.iloc[0, 0]        # by position (row, col index)
df.at[1, 'Date']     # by label

# Modifying data
df['Salary'] = [1000, 2000, 3000]               # add/replace column
df['Age']   += 2                                # arithmetic on column
df.drop('Salary', axis=1, inplace=True)         # drop column permanently
df.rename(columns={'Date': 'Sale Date'})         # rename column

# Handling missing values
df['Sales_filled'] = df['Sales'].fillna(df['Sales'].mean())

# Apply a function
df['New Value'] = df['Value'].apply(lambda x: x**2)

# Grouping and aggregation
grouped     = df.groupby('Product')['Value'].mean()
multi_group = df.groupby(['Region', 'Product'])['Value'].sum()
multi_agg   = df.groupby(['Region', 'Product'])['Value'].agg(['mean', 'sum', 'std'])

# Merging DataFrames
df1 = pd.DataFrame({'Key': ['A','B','C'], 'Value1': [1,2,3]})
df2 = pd.DataFrame({'Key': ['A','B','D'], 'Value1': [4,5,6]})

pd.merge(df1, df2, on='Key', how='inner')  # only A and B (common keys)
pd.merge(df1, df2, on='Key', how='outer')  # A, B, C, D (all keys, NaN where no match)
pd.merge(df1, df2, on='Key', how='left')   # all from df1; NaN from df2
pd.merge(df1, df2, on='Key', how='right')  # all from df2; NaN from df1
```

### SQL with Python (SQLite & Logging)

```python
import sqlite3

#%% SQLite — lightweight file-based SQL database

with sqlite3.connect("example.db") as connection:
    cursor = connection.cursor()   # cursor navigates and executes queries

    # Create table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS employees (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            name       TEXT NOT NULL UNIQUE,
            age        INTEGER,
            department TEXT
        )
    ''')

    # Insert single row
    cursor.execute('''
        INSERT INTO employees (name, age, department)
        VALUES ('Raed', 28, 'Department of color')
    ''')

    # Insert multiple rows (executemany with ? placeholders)
    sales_data = [
        ('2023-01-01', 'Product1', 100, 'North'),
        ('2023-01-02', 'Product2', 200, 'South'),
    ]
    cursor.executemany('''
        INSERT INTO sales (date, product, sales, region) VALUES (?, ?, ?, ?)
    ''', sales_data)

    connection.commit()             # save changes to database

    # Query data
    cursor.execute('SELECT * FROM employees')
    rows = cursor.fetchall()        # returns list of tuples
    for row in rows:
        print(row)

    # Update
    cursor.execute("UPDATE employees SET age = '29' WHERE name = 'Raed'")

    # Delete
    cursor.execute("DELETE FROM employees WHERE name = 'Raed'")

    # Drop table
    cursor.execute('DROP TABLE employees')


#%% Logging — track events during program execution

import logging

logging.basicConfig(
    filename='app.log',
    filemode='w',                        # 'w' = overwrite; 'a' = append
    level=logging.INFO,                  # minimum severity to log
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Severity levels (low to high):
logging.debug('Debug — detailed diagnostic info')    # ignored if level=INFO
logging.info('Info — general flow info')
logging.warning('Warning — unexpected but not error')
logging.error('Error — a real problem occurred')
logging.critical('Critical — severe error, may crash')

# Named loggers — identify which module logged each message
logger_db  = logging.getLogger('database')
logger_api = logging.getLogger('api')
logger_db.setLevel(logging.DEBUG)
logger_api.setLevel(logging.WARNING)

logger_db.debug('Database connected successfully')
logger_api.warning('API rate limit approaching')

# Arithmetic logging example
logger = logging.getLogger('ArithmeticApp')

def add(a, b):
    result = a + b
    logger.debug(f"Adding {a} + {b} = {result}")
    return result

def divide(a, b):
    try:
        result = a / b
        logger.debug(f"Dividing {a} / {b} = {result}")
        return result
    except ZeroDivisionError:
        logger.error("Division by zero attempted")
        return None

add(10, 15)
divide(20, 0)
```

---

## 22. PySpark — Code Reference

### Spark Session and RDD Basics

```python
from pyspark.sql import SparkSession

# Start or create a Spark Session
spark = SparkSession.builder \
    .appName('My Spark Application') \
    .master('yarn') \                      # optional: 'yarn', 'local', 'local[*]'
    .getOrCreate()

sc = spark.sparkContext                    # SparkContext for low-level RDD API

# Check cluster configuration
print(sc.defaultParallelism)              # total CPU cores available
print(sc.defaultMinPartitions)            # minimum partitions for file reading (usually 2)


#%% Creating RDDs

# From inline data
data    = ["Goku Vegeta Gohan", "Goku Frieza Goku"]
rdd     = sc.parallelize(data)

# From a file (HDFS, S3, or local)
file_rdd = sc.textFile("/path/to/file.txt")

# Check partition count
print(file_rdd.getNumPartitions())


#%% Basic RDD Operations

customer_data = [
    "0,Customer_0,Bangalore,Karnataka,India,2023-11-11,True",
    "1,Customer_1,Hyderabad,Delhi,India,2023-08-26,True",
]

data_rdd = sc.parallelize(customer_data)

# Remove header
header   = data_rdd.first()
data_rdd = data_rdd.filter(lambda line: line != header)

# Parse each row into a tuple
def parse_row(row):
    fields = row.split(',')
    return (int(fields[0]), fields[1], fields[2], fields[3], fields[4], fields[5], fields[6] == 'True')

parsed_rdd = data_rdd.map(parse_row)  # apply to every row
parsed_rdd.collect()                   # action: bring all results to driver


#%% Advanced RDD Operations

name_city_rdd = parsed_rdd.map(lambda row: (row[1], row[2]))   # (name, city)
active_rdd    = parsed_rdd.filter(lambda row: row[6] == True)  # only active customers
cities_rdd    = parsed_rdd.map(lambda row: row[2]).distinct()  # unique cities only
cities_rdd.take(2)                                              # first 2 elements

# Chain multiple operations
active_cities = parsed_rdd \
    .filter(lambda row: row[6]) \    # filter inactive
    .map(lambda row: row[2]) \       # extract city
    .distinct()                      # remove duplicates
active_cities.collect()

# Save to file (HDFS, not local filesystem)
active_cities.saveAsTextFile("/tmp/active_cities")


#%% reduceByKey vs. groupByKey

# reduceByKey — PREFERRED
# Aggregates locally on each partition first, then shuffles only the results
customers_per_city = parsed_rdd \
    .map(lambda row: (row[2], 1)) \    # (city, 1) for each record
    .reduceByKey(lambda x, y: x + y)  # sum per city; local aggregation before shuffle

# groupByKey — USE ONLY WHEN FULL VALUE LIST IS NEEDED
rdd.groupByKey().map(lambda x: (x[0], len(x[1])))            # count per key
rdd.groupByKey().map(lambda x: (x[0], sum(x[1]) / len(x[1])))  # average (needs full list!)


#%% Partition Management

rdd.getNumPartitions()

# Increase partitions (full shuffle)
rdd_more = rdd.repartition(16)    # underutilized cluster, skewed data

# Decrease partitions (no full shuffle — cheaper)
rdd_less = rdd.coalesce(4)        # data shrank after filter, before saving output

# Best: target 2-4 partitions per CPU core
rdd.coalesce(2).saveAsTextFile("hdfs:///output/result")
```

### Spark DataFrame Operations

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('DataFrame Demo').getOrCreate()


#%% Creating DataFrames

# From inline data
data = [
    (0, "Customer_0", "Bangalore", "Karnataka", "India", "2023-11-11", True),
    (1, "Customer_1", "Delhi",     "Delhi",     "India", "2023-08-26", True),
]
columns = ["customer_id", "name", "city", "state", "country", "registration_date", "is_active"]
df = spark.createDataFrame(data, columns)

# Explicit schema (StructType)
schema = StructType([
    StructField("customer_id",       IntegerType(), True),
    StructField("name",              StringType(),  True),
    StructField("city",              StringType(),  True),
    StructField("is_active",         BooleanType(), True),
])

# DDL string schema (alternative)
schema_ddl = "customer_id INT, name STRING, city STRING, is_active BOOLEAN"

# Read from file with explicit schema
df = spark.read.format('csv').option('header', 'true').schema(schema_ddl).load('/data/customers.csv')


#%% Read Modes

df = spark.read \
    .option("mode", "PERMISSIVE") \
    .option("columnNameOfCorruptRecord", "_corrupt_record") \
    .csv("data.csv", header=True)

# Replace string "null" with actual null
df = df.replace("null", None)
df = df.withColumn("name", when(col("name") == "null", lit(None)).otherwise(col("name")))


#%% DataFrame Operations

df.columns; df.printSchema(); df.describe().show(); df.count()
df.select("name", "city").show()
df.filter(df.is_active == True).show()
df.where(df.city == "Delhi").show()
df.distinct().show()
df.orderBy("name").show()
df.orderBy(df.name.desc()).show()

# Column modifications
df = df.withColumn("new_col", df.customer_id + 100)
df = df.withColumnRenamed("city", "location")
df = df.drop("country")

# Aggregation
df.groupBy("city").count().show()
df.groupBy("city").agg({"customer_id": "count"}).show()

# Null handling
df.na.drop("all")
df.na.drop(subset=["name", "city"])
df.na.fill({"city": "Unknown"})


#%% Writing DataFrames

df.write.format("csv").option("header", "true").mode("overwrite").save("/output/path/")
df.coalesce(1).write.format("csv").option("header", "true").save("/output/single/")


#%% Joins

customers.join(orders, on="customer_id", how="inner")
customers.join(orders, on="customer_id", how="left")
customers.join(orders, on="customer_id", how="left_anti")
customers.join(orders, on="customer_id", how="left_semi")
customers.join(broadcast(small_table), on="key", how="inner")  # broadcast for small tables


#%% Data Type Handling

df = df.withColumn("id", col("id").cast(IntegerType()))
df = df.withColumn("name_upper", upper(df.name))
df = df.withColumn("date_parsed", to_date(df.date_str, "yyyy-MM-dd"))
df = df.withColumn("ts_parsed",   to_timestamp(df.ts_str, "yyyy-MM-dd HH:mm:ss"))
df = df.withColumn("year", year(df.ts_parsed)).withColumn("month", month(df.ts_parsed))
df = df.withColumn("days_since", datediff(current_date(), df.date_parsed))
```

### Spark SQL

```python
spark = SparkSession.builder \
    .appName('Spark SQL Demo') \
    .enableHiveSupport() \          # required for persistent tables
    .getOrCreate()

# Temporary view (session-scoped)
df.createOrReplaceTempView('customers')
spark.sql('SHOW TABLES').show()
spark.sql('SELECT city, COUNT(*) as cnt FROM customers GROUP BY city').show()

# Global temporary view (application-scoped)
df.createOrReplaceGlobalTempView('GlobalTemp_customers')
spark.sql('SHOW TABLES IN global_temp').show()
spark.sql('SELECT * FROM global_temp.GlobalTemp_customers LIMIT 5').show()

# Persistent tables (require enableHiveSupport())
spark.sql('SHOW DATABASES').show()
spark.sql('USE default')
spark.sql('CREATE DATABASE IF NOT EXISTS ecommerce')

# Managed table (Spark owns data; DROP deletes data permanently)
df.write.mode('overwrite').saveAsTable('default.customers_managed')

# External table (you own data; DROP only removes metadata)
spark.sql("""
    CREATE EXTERNAL TABLE customers_external
    USING csv
    LOCATION '/path/to/existing/data'
""")

# Inspect table metadata
spark.sql('DESCRIBE EXTENDED customers_managed').show(truncate=False)

spark.stop()  # ALWAYS stop when done
```

---

## 23. The Role of a Data Engineer — Best Practices

### What Does a Data Engineer Actually Do?

A Data Engineer is a software engineer specialized in **data infrastructure**. While data scientists analyze data and data analysts derive insights, the data engineer **builds and maintains the systems that make data available, reliable, and usable at scale**.

**Core responsibilities:**
1. **Data ingestion** — collecting data from diverse sources (databases, APIs, IoT sensors, logs, streams) and landing it in the data platform
2. **Data transformation** — cleaning, validating, reshaping, and enriching raw data into analytics-ready formats (ETL/ELT)
3. **Data storage design** — choosing and implementing the right storage solutions (data lakes, warehouses, lakehouses) for the access patterns required
4. **Pipeline orchestration** — automating, scheduling, and monitoring workflows that move data from source to destination reliably
5. **Data quality** — defining and enforcing data quality standards; detecting and handling corrupt, missing, or anomalous data
6. **Performance optimization** — tuning queries, partitioning strategies, caching, and cluster configurations for efficiency
7. **Infrastructure management** — managing cloud infrastructure, containers (Docker), and cluster configurations
8. **Enablement** — ensuring downstream consumers (analysts, data scientists, ML engineers) have access to clean, well-documented data

### Best Practices by Tool

#### SQL Best Practices
- Always use **explicit column names** in SELECT (avoid `SELECT *` in production)
- Use **JOINs appropriately**: prefer LEFT JOIN when you want to keep all records from one side
- Create **indexes** on columns used frequently in WHERE, JOIN ON, and ORDER BY clauses
- Use **EXPLAIN** to understand query execution plans and identify bottlenecks
- Always use **WHERE clauses** with DELETE and UPDATE — never update/delete all rows accidentally
- Use **transactions** (BEGIN, COMMIT, ROLLBACK) for operations that must be atomic

#### Python Best Practices
- Use **virtual environments** (`venv`, `conda`) to isolate project dependencies
- Follow **PEP 8** style conventions: snake_case for variables/functions, PascalCase for classes
- Use **type hints** to make function signatures clear
- Write **docstrings** for all functions and classes
- Use **logging** (not `print`) for production code — include severity levels
- Handle **exceptions** explicitly — never use bare `except:` clauses
- Use **context managers** (`with` blocks) for file and database connections — ensures cleanup
- Use **generators** for processing large files — memory-efficient
- Always **close database connections** or use `with` blocks

#### Spark Best Practices
- **Always define schema explicitly** — never rely on `inferSchema=True` in production
- **Use PERMISSIVE mode** with `_corrupt_record` column to capture bad data without crashing
- **Filter early** — apply WHERE/filter conditions as close to the data source as possible
- **Avoid `groupByKey()`** — prefer `reduceByKey()` for aggregations
- **Broadcast small tables** — tables under ~10MB should be broadcast to avoid expensive shuffle joins
- **Cache strategically** — cache DataFrames reused multiple times; always unpersist when done
- **Avoid `collect()` on large DataFrames** — use `show()`, `take()`, or `write()` instead
- **Partition appropriately** — target 2–4 partitions per CPU core; `repartition()` to increase, `coalesce()` to reduce
- **Use Parquet or Delta** for storage — columnar formats are orders of magnitude faster than CSV
- **Monitor via Spark UI** — check stage timings, skewed partitions, and disk spill regularly
- **Stop the session when done**: `spark.stop()`

#### Hive Best Practices
- Use **Beeline** instead of the legacy Hive CLI
- Set execution engine to **Tez or Spark** for better performance
- Use **ORC or Parquet** format for tables (much faster than TextFile)
- **Partition large tables** on frequently filtered columns (year, month, region) to enable partition pruning
- Avoid row-level updates in Hive — it is designed for bulk operations, not OLTP
- Use **external tables** for shared data

#### Kafka Best Practices
- Always set a **replication factor of at least 3** in production
- Use **key-based partitioning** when you need ordering guarantees per entity
- Monitor **ISR (In-Sync Replicas)** — a shrinking ISR indicates performance issues
- Use **schema registry** to enforce message schemas and prevent breaking changes
- Design **consumer groups** carefully — more consumers than partitions means idle consumers

#### Docker Best Practices
- **Keep images small** — use slim or alpine base images
- **One process per container** — don't run multiple services in the same container
- **Use `.dockerignore`** to exclude large files from the build context
- **Never hardcode secrets** — use environment variables or a secrets manager
- **Use named volumes** for production data persistence
- **Tag your images** with semantic versions — never rely solely on `latest`

#### Airflow Best Practices
- **Use the TaskFlow API** (`@dag`, `@task`) for new pipelines
- **Set `catchup=False`** unless backfilling is intentional
- **Keep tasks small and atomic** — each task should do one thing
- **Use XCom sparingly** — don't pass large datasets through XCom (write to S3/HDFS, pass the path)
- **Always set retry policies** — `retries=3, retry_delay=timedelta(minutes=5)`
- **Use Airflow connections** for all external service credentials — never hardcode
- **Document your DAGs** — use `doc_md` in the DAG definition

#### Databricks and Cloud Best Practices
- **Always define explicit schemas** when reading data
- **Use Delta format** for all production tables — reliability, time travel, and ACID are worth it
- **Use external tables** when data must persist independently of Databricks
- **Aggregate in Spark before converting to pandas** — never `toPandas()` on a large DataFrame
- **Use broadcast joins** for small dimension tables
- **Cache expensive joins/aggregations** that will be reused; trigger caching with `.count()`
- **Pause dedicated SQL pools** in Azure Synapse when not in use
- **Use managed identities** for authentication to cloud storage

---

## 24. How the Tools Connect — The Big Picture

### The Modern Data Engineering Stack

```
                        DATA SOURCES
+---------------------------------------------------------------+
| Operational DBs | REST APIs | IoT Sensors | Event Streams    |
| (MySQL, Postgres)| (JSON/XML) | (CSV/Binary)| (Kafka Topics) |
+-------+----------+-----+------+-------+--------+-------------+
        |                |              |               |
        v                v              v               v
+---------------------------------------------------------------+
|                    INGESTION LAYER                            |
|   Kafka (real-time streaming)  | ADF / Airflow (batch)       |
+---------------------------------------------------------------+
                                 |
                                 v
+---------------------------------------------------------------+
|            STORAGE LAYER — BRONZE (Raw)                      |
|    HDFS (on-premise) | S3 (AWS) | ADLS Gen2 (Azure) | GCS    |
+---------------------------------------------------------------+
                                 |
                                 v
+---------------------------------------------------------------+
|                  PROCESSING LAYER                            |
|       Apache Spark (batch + streaming + ML)                  |
|       Hive (SQL interface over HDFS/S3)                      |
|       Databricks (managed Spark + Delta Lake)                |
+---------------------------------------------------------------+
                                 |
                                 v
+---------------------------------------------------------------+
|     STORAGE LAYER — SILVER (Cleaned) & GOLD (Aggregated)     |
|    Parquet / Delta / ORC files on HDFS, S3, or ADLS          |
|    Hive Metastore (table definitions and schemas)            |
+---------------------------------------------------------------+
                                 |
                                 v
+---------------------------------------------------------------+
|                  SERVING LAYER                               |
|   Hive/Spark SQL | Synapse Serverless | Redshift / BigQuery  |
+---------------------------------------------------------------+
                                 |
                                 v
+---------------------------------------------------------------+
|             VISUALIZATION & CONSUMPTION                      |
|    Power BI | Tableau | Jupyter Notebooks | APIs             |
+---------------------------------------------------------------+
                                 |
              +------------------+------------------+
              v                  v                  v
       ORCHESTRATION       CONTAINERIZATION    RESOURCE MGMT
       Apache Airflow           Docker              YARN
```

### How Each Tool Relates to Others

| Tool | Relationship |
|---|---|
| **Hadoop HDFS** | Storage foundation. Both Spark and Hive read/write data here. YARN manages resources on the same cluster. |
| **YARN** | Resource manager that allocates CPU/RAM to Spark jobs, Hive queries, and other applications. |
| **MapReduce** | The original processing layer. Hive originally translated HQL to MR. Spark replaced MR as the default engine. |
| **Spark** | Core processing engine. Reads from HDFS/S3/ADLS. Uses YARN for cluster resources. Powers Hive's execution engine (when `set hive.execution.engine=spark`). Runs on Databricks as the underlying engine. |
| **Hive** | SQL interface over HDFS. Its **metastore** is shared with Spark (`enableHiveSupport()`). Provides schema/table definitions for Spark to use. |
| **Kafka** | Real-time event stream. Producers push events; Spark Streaming (or Flink) consumes and processes them. Output lands in HDFS/S3/Delta. |
| **Docker** | Packages Airflow, Spark, databases, and other tools into isolated, reproducible environments. Airflow is commonly deployed via Docker Compose (Astronomer). |
| **Airflow** | Orchestrates everything else. Triggers Spark jobs, Hive queries, Python scripts, and data transfers on schedule. Monitors pipeline health and handles failures. |
| **Databricks** | Managed Spark + Delta Lake. Integrates with cloud storage (S3/ADLS/GCS) and optionally with Airflow via the Databricks operator. |
| **Azure (ADF + ADLS + Synapse)** | Full cloud stack. ADF = ingestion (like Airflow but cloud-native). ADLS = storage layer. Synapse = processing + serving. Databricks = heavy transformation. |

### The Medallion Architecture — Connecting Everything

```
Bronze Layer (Raw)    <- Kafka ingests events    -> HDFS/S3/ADLS
                      <- ADF copies files        -> HDFS/S3/ADLS
                      <- Airflow schedules       -> Ingestion jobs

Silver Layer (Clean)  <- Spark reads Bronze      -> cleans, validates, types
                      <- Hive provides schema    -> for Spark to enforce
                      <- Stored as Delta/Parquet on HDFS/S3/ADLS

Gold Layer (Business) <- Spark aggregates Silver -> business-level metrics
                      <- Stored as Delta on HDFS/S3/ADLS
                      <- Registered in Hive Metastore as tables

Serving Layer         <- Hive/Spark SQL query Gold tables
                      <- Synapse CETAS materializes aggregations
                      <- Power BI / Tableau reads from Serving Layer

Orchestration         <- Airflow DAGs schedule and monitor all of the above
Containerization      <- Docker ensures consistent environments everywhere
```

---

## 25. Alternatives to Every Tool in This Stack

| Tool in Course | Alternatives | Key Differences |
|---|---|---|
| **Hadoop HDFS** | Amazon S3, Azure ADLS Gen2, Google GCS | Cloud object storage is cheaper, easier to manage. S3/ADLS/GCS are the modern default. HDFS is primarily on-premise. |
| **MapReduce** | Apache Spark, Apache Flink | Spark is the de facto replacement. Flink is preferred for stateful stream processing. MR is considered legacy. |
| **YARN** | Kubernetes (K8s), Apache Mesos | K8s is the modern standard for container orchestration. Mesos is largely deprecated. |
| **Apache Spark** | Apache Flink, Dask, Ray | Flink excels at true real-time streaming and stateful computations. Dask and Ray are Python-native for ML workloads on smaller clusters. Spark remains the gold standard for large-scale batch. |
| **Apache Hive** | Presto/Trino, AWS Athena, Google BigQuery, Snowflake | Presto/Trino are much faster for interactive SQL. BigQuery and Snowflake are fully managed cloud data warehouses with much lower operational overhead. |
| **Apache Kafka** | Amazon Kinesis, Azure Event Hubs, Google Pub/Sub, Apache Pulsar, RabbitMQ | Kinesis/Event Hubs/Pub/Sub are fully managed cloud equivalents. Pulsar is newer with multi-tenancy. RabbitMQ is for lower-throughput traditional queuing. |
| **Docker** | Podman, LXC/LXD, containerd | Podman is daemonless and rootless (better security). containerd is Docker's underlying runtime now used directly by Kubernetes. |
| **Apache Airflow** | Prefect, Dagster, Luigi, AWS Step Functions, Azure Logic Apps | Prefect and Dagster are modern, more Pythonic alternatives with better observability. Step Functions and Logic Apps are fully managed cloud-native orchestrators. |
| **Databricks** | Amazon EMR, Google Dataproc, Azure HDInsight, Cloudera | EMR/Dataproc/HDInsight are managed Spark services from each major cloud provider. Cheaper than Databricks but fewer features (no Delta Lake natively, less optimized runtime). |
| **Azure ADLS Gen2** | Amazon S3, Google GCS, Hadoop HDFS | All are distributed object/file storage. S3 and GCS are the AWS and GCP equivalents. HDFS is on-premise. |
| **Azure Data Factory** | Apache Airflow, AWS Glue, Talend | AWS Glue is the managed ETL equivalent on AWS. Talend is an enterprise data integration platform. |
| **Azure Synapse** | Amazon Redshift, Google BigQuery, Snowflake | Redshift and BigQuery are MPP data warehouses on AWS and GCP. Snowflake is cloud-agnostic. Synapse uniquely combines warehousing AND big data in one platform. |
| **MySQL** | PostgreSQL, SQLite, MariaDB | PostgreSQL is more feature-rich. SQLite is embedded and serverless (perfect for development). MariaDB is a MySQL fork with some performance improvements. |
| **MongoDB** | Cassandra, DynamoDB, Couchbase, Firestore | Cassandra is better for write-heavy, high-availability time-series use cases. DynamoDB is the fully managed AWS NoSQL equivalent. |

---

## 26. Course Summary

This course covered the complete modern data engineering stack, progressing from foundational concepts to production-grade systems.

**The progression:**

**Phase 1 — Foundations**: The fundamentals of Big Data — why traditional systems fall short, what distributed systems enable, the 5 V's, roles (engineer vs. analyst), ETL vs. ELT, and the database/warehouse/lake taxonomy. This gave you the mental model for everything that follows.

**Phase 2 — Hadoop Ecosystem**: The foundational distributed systems that shaped modern data engineering. HDFS taught you how large files are distributed, replicated, and fault-tolerantly stored across many machines. MapReduce showed you the original distributed processing paradigm and its limitations. YARN taught you resource management in a multi-application cluster. These are the intellectual ancestors of Spark.

**Phase 3 — Apache Spark (The Core)**: The largest and most important portion. Spark from the lowest level (RDDs, lineage, DAG, transformations vs. actions, narrow vs. wide, combiner) up to high-level APIs (DataFrames, Spark SQL, schema management, join strategies, caching). Architecture (standalone, YARN, deployment modes). Why in-memory processing makes Spark 10–100x faster.

**Phase 4 — Data Warehousing**: Apache Hive gave you the SQL interface over the distributed ecosystem. The Hive metastore is critical because both Spark and Databricks use it. Architecture from client to HDFS, query flow, HiveQL, and the distinction between Hive as a query interface and HDFS as the actual data store.

**Phase 5 — Real-Time Streaming**: Kafka completed the picture with real-time data ingestion. The producer-consumer-broker-partition model, offset management, replication and fault tolerance, and practical use via Confluent Cloud and CLI.

**Phase 6 — Infrastructure and Orchestration**: Docker for packaging and deploying pipelines consistently. Airflow for orchestrating, scheduling, and monitoring complex multi-step pipelines — transforming ad-hoc scripts into reliable production systems. Docker + Airflow is the backbone of most production data platforms.

**Phase 7 — Cloud and Modern Platforms**: Databricks showed the modern way to use Spark — managed, optimized, with Delta Lake for reliability and Unity Catalog for governance. The Azure final project tied everything together: ADLS for storage, ADF for ingestion, Databricks for processing, Synapse for serving, and Power BI for visualization — the complete real-world production stack.

**The unifying theme:** Every tool in this course exists to solve one of three fundamental problems:
1. **How to store data that doesn't fit on one machine** → HDFS, S3, ADLS, Delta Lake
2. **How to process data that doesn't fit in one machine's memory** → MapReduce, Spark, Hive, Databricks
3. **How to make it all run reliably and automatically** → YARN, Kafka, Docker, Airflow

Mastering these three dimensions — distributed storage, distributed processing, and orchestration/reliability — is what it means to be a data engineer.

---

## 27. Suggested Projects to Build

The best way to solidify this knowledge is to build things. Here are projects spanning the full stack, ordered from simpler to more complex.

### Project 1: Personal Finance Pipeline (Beginner)
**Tools:** Python, SQL (MySQL/SQLite), Pandas, Airflow (basic)

**What to build:** An automated pipeline that imports your bank transactions (CSV export), cleans the data with Python/Pandas, loads it into a local SQL database, and runs daily via Airflow. Add a spending-by-category summary report.

**Skills practiced:** Python data manipulation, SQL schema design, Airflow DAG creation, basic ETL, file operations.

---

### Project 2: Wikipedia Edit Stream (Intermediate)
**Tools:** Python, Kafka, Spark Streaming, HDFS/S3

**What to build:** Connect to the Wikipedia real-time edit stream (publicly available via EventStreams API), produce edits as Kafka messages, consume with Spark Streaming, aggregate edit counts by language and country in near-real-time, write results to a data lake.

**Skills practiced:** Kafka producer/consumer setup, Spark Structured Streaming, window aggregations, writing to persistent storage.

---

### Project 3: E-commerce Analytics Platform (Intermediate-Advanced)
**Tools:** Python, Spark, Hive (or Databricks), Airflow, Parquet/Delta, SQL

**What to build:** Use the Brazilian E-commerce Olist dataset (already familiar from this course). Build a complete pipeline: ingest CSVs → clean with Spark → join all tables → store in Parquet/Delta format → create Hive tables → orchestrate with Airflow → produce KPIs (revenue by state, top sellers, customer cohort analysis).

**Skills practiced:** Full Spark pipeline (ingest → clean → join → aggregate → store), Hive metastore integration, Airflow DAG with multiple Spark tasks, Medallion Architecture, data quality checks.

---

### Project 4: Real-Time Dashboard for IoT Sensor Data (Advanced)
**Tools:** Kafka, Spark Streaming, PostgreSQL, Airflow, Docker, Docker Compose, Python

**What to build:** Simulate IoT sensor readings (temperature, humidity) as a Python Kafka producer. Consume with Spark Streaming, detect anomalies (readings outside normal ranges), write clean data to PostgreSQL. Deploy everything locally with Docker Compose (Kafka + Zookeeper + Spark + PostgreSQL + Airflow). Airflow schedules daily aggregation reports.

**Skills practiced:** Kafka + Spark Streaming integration, anomaly detection in streams, Docker Compose multi-service setup, orchestrating streaming + batch in one platform.

---

### Project 5: Cloud-Native Data Lakehouse (Advanced)
**Tools:** Cloud platform (AWS/Azure/GCP), object storage (S3/ADLS/GCS), Spark/Databricks, Delta Lake, Airflow/ADF, SQL analytics (Synapse/Athena/BigQuery)

**What to build:** Choose a public dataset (NYC Taxi data, GitHub Archive, or Airbnb listings). Ingest raw data to cloud storage (Bronze). Schedule daily ingestion via Airflow (or ADF). Clean and transform with Spark/Databricks (Silver). Create business-level aggregations (Gold) in Delta format. Expose Gold layer via Synapse Serverless SQL or Athena. Connect to Power BI/Tableau for a dashboard.

**Skills practiced:** Full cloud pipeline, Medallion Architecture on real cloud storage, managed Spark service, serverless SQL serving, cost management.

---

### Project 6: ML Feature Store Pipeline (Advanced — MLOps)
**Tools:** Python, Spark/Databricks, Delta Lake, Airflow, MLflow, Docker

**What to build:** Build a feature engineering pipeline that: ingests raw data → computes ML features (with Spark) → stores them as Delta tables with versioning → triggers model retraining when new features arrive (via Airflow DAG) → logs experiments with MLflow → deploys the best model as a REST API (FastAPI in Docker).

**Skills practiced:** Feature engineering at scale, Delta Lake versioning (time travel for point-in-time correct features), Airflow for ML orchestration, MLflow experiment tracking, model serving with Docker.

---

### Quick-Start Tips for All Projects

1. **Start small**: Use a toy dataset to validate the pipeline end-to-end before scaling up
2. **Use version control**: Put everything in a Git repository from day one
3. **Document everything**: Write a README explaining what the pipeline does, data sources, schema, and how to run it
4. **Monitor and log**: Use Python logging, Airflow's built-in monitoring, and Spark UI from the start
5. **Use Docker Compose**: Even for local development, `docker-compose` ensures your environment is reproducible
6. **Write tests**: At minimum, test that your transformations handle null values and edge cases correctly
7. **Automate from day one**: If you can run it manually, the next step is to put it in Airflow

---

*This document was compiled from course notes taken between March and May 2026, enriched with explanations, code comments, best practices, ecosystem context, and project suggestions.*
