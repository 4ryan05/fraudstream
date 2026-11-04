# Big Data Analytics (BISBD713): Activity Based Assessment
## Topic Proposal

**Title:** FraudStream: Real-Time UPI Transaction Fraud Detection Using Apache Flink

**Tool:** Apache Flink (open-source distributed stream processing engine)

**Team:** _(names and USNs)_

**Guide:** Prof. Spoorthi M

---

### 1. Objective
Build a working pipeline that ingests a continuous stream of simulated UPI transactions, uses Apache Flink to detect suspicious and fraudulent activity in real time, and shows the results on a live dashboard.

### 2. Why Apache Flink
- It is a true stream processor: it handles each event as it arrives, while Spark works in batches or micro-batches.
- It supports event-time processing, watermarks, windows and keyed state, which are the main building blocks of real-time analytics.
- It is fault tolerant through checkpointing and exactly-once state guarantees.
- It belongs to the Hadoop ecosystem: it runs on YARN and reads and writes HDFS.
- No other team has chosen it.

### 3. Architecture
```
Python Simulator / CSV upload
        │  (UPI transactions: txn_id, user_id, amount, merchant, city, timestamp)
        ▼
Apache Flink job (PyFlink)
   keyBy(user_id) → windows + keyed state → rules → tag: NORMAL / SUSPICIOUS / FRAUD
        ▼
Database (SQLite / PostgreSQL)
        ▼
FastAPI backend → Web dashboard (live counts, fraud rate, flagged users, per-minute chart)
```

### 4. Fraud Detection Rules (implemented in Flink)
| Rule | Flink concept used | Result |
|---|---|---|
| More than 5 transactions by one user in 1 minute | Tumbling window | SUSPICIOUS |
| Amount more than 10× the user's running average | Keyed state | FRAUD |
| Same user in 2 different cities within 2 minutes | Sliding window | FRAUD |
| Missing fields or a negative amount | Filter / side output | INVALID |

### 5. Demonstration
1. Run Flink with `docker compose up` and open the Flink Web UI (job graph, parallelism, throughput).
2. Start the simulator. Normal and fraud transactions show up on the dashboard within seconds.
3. Fault-tolerance demo: stop the TaskManager mid-stream, restart it, and show that Flink resumes from the last checkpoint without losing data.
4. Show results: total transactions, fraud rate, top flagged users, and alerts per minute.

### 6. Syllabus Mapping
| Syllabus item | How the project covers it |
|---|---|
| Module 1: Scalability, parallel processing, data pre-processing, data quality | Parallel keyed operators, validation of incoming records |
| Module 2: Hadoop ecosystem tools, YARN | Flink as a Hadoop-ecosystem engine (YARN / HDFS support) |
| Module 5: Data analysis, ETL process, analytics, reporting and visualizing | Streaming ETL, real-time analytics, dashboard |
| CO4: Demonstrate a modern Hadoop tool to produce data analytics | The full working project and report |

### 7. Deliverables
- Working project (Docker setup, PyFlink job, simulator, dashboard)
- Report in the standard format: tool introduction, tool description, how it is used, execution procedure, output and results, conclusion
- Live demo during the presentation

---
**Backup topics (if Flink is not approved):** Apache HBase (patient vitals store), Trino/Presto (federated query across multiple databases), Apache Druid (real-time clickstream OLAP).
