# FraudStream: Real-Time UPI Fraud Detection with Apache Flink

Big Data Analytics (BISBD713) activity project. A simulator produces UPI transactions. An Apache Flink streaming job validates and scores each transaction against the user's recent behaviour, then aggregates the results in 1-minute event-time windows. A dashboard shows the results live.

```
Simulator / CSV ──► data/incoming/*.jsonl ──► Apache Flink job ──► data/output/{scored,invalid,city_minute}
                                                (validate, keyed state,        │  (files committed on
                                                 fraud rules, windows)         │   every checkpoint)
                                                                               ▼
                                          Dashboard (http://localhost:8000) ◄── FastAPI + SQLite
```

## Requirements
- Docker Desktop with Docker Compose (8 GB RAM recommended)
- A web browser. The dashboard loads Chart.js from a CDN, so you need internet access.

## Run it
```bash
docker compose up -d --build
```
The first build downloads Flink and PyFlink, which takes a few minutes. The `job-submit` container waits for Flink and then submits the job. Check that it worked:

```bash
docker compose logs -f job-submit
```

| URL | What |
|---|---|
| http://localhost:8000 | FraudStream dashboard |
| http://localhost:8081 | Apache Flink Web UI (job graph, checkpoints, back pressure) |
| http://localhost:8000/docs | API documentation |

## Demo script
1. **Start the live stream** on the dashboard (3 txn/s, 300 simulated users across 8 Karnataka cities).
2. Wait about 2 minutes so Flink builds a spending profile for each user. The per-minute chart fills as event-time windows close.
3. Click each fraud scenario:
   - **Velocity burst**: 8 payments in 20 s. From the 6th payment onward the result is SUSPICIOUS.
   - **Big-ticket payment**: 15× the user's average. The result is FRAUD.
   - **Impossible travel**: the same user in two cities 45 s apart. The result is FRAUD.
   - **Invalid records**: 6 bad records go to the Flink side output and appear in the *Rejected records* tab.
4. **Upload CSV** with `backend/sample_transactions.csv`. The timestamps are shifted so the last row is "now". Expected result: 13 normal, 2 suspicious (demo-14, demo-15), 2 fraud (demo-08, demo-16), 3 rejected (demo-18, 19, 20).
5. **Fault tolerance:**
   ```bash
   docker compose stop taskmanager
   ```
   The job goes to RESTARTING, while the simulator keeps writing files to `data/incoming`.
   ```bash
   docker compose start taskmanager
   ```
   Flink restores the last checkpoint and catches up. On the *Flink job* tab, "Restored from checkpoint #N" appears and **Duplicate rows stays 0**.

## Fraud rules (in `flink/fraud_job.py`)
| Rule | Flink feature | Result |
|---|---|---|
| More than 5 payments by one user in the trailing 60 s | keyed state | SUSPICIOUS |
| Amount > 10× the user's running average (after 3 normal payments) | keyed state | FRAUD |
| Payments from 2 different cities within 2 minutes (the account stays flagged for 2 minutes) | keyed state | FRAUD |
| Malformed JSON, a missing field, an amount ≤ 0 or > Rs 1,00,000, or a bad timestamp | side output | rejected |
| Per-city counts per minute | tumbling event-time windows, watermarks (5 s out-of-orderness), allowed lateness 10 min | window output |

## Useful commands
```bash
docker compose ps                         # container status
docker compose logs -f taskmanager        # Flink worker logs (Python errors show here)
docker compose down                       # stop everything
docker compose down -v; rm -rf data       # stop and delete all data and Flink checkpoints
```
To resubmit the job after changing `fraud_job.py`, cancel it in the Flink UI and then run:
```bash
docker compose up -d --build job-submit
```

## Project layout
```
docker-compose.yml        Flink JobManager + TaskManager, job submitter, backend
flink/Dockerfile          flink:1.20.1 + Python 3 + apache-flink (PyFlink)
flink/fraud_job.py        the Flink job
flink/submit.sh           waits for Flink and submits the job once
backend/main.py           FastAPI: simulator controls, results API, Flink REST proxy
backend/simulator.py      UPI transaction generator, fraud scenarios, CSV replay
backend/store.py          SQLite schema and the loader for Flink's committed output files
backend/static/index.html dashboard (4 tabs)
backend/sample_transactions.csv
```
