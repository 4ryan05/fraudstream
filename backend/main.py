"""FraudStream backend: simulator controls, Flink output loader and dashboard API."""
import json
import os
import urllib.request
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles

from simulator import Simulator
from store import Store, now_ms

HERE = Path(__file__).parent
DATA_DIR = Path(os.environ.get("DATA_DIR", HERE.parent / "data"))
DB_PATH = Path(os.environ.get("DB_PATH", DATA_DIR / "fraudstream.db"))
FLINK_URL = os.environ.get("FLINK_URL", "http://localhost:8081").rstrip("/")
INCOMING = DATA_DIR / "incoming"
OUTPUT = DATA_DIR / "output"

# Flink runs as another user inside its container, so the shared folders are left writable.
for d in (INCOMING, OUTPUT, DATA_DIR / "checkpoints", DB_PATH.parent):
    d.mkdir(parents=True, exist_ok=True)
    try:
        os.chmod(d, 0o777)
    except OSError:
        pass

store = Store(str(DB_PATH))
sim = Simulator(str(INCOMING), store.add_batch)
store.watch(str(OUTPUT))

app = FastAPI(title="FraudStream API")


# -- simulator -------------------------------------------------------------------
@app.get("/api/stream")
def stream_status():
    return {"running": sim.running, "rate": sim.rate}


@app.post("/api/stream/start")
def stream_start(rate: int = 3):
    sim.start(rate)
    return stream_status()


@app.post("/api/stream/stop")
def stream_stop():
    sim.stop()
    return stream_status()


@app.post("/api/attack/{kind}")
def attack(kind: str):
    # Big-ticket fraud needs a user Flink already has a spending average for.
    user = store.one(
        "SELECT user_id FROM transactions WHERE status = 'NORMAL' AND user_id LIKE 'U1%' "
        "GROUP BY user_id HAVING COUNT(*) >= 3 ORDER BY RANDOM() LIMIT 1"
    )
    try:
        label = sim.attack(kind, user["user_id"] if user else None)
    except ValueError as exc:
        raise HTTPException(400, str(exc))
    return {"label": label}


@app.post("/api/upload")
async def upload(request: Request, filename: str = "upload.csv"):
    text = (await request.body()).decode("utf-8", errors="replace")
    try:
        batch_id, n = sim.upload_csv(text, filename)
    except (ValueError, KeyError) as exc:
        raise HTTPException(400, f"Could not read CSV: {exc}")
    return {"batch_id": batch_id, "records": n}


@app.get("/api/sample.csv")
def sample_csv():
    return FileResponse(HERE / "sample_transactions.csv", media_type="text/csv", filename="sample_transactions.csv")


@app.post("/api/reset")
def reset():
    sim.stop()
    store.reset()
    return {"ok": True}


# -- results ---------------------------------------------------------------------
@app.get("/api/health")
def health():
    return {"status": "ok", "time": now_ms()}


@app.get("/api/summary")
def summary():
    t = store.one(
        """SELECT COUNT(*) total,
                  SUM(status='NORMAL') normal, SUM(status='SUSPICIOUS') suspicious, SUM(status='FRAUD') fraud,
                  COALESCE(SUM(amount), 0) amount, COALESCE(SUM(CASE WHEN status='FRAUD' THEN amount END), 0) fraud_amount,
                  AVG(CASE WHEN batch_id LIKE 'stream-%' THEN flink_ms - event_ms END) flink_latency,
                  AVG(CASE WHEN batch_id LIKE 'stream-%' THEN stored_ms - event_ms END) e2e_latency
           FROM transactions"""
    )
    invalid = store.one("SELECT COUNT(*) n FROM invalid_records")["n"]
    written = store.one("SELECT COALESCE(SUM(records), 0) n FROM batches")["n"]
    meta = {r["key"]: r["value"] for r in store.query("SELECT key, value FROM meta")}
    seen = t["total"] + invalid
    return {
        **{k: (t[k] or 0) for k in ("total", "normal", "suspicious", "fraud", "amount", "fraud_amount")},
        "invalid": invalid,
        "fraud_rate": round(100 * (t["fraud"] or 0) / t["total"], 2) if t["total"] else 0,
        "flagged_rate": round(100 * ((t["fraud"] or 0) + (t["suspicious"] or 0)) / t["total"], 2) if t["total"] else 0,
        "flink_latency_ms": round(t["flink_latency"] or 0),
        "e2e_latency_ms": round(t["e2e_latency"] or 0),
        "records_written": written,
        "records_pending": max(0, written - seen),
        "delivered": meta.get("scored_delivered", 0),
        "duplicates": meta.get("duplicates", 0),
        "stream": stream_status(),
    }


def decode(rows):
    for r in rows:
        if "reasons" in r:
            r["reasons"] = json.loads(r["reasons"] or "[]")
    return rows


@app.get("/api/transactions")
def transactions(status: str = "", limit: int = 50):
    where, params = ("WHERE status = ?", (status.upper(),)) if status else ("", ())
    return decode(store.query(f"SELECT * FROM transactions {where} ORDER BY event_ms DESC LIMIT ?", (*params, limit)))


@app.get("/api/users")
def users(limit: int = 15):
    return store.query(
        """SELECT user_id, COUNT(*) txns, SUM(status='FRAUD') fraud, SUM(status='SUSPICIOUS') suspicious,
                  ROUND(SUM(amount), 2) amount, MAX(event_time) last_seen,
                  GROUP_CONCAT(DISTINCT city) cities
           FROM transactions GROUP BY user_id HAVING fraud + suspicious > 0
           ORDER BY fraud DESC, suspicious DESC, txns DESC LIMIT ?""",
        (limit,),
    )


@app.get("/api/windows")
def windows(minutes: int = 15):
    """Output of Flink's 1-minute tumbling event-time windows."""
    rows = store.query(
        "SELECT * FROM city_minute WHERE window_start >= (SELECT MAX(window_start) FROM city_minute) - ? "
        "ORDER BY window_start, city",
        ((minutes - 1) * 60_000,),
    )
    return rows


@app.get("/api/subtasks")
def subtasks():
    return store.query(
        "SELECT subtask, COUNT(*) txns, COUNT(DISTINCT user_id) users FROM transactions GROUP BY subtask ORDER BY subtask"
    )


@app.get("/api/invalid")
def invalid(limit: int = 50):
    return {
        "reasons": store.query("SELECT reason, COUNT(*) n FROM invalid_records GROUP BY reason ORDER BY n DESC"),
        "records": store.query("SELECT * FROM invalid_records ORDER BY stored_ms DESC LIMIT ?", (limit,)),
    }


@app.get("/api/batches")
def batches(limit: int = 20):
    return store.query(
        """SELECT b.*, (SELECT COUNT(*) FROM transactions t WHERE t.batch_id = b.batch_id)
                     + (SELECT COUNT(*) FROM invalid_records i WHERE i.batch_id = b.batch_id) processed
           FROM batches b ORDER BY created_ms DESC LIMIT ?""",
        (limit,),
    )


@app.get("/api/files")
def files():
    pending = len([n for n in os.listdir(INCOMING) if n.endswith(".jsonl")])
    by_kind = store.query("SELECT kind, COUNT(*) files, SUM(rows) rows FROM ingested_files GROUP BY kind")
    return {"incoming_files": pending, "committed": by_kind}


# -- Flink REST API --------------------------------------------------------------
def flink_get(path):
    with urllib.request.urlopen(FLINK_URL + path, timeout=1.5) as resp:
        return json.load(resp)


@app.get("/api/flink")
def flink():
    try:
        overview = flink_get("/overview")
    except Exception:
        return {"reachable": False, "url": FLINK_URL}
    info = {
        "reachable": True,
        "version": overview.get("flink-version"),
        "taskmanagers": overview.get("taskmanagers"),
        "slots_total": overview.get("slots-total"),
        "slots_available": overview.get("slots-available"),
        "job": None,
    }
    jobs = flink_get("/jobs/overview").get("jobs", [])
    jobs = [j for j in jobs if j["name"].startswith("FraudStream")] or jobs
    if not jobs:
        return info
    order = {"RUNNING": 0, "RESTARTING": 1, "CREATED": 2, "INITIALIZING": 3}
    job = sorted(jobs, key=lambda j: (order.get(j["state"], 9), -j["start-time"]))[0]
    jid = job["jid"]
    detail = flink_get(f"/jobs/{jid}")
    ckpt = flink_get(f"/jobs/{jid}/checkpoints")
    latest = (ckpt.get("latest") or {})
    completed, restored = latest.get("completed") or {}, latest.get("restored") or {}
    restarts = 0
    try:
        restarts = int(flink_get(f"/jobs/{jid}/metrics?get=numRestarts")[0]["value"])
    except Exception:
        pass
    info["job"] = {
        "id": jid,
        "name": job["name"],
        "state": job["state"],
        "start_time": job["start-time"],
        "duration": job["duration"],
        "restarts": restarts,
        "vertices": [
            {"name": v["name"], "parallelism": v["parallelism"], "status": v["status"],
             "read": v.get("metrics", {}).get("read-records", 0), "write": v.get("metrics", {}).get("write-records", 0)}
            for v in detail.get("vertices", [])
        ],
        "checkpoints": {
            "completed": ckpt.get("counts", {}).get("completed", 0),
            "failed": ckpt.get("counts", {}).get("failed", 0),
            "restored": ckpt.get("counts", {}).get("restored", 0),
            "latest_id": completed.get("id"),
            "latest_time": completed.get("latest_ack_timestamp"),
            "latest_duration": completed.get("end_to_end_duration"),
            "latest_size": completed.get("checkpointed_size", completed.get("state_size")),
            "restored_id": restored.get("id"),
            "restored_time": restored.get("restore_timestamp"),
        },
    }
    return info


@app.get("/", include_in_schema=False)
def index():
    return FileResponse(HERE / "static" / "index.html")


app.mount("/static", StaticFiles(directory=HERE / "static"), name="static")


@app.exception_handler(FileNotFoundError)
def not_found(_, exc):
    return PlainTextResponse(str(exc), status_code=404)
