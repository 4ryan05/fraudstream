"""SQLite storage and the watcher that loads Flink's committed output files."""
import hashlib
import json
import os
import sqlite3
import threading
import time

SCHEMA = """
CREATE TABLE IF NOT EXISTS transactions (
    txn_id TEXT PRIMARY KEY, batch_id TEXT, user_id TEXT, amount REAL, merchant TEXT, city TEXT,
    event_time TEXT, event_ms INTEGER, status TEXT, reasons TEXT, avg_before REAL, count_60s INTEGER,
    subtask INTEGER, flink_ms INTEGER, stored_ms INTEGER
);
CREATE INDEX IF NOT EXISTS idx_txn_status ON transactions(status);
CREATE INDEX IF NOT EXISTS idx_txn_event ON transactions(event_ms);
CREATE TABLE IF NOT EXISTS invalid_records (
    id TEXT PRIMARY KEY, txn_id TEXT, batch_id TEXT, reason TEXT, raw TEXT, flink_ms INTEGER, stored_ms INTEGER
);
CREATE TABLE IF NOT EXISTS city_minute (
    city TEXT, window_start INTEGER, window_end INTEGER, total INTEGER, normal INTEGER,
    suspicious INTEGER, fraud INTEGER, amount REAL, fired INTEGER DEFAULT 1, flink_ms INTEGER,
    PRIMARY KEY (city, window_start)
);
CREATE TABLE IF NOT EXISTS batches (
    batch_id TEXT PRIMARY KEY, source TEXT, label TEXT, records INTEGER, created_ms INTEGER
);
CREATE TABLE IF NOT EXISTS ingested_files (
    path TEXT PRIMARY KEY, kind TEXT, rows INTEGER, ingested_ms INTEGER
);
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value INTEGER);
"""

KINDS = ("scored", "invalid", "city_minute")


def now_ms():
    return int(time.time() * 1000)


class Store:
    def __init__(self, db_path):
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.lock = threading.Lock()
        with self.lock:
            self.conn.executescript(SCHEMA)
            self.conn.commit()

    def query(self, sql, params=()):
        with self.lock:
            return [dict(r) for r in self.conn.execute(sql, params).fetchall()]

    def one(self, sql, params=()):
        rows = self.query(sql, params)
        return rows[0] if rows else None

    def execute(self, sql, params=()):
        with self.lock:
            self.conn.execute(sql, params)
            self.conn.commit()

    def add_batch(self, batch_id, source, label, records):
        self.execute("INSERT OR REPLACE INTO batches VALUES (?,?,?,?,?)", (batch_id, source, label, records, now_ms()))

    def reset(self):
        """Clears the dashboard data. Ingested file names are kept so old files are not loaded again."""
        with self.lock:
            for table in ("transactions", "invalid_records", "city_minute", "batches", "meta"):
                self.conn.execute(f"DELETE FROM {table}")
            self.conn.commit()

    def bump(self, cur, key, by):
        cur.execute("INSERT INTO meta VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = value + ?", (key, by, by))

    # -- loading Flink output ------------------------------------------------
    def ingest_file(self, kind, path):
        with open(path, encoding="utf-8") as f:
            lines = [json.loads(line) for line in f if line.strip()]
        stored = now_ms()
        with self.lock:
            cur = self.conn.cursor()
            if kind == "scored":
                before = cur.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
                cur.executemany(
                    "INSERT OR IGNORE INTO transactions VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    [(t["txn_id"], t.get("batch_id"), t["user_id"], t["amount"], t["merchant"], t["city"],
                      t["timestamp"], t["event_ms"], t["status"], json.dumps(t["reasons"]), t["avg_before"],
                      t["count_60s"], t["subtask"], t["flink_ms"], stored) for t in lines],
                )
                inserted = cur.execute("SELECT COUNT(*) FROM transactions").fetchone()[0] - before
                self.bump(cur, "scored_delivered", len(lines))
                self.bump(cur, "duplicates", len(lines) - inserted)
            elif kind == "invalid":
                cur.executemany(
                    "INSERT OR IGNORE INTO invalid_records VALUES (?,?,?,?,?,?,?)",
                    [(hashlib.sha1(r["raw"].encode()).hexdigest(), r.get("txn_id"), r.get("batch_id"),
                      r["reason"], r["raw"], r["flink_ms"], stored) for r in lines],
                )
            else:
                # Windows fire again for late records; the newest firing replaces the older one.
                for w in lines:
                    cur.execute(
                        """INSERT INTO city_minute VALUES (?,?,?,?,?,?,?,?,1,?)
                           ON CONFLICT(city, window_start) DO UPDATE SET total=excluded.total,
                           normal=excluded.normal, suspicious=excluded.suspicious, fraud=excluded.fraud,
                           amount=excluded.amount, fired=fired+1, flink_ms=excluded.flink_ms""",
                        (w["city"], w["window_start"], w["window_end"], w["total"], w["normal"],
                         w["suspicious"], w["fraud"], w["amount"], w["flink_ms"]),
                    )
            cur.execute("INSERT INTO ingested_files VALUES (?,?,?,?)", (path, kind, len(lines), stored))
            self.conn.commit()

    def watch(self, output_dir, interval=1.0):
        def loop():
            while True:
                try:
                    known = {r["path"] for r in self.query("SELECT path FROM ingested_files")}
                    for kind in KINDS:
                        folder = os.path.join(output_dir, kind)
                        if not os.path.isdir(folder):
                            continue
                        # Committed part files only; in-progress files are hidden (start with '.').
                        for name in sorted(os.listdir(folder)):
                            path = os.path.join(folder, name)
                            if name.startswith(".") or path in known or not os.path.isfile(path):
                                continue
                            self.ingest_file(kind, path)
                except Exception as exc:  # keep watching even if one file is bad
                    print("watcher error:", exc, flush=True)
                time.sleep(interval)

        threading.Thread(target=loop, daemon=True).start()
