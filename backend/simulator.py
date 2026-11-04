"""Generates UPI transactions and writes them as JSON-lines files for Flink to pick up."""
import csv
import io
import json
import os
import random
import threading
import time
from datetime import datetime, timedelta, timezone

CITIES = ["Mysuru", "Bengaluru", "Mangaluru", "Hubballi", "Belagavi", "Kalaburagi", "Davanagere", "Shivamogga"]
MERCHANTS = [
    "Swiggy", "Zomato", "Amazon", "Flipkart", "BigBasket", "Reliance Fresh", "Indian Oil", "HP Petrol",
    "BookMyShow", "IRCTC", "Apollo Pharmacy", "More Supermarket", "Croma", "Myntra", "Local Kirana", "BESCOM",
]


def iso(dt):
    return dt.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def utcnow():
    return datetime.now(timezone.utc)


class Simulator:
    def __init__(self, incoming_dir, on_batch):
        self.incoming = incoming_dir
        self.on_batch = on_batch  # callback(batch_id, source, label, records)
        rng = random.Random(42)
        self.users = {
            f"U{1001 + i}": {"city": rng.choice(CITIES), "avg": round(rng.lognormvariate(6.2, 0.6), 2)}
            for i in range(300)
        }
        self.rng = random.Random()
        self.seq = 0
        self.lock = threading.Lock()
        self.rate = 3
        self.running = False
        self.thread = None

    # -- writing ---------------------------------------------------------------
    def write_batch(self, source, label, records, raw_lines=()):
        """Writes records atomically: a hidden temp file is renamed, so Flink never sees half a file."""
        with self.lock:
            self.seq += 1
            batch_id = f"{source}-{int(time.time() * 1000)}-{self.seq}"
        lines = []
        for i, r in enumerate(records):
            r = dict(r, batch_id=batch_id)
            r.setdefault("txn_id", f"{batch_id}-{i}")
            lines.append(json.dumps(r))
        lines.extend(raw_lines)
        tmp = os.path.join(self.incoming, f".{batch_id}.tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            f.write("\n".join(lines) + "\n")
        os.replace(tmp, os.path.join(self.incoming, f"{batch_id}.jsonl"))
        self.on_batch(batch_id, source, label, len(lines))
        return batch_id, len(lines)

    def txn(self, user_id, when, amount=None, city=None):
        u = self.users[user_id]
        if amount is None:
            amount = max(10.0, self.rng.gauss(u["avg"], u["avg"] * 0.35))
        return {
            "user_id": user_id,
            "amount": round(amount, 2),
            "merchant": self.rng.choice(MERCHANTS),
            "city": city or u["city"],
            "timestamp": iso(when),
        }

    # -- live stream -----------------------------------------------------------
    def start(self, rate=None):
        if rate:
            self.rate = max(1, min(20, int(rate)))
        if not self.running:
            self.running = True
            self.thread = threading.Thread(target=self._loop, daemon=True)
            self.thread.start()

    def stop(self):
        self.running = False

    def _loop(self):
        while self.running:
            now = utcnow()
            n = sum(1 for _ in range(self.rate * 2) if self.rng.random() < 0.5) or 1
            records = [
                self.txn(self.rng.choice(list(self.users)), now - timedelta(milliseconds=self.rng.randint(0, 999)))
                for _ in range(n)
            ]
            self.write_batch("stream", f"{n} live transactions", records)
            time.sleep(1)

    # -- fraud scenarios -------------------------------------------------------
    def attack(self, kind, user_id=None):
        user_id = user_id if user_id in self.users else self.rng.choice(list(self.users))
        u, now = self.users[user_id], utcnow()
        if kind == "velocity":
            records = [self.txn(user_id, now - timedelta(seconds=20 - i * 2.5)) for i in range(8)]
            label = f"Velocity burst: 8 payments in 20 s by {user_id}"
        elif kind == "amount":
            records = [self.txn(user_id, now, amount=min(99_000, u["avg"] * 15))]
            label = f"Big-ticket payment: 15x average by {user_id}"
        elif kind == "travel":
            far = self.rng.choice([c for c in CITIES if c != u["city"]])
            records = [self.txn(user_id, now - timedelta(seconds=45)), self.txn(user_id, now, city=far)]
            label = f"Impossible travel: {user_id} in {u['city']} then {far} 45 s later"
        elif kind == "invalid":
            base = self.txn(user_id, now)
            records = [
                dict(base, amount=-250),
                {k: v for k, v in base.items() if k != "city"},
                dict(base, amount=150_000),
                dict(base, amount="five hundred"),
                dict(base, timestamp="yesterday evening"),
            ]
            self.write_batch("attack", "Invalid records (6)", records, raw_lines=['{"user_id": "U1001", "amount": 12'])
            return "Invalid records (6)"
        else:
            raise ValueError(f"unknown scenario {kind}")
        self.write_batch("attack", label, records)
        return label

    # -- CSV upload ------------------------------------------------------------
    def upload_csv(self, text, filename="upload.csv"):
        """Replays a CSV. Timestamps are shifted so the last row is 'now' and the gaps are kept."""
        rows = list(csv.DictReader(io.StringIO(text.lstrip("﻿"))))
        if not rows:
            raise ValueError("CSV has no rows")
        parsed = []
        for r in rows:
            try:
                parsed.append(datetime.fromisoformat(r.get("timestamp", "").replace("Z", "+00:00")))
            except ValueError:
                parsed.append(None)
        valid = [p for p in parsed if p is not None]
        shift = (utcnow() - max(valid)) if valid else timedelta(0)

        records = []
        for r, ts in zip(rows, parsed):
            rec = {k.strip(): v.strip() for k, v in r.items() if k and v not in (None, "")}
            if ts is not None:
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                rec["timestamp"] = iso(ts + shift)
            if "amount" in rec:
                try:
                    rec["amount"] = float(rec["amount"])
                except ValueError:
                    pass
            records.append(rec)
        with self.lock:
            prefix = f"csv{self.seq + 1}"
        for rec in records:
            if "txn_id" in rec:
                rec["txn_id"] = f"{prefix}:{rec['txn_id']}"
        batch_id, n = self.write_batch("csv", f"CSV upload: {filename}", records)
        return batch_id, n
