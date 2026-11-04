"""FraudStream: real-time UPI transaction fraud detection with Apache Flink.

Pipeline (PyFlink DataStream API):

  FileSource (data/incoming, monitored every second)
    -> Validate          (ProcessFunction, rejects go to a side output)
    -> key_by(user_id)
    -> FraudDetector     (KeyedProcessFunction with per-user keyed state)
    -> FileSink scored/  (exactly-once, files committed on every checkpoint)
    -> event-time watermarks
    -> key_by(city) -> 1-minute tumbling event-time windows -> FileSink city_minute/
  invalid side output -> FileSink invalid/

Every record is one JSON object per line.
"""
import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path

from pyflink.common import Configuration, Duration, Encoder, Types, WatermarkStrategy
from pyflink.common.time import Time
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import OutputTag, StreamExecutionEnvironment
from pyflink.datastream.connectors.file_system import (
    BucketAssigner,
    FileSink,
    FileSource,
    OutputFileConfig,
    RollingPolicy,
    StreamFormat,
)
from pyflink.datastream.functions import KeyedProcessFunction, ProcessFunction, ProcessWindowFunction
from pyflink.datastream.state import StateTtlConfig, ValueStateDescriptor
from pyflink.datastream.window import TumblingEventTimeWindows

# Fraud rules
VELOCITY_WINDOW_MS = 60_000       # rule 1: more than VELOCITY_LIMIT txns in 60 s -> SUSPICIOUS
VELOCITY_LIMIT = 5
AMOUNT_MULTIPLIER = 10            # rule 2: amount > 10x the user's running average -> FRAUD
MIN_HISTORY = 3                   #         (only once the user has 3 normal txns)
TRAVEL_WINDOW_MS = 120_000        # rule 3: two different cities within 2 minutes -> FRAUD
UPI_LIMIT = 100_000               # validation: UPI per-transaction limit in rupees

REQUIRED_FIELDS = ("txn_id", "user_id", "amount", "merchant", "city", "timestamp")
INVALID_TAG = OutputTag("invalid", Types.STRING())


def now_ms():
    return int(time.time() * 1000)


def parse_ts(value):
    dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


class Validate(ProcessFunction):
    """Checks each raw line; valid transactions continue, rejects go to the invalid side output."""

    def process_element(self, value, ctx):
        reason, txn = None, None
        try:
            txn = json.loads(value)
            if not isinstance(txn, dict):
                raise ValueError
        except ValueError:
            reason = "malformed JSON"

        if reason is None:
            missing = [f for f in REQUIRED_FIELDS if txn.get(f) in (None, "")]
            if missing:
                reason = "missing field(s): " + ", ".join(missing)
        if reason is None:
            try:
                txn["amount"] = round(float(txn["amount"]), 2)
            except (TypeError, ValueError):
                reason = "amount is not numeric"
        if reason is None and txn["amount"] <= 0:
            reason = "amount must be positive"
        if reason is None and txn["amount"] > UPI_LIMIT:
            reason = "amount exceeds UPI limit of Rs 1,00,000"
        if reason is None:
            try:
                txn["event_ms"] = parse_ts(txn["timestamp"])
            except (TypeError, ValueError):
                reason = "invalid timestamp"

        if reason is None:
            yield json.dumps(txn)
        else:
            batch_id = txn.get("batch_id") if isinstance(txn, dict) else None
            txn_id = txn.get("txn_id") if isinstance(txn, dict) else None
            yield INVALID_TAG, json.dumps({
                "txn_id": txn_id,
                "batch_id": batch_id,
                "reason": reason,
                "raw": value,
                "flink_ms": now_ms(),
            })


class FraudDetector(KeyedProcessFunction):
    """Scores each transaction against the user's recent history kept in Flink keyed state."""

    def open(self, runtime_context):
        descriptor = ValueStateDescriptor("user_profile", Types.PICKLED_BYTE_ARRAY())
        descriptor.enable_time_to_live(StateTtlConfig.new_builder(Time.hours(6)).build())
        self.profile = runtime_context.get_state(descriptor)
        self.subtask = runtime_context.get_index_of_this_subtask()

    def process_element(self, value, ctx):
        txn = json.loads(value)
        t, amount, city = txn["event_ms"], txn["amount"], txn["city"]

        profile = self.profile.value() or {"count": 0, "total": 0.0, "recent": [], "max_ts": 0}
        profile["max_ts"] = max(profile["max_ts"], t)
        recent = [r for r in profile["recent"] if r[0] > profile["max_ts"] - TRAVEL_WINDOW_MS]
        recent.append((t, city))

        reasons, status = [], "NORMAL"
        avg = profile["total"] / profile["count"] if profile["count"] else None

        if avg is not None and profile["count"] >= MIN_HISTORY and amount > AMOUNT_MULTIPLIER * avg:
            reasons.append(f"amount Rs {amount:,.0f} is {amount / avg:.1f}x the user's average Rs {avg:,.0f}")
            status = "FRAUD"

        cities = sorted({c for ts, c in recent if abs(ts - t) <= TRAVEL_WINDOW_MS})
        if len(cities) > 1:
            reasons.append("used in " + " and ".join(cities) + " within 2 minutes")
            status = "FRAUD"

        count_60s = sum(1 for ts, _ in recent if t - VELOCITY_WINDOW_MS < ts <= t)
        if count_60s > VELOCITY_LIMIT:
            reasons.append(f"{count_60s} transactions within 60 seconds")
            if status == "NORMAL":
                status = "SUSPICIOUS"

        # Only normal spending updates the running average, so a fraud does not hide the next one.
        if status != "FRAUD":
            profile["count"] += 1
            profile["total"] += amount
        profile["recent"] = recent
        self.profile.update(profile)

        txn.update({
            "status": status,
            "reasons": reasons,
            "avg_before": round(avg, 2) if avg is not None else None,
            "count_60s": count_60s,
            "subtask": self.subtask,
            "flink_ms": now_ms(),
        })
        yield json.dumps(txn)


class EventTime(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        return json.loads(value)["event_ms"]


class CityMinuteStats(ProcessWindowFunction):
    """Totals per city for one 1-minute event-time window."""

    def process(self, key, context, elements):
        counts = {"NORMAL": 0, "SUSPICIOUS": 0, "FRAUD": 0}
        amount = 0.0
        for e in elements:
            txn = json.loads(e)
            counts[txn["status"]] += 1
            amount += txn["amount"]
        window = context.window()
        yield json.dumps({
            "city": key,
            "window_start": window.start,
            "window_end": window.end,
            "total": sum(counts.values()),
            "normal": counts["NORMAL"],
            "suspicious": counts["SUSPICIOUS"],
            "fraud": counts["FRAUD"],
            "amount": round(amount, 2),
            "flink_ms": now_ms(),
        })


def file_sink(path, prefix):
    return (
        FileSink.for_row_format(path, Encoder.simple_string_encoder())
        .with_bucket_assigner(BucketAssigner.base_path_bucket_assigner())
        .with_output_file_config(OutputFileConfig.builder().with_part_prefix(prefix).with_part_suffix(".jsonl").build())
        .with_rolling_policy(RollingPolicy.on_checkpoint_rolling_policy())
        .build()
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", default="/opt/flink/data")
    parser.add_argument("--parallelism", type=int, default=2)
    parser.add_argument("--local-rest-port", type=int, help="only for running without a cluster")
    args = parser.parse_args()
    data = Path(args.data_dir).resolve()
    uri = lambda sub: (data / sub).as_uri()

    config = Configuration()
    if args.local_rest_port:
        config.set_string("rest.port", str(args.local_rest_port))
    env = StreamExecutionEnvironment.get_execution_environment(config)
    env.set_parallelism(args.parallelism)
    env.enable_checkpointing(5_000)  # output files are committed on every checkpoint (exactly-once)
    env.get_checkpoint_config().set_checkpoint_storage_dir(uri("checkpoints"))

    source = (
        FileSource.for_record_stream_format(StreamFormat.text_line_format(), uri("incoming"))
        .monitor_continuously(Duration.of_seconds(1))
        .build()
    )
    lines = env.from_source(source, WatermarkStrategy.no_watermarks(), "UPI transaction files").set_parallelism(1)

    valid = lines.process(Validate(), output_type=Types.STRING()).name("Validate")
    valid.get_side_output(INVALID_TAG).sink_to(file_sink(uri("output/invalid"), "invalid")).name("Invalid sink")

    scored = (
        valid.key_by(lambda s: json.loads(s)["user_id"], key_type=Types.STRING())
        .process(FraudDetector(), output_type=Types.STRING())
        .name("Fraud detector")
    )
    scored.sink_to(file_sink(uri("output/scored"), "scored")).name("Scored sink")

    watermarks = (
        WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(5))
        .with_timestamp_assigner(EventTime())
        .with_idleness(Duration.of_seconds(10))
    )
    (
        scored.assign_timestamps_and_watermarks(watermarks)
        .key_by(lambda s: json.loads(s)["city"], key_type=Types.STRING())
        .window(TumblingEventTimeWindows.of(Time.minutes(1)))
        .allowed_lateness(10 * 60_000)  # late records re-fire the window; the backend upserts it
        .process(CityMinuteStats(), output_type=Types.STRING())
        .name("City x minute window")
        .sink_to(file_sink(uri("output/city_minute"), "city_minute"))
        .name("Window sink")
    )

    env.execute("FraudStream - UPI Fraud Detection")


if __name__ == "__main__":
    main()
