#!/usr/bin/env bash
# Waits for the JobManager, then submits the fraud job unless it is already running.
set -e
JM="${JOBMANAGER:-jobmanager:8081}"

until flink list -m "$JM" >/dev/null 2>&1; do
  echo "Waiting for the Flink JobManager at $JM ..."
  sleep 3
done

if flink list -r -m "$JM" 2>/dev/null | grep -q "FraudStream"; then
  echo "FraudStream job is already running."
  exit 0
fi

echo "Submitting FraudStream job ..."
flink run -m "$JM" -d -py /opt/flink/usrlib/fraud_job.py -- --data-dir /opt/flink/data
