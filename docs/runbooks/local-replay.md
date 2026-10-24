# Runbook: local replay and failure recovery

## Replay from Kafka (short-term, operational)

Kafka retains 7 days locally (`KAFKA_LOG_RETENTION_DAYS`). To replay everything
still in the topic into a *fresh* Bronze table:

```powershell
# Point Bronze at a new table + checkpoint; do NOT reuse the old checkpoint.
$env:HVACPULSE_DATA_ROOT = (Resolve-Path .).Path + "\.local-replay"
.\scripts\local\run_local_stack.ps1 -Task bronze
.\scripts\local\run_local_stack.ps1 -Task silver
.\scripts\local\run_local_stack.ps1 -Task gold
```

Expected result: Silver row count equals the previous run's Silver row count
(retries are no-ops), and Gold metrics are unchanged (acceptance criteria).

## Replay from Bronze (long-term, analytical)

Bronze is the system of record. To rebuild Silver+Gold after Kafka retention
expired:

```powershell
# Keep bronze_telemetry, delete derived tables and the silver/gold checkpoints.
Remove-Item -Recurse -Force .\.local\delta\silver_telemetry_history
Remove-Item -Recurse -Force .\.local\delta\silver_telemetry_current
Remove-Item -Recurse -Force .\.local\delta\quarantine_events
Remove-Item -Recurse -Force .\.local\delta\gold_hvac_metrics_*
Remove-Item -Recurse -Force .\.local\checkpoints\silver
.\scripts\local\run_local_stack.ps1 -Task silver
.\scripts\local\run_local_stack.ps1 -Task gold
```

## Simulating a pipeline crash

1. Start Bronze without `--once`: `python -m streaming.jobs.bronze_ingest`
   (continuous mode).
2. Kill the process mid-run (Ctrl+C), produce more events, then rerun with
   `--once`. The checkpoint resumes from the last committed Kafka offsets —
   no manual offset management, no duplicates in Bronze.

## Inspecting quarantine

```python
from streaming.spark_session import create_spark
from streaming.config import load_settings
spark = create_spark("inspect", delta=True)
s = load_settings()
spark.read.format("delta").load(s.quarantine_path) \
    .groupBy("error_code").count().show()
```

Each quarantine row carries `payload_hex` (original bytes), the Kafka
coordinates, and the first hard-failure `error_code`.

## Checkpoints

Checkpoints live under `.local/checkpoints/<query>` and are tied to their
query. Delete a checkpoint only when retiring or resetting that pipeline
stage — deleting the Bronze checkpoint while keeping the Bronze table causes
the topic to be re-ingested on top of existing data (Delta append), which
breaks the exactly-once guarantee.
