"""Benchmark report: end-to-end latency percentiles from the last local run.

Reads Bronze/Silver/Quarantine via delta-rs (no Spark) and reports:
  - source latency   (kafka_timestamp - event_time)
  - bronze processing (bronze_ingest_ts - kafka_timestamp)
  - silver processing (silver_commit_ts - bronze_ingest_ts)
  - end-to-end latency (silver_commit_ts - event_time)
  - throughput estimate and quarantine rate

All latencies measure wall-clock observability boundaries, not synthetic event time.

Usage: python scripts/local/benchmark.py [--csv out.csv]
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

import pandas as pd
from deltalake import DeltaTable

REPO_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = Path(os.environ.get("HVACPULSE_DATA_ROOT", str(REPO_ROOT / ".local")))


def load(name: str) -> pd.DataFrame:
    path = DATA_ROOT / "delta" / name
    if not (path / "_delta_log").exists():
        return pd.DataFrame()
    return DeltaTable(str(path)).to_pandas()


def percentiles(series: pd.Series) -> dict[str, float]:
    if series.empty:
        return {"p50": float("nan"), "p95": float("nan"), "p99": float("nan"), "max": float("nan")}
    q = series.quantile([0.5, 0.95, 0.99])
    return {
        "p50": round(float(q.loc[0.5]), 3),
        "p95": round(float(q.loc[0.95]), 3),
        "p99": round(float(q.loc[0.99]), 3),
        "max": round(float(series.max()), 3),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", type=Path, default=None, help="Optional CSV output path")
    args = parser.parse_args()

    silver = load("silver_telemetry_history")
    quarantine = load("quarantine_events")
    if silver.empty:
        print("No Silver data found. Run the local pipeline first.")
        return 1

    event_time = pd.to_datetime(silver["event_time"], utc=True)
    kafka_ts = pd.to_datetime(silver["kafka_timestamp"], utc=True)
    bronze_ts = pd.to_datetime(silver["bronze_ingest_ts"], utc=True)
    silver_ts = pd.to_datetime(silver["silver_commit_ts"], utc=True)

    rows = {
        "source_latency_s": percentiles((kafka_ts - event_time).dt.total_seconds()),
        "bronze_processing_s": percentiles((bronze_ts - kafka_ts).dt.total_seconds()),
        "silver_processing_s": percentiles((silver_ts - bronze_ts).dt.total_seconds()),
        "end_to_end_latency_s": percentiles((silver_ts - event_time).dt.total_seconds()),
    }
    report = pd.DataFrame(rows).T
    report.index.name = "metric"

    span_s = max((silver_ts.max() - event_time.min()).total_seconds(), 1.0)
    received = len(silver) + len(quarantine)
    summary = {
        "silver_rows": len(silver),
        "quarantine_rows": len(quarantine),
        "quarantine_rate": round(len(quarantine) / received, 4) if received else 0.0,
        "devices": int(silver["device_id"].nunique()),
        "span_seconds": round(span_s, 1),
        "avg_throughput_eps": round(received / span_s, 2),
    }

    print("== Latency percentiles (seconds) ==")
    print(report.to_string())
    print("\n== Run summary ==")
    for key, value in summary.items():
        print(f"{key}: {value}")

    if args.csv:
        report.to_csv(args.csv)
        print(f"\nwrote {args.csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
