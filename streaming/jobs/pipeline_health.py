"""Pipeline health check over ops_pipeline_stage_runs and ops_dataset_publications.

Reports every stage the environment must run (reference/monitoring.yml) as
HEALTHY, RUNNING, IDLE, DELAYED, STUCK, FAILED, NOT_READY or MISSING, and
fails when any stage is critical. It reads only the ops tables, so a stopped
job is detected even though its last Gold output still looks fine.

Usage: python -m streaming.jobs.pipeline_health
"""

from __future__ import annotations

import argparse
import time

from pyspark.sql.functions import col
from pyspark.sql.functions import lit

from streaming.config import Settings
from streaming.config import load_settings
from streaming.ops import CRITICAL
from streaming.ops import StageHealth
from streaming.ops import evaluate_stage
from streaming.ops import load_monitoring
from streaming.ops import profile_for
from streaming.spark_session import create_spark

_EPOCH_COLUMNS = ("event_at", "started_at", "finished_at")


def _recent_rows(spark, settings: Settings, target: str, time_column: str, since: float) -> list[dict]:
    if not settings.delta_exists(spark, target):
        return []
    frame = settings.read_delta(spark, target)
    # Epoch seconds: collected Spark timestamps would be converted to the driver's local time zone.
    for name in _EPOCH_COLUMNS:
        if name in frame.columns:
            frame = frame.withColumn(name, col(name).cast("double"))
    since_column = col(time_column) if time_column in _EPOCH_COLUMNS else col(time_column).cast("double")
    return [row.asDict() for row in frame.filter(since_column >= lit(since)).collect()]


def check_health(spark, settings: Settings, *, now: float | None = None) -> list[StageHealth]:
    monitoring = load_monitoring()
    now = time.time() if now is None else now
    since = now - monitoring.lookback_hours * 3600
    stage_rows = _recent_rows(spark, settings, settings.ops_stage_runs_path, "event_at", since)
    publications = _recent_rows(spark, settings, settings.ops_publications_path, "recorded_at", since)
    return [
        evaluate_stage(policy, stage_rows, publications, monitoring, now)
        for policy in monitoring.profiles[profile_for(settings)]
    ]


def run(settings: Settings) -> list[StageHealth]:
    spark = create_spark("hvacpulse-pipeline-health", delta=True)
    results = check_health(spark, settings)
    width = max(len(r.stage_name) for r in results)
    print(f"=== HVACPulse pipeline health ({profile_for(settings)}) ===")
    for result in results:
        print(f"{result.stage_name:<{width}}  {result.status:<9}  {result.detail}")
    critical = [r.stage_name for r in results if r.status in CRITICAL]
    print("Pipeline health:", f"UNHEALTHY ({', '.join(critical)})" if critical else "OK")
    return results


def main(argv: list[str] | None = None) -> int:
    argparse.ArgumentParser(description="Pipeline health check").parse_args(argv)
    results = run(load_settings())
    return 1 if any(r.status in CRITICAL for r in results) else 0


if __name__ == "__main__":
    raise SystemExit(main())
