"""Reconciliation job: track late corrections and Gold restatements.

1. ``late_corrections`` — Silver revisions (revision_number > 1) that arrived
   later than the watermark, so they may affect already-FINAL Gold windows.
   Idempotent: merge on (event_id, revision_number).
2. ``metric_restatement_events`` — one event per RESTATED Gold version: a
   FINAL window whose content changed. OPEN windows that update while still
   receiving data are provisional and produce no event.
   Idempotent: merge on (device_id, window_start, metric_version).

Usage: python -m streaming.jobs.reconciliation
"""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import expr
from pyspark.sql.functions import lit
from pyspark.sql.functions import to_date

from streaming.config import Settings
from streaming.config import load_settings
from streaming.jobs.gold_metrics import RESTATED
from streaming.spark_session import create_spark

WATERMARK_INTERVAL = "interval 10 minutes"


def reconcile_late_corrections(spark: SparkSession, settings: Settings) -> int:
    late = (
        settings.read_delta(spark, settings.silver_history_path)
        .filter(col("revision_number") > 1)
        .filter(col("bronze_ingest_ts") > col("event_time") + expr(WATERMARK_INTERVAL))
        .select(
            "event_id",
            "device_id",
            "site_id",
            "event_time",
            col("bronze_ingest_ts").alias("ingest_ts"),
            "revision_number",
            "supersedes_event_id",
            "payload_hash",
        )
        .withColumn("tracked_at", current_timestamp())
        .withColumn("ingest_date", to_date(col("ingest_ts")))
    )
    if settings.delta_exists(spark, settings.late_corrections_path):
        if late.limit(1).count() == 0:
            return 0
        target = settings.delta_table(spark, settings.late_corrections_path)
        (
            target.alias("t")
            .merge(
                late.alias("s"),
                "t.event_id = s.event_id AND t.revision_number = s.revision_number",
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        settings.write_delta(late, settings.late_corrections_path, partition_by=["ingest_date"])
    return late.count()


def reconcile_restatements(spark: SparkSession, settings: Settings) -> int:
    if not settings.delta_exists(spark, settings.gold_history_path):
        return 0
    restated = (
        settings.read_delta(spark, settings.gold_history_path)
        .filter(col("window_status") == lit(RESTATED))
        .select(
            "site_id",
            "device_id",
            "window_start",
            "window_end",
            "metric_version",
            "window_status",
            "restated_at",
            "_content_hash",
        )
        .withColumnRenamed("_content_hash", "content_hash")
        .withColumn("event_type", lit("METRIC_RESTATED"))
        .withColumn("tracked_at", current_timestamp())
        .withColumn("window_date", to_date(col("window_start")))
    )
    if settings.delta_exists(spark, settings.restatement_events_path):
        if restated.limit(1).count() == 0:
            return 0
        target = settings.delta_table(spark, settings.restatement_events_path)
        (
            target.alias("t")
            .merge(
                restated.alias("s"),
                "t.device_id = s.device_id AND t.window_start = s.window_start"
                " AND t.metric_version = s.metric_version",
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        settings.write_delta(restated, settings.restatement_events_path, partition_by=["window_date"])
    return restated.count()


def run(settings: Settings) -> None:
    spark = create_spark("hvacpulse-reconciliation", delta=True)
    late_count = reconcile_late_corrections(spark, settings)
    restated_count = reconcile_restatements(spark, settings)
    print(f"late corrections tracked: {late_count}; restatement events: {restated_count}")


def main(argv: list[str] | None = None) -> int:
    argparse.ArgumentParser(description="Late-correction reconciliation job").parse_args(argv)
    run(load_settings())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
