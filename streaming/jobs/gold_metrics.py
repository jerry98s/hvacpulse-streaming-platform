"""Silver → Gold windowed metrics with versioning and restatement.

Gold current  (keyed site_id + device_id + window_start)      — idempotent MERGE
Gold history  (key + metric_version, append-only)             — every published
                                                                  version kept

A window whose inputs changed after publication is republished with an
incremented ``metric_version`` and status RESTATED; the previous version stays
in history so a customer report can be reproduced as of any date.

Energy treats each reading as covering its device's expected report interval
from ``reference/equipment_master.csv``:
``energy_kwh = sum(power_kw * expected_report_interval_sec) / 3600``. Replace
with measured per-event deltas when real devices report at variable cadence.

Usage: python -m streaming.jobs.gold_metrics
"""

from __future__ import annotations

import argparse
from datetime import datetime
from datetime import timedelta
from datetime import timezone

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg
from pyspark.sql.functions import coalesce
from pyspark.sql.functions import col
from pyspark.sql.functions import concat_ws
from pyspark.sql.functions import count
from pyspark.sql.functions import create_map
from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import lit
from pyspark.sql.functions import max as max_
from pyspark.sql.functions import round as round_
from pyspark.sql.functions import sha2
from pyspark.sql.functions import sum as sum_
from pyspark.sql.functions import to_date
from pyspark.sql.functions import when
from pyspark.sql.functions import window

from quality.signals import load_equipment_limits
from streaming.config import Settings
from streaming.config import load_settings
from streaming.spark_session import create_spark

CALCULATION_VERSION = "gold-v2"
GOLD_KEY = ["site_id", "device_id", "window_start"]
METRIC_VALUE_COLUMNS = [
    "event_count",
    "avg_power_kw",
    "max_power_kw",
    "avg_zone_temp_c",
    "avg_humidity_pct",
    "energy_kwh",
]
ROUNDED_COLUMNS = [c for c in METRIC_VALUE_COLUMNS if c != "event_count"]
METRIC_DECIMALS = 6

OPEN = "OPEN"
FINAL = "FINAL"
RESTATED = "RESTATED"


def compute_window_metrics(
    silver_current: DataFrame,
    report_interval_sec: dict[str, float],
    *,
    window_duration: str = "1 minute",
    finalize_after: timedelta = timedelta(minutes=10),
    now: datetime | None = None,
) -> DataFrame:
    """Complete-window aggregates from the current (latest-revision) Silver state."""
    if not report_interval_sec or any(v <= 0 for v in report_interval_sec.values()):
        raise ValueError("Every device needs a positive expected report interval")
    now = now or datetime.now(timezone.utc)
    finalize_before = now - finalize_after
    interval = create_map(*[lit(x) for item in report_interval_sec.items() for x in item])[col("device_id")]
    metrics = (
        silver_current.groupBy("site_id", "device_id", window(col("event_time"), window_duration))
        .agg(
            count("*").alias("event_count"),
            avg("power_kw").alias("avg_power_kw"),
            max_("power_kw").alias("max_power_kw"),
            sum_(col("power_kw") * interval).alias("_power_seconds"),
            avg("zone_temp_c").alias("avg_zone_temp_c"),
            avg("relative_humidity_pct").alias("avg_humidity_pct"),
        )
        .withColumn("window_start", col("window.start"))
        .withColumn("window_end", col("window.end"))
        .drop("window")
        .withColumn("energy_kwh", col("_power_seconds") / lit(3600.0))
        .drop("_power_seconds")
        # Spark double sums vary in the last bits with partition order; rounding
        # keeps that noise out of _content_hash so it never restates a window.
        .withColumns({c: round_(col(c), METRIC_DECIMALS) for c in ROUNDED_COLUMNS})
        .withColumn(
            "window_status",
            when(col("window_end") < lit(finalize_before), lit(FINAL)).otherwise(lit(OPEN)),
        )
    )
    content = concat_ws("|", *[coalesce(col(c).cast("string"), lit("")) for c in METRIC_VALUE_COLUMNS])
    return metrics.withColumn("_content_hash", sha2(content, 256))


def _merge_history(
    spark: SparkSession, settings: Settings, published: DataFrame
) -> None:
    """Publish each business-window version once, including after job retries."""
    if published.limit(1).count() == 0:
        return
    if not settings.delta_exists(spark, settings.gold_history_path):
        settings.write_delta(published, settings.gold_history_path, partition_by=["window_date"])
        return
    target = settings.delta_table(spark, settings.gold_history_path)
    history_key = [*GOLD_KEY, "metric_version"]
    condition = " AND ".join(f"t.{column} = s.{column}" for column in history_key)
    target.alias("t").merge(published.alias("s"), condition).whenNotMatchedInsertAll().execute()


def publish_gold(spark: SparkSession, metrics: DataFrame, settings: Settings) -> None:
    """Versioned publish: append new/changed versions to history, merge current."""
    metrics = metrics.persist()

    if settings.delta_exists(spark, settings.gold_current_path):
        existing = settings.read_delta(spark, settings.gold_current_path).select(
            *GOLD_KEY,
            col("metric_version").alias("_prior_version"),
            col("_content_hash").alias("_prior_hash"),
            col("window_status").alias("_prior_status"),
            col("restated_at").alias("_prior_restated_at"),
        )
        joined = metrics.join(existing, GOLD_KEY, "left").withColumn(
            "_content_changed",
            col("_prior_hash").isNull() | (col("_prior_hash") != col("_content_hash")),
        ).withColumn(
            "_status_changed",
            col("_prior_status").isNull() | (col("_prior_status") != col("window_status")),
        ).withColumn(
            "_changed",
            col("_content_changed") | col("_status_changed"),
        )
        versioned = (
            joined.withColumn(
                "metric_version",
                when(col("_prior_hash").isNull(), lit(1)).otherwise(
                    when(col("_changed"), col("_prior_version") + 1).otherwise(col("_prior_version"))
                ),
            )
            .withColumn(
                "window_status",
                when(col("_prior_hash").isNull(), col("window_status"))
                .when(
                    col("_content_changed")
                    & col("_prior_status").isin(FINAL, RESTATED),
                    lit(RESTATED),
                )
                .when(~col("_content_changed") & (col("_prior_status") == lit(RESTATED)), lit(RESTATED))
                .otherwise(col("window_status")),
            )
            .withColumn(
                "restated_at",
                when(
                    col("_content_changed") & col("_prior_status").isin(FINAL, RESTATED),
                    current_timestamp(),
                ).otherwise(col("_prior_restated_at")),
            )
            .drop("_prior_version", "_prior_hash", "_prior_status", "_prior_restated_at")
        )
        to_publish = versioned.filter(col("_changed")).drop("_changed", "_content_changed", "_status_changed")
        versioned = versioned.drop("_changed", "_content_changed", "_status_changed")
    else:
        versioned = metrics.withColumn("metric_version", lit(1)).withColumn(
            "restated_at", lit(None).cast("timestamp")
        )
        to_publish = versioned

    published = to_publish.withColumn("published_at", current_timestamp()).withColumn(
        "calculation_version", lit(CALCULATION_VERSION)
    ).withColumn("window_date", to_date(col("window_start")))
    _merge_history(spark, settings, published)

    versioned = versioned.withColumn("window_date", to_date(col("window_start")))
    if not settings.delta_exists(spark, settings.gold_current_path):
        settings.write_delta(versioned, settings.gold_current_path, partition_by=["window_date"])
    else:
        target = settings.delta_table(spark, settings.gold_current_path)
        condition = " AND ".join(f"t.{k} = s.{k}" for k in GOLD_KEY)
        (
            target.alias("t")
            .merge(versioned.alias("s"), condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    metrics.unpersist()


def run(settings: Settings) -> None:
    spark = create_spark("hvacpulse-gold-metrics", delta=True)
    silver_current = settings.read_delta(spark, settings.silver_current_path)
    intervals = {
        device: limits.expected_report_interval_sec
        for device, limits in load_equipment_limits(settings.known_devices_csv).items()
    }
    metrics = compute_window_metrics(silver_current, intervals)
    publish_gold(spark, metrics, settings)


def main(argv: list[str] | None = None) -> int:
    argparse.ArgumentParser(description="Gold metrics job").parse_args(argv)
    run(load_settings())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
