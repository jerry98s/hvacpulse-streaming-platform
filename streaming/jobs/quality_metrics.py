"""Gold observability outputs: DQ scorecard, freshness, compliance, health.

All four outputs are batch-recomputed from Silver + Quarantine + reference
limits, so re-running the job is idempotent (full overwrite per table).

Usage: python -m streaming.jobs.quality_metrics
"""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg
from pyspark.sql.functions import broadcast
from pyspark.sql.functions import coalesce
from pyspark.sql.functions import col
from pyspark.sql.functions import count
from pyspark.sql.functions import countDistinct
from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import lit
from pyspark.sql.functions import max as max_
from pyspark.sql.functions import round as round_
from pyspark.sql.functions import sum as sum_
from pyspark.sql.functions import when
from pyspark.sql.functions import window

from quality.signals import load_equipment_limits
from streaming.config import Settings
from streaming.config import load_settings
from streaming.spark_session import create_spark

WINDOW = "5 minutes"


def _limits_df(spark: SparkSession, settings: Settings):
    # Reference data is deliberately small. Loading it on the driver avoids
    # requiring every serverless executor to resolve a Workspace file path.
    rows = [
        {
            "device_id": limits.device_id,
            "rated_power_kw": limits.rated_power_kw,
            "temp_min_c": limits.temp_min_c,
            "temp_max_c": limits.temp_max_c,
            "humidity_min_pct": limits.humidity_min_pct,
            "humidity_max_pct": limits.humidity_max_pct,
            "expected_report_interval_sec": limits.expected_report_interval_sec,
            "commissioning_status": limits.commissioning_status,
        }
        for limits in load_equipment_limits(settings.known_devices_csv).values()
    ]
    return spark.createDataFrame(rows)


def build_dq_scorecard(spark: SparkSession, settings: Settings):
    """Per site/device/5-minute window: received vs valid vs quarantined."""
    silver = (
        settings.read_delta(spark, settings.silver_history_path)
        .groupBy("site_id", "device_id", window(col("bronze_ingest_ts"), WINDOW))
        .agg(
            count("*").alias("valid_events"),
            # NULL-rate SLA: critical measurements
            round_(
                sum_(when(col("zone_temp_c").isNull(), 1).otherwise(0)) / count("*"), 4
            ).alias("null_zone_temp_rate"),
            round_(
                sum_(when(col("power_kw").isNull(), 1).otherwise(0)) / count("*"), 4
            ).alias("null_power_rate"),
            round_(
                sum_(when(col("relative_humidity_pct").isNull(), 1).otherwise(0)) / count("*"), 4
            ).alias("null_humidity_rate"),
        )
    )
    if settings.delta_exists(spark, settings.quarantine_path):
        quarantine = (
            settings.read_delta(spark, settings.quarantine_path)
            .groupBy("site_id", "device_id", window(col("ingest_ts"), WINDOW))
            .agg(
                count("*").alias("quarantined_events"),
                countDistinct("error_code").alias("distinct_error_codes"),
                sum_(when(col("error_code") == "IDEMPOTENCY_CONFLICT", 1).otherwise(0)).alias(
                    "idempotency_conflicts"
                ),
            )
        )
    else:
        quarantine = None

    if quarantine is not None:
        silver = silver.alias("s")
        quarantine = quarantine.alias("q")
        joined = silver.join(
            quarantine,
            (silver["site_id"] == quarantine["site_id"])
            & (silver["device_id"] == quarantine["device_id"])
            & (silver["window"] == quarantine["window"]),
            "full",
        ).select(
            coalesce(silver["site_id"], quarantine["site_id"]).alias("site_id"),
            coalesce(silver["device_id"], quarantine["device_id"]).alias("device_id"),
            coalesce(silver["window"], quarantine["window"]).alias("window"),
            "valid_events",
            col("quarantined_events"),
            col("distinct_error_codes"),
            col("idempotency_conflicts"),
            col("null_zone_temp_rate"),
            col("null_power_rate"),
            col("null_humidity_rate"),
        )
    else:
        joined = (
            silver.withColumn("quarantined_events", lit(0))
            .withColumn("distinct_error_codes", lit(0))
            .withColumn("idempotency_conflicts", lit(0))
        )

    return (
        joined.fillna(
            0,
            subset=[
                "valid_events",
                "quarantined_events",
                "distinct_error_codes",
                "idempotency_conflicts",
            ],
        )
        .withColumn("received_events", col("valid_events") + col("quarantined_events"))
        .withColumn(
            "quality_score",
            when(col("received_events") > 0, round_(col("valid_events") / col("received_events"), 4)),
        )
        .withColumn(
            "dq_status",
            when(col("quality_score") < 0.9, "FAIL")
            .when(
                (col("null_zone_temp_rate") > 0.05)
                | (col("null_power_rate") > 0.05)
                | (col("null_humidity_rate") > 0.05),
                "WARN",
            )
            .otherwise("PASS"),
        )
        .withColumn("window_start", col("window.start"))
        .withColumn("window_end", col("window.end"))
        .drop("window")
        .withColumn("window_date", col("window_start").cast("date"))
    )


def build_sensor_freshness(spark: SparkSession, settings: Settings):
    """Per device: last observed event vs expected reporting interval."""
    silver = settings.read_delta(spark, settings.silver_current_path)
    limits = _limits_df(spark, settings)
    return (
        silver.groupBy("site_id", "device_id")
        .agg(max_("event_time").alias("last_event_time"), count("*").alias("total_events"))
        # Reference limits are tiny (a few KB): broadcast bypasses the shuffle.
        .join(broadcast(limits), "device_id", "left")
        .withColumn(
            "age_seconds",
            (current_timestamp().cast("long") - col("last_event_time").cast("long")),
        )
        .withColumn(
            "is_stale",
            col("age_seconds") > col("expected_report_interval_sec") * lit(3.0),
        )
        .withColumn("computed_at", current_timestamp())
    )


def build_compliance(spark: SparkSession, settings: Settings):
    """Per device/5-minute window: time outside facility environmental limits."""
    silver = settings.read_delta(spark, settings.silver_current_path)
    limits = _limits_df(spark, settings)
    enriched = silver.join(broadcast(limits), "device_id", "left")
    flagged = enriched.withColumn(
        "temp_violation",
        col("zone_temp_c").isNotNull()
        & ((col("zone_temp_c") < col("temp_min_c")) | (col("zone_temp_c") > col("temp_max_c"))),
    ).withColumn(
        "humidity_violation",
        col("relative_humidity_pct").isNotNull()
        & (
            (col("relative_humidity_pct") < col("humidity_min_pct"))
            | (col("relative_humidity_pct") > col("humidity_max_pct"))
        ),
    )
    return (
        flagged.groupBy("site_id", "device_id", window(col("event_time"), WINDOW))
        .agg(
            count("*").alias("events"),
            sum_(when(col("temp_violation"), 1).otherwise(0)).alias("temp_violation_events"),
            sum_(when(col("humidity_violation"), 1).otherwise(0)).alias("humidity_violation_events"),
            avg("zone_temp_c").alias("avg_zone_temp_c"),
            avg("relative_humidity_pct").alias("avg_humidity_pct"),
            sum_(
                (when(col("temp_violation"), 1).otherwise(0) + when(col("humidity_violation"), 1).otherwise(0))
                * col("expected_report_interval_sec")
            ).alias("_violation_seconds"),
        )
        .withColumn("window_start", col("window.start"))
        .withColumn("window_end", col("window.end"))
        .drop("window")
        .withColumn("violation_minutes", round_(col("_violation_seconds") / lit(60.0), 3))
        .drop("_violation_seconds")
        .withColumn("window_date", col("window_start").cast("date"))
    )


def build_equipment_health(spark: SparkSession, settings: Settings):
    """Per device: cross-field inconsistency counts over the current state."""
    silver = settings.read_delta(spark, settings.silver_current_path)
    rated = _limits_df(spark, settings).select("device_id", "rated_power_kw")
    enriched = silver.join(broadcast(rated), "device_id", "left")
    return (
        enriched.withColumn(
            "not_running_high_power",
            (col("equipment_running") == lit(False))
            & (col("power_kw") > col("rated_power_kw") * lit(0.30)),
        )
        .withColumn(
            "fan_stopped_high_airflow",
            (col("fan_speed_pct") == 0.0) & (col("airflow_m3h") > lit(1000.0)),
        )
        .withColumn(
            "valve_closed_high_cooling",
            (col("chilled_water_valve_pct") == 0.0)
            & (col("return_air_temp_c") - col("supply_air_temp_c") > lit(2.0)),
        )
        .groupBy("site_id", "device_id")
        .agg(
            count("*").alias("events"),
            sum_(when(col("not_running_high_power"), 1).otherwise(0)).alias(
                "not_running_high_power_events"
            ),
            sum_(when(col("fan_stopped_high_airflow"), 1).otherwise(0)).alias(
                "fan_stopped_high_airflow_events"
            ),
            sum_(when(col("valve_closed_high_cooling"), 1).otherwise(0)).alias(
                "valve_closed_high_cooling_events"
            ),
        )
        .withColumn(
            "has_active_anomaly",
            (col("not_running_high_power_events") > 0)
            | (col("fan_stopped_high_airflow_events") > 0)
            | (col("valve_closed_high_cooling_events") > 0),
        )
        .withColumn("computed_at", current_timestamp())
    )


def run(settings: Settings) -> None:
    spark = create_spark("hvacpulse-quality-metrics", delta=True)
    outputs = {
        settings.gold_dq_scorecard_path: build_dq_scorecard(spark, settings),
        settings.gold_sensor_freshness_path: build_sensor_freshness(spark, settings),
        settings.gold_compliance_path: build_compliance(spark, settings),
        settings.gold_equipment_health_path: build_equipment_health(spark, settings),
    }
    for path, df in outputs.items():
        settings.write_delta(df, path, mode="overwrite", overwrite_schema=True)
        print(f"wrote {path}")


def main(argv: list[str] | None = None) -> int:
    argparse.ArgumentParser(description="Gold quality/freshness/compliance job").parse_args(argv)
    run(load_settings())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
