"""Advisory recommendation job: Silver current + quality gates → Gold recommendations.

Spark computes per-device 5-minute window aggregates; rule evaluation then runs
on the driver through ``streaming.recommendations`` (single tested
implementation, advisory path only — never a physical control action).

Revocation: if a Gold window that previously produced a recommendation is
RESTATED, the original recommendation is revoked via a compensating event and
a replacement is emitted only if the corrected data still passes every gate.

Usage: python -m streaming.jobs.recommendations_job
"""

from __future__ import annotations

import argparse
from datetime import datetime
from datetime import timedelta
from datetime import timezone

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg
from pyspark.sql.functions import col
from pyspark.sql.functions import count
from pyspark.sql.functions import lit
from pyspark.sql.functions import max as max_
from pyspark.sql.functions import row_number
from pyspark.sql.functions import window
from pyspark.sql.types import MapType
from pyspark.sql.types import StringType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType
from pyspark.sql.types import TimestampType
from pyspark.sql.window import Window

from quality import signals
from streaming import recommendations as rec
from streaming.config import Settings
from streaming.config import load_settings
from streaming.spark_session import create_spark

WINDOW = "5 minutes"
RECENT_QUALITY_WINDOW = timedelta(minutes=5)

RECOMMENDATION_SCHEMA = StructType(
    [
        StructField("recommendation_id", StringType(), False),
        StructField("device_id", StringType(), False),
        StructField("site_id", StringType(), False),
        StructField("window_start", TimestampType(), False),
        StructField("window_end", TimestampType(), False),
        StructField("recommendation_type", StringType(), False),
        StructField("rule_version", StringType(), False),
        StructField("mode", StringType(), False),
        StructField("rationale", MapType(StringType(), StringType()), True),
        StructField("status", StringType(), False),
        StructField("created_at", TimestampType(), False),
    ]
)

REVOCATION_SCHEMA = StructType(
    [
        StructField("event_type", StringType(), False),
        StructField("recommendation_id", StringType(), False),
        StructField("revoked_at", StringType(), False),
        StructField("reason", StringType(), False),
        StructField("replacement_recommendation_id", StringType(), True),
    ]
)


def _window_aggregates(silver_current: DataFrame) -> DataFrame:
    return (
        silver_current.groupBy("site_id", "device_id", window(col("event_time"), WINDOW))
        .agg(
            count("*").alias("event_count"),
            avg("zone_temp_c").alias("avg_zone_temp_c"),
            avg("temperature_setpoint_c").alias("avg_setpoint_c"),
            avg("fan_speed_pct").alias("avg_fan_speed_pct"),
            avg("power_kw").alias("avg_power_kw"),
            max_("equipment_running").alias("equipment_running"),
            max_("event_time").alias("max_event_time"),
        )
        .withColumn("window_start", col("window.start"))
        .withColumn("window_end", col("window.end"))
        .drop("window")
    )


def _latest_device_rows(silver: DataFrame) -> DataFrame:
    """Select one deterministic latest reading per device for safety gates."""
    latest_first = Window.partitionBy("device_id").orderBy(
        col("event_time").desc(),
        col("ingest_ts").desc(),
        col("kafka_partition").desc(),
        col("kafka_offset").desc(),
    )
    return silver.withColumn("_rn", row_number().over(latest_first)).filter(col("_rn") == 1)


def _recent_quarantine_counts(spark: SparkSession, settings: Settings, now: datetime) -> dict:
    if not settings.delta_exists(spark, settings.quarantine_path):
        return {}
    cutoff = now - RECENT_QUALITY_WINDOW
    rows = (
        settings.read_delta(spark, settings.quarantine_path)
        .filter(col("ingest_ts") >= lit(cutoff))
        .groupBy("device_id")
        .count()
        .collect()
    )
    return {r["device_id"]: r["count"] for r in rows if r["device_id"]}


def evaluate_recommendations(
    spark: SparkSession, settings: Settings, *, now: datetime | None = None
) -> tuple[list[dict], list[dict]]:
    """Return (new_recommendations, revocations) for the current Silver state."""
    now = now or datetime.now(timezone.utc)
    limits = signals.load_equipment_limits(settings.known_devices_csv)
    silver = settings.read_delta(spark, settings.silver_current_path)
    aggregates = _window_aggregates(silver).collect()
    quarantine_counts = _recent_quarantine_counts(spark, settings, now)

    latest_rows = (
        _latest_device_rows(silver)
        .select(
            "device_id",
            "equipment_running",
            "power_kw",
            "fan_speed_pct",
            "airflow_m3h",
            "chilled_water_valve_pct",
            "supply_air_temp_c",
            "return_air_temp_c",
        )
        .collect()
    )
    latest_by_device = {r["device_id"]: r.asDict() for r in latest_rows}

    recommendations: list[dict] = []
    for row in aggregates:
        device_id = row["device_id"]
        device_limits = limits.get(device_id)
        if device_limits is None:
            continue
        latest = latest_by_device.get(device_id, {})
        active_signals = tuple(
            signals.equipment_state_inconsistencies(
                {
                    "measurements": {
                        "power_kw": latest.get("power_kw"),
                        "fan_speed_pct": latest.get("fan_speed_pct"),
                        "airflow_m3h": latest.get("airflow_m3h"),
                        "chilled_water_valve_pct": latest.get("chilled_water_valve_pct"),
                        "supply_air_temp_c": latest.get("supply_air_temp_c"),
                        "return_air_temp_c": latest.get("return_air_temp_c"),
                    },
                    "operating_state": {
                        "equipment_running": bool(latest.get("equipment_running") or False)
                    },
                },
                device_limits,
            )
        )
        reading = rec.WindowReading(
            device_id=device_id,
            site_id=row["site_id"],
            window_start=row["window_start"].isoformat(),
            window_end=row["window_end"].isoformat(),
            event_count=row["event_count"],
            avg_zone_temp_c=row["avg_zone_temp_c"],
            avg_setpoint_c=row["avg_setpoint_c"],
            avg_fan_speed_pct=row["avg_fan_speed_pct"],
            avg_power_kw=row["avg_power_kw"],
            equipment_running=bool(row["equipment_running"]),
            max_event_time=row["max_event_time"].isoformat().replace("+00:00", "Z"),
        )
        ctx = rec.GateContext(
            now=now,
            expected_interval_sec=device_limits.expected_report_interval_sec,
            quarantine_errors_recent=quarantine_counts.get(device_id, 0),
            active_signals=active_signals,
            commissioning_status=device_limits.commissioning_status,
        )
        result = rec.evaluate(reading, ctx)
        if result:
            recommendations.append(result)

    revocations = _revocations_for_restated_windows(spark, settings, recommendations, now)
    return recommendations, revocations


def _window_key(device_id: str, value: str | datetime) -> tuple[str, str]:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00")) if isinstance(value, str) else value
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return device_id, parsed.isoformat(timespec="seconds")


def build_restatement_revocations(
    existing_recommendations,
    restated_windows: set[tuple[str, str]],
    current_recommendations: list[dict],
    now: datetime,
) -> list[dict]:
    """Build revocations using only a replacement from the same device/window."""
    current_by_window = {
        _window_key(row["device_id"], row["window_end"]): row["recommendation_id"]
        for row in current_recommendations
    }
    revocations: list[dict] = []
    for row in existing_recommendations:
        key = _window_key(row["device_id"], row["window_end"])
        if key not in restated_windows:
            continue
        replacement = current_by_window.get(key)
        if replacement == row["recommendation_id"]:
            continue
        revocations.append(
            rec.build_revocation(
                row["recommendation_id"],
                replacement_recommendation_id=replacement,
                now=now,
            )
        )
    return revocations


def _revocations_for_restated_windows(
    spark: SparkSession,
    settings: Settings,
    current_recommendations: list[dict],
    now: datetime,
) -> list[dict]:
    """Revoke recommendations whose source window was restated after publication."""
    if not (
        settings.delta_exists(spark, settings.gold_current_path)
        and settings.delta_exists(spark, settings.gold_recommendations_path)
    ):
        return []

    restated = {
        _window_key(r["device_id"], r["window_end"])
        for r in settings.read_delta(spark, settings.gold_current_path)
        .filter(col("window_status") == "RESTATED")
        .select("device_id", "window_end")
        .collect()
    }
    if not restated:
        return []
    existing = (
        settings.read_delta(spark, settings.gold_recommendations_path)
        .filter(col("status") == "ACTIVE")
        .select("recommendation_id", "device_id", "window_end")
        .collect()
    )
    return build_restatement_revocations(existing, restated, current_recommendations, now)


def run(settings: Settings) -> None:
    spark = create_spark("hvacpulse-recommendations", delta=True)
    now = datetime.now(timezone.utc)
    recommendations, revocations = evaluate_recommendations(spark, settings, now=now)

    if recommendations:
        recs_df = spark.createDataFrame(
            [
                (
                    r["recommendation_id"],
                    r["device_id"],
                    r["site_id"],
                    datetime.fromisoformat(r["window_start"]),
                    datetime.fromisoformat(r["window_end"]),
                    r["recommendation_type"],
                    r["rule_version"],
                    r["mode"],
                    {k: str(v) for k, v in r["rationale"].items()},
                    "ACTIVE",
                    now,
                )
                for r in recommendations
            ],
            RECOMMENDATION_SCHEMA,
        ).withColumn("window_date", col("window_start").cast("date"))
        if settings.delta_exists(spark, settings.gold_recommendations_path):
            target = settings.delta_table(spark, settings.gold_recommendations_path)
            (
                target.alias("t")
                .merge(recs_df.alias("s"), "t.recommendation_id = s.recommendation_id")
                .whenNotMatchedInsertAll()
                .execute()
            )
        else:
            settings.write_delta(recs_df, settings.gold_recommendations_path, partition_by=["window_date"])

    if revocations:
        revoked_ids = [r["recommendation_id"] for r in revocations]
        revocations_df = spark.createDataFrame(
            [
                (
                    r["event_type"],
                    r["recommendation_id"],
                    r["revoked_at"],
                    r["reason"],
                    r["replacement_recommendation_id"],
                )
                for r in revocations
            ],
            REVOCATION_SCHEMA,
        )
        if settings.delta_exists(spark, settings.recommendation_events_path):
            events = settings.delta_table(spark, settings.recommendation_events_path)
            events.alias("t").merge(
                revocations_df.alias("s"),
                "t.event_type = s.event_type AND t.recommendation_id = s.recommendation_id",
            ).whenNotMatchedInsertAll().execute()
        else:
            settings.write_delta(revocations_df, settings.recommendation_events_path)
        if settings.delta_exists(spark, settings.gold_recommendations_path):
            target = settings.delta_table(spark, settings.gold_recommendations_path)
            (
                target.alias("t")
                .merge(
                    spark.createDataFrame(revoked_ids, StringType()).toDF("recommendation_id").alias("s"),
                    "t.recommendation_id = s.recommendation_id",
                )
                .whenMatchedUpdate(set={"status": lit("REVOKED")})
                .execute()
            )

    print(f"recommendations evaluated: {len(recommendations)} new, {len(revocations)} revoked")


def main(argv: list[str] | None = None) -> int:
    argparse.ArgumentParser(description="Advisory recommendation job").parse_args(argv)
    run(load_settings())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
