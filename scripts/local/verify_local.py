"""Print Bronze / Silver / Quarantine / Gold row counts after a local run."""

from __future__ import annotations

from pyspark.sql.functions import col

from streaming.config import load_settings
from streaming.spark_session import create_spark


def _count(spark, path: str) -> int:
    from streaming.config import filesystem_path

    if not (filesystem_path(path) / "_delta_log").exists():
        return -1
    return spark.read.format("delta").load(path).count()


def main() -> int:
    settings = load_settings()
    spark = create_spark("hvacpulse-verify", delta=True)
    bronze = _count(spark, settings.bronze_path)
    silver_h = _count(spark, settings.silver_history_path)
    silver_c = _count(spark, settings.silver_current_path)
    quarantine = _count(spark, settings.quarantine_path)
    gold_c = _count(spark, settings.gold_current_path)
    gold_h = _count(spark, settings.gold_history_path)
    print("=== HVACPulse local E2E counts ===")
    print(f"bronze_telemetry          {bronze}")
    print(f"silver_telemetry_history  {silver_h}")
    print(f"silver_telemetry_current  {silver_c}")
    print(f"quarantine_events         {quarantine}")
    print(f"gold_hvac_metrics_current {gold_c}")
    print(f"gold_hvac_metrics_history {gold_h}")
    if quarantine >= 0:
        print("--- quarantine reasons ---")
        spark.read.format("delta").load(settings.quarantine_path).groupBy("error_code").count().orderBy(
            col("count").desc()
        ).show(truncate=False)
    if gold_c >= 0:
        print("--- gold current sample ---")
        spark.read.format("delta").load(settings.gold_current_path).select(
            "site_id", "device_id", "window_start", "event_count", "energy_kwh", "window_status"
        ).orderBy("window_start").show(10, truncate=False)
    ok = bronze > 0 and silver_h > 0 and silver_c > 0 and gold_c > 0
    print("E2E result:", "PASS" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
