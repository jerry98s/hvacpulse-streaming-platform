"""Delta Lake table maintenance: OPTIMIZE + VACUUM.

Schedules file compaction and retention cleanup. Run periodically (daily or
weekly) in production; unnecessary for short demos. Uses the same settings
abstraction as all jobs — works with local paths and Unity Catalog.

Usage: python -m streaming.jobs.maintenance [--optimize] [--vacuum] [--retention-days 7]
"""

from __future__ import annotations

import argparse

from streaming.config import Settings
from streaming.config import load_settings
from streaming.spark_session import create_spark

# Tables that accumulate small files or time-travel history
MAINTAINED_TABLES = (
    "bronze_telemetry",
    "silver_telemetry_history",
    "silver_telemetry_current",
    "quarantine_events",
    "gold_hvac_metrics_history",
    "gold_hvac_metrics_current",
    "gold_data_quality_scorecard",
    "gold_sensor_freshness",
    "gold_compliance",
    "gold_equipment_health",
    "gold_recommendations",
    "late_corrections",
    "metric_restatement_events",
)


def _target(settings: Settings, name: str) -> str:
    if settings.uses_unity_catalog:
        return f"{settings.catalog}.{settings.schema}.{name}"
    return f"{settings.root}/delta/{name}"


def sql_identifier(settings: Settings, name: str) -> str:
    """Delta SQL reference: three-part UC name, or delta.`<path>` for a local table."""
    if settings.uses_unity_catalog:
        return f"`{settings.catalog}`.`{settings.schema}`.`{name}`"
    return f"delta.`{_target(settings, name)}`"


def optimize_table(spark, settings: Settings, name: str) -> None:
    target = _target(settings, name)
    if not settings.delta_exists(spark, target):
        print(f"SKIP optimize (missing): {target}")
        return
    spark.sql(f"OPTIMIZE {sql_identifier(settings, name)}")
    print(f"OPTIMIZE done: {target}")


def vacuum_table(spark, settings: Settings, name: str, retention_days: int) -> None:
    target = _target(settings, name)
    if not settings.delta_exists(spark, target):
        print(f"SKIP vacuum (missing): {target}")
        return
    spark.sql(f"VACUUM {sql_identifier(settings, name)} RETAIN {retention_days * 24} HOURS")
    print(f"VACUUM {retention_days} days done: {target}")


def run(settings: Settings, *, optimize: bool, vacuum: bool, retention_days: int) -> None:
    spark = create_spark("hvacpulse-maintenance", delta=True)
    for name in MAINTAINED_TABLES:
        if optimize:
            optimize_table(spark, settings, name)
        if vacuum:
            vacuum_table(spark, settings, name, retention_days)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Delta table maintenance")
    parser.add_argument("--optimize", action="store_true", help="compact small files")
    parser.add_argument("--vacuum", action="store_true", help="delete old time-travel files")
    parser.add_argument("--retention-days", type=int, default=7, help="VACUUM retention")
    args = parser.parse_args(argv)
    if not args.optimize and not args.vacuum:
        parser.error("Specify --optimize and/or --vacuum")
    run(load_settings(), optimize=args.optimize, vacuum=args.vacuum, retention_days=args.retention_days)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
