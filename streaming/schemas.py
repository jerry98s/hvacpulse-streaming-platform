"""Avro contract loading for Spark ``from_avro`` and canonical column lists."""

from __future__ import annotations

from pathlib import Path

CONTRACTS_DIR = Path(__file__).resolve().parents[1] / "contracts" / "avro"
SUPPORTED_SCHEMA_VERSIONS = (1, 2)


def avro_schema_json(version: int) -> str:
    path = CONTRACTS_DIR / f"hvac_telemetry_v{version}.avsc"
    if not path.exists():
        raise ValueError(f"Unsupported Avro schema version: {version}")
    return path.read_text(encoding="utf-8")


# Canonical Silver measurement columns (v2 superset; v1 rows keep NULL co2_ppm).
MEASUREMENT_COLUMNS = [
    "supply_air_temp_c",
    "return_air_temp_c",
    "zone_temp_c",
    "relative_humidity_pct",
    "airflow_m3h",
    "static_pressure_pa",
    "fan_speed_pct",
    "chilled_water_valve_pct",
    "power_kw",
    "co2_ppm",
]

SILVER_COLUMNS = [
    "event_id",
    "logical_event_id",
    "revision_number",
    "supersedes_event_id",
    "schema_id",
    "schema_version",
    "firmware_version",
    "event_time",
    "gateway_id",
    "device_id",
    "boot_id",
    "sequence_number",
    "site_id",
    "zone_id",
    *MEASUREMENT_COLUMNS,
    "mode",
    "temperature_setpoint_c",
    "equipment_running",
    "payload_hash",
    "kafka_topic",
    "kafka_partition",
    "kafka_offset",
    "kafka_timestamp",
    "bronze_ingest_ts",
    "silver_commit_ts",
    "ingest_date",
]
