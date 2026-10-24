"""Shared plant normalization for local streaming and Databricks batch jobs.

Raw Avro is normalized on executors. Invalid snapshots retain their transport
identity and reasons, but have no feature vector or target labels.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from datetime import timezone

from ingestion.plant_events import decode
from ingestion.plant_events import timestamp
from plant.contracts import Features
from plant.contracts import Plant
from plant.contracts import Reading
from plant.contracts import Scope
from plant.telemetry import normalize

NORMALIZED_SCHEMA = """event_id string, scope string, config_version string,
    captured_at string, available_at string, synthetic boolean, eligible boolean,
    issues string, completeness double, values_json string, operating boolean"""
FEATURE_SCHEMA = """outdoor_c double, humidity_pct double, wet_bulb_c double,
    occupancy_pct double, production_kw double, operating boolean,
    cooling_kw double, power_kw double"""
FEATURE_VERSION = "plant-instant-v1"


def plant_records(df):
    from pyspark.sql import functions as F

    return F.coalesce(
        F.exists(
            df.headers, lambda h: (h.key == "event_type") & (h.value.cast("string") == "plant_snapshot")
        ),
        F.lit(False),
    )


def normalize_record(value: bytes, available_at: datetime | str, plant: Plant) -> dict:
    if isinstance(available_at, str):
        available_at = datetime.fromisoformat(available_at.replace("Z", "+00:00"))
    if available_at.tzinfo is None:
        available_at = available_at.replace(tzinfo=timezone.utc)
    result = dict(
        event_id=None,
        scope=None,
        config_version=None,
        captured_at=None,
        available_at=timestamp(available_at),
        synthetic=True,
        eligible=False,
        issues="[]",
        completeness=0.0,
        values_json="{}",
        operating=False,
    )
    try:
        event = decode(bytes(value))
        scope = Scope(event["tenant_id"], event["site_id"], event["plant_id"])
        captured = datetime.fromisoformat(event["captured_at"])
        result.update(
            event_id=event["event_id"],
            scope=scope.key,
            config_version=event["config_version"],
            captured_at=timestamp(captured),
            operating=event["operating"],
        )
        if not event["event_id"] or event["schema_version"] != 1 or event["synthetic"] is not True:
            raise ValueError("UNSUPPORTED_CONTRACT_OR_NONMOCK_SOURCE")
        if scope != plant.scope or event["config_version"] != plant.version:
            raise ValueError("SCOPE_OR_CONFIG_MISMATCH")
        if captured > available_at:
            raise ValueError("FUTURE_CAPTURE")
        readings = [
            Reading(**{**r, "source_time": datetime.fromisoformat(r["source_time"])})
            for r in event["readings"]
        ]
        snapshot = normalize(plant, readings, captured)
        result.update(
            eligible=snapshot.eligible,
            issues=json.dumps(snapshot.issues),
            completeness=snapshot.completeness,
            values_json=json.dumps(snapshot.values),
        )
        if snapshot.eligible:
            # Validate the downstream feature contract before accepting Silver.
            extract_features(result, plant)
    except (ValueError, TypeError, KeyError, EOFError, IndexError, OverflowError) as exc:
        result.update(eligible=False, issues=json.dumps([f"INVALID_SNAPSHOT:{exc}"]))
    return result


def extract_features(record: dict, plant: Plant) -> dict:
    values = json.loads(record["values_json"])
    processes = [item for item in plant.equipment if item.kind == "process"]
    if len(processes) != 1:
        raise ValueError("Exactly one process context is required")
    context = values[processes[0].equipment_id]
    features = Features(
        plant.scope,
        datetime.fromisoformat(record["captured_at"]),
        datetime.fromisoformat(record["available_at"]),
        context["outdoor_temp_c"],
        context["humidity_pct"],
        context["wet_bulb_c"],
        context["occupancy_pct"],
        context["heat_load_kw"],
        record["operating"],
    )
    chillers = [values[item.equipment_id] for item in plant.equipment if item.kind == "chiller"]
    cooling = sum(1.163 * v["flow_m3h"] * (v["return_temp_c"] - v["supply_temp_c"]) for v in chillers)
    # A missing meter is never silently treated as zero power.
    metered = [
        item for item in plant.equipment if item.kind in {"chiller", "cooling_tower", "chw_pump", "cw_pump"}
    ]
    power = sum(values[item.equipment_id]["power_kw"] for item in metered)
    return {
        "outdoor_c": features.outdoor_c,
        "humidity_pct": features.humidity_pct,
        "wet_bulb_c": features.wet_bulb_c,
        "occupancy_pct": features.occupancy_pct,
        "production_kw": features.production_kw,
        "operating": features.operating,
        "cooling_kw": float(cooling),
        "power_kw": float(power),
    }


def normalized_frame(bronze, plant: Plant):
    from pyspark.sql import functions as F

    normalize_udf = F.udf(lambda value, at: normalize_record(value, at, plant), NORMALIZED_SCHEMA)
    versions = F.filter(F.col("headers"), lambda h: h.key == "schema_version")
    valid_header = F.coalesce(
        (F.size(versions) == 1) & (F.get(versions, 0).value.cast("string") == "1"), F.lit(False)
    )
    return (
        bronze.filter(plant_records(bronze))
        .select(
            # TimestampType enters Python UDFs as host-local naive datetime.
            # Format in Spark's UTC session to keep Windows and cloud identical.
            normalize_udf("value", F.date_format("ingest_ts", "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX")).alias("p"),
            F.col("topic").alias("kafka_topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            "payload_hash",
            valid_header.alias("_valid_header"),
        )
        .select("p.*", "kafka_topic", "kafka_partition", "kafka_offset", "payload_hash", "_valid_header")
        .withColumn("eligible", F.col("eligible") & F.col("_valid_header"))
        .withColumn(
            "issues",
            F.when(F.col("_valid_header"), F.col("issues")).otherwise(F.lit('["SCHEMA_HEADER_INVALID"]')),
        )
        .drop("_valid_header")
    )


def feature_frame(silver, plant: Plant):
    from pyspark.sql import functions as F

    extract = F.udf(lambda r: extract_features(r.asDict(), plant), FEATURE_SCHEMA)
    return silver.withColumn("f", extract(F.struct(*silver.columns))).select(
        "row_id",
        "scope",
        "config_version",
        "captured_at",
        "available_at",
        "synthetic",
        "payload_hash",
        F.lit(FEATURE_VERSION).alias("feature_version"),
        "f.*",
    )


def identity(*values) -> str:
    return hashlib.sha256(json.dumps(values, sort_keys=True).encode()).hexdigest()
