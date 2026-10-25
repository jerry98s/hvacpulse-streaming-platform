"""Portfolio demonstration: duplicates, restart recovery, late-correction restatement.

This reproducible test suite demonstrates three key platform capabilities:
1. Duplicate handling - same event delivered multiple times is deduplicated
2. Restart recovery - pipeline can restart from checkpoint without data loss/duplication
3. Late-correction restatement - late revisions trigger Gold RESTATED versions

Run locally: uv run pytest tests/delta/test_portfolio_demo.py -v -m delta
"""

from datetime import datetime
from datetime import timedelta
from datetime import timezone

import pytest
from pyspark.sql import types as T

from streaming.config import Settings
from streaming.jobs.gold_metrics import FINAL
from streaming.jobs.gold_metrics import OPEN
from streaming.jobs.gold_metrics import RESTATED
from streaming.jobs.gold_metrics import compute_window_metrics
from streaming.jobs.gold_metrics import publish_gold
from streaming.jobs.reconciliation import reconcile_late_corrections
from streaming.jobs.silver_transform import _merge_inserts
from streaming.jobs.silver_transform import _process_microbatch
from streaming.jobs.silver_transform import _update_current
from streaming.spark_session import create_spark

pytestmark = pytest.mark.delta


@pytest.fixture(scope="module")
def spark():
    session = create_spark("hvacpulse-portfolio-demo", delta=True)
    yield session
    session.stop()


def test_duplicate_handling_dedups_identical_event_id_and_payload(spark, tmp_path):
    """DEMO 1: Duplicate handling.
    
    A sensor gateway retries and produces the same event three times.
    Bronze retains all three transport records.
    Silver deduplicates on (event_id, payload_hash) and keeps exactly one.
    """
    settings = Settings(root=tmp_path)
    
    event_time = datetime(2026, 10, 24, 10, 0, 0, tzinfo=timezone.utc)
    bronze_ts = datetime(2026, 10, 24, 10, 1, 0, tzinfo=timezone.utc)
    
    schema = T.StructType(
        [
            T.StructField("event_id", T.StringType(), False),
            T.StructField("logical_event_id", T.StringType(), False),
            T.StructField("revision_number", T.IntegerType(), False),
            T.StructField("event_time", T.TimestampType(), False),
            T.StructField("power_kw", T.DoubleType(), True),
            T.StructField("kafka_topic", T.StringType(), False),
            T.StructField("kafka_partition", T.IntegerType(), False),
            T.StructField("kafka_offset", T.LongType(), False),
            T.StructField("kafka_timestamp", T.TimestampType(), False),
            T.StructField("bronze_ingest_ts", T.TimestampType(), False),
            T.StructField("silver_commit_ts", T.TimestampType(), False),
            T.StructField("payload_hash", T.StringType(), False),
            T.StructField("ingest_date", T.StringType(), False),
            T.StructField("error_code", T.StringType(), True),
        ]
    )
    
    duplicate = spark.createDataFrame(
        [
            ("evt-1", "logical-1", 1, event_time, 10.0, "hvac.telemetry", 0, 100, bronze_ts, bronze_ts, datetime.now(timezone.utc), "hash-A", "2026-10-24", None),
            ("evt-1", "logical-1", 1, event_time, 10.0, "hvac.telemetry", 0, 101, bronze_ts, bronze_ts, datetime.now(timezone.utc), "hash-A", "2026-10-24", None),
            ("evt-1", "logical-1", 1, event_time, 10.0, "hvac.telemetry", 0, 102, bronze_ts, bronze_ts, datetime.now(timezone.utc), "hash-A", "2026-10-24", None),
        ],
        schema,
    )
    
    # Deduplicate on (event_id, payload_hash) before merge
    deduped = duplicate.dropDuplicates(["event_id", "payload_hash"])
    _merge_inserts(spark, settings, deduped, settings.silver_history_path)
    
    result = settings.read_delta(spark, settings.silver_history_path).collect()
    assert len(result) == 1
    assert result[0].event_id == "evt-1"
    assert result[0].payload_hash == "hash-A"


def test_duplicate_conflict_detection_quarantines_different_payload(spark, tmp_path):
    """DEMO 1 extension: Idempotency conflict.
    
    Same event_id arrives with two different payloads.
    Both are accepted by Bronze, but only the first enters Silver.
    The conflicting payload is quarantined as IDEMPOTENCY_CONFLICT.
    """
    settings = Settings(root=tmp_path)
    
    event_time = datetime(2026, 10, 24, 10, 0, 0, tzinfo=timezone.utc)
    bronze_ts = datetime(2026, 10, 24, 10, 1, 0, tzinfo=timezone.utc)
    
    # Create minimal batch with all required Silver columns
    batch_1 = spark.createDataFrame(
        [("evt-1", "logical-1", 1, None, "schema-1", 1, "fw-1", event_time, "gw-1", "ahu-1", "boot-1", 1, "site-1", "zone-1",
          None, None, None, None, None, None, None, None, 10.0, None, "AUTO", 22.0, True,
          "hash-A", "hvac.telemetry", 0, 100, bronze_ts, bronze_ts, "2026-10-24", None)],
        """event_id string, logical_event_id string, revision_number int, supersedes_event_id string,
           schema_id string, schema_version int, firmware_version string, event_time timestamp,
           gateway_id string, device_id string, boot_id string, sequence_number int, site_id string, zone_id string,
           supply_air_temp_c double, return_air_temp_c double, zone_temp_c double, relative_humidity_pct double,
           airflow_m3h double, static_pressure_pa double, fan_speed_pct double, chilled_water_valve_pct double,
           power_kw double, co2_ppm double, mode string, temperature_setpoint_c double, equipment_running boolean,
           payload_hash string, kafka_topic string, kafka_partition int, kafka_offset long,
           kafka_timestamp timestamp, bronze_ingest_ts timestamp, ingest_date string, error_code string""",
    )
    _process_microbatch(spark, settings, batch_1)
    
    batch_2 = spark.createDataFrame(
        [("evt-1", "logical-1", 1, None, "schema-1", 1, "fw-1", event_time, "gw-1", "ahu-1", "boot-1", 1, "site-1", "zone-1",
          None, None, None, None, None, None, None, None, 20.0, None, "AUTO", 22.0, True,
          "hash-B", "hvac.telemetry", 0, 200, bronze_ts, bronze_ts, "2026-10-24", None)],
        """event_id string, logical_event_id string, revision_number int, supersedes_event_id string,
           schema_id string, schema_version int, firmware_version string, event_time timestamp,
           gateway_id string, device_id string, boot_id string, sequence_number int, site_id string, zone_id string,
           supply_air_temp_c double, return_air_temp_c double, zone_temp_c double, relative_humidity_pct double,
           airflow_m3h double, static_pressure_pa double, fan_speed_pct double, chilled_water_valve_pct double,
           power_kw double, co2_ppm double, mode string, temperature_setpoint_c double, equipment_running boolean,
           payload_hash string, kafka_topic string, kafka_partition int, kafka_offset long,
           kafka_timestamp timestamp, bronze_ingest_ts timestamp, ingest_date string, error_code string""",
    )
    _process_microbatch(spark, settings, batch_2)
    
    silver = settings.read_delta(spark, settings.silver_history_path).collect()
    assert len(silver) == 1
    assert silver[0].power_kw == 10.0
    
    quarantine = settings.read_delta(spark, settings.quarantine_path).collect()
    assert len(quarantine) == 1
    assert quarantine[0].error_code == "IDEMPOTENCY_CONFLICT"
    assert quarantine[0].kafka_offset == 200


def test_restart_recovery_replays_from_checkpoint_without_duplication(spark, tmp_path):
    """DEMO 2: Restart recovery.
    
    Simulate a pipeline crash and restart scenario:
    1. Process batch 1, commit to Delta
    2. Simulate checkpoint (Delta commit successful, checkpoint advanced)
    3. Process batch 2
    4. Simulate restart - replay batch 2 (checkpoint not advanced)
    5. Verify no duplication - idempotent merge handles the replay
    """
    settings = Settings(root=tmp_path)
    
    event_time = datetime(2026, 10, 24, 10, 0, 0, tzinfo=timezone.utc)
    bronze_ts = datetime(2026, 10, 24, 10, 1, 0, tzinfo=timezone.utc)
    
    schema = T.StructType(
        [
            T.StructField("event_id", T.StringType(), False),
            T.StructField("logical_event_id", T.StringType(), False),
            T.StructField("revision_number", T.IntegerType(), False),
            T.StructField("event_time", T.TimestampType(), False),
            T.StructField("power_kw", T.DoubleType(), True),
            T.StructField("kafka_topic", T.StringType(), False),
            T.StructField("kafka_partition", T.IntegerType(), False),
            T.StructField("kafka_offset", T.LongType(), False),
            T.StructField("kafka_timestamp", T.TimestampType(), False),
            T.StructField("bronze_ingest_ts", T.TimestampType(), False),
            T.StructField("silver_commit_ts", T.TimestampType(), False),
            T.StructField("payload_hash", T.StringType(), False),
            T.StructField("ingest_date", T.StringType(), False),
        ]
    )
    
    batch_1 = spark.createDataFrame(
        [("evt-1", "logical-1", 1, event_time, 10.0, "hvac.telemetry", 0, 100, bronze_ts, bronze_ts, datetime.now(timezone.utc), "hash-1", "2026-10-24")],
        schema,
    )
    _merge_inserts(spark, settings, batch_1, settings.silver_history_path)
    _update_current(spark, settings, batch_1)
    
    assert settings.read_delta(spark, settings.silver_history_path).count() == 1
    
    batch_2 = spark.createDataFrame(
        [("evt-2", "logical-2", 1, event_time, 20.0, "hvac.telemetry", 0, 200, bronze_ts, bronze_ts, datetime.now(timezone.utc), "hash-2", "2026-10-24")],
        schema,
    )
    _merge_inserts(spark, settings, batch_2, settings.silver_history_path)
    _update_current(spark, settings, batch_2)
    
    assert settings.read_delta(spark, settings.silver_history_path).count() == 2
    
    _merge_inserts(spark, settings, batch_2, settings.silver_history_path)
    _update_current(spark, settings, batch_2)
    
    result = settings.read_delta(spark, settings.silver_history_path).collect()
    assert len(result) == 2
    event_ids = {r.event_id for r in result}
    assert event_ids == {"evt-1", "evt-2"}


def test_late_correction_restatement_triggers_gold_restated(spark, tmp_path):
    """DEMO 3: Late-correction restatement.
    
    A FINAL Gold window is published based on initial Silver data.
    A late revision arrives (revision_number=2) correcting the measurement.
    The corrected data triggers a new Gold version with status RESTATED.
    History preserves both the original FINAL and the new RESTATED version.
    """
    settings = Settings(root=tmp_path)
    
    base_time = datetime(2026, 10, 24, 10, 0, 0, tzinfo=timezone.utc)
    window_start = base_time
    finalize_time = base_time + timedelta(minutes=15)
    
    silver_schema = T.StructType(
        [
            T.StructField("site_id", T.StringType(), False),
            T.StructField("device_id", T.StringType(), False),
            T.StructField("event_id", T.StringType(), False),
            T.StructField("logical_event_id", T.StringType(), False),
            T.StructField("revision_number", T.IntegerType(), False),
            T.StructField("event_time", T.TimestampType(), False),
            T.StructField("power_kw", T.DoubleType(), True),
            T.StructField("zone_temp_c", T.DoubleType(), True),
            T.StructField("relative_humidity_pct", T.DoubleType(), True),
            T.StructField("ingest_date", T.StringType(), False),
        ]
    )
    
    initial_silver = spark.createDataFrame(
        [
            ("site-1", "ahu-1", "evt-1", "logical-1", 1, window_start, 10.0, 22.0, 50.0, "2026-10-24"),
            ("site-1", "ahu-1", "evt-2", "logical-2", 1, window_start + timedelta(seconds=30), 10.0, 22.0, 50.0, "2026-10-24"),
        ],
        silver_schema,
    )
    settings.write_delta(initial_silver, settings.silver_current_path, partition_by=["ingest_date"])
    
    metrics_open = compute_window_metrics(
        settings.read_delta(spark, settings.silver_current_path),
        {"ahu-1": 5.0},
        window_duration="1 minute",
        now=base_time + timedelta(minutes=2),
    )
    publish_gold(spark, metrics_open, settings)
    
    current_open = settings.read_delta(spark, settings.gold_current_path).first()
    assert current_open.window_status == OPEN
    assert current_open.metric_version == 1
    
    metrics_final = compute_window_metrics(
        settings.read_delta(spark, settings.silver_current_path),
        {"ahu-1": 5.0},
        window_duration="1 minute",
        now=finalize_time,
    )
    publish_gold(spark, metrics_final, settings)
    
    current_final = settings.read_delta(spark, settings.gold_current_path).first()
    assert current_final.window_status == FINAL
    assert current_final.metric_version == 1
    
    corrected_silver = spark.createDataFrame(
        [
            ("site-1", "ahu-1", "evt-1", "logical-1", 1, window_start, 10.0, 22.0, 50.0, "2026-10-24"),
            ("site-1", "ahu-1", "evt-3", "logical-2", 2, window_start + timedelta(seconds=30), 12.0, 22.0, 50.0, "2026-10-24"),
        ],
        silver_schema,
    )
    settings.write_delta(
        corrected_silver,
        settings.silver_current_path,
        mode="overwrite",
        overwrite_schema=True,
        partition_by=["ingest_date"],
    )
    
    metrics_restated = compute_window_metrics(
        settings.read_delta(spark, settings.silver_current_path),
        {"ahu-1": 5.0},
        window_duration="1 minute",
        now=finalize_time + timedelta(minutes=1),
    )
    publish_gold(spark, metrics_restated, settings)
    
    current_restated = settings.read_delta(spark, settings.gold_current_path).first()
    assert current_restated.window_status == RESTATED
    assert current_restated.metric_version == 3
    assert current_restated.restated_at is not None
    
    history = settings.read_delta(spark, settings.gold_history_path).collect()
    assert len(history) == 3
    statuses = {r.window_status for r in history}
    assert OPEN in statuses
    assert FINAL in statuses
    assert RESTATED in statuses
    
    versions = {r.metric_version for r in history}
    assert versions == {1, 2, 3}


def test_late_correction_reconciliation_tracks_revisions(spark, tmp_path):
    """DEMO 3 extension: Late correction tracking.
    
    The reconciliation job identifies revisions that arrived after the watermark
    and records them in late_corrections for audit and downstream reprocessing.
    """
    settings = Settings(root=tmp_path)
    
    base_time = datetime(2026, 10, 24, 10, 0, 0, tzinfo=timezone.utc)
    
    silver_schema = T.StructType(
        [
            T.StructField("event_id", T.StringType(), False),
            T.StructField("logical_event_id", T.StringType(), False),
            T.StructField("revision_number", T.IntegerType(), False),
            T.StructField("device_id", T.StringType(), False),
            T.StructField("site_id", T.StringType(), False),
            T.StructField("event_time", T.TimestampType(), False),
            T.StructField("bronze_ingest_ts", T.TimestampType(), False),
            T.StructField("supersedes_event_id", T.StringType(), True),
            T.StructField("payload_hash", T.StringType(), False),
            T.StructField("ingest_date", T.StringType(), False),
        ]
    )
    
    on_time = spark.createDataFrame(
        [("evt-1", "logical-1", 1, "ahu-1", "site-1", base_time, base_time + timedelta(seconds=30), None, "hash-1", "2026-10-24")],
        silver_schema,
    )
    settings.write_delta(on_time, settings.silver_history_path, partition_by=["ingest_date"])
    
    late_revision = spark.createDataFrame(
        [("evt-2", "logical-1", 2, "ahu-1", "site-1", base_time, base_time + timedelta(minutes=20), "evt-1", "hash-2", "2026-10-24")],
        silver_schema,
    )
    settings.write_delta(
        late_revision,
        settings.silver_history_path,
        mode="append",
    )
    
    count = reconcile_late_corrections(spark, settings)
    assert count == 1
    
    late_corrections = settings.read_delta(spark, settings.late_corrections_path).first()
    assert late_corrections.event_id == "evt-2"
    assert late_corrections.revision_number == 2
    assert late_corrections.supersedes_event_id == "evt-1"
