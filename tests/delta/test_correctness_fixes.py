"""Tests for four correctness/measurement fixes.

Covers:
1. Quarantine identity across partitions
2. Silver observability timestamps vs event time  
3. Gold history with OPEN→FINAL unchanged values
4. Silver current MERGE with equal revisions
"""

from datetime import datetime
from datetime import timezone

import pytest
from pyspark.sql import types as T

from streaming.config import Settings
from streaming.jobs.gold_metrics import FINAL
from streaming.jobs.gold_metrics import OPEN
from streaming.jobs.gold_metrics import publish_gold
from streaming.jobs.silver_transform import _merge_quarantine
from streaming.jobs.silver_transform import _update_current
from streaming.spark_session import create_spark

pytestmark = pytest.mark.delta


@pytest.fixture(scope="module")
def spark():
    session = create_spark("hvacpulse-test-correctness-fixes", delta=True)
    yield session
    session.stop()


def test_quarantine_identity_includes_partition(spark, tmp_path):
    """Fix #3: Corrupt records at same offset in different partitions both survive."""
    settings = Settings(root=tmp_path)
    schema = T.StructType(
        [
            T.StructField("kafka_topic", T.StringType(), False),
            T.StructField("kafka_partition", T.IntegerType(), False),
            T.StructField("kafka_offset", T.LongType(), False),
            T.StructField("error_code", T.StringType(), False),
            T.StructField("payload_hash", T.StringType(), True),
            T.StructField("ingest_date", T.StringType(), True),
        ]
    )
    partition_0 = spark.createDataFrame(
        [("hvac.telemetry", 0, 100, "SCHEMA_INVALID", "hash-p0", "2026-10-24")], schema
    )
    partition_1 = spark.createDataFrame(
        [("hvac.telemetry", 1, 100, "SCHEMA_INVALID", "hash-p1", "2026-10-24")], schema
    )
    _merge_quarantine(spark, settings, partition_0, settings.quarantine_path)
    _merge_quarantine(spark, settings, partition_1, settings.quarantine_path)
    
    result = settings.read_delta(spark, settings.quarantine_path).collect()
    assert len(result) == 2
    hashes = {row.payload_hash for row in result}
    assert hashes == {"hash-p0", "hash-p1"}


def test_silver_observability_timestamps_distinct_from_event_time(spark, tmp_path):
    """Fix #1: silver_commit_ts measures when data becomes observable in Silver.
    
    bronze_ingest_ts (from Bronze) and silver_commit_ts (from Silver) are
    separate wall-clock boundaries, not synthetic event_time.
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
            T.StructField("kafka_timestamp", T.TimestampType(), False),
            T.StructField("bronze_ingest_ts", T.TimestampType(), False),
            T.StructField("silver_commit_ts", T.TimestampType(), False),
            T.StructField("ingest_date", T.StringType(), False),
        ]
    )
    
    before_commit = datetime.now(timezone.utc)
    silver_row = spark.createDataFrame(
        [(
            "evt-1",
            "logical-1",
            1,
            event_time,
            bronze_ts,
            bronze_ts,
            datetime.now(timezone.utc),
            "2026-10-24",
        )],
        schema,
    )
    after_commit = datetime.now(timezone.utc)
    
    settings.write_delta(silver_row, settings.silver_history_path, partition_by=["ingest_date"])
    result = settings.read_delta(spark, settings.silver_history_path).first()
    
    # Spark may return naive datetimes; compare as timestamps or strip timezone for comparison
    assert result.event_time.replace(tzinfo=timezone.utc) == event_time
    assert result.bronze_ingest_ts.replace(tzinfo=timezone.utc) == bronze_ts
    result_commit_ts = result.silver_commit_ts.replace(tzinfo=timezone.utc)
    assert before_commit <= result_commit_ts <= after_commit
    assert result.silver_commit_ts != result.bronze_ingest_ts


def test_gold_history_publishes_open_to_final_with_unchanged_values(spark, tmp_path):
    """Fix #2: OPEN→FINAL status transition writes history even with identical metrics."""
    settings = Settings(root=tmp_path)
    
    start = datetime(2026, 10, 24, 10, 0, tzinfo=timezone.utc)
    end = datetime(2026, 10, 24, 10, 1, tzinfo=timezone.utc)
    
    metrics_schema = T.StructType(
        [
            T.StructField("site_id", T.StringType(), False),
            T.StructField("device_id", T.StringType(), False),
            T.StructField("window_start", T.TimestampType(), False),
            T.StructField("window_end", T.TimestampType(), False),
            T.StructField("event_count", T.LongType(), False),
            T.StructField("avg_power_kw", T.DoubleType(), True),
            T.StructField("max_power_kw", T.DoubleType(), True),
            T.StructField("avg_zone_temp_c", T.DoubleType(), True),
            T.StructField("avg_humidity_pct", T.DoubleType(), True),
            T.StructField("energy_kwh", T.DoubleType(), True),
            T.StructField("window_status", T.StringType(), False),
            T.StructField("_content_hash", T.StringType(), False),
        ]
    )
    
    open_metrics = spark.createDataFrame(
        [("site-1", "ahu-1", start, end, 10, 5.0, 6.0, 22.0, 50.0, 0.05, OPEN, "hash-v1")],
        metrics_schema,
    )
    publish_gold(spark, open_metrics, settings)
    
    final_metrics = spark.createDataFrame(
        [("site-1", "ahu-1", start, end, 10, 5.0, 6.0, 22.0, 50.0, 0.05, FINAL, "hash-v1")],
        metrics_schema,
    )
    publish_gold(spark, final_metrics, settings)
    
    history = settings.read_delta(spark, settings.gold_history_path).collect()
    assert len(history) == 2
    
    statuses = {row.window_status for row in history}
    assert OPEN in statuses
    assert FINAL in statuses
    
    versions = {row.metric_version for row in history}
    assert versions == {1, 2}
    
    current = settings.read_delta(spark, settings.gold_current_path).first()
    assert current.window_status == FINAL
    assert current.metric_version == 2


def test_silver_current_merge_with_equal_revision(spark, tmp_path):
    """Fix #4: Equal-revision conflicts resolved by event_time, bronze_ingest_ts, event_id."""
    settings = Settings(root=tmp_path)
    
    event_time_early = datetime(2026, 10, 24, 10, 0, 0, tzinfo=timezone.utc)
    event_time_late = datetime(2026, 10, 24, 10, 1, 0, tzinfo=timezone.utc)
    bronze_ts_1 = datetime(2026, 10, 24, 10, 5, 0, tzinfo=timezone.utc)
    bronze_ts_2 = datetime(2026, 10, 24, 10, 6, 0, tzinfo=timezone.utc)
    
    schema = T.StructType(
        [
            T.StructField("event_id", T.StringType(), False),
            T.StructField("logical_event_id", T.StringType(), False),
            T.StructField("revision_number", T.IntegerType(), False),
            T.StructField("event_time", T.TimestampType(), False),
            T.StructField("bronze_ingest_ts", T.TimestampType(), False),
            T.StructField("silver_commit_ts", T.TimestampType(), False),
            T.StructField("power_kw", T.DoubleType(), True),
            T.StructField("ingest_date", T.StringType(), False),
        ]
    )
    
    revision_1_early = spark.createDataFrame(
        [(
            "evt-1",
            "logical-1",
            1,
            event_time_early,
            bronze_ts_1,
            datetime.now(timezone.utc),
            10.0,
            "2026-10-24",
        )],
        schema,
    )
    settings.write_delta(revision_1_early, settings.silver_history_path, partition_by=["ingest_date"])
    
    revision_1_late = spark.createDataFrame(
        [(
            "evt-2",
            "logical-1",
            1,
            event_time_late,
            bronze_ts_2,
            datetime.now(timezone.utc),
            20.0,
            "2026-10-24",
        )],
        schema,
    )
    settings.write_delta(
        revision_1_late,
        settings.silver_history_path,
        mode="append",
    )
    
    _update_current(spark, settings, revision_1_early)
    _update_current(spark, settings, revision_1_late)
    
    current = settings.read_delta(spark, settings.silver_current_path).first()
    assert current.logical_event_id == "logical-1"
    assert current.revision_number == 1
    assert current.event_id == "evt-2"
    assert current.power_kw == 20.0


def test_silver_current_updates_only_affected_ids(spark, tmp_path):
    """Fix #4: _update_current modifies only affected logical IDs, not all history."""
    settings = Settings(root=tmp_path)
    
    ts = datetime.now(timezone.utc)
    schema = T.StructType(
        [
            T.StructField("event_id", T.StringType(), False),
            T.StructField("logical_event_id", T.StringType(), False),
            T.StructField("revision_number", T.IntegerType(), False),
            T.StructField("event_time", T.TimestampType(), False),
            T.StructField("bronze_ingest_ts", T.TimestampType(), False),
            T.StructField("silver_commit_ts", T.TimestampType(), False),
            T.StructField("power_kw", T.DoubleType(), True),
            T.StructField("ingest_date", T.StringType(), False),
        ]
    )
    
    initial = spark.createDataFrame(
        [
            ("evt-1", "logical-1", 1, ts, ts, ts, 10.0, "2026-10-24"),
            ("evt-2", "logical-2", 1, ts, ts, ts, 20.0, "2026-10-24"),
        ],
        schema,
    )
    settings.write_delta(initial, settings.silver_history_path, partition_by=["ingest_date"])
    _update_current(spark, settings, initial)
    
    assert settings.read_delta(spark, settings.silver_current_path).count() == 2
    
    update = spark.createDataFrame(
        [("evt-3", "logical-1", 2, ts, ts, ts, 15.0, "2026-10-24")],
        schema,
    )
    settings.write_delta(update, settings.silver_history_path, mode="append")
    _update_current(spark, settings, update)
    
    current_rows = {row.logical_event_id: row for row in settings.read_delta(spark, settings.silver_current_path).collect()}
    assert len(current_rows) == 2
    assert current_rows["logical-1"].revision_number == 2
    assert current_rows["logical-1"].power_kw == 15.0
    assert current_rows["logical-2"].revision_number == 1
    assert current_rows["logical-2"].power_kw == 20.0
