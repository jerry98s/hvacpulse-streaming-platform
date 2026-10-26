from datetime import datetime
from datetime import timezone

import pytest
from pyspark.sql import types as T

from streaming.config import Settings
from streaming.jobs.gold_metrics import FINAL
from streaming.jobs.gold_metrics import RESTATED
from streaming.jobs.gold_metrics import _merge_history
from streaming.jobs.gold_metrics import publish_gold
from streaming.jobs.quality_metrics import build_dq_scorecard
from streaming.jobs.recommendations_job import _latest_device_rows
from streaming.jobs.silver_transform import _merge_quarantine
from streaming.spark_session import create_spark

pytestmark = pytest.mark.delta


@pytest.fixture(scope="module")
def spark():
    session = create_spark("hvacpulse-test-delta-idempotency", delta=True)
    yield session
    session.stop()


def test_quarantine_merge_is_retry_idempotent(spark, tmp_path):
    settings = Settings(root=tmp_path)
    schema = T.StructType(
        [
            T.StructField("kafka_topic", T.StringType(), False),
            T.StructField("kafka_partition", T.IntegerType(), False),
            T.StructField("kafka_offset", T.LongType(), False),
            T.StructField("error_code", T.StringType(), False),
            T.StructField("payload_hash", T.StringType(), False),
            T.StructField("ingest_date", T.StringType(), True),
        ]
    )
    rejected = spark.createDataFrame([("hvac.telemetry", 0, 7, "SCHEMA_INVALID", "hash", "2026-08-26")], schema)
    _merge_quarantine(spark, settings, rejected, settings.quarantine_path)
    _merge_quarantine(spark, settings, rejected, settings.quarantine_path)
    assert settings.read_delta(spark, settings.quarantine_path).count() == 1


def test_gold_history_and_restatement_are_retry_idempotent(spark, tmp_path):
    settings = Settings(root=tmp_path)
    schema = T.StructType(
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
    start = datetime(2026, 8, 26, 10, 0)
    end = datetime(2026, 8, 26, 10, 1)

    def metrics(event_count: int, content_hash: str):
        return spark.createDataFrame(
            [
                (
                    "site-1",
                    "ahu-1",
                    start,
                    end,
                    event_count,
                    10.0,
                    12.0,
                    22.0,
                    50.0,
                    0.1,
                    FINAL,
                    content_hash,
                )
            ],
            schema,
        )

    # First publish: FINAL with hash-v1 → version 1
    publish_gold(spark, metrics(10, "hash-v1"), settings)
    # Second publish: FINAL with hash-v2 (content changed) → version 2, status RESTATED
    publish_gold(spark, metrics(11, "hash-v2"), settings)
    
    # Read current state to create proper retry (RESTATED, not FINAL)
    current_before = settings.read_delta(spark, settings.gold_current_path).first()
    assert current_before.window_status == RESTATED
    
    # Third publish: retry with RESTATED status → idempotent, no change
    restated_metrics = spark.createDataFrame(
        [("site-1", "ahu-1", start, end, 11, 10.0, 12.0, 22.0, 50.0, 0.1, RESTATED, "hash-v2")],
        schema,
    )
    publish_gold(spark, restated_metrics, settings)
    
    current = settings.read_delta(spark, settings.gold_current_path).first()
    assert current.metric_version == 2
    assert current.window_status == RESTATED
    assert current.restated_at is not None
    assert settings.read_delta(spark, settings.gold_history_path).count() == 2


def test_gold_history_merge_replays_a_partial_publication_once(spark, tmp_path):
    settings = Settings(root=tmp_path)
    published = spark.createDataFrame(
        [
            (
                "site-1",
                "ahu-1",
                datetime(2026, 8, 26, 10, 0),
                2,
                "hash-v2",
                "2026-08-26",
            )
        ],
        "site_id string, device_id string, window_start timestamp, "
        "metric_version int, _content_hash string, window_date string",
    )
    _merge_history(spark, settings, published)
    _merge_history(spark, settings, published)
    assert settings.read_delta(spark, settings.gold_history_path).count() == 1


def test_quality_scorecard_keeps_quarantine_only_windows(spark, tmp_path):
    settings = Settings(root=tmp_path)
    observed_at = datetime(2026, 8, 26, 10, 1, tzinfo=timezone.utc)
    valid = spark.createDataFrame(
        [("site-valid", "ahu-valid", observed_at, None, None, None)],
        "site_id string, device_id string, bronze_ingest_ts timestamp, "
        "zone_temp_c double, power_kw double, relative_humidity_pct double",
    )
    rejected = spark.createDataFrame(
        [("site-bad", "ahu-bad", observed_at, "SCHEMA_INVALID")],
        "site_id string, device_id string, ingest_ts timestamp, error_code string",
    )
    settings.write_delta(valid, settings.silver_history_path)
    settings.write_delta(rejected, settings.quarantine_path)

    rows = {row.device_id: row for row in build_dq_scorecard(spark, settings).collect()}
    assert rows["ahu-bad"].valid_events == 0
    assert rows["ahu-bad"].quarantined_events == 1
    assert rows["ahu-bad"].quality_score == 0.0


def test_latest_device_row_uses_event_time_then_transport_order(spark):
    rows = spark.createDataFrame(
        [
            ("ahu-1", datetime(2026, 8, 26, 10, 0), datetime(2026, 8, 26, 10, 0), 0, 1, 1.0),
            ("ahu-1", datetime(2026, 8, 26, 10, 1), datetime(2026, 8, 26, 10, 1), 0, 2, 2.0),
        ],
        "device_id string, event_time timestamp, bronze_ingest_ts timestamp, "
        "kafka_partition int, kafka_offset long, power_kw double",
    )
    assert _latest_device_rows(rows).first().power_kw == 2.0
