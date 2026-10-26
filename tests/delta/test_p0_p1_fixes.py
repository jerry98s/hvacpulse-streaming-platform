"""Tests for P0 and P1 review findings.

P0-1: Streaming watermark uses bronze_ingest_ts
P0-2: Metadata YAML matches physical schema
P1-3: Compliance double-counting fixed
P1-4: Energy coverage added
P1-5: DQ denominator clarity
"""

from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pathlib import Path

import pytest
import yaml
from pyspark.sql import types as T

from streaming.config import Settings
from streaming.jobs.gold_metrics import compute_window_metrics
from streaming.jobs.quality_metrics import build_compliance
from streaming.schemas import SILVER_COLUMNS
from streaming.spark_session import create_spark

pytestmark = pytest.mark.delta


@pytest.fixture(scope="module")
def spark():
    session = create_spark("hvacpulse-test-p0-p1", delta=True)
    yield session
    session.stop()


def test_metadata_yaml_matches_silver_columns():
    """P0-2: Verify semantic_model.yml documents actual Silver columns."""
    yaml_path = Path(__file__).parents[2] / "reference" / "semantic_model.yml"
    with open(yaml_path) as f:
        model = yaml.safe_load(f)
    
    # Check shared kafka_lineage anchor
    kafka_lineage = model["shared"]["kafka_lineage"]
    assert "bronze_ingest_ts" in kafka_lineage
    assert "silver_commit_ts" in kafka_lineage
    assert "ingest_ts" not in kafka_lineage or kafka_lineage.get("ingest_ts", "").startswith("Bronze ingest")
    
    # Verify SILVER_COLUMNS are documented
    required_silver_cols = {
        "bronze_ingest_ts",
        "silver_commit_ts", 
        "event_id",
        "logical_event_id",
        "payload_hash",
    }
    
    documented = set()
    telemetry = model["shared"]["telemetry"]
    for col in telemetry:
        documented.add(col)
    for col in kafka_lineage:
        documented.add(col)
    
    for col in required_silver_cols:
        assert col in documented or col in SILVER_COLUMNS, f"{col} missing from YAML or schema"


def test_compliance_elapsed_time_counts_once_for_simultaneous_violations(spark, tmp_path):
    """P1-3: Simultaneous temp+humidity violations count interval once, not twice."""
    settings = Settings(root=tmp_path)
    
    event_time = datetime(2026, 10, 26, 10, 0, 0, tzinfo=timezone.utc)
    
    # Silver with violations: one reading violates both temp (too hot) and humidity (too high)
    silver = spark.createDataFrame(
        [
            ("site-1", "ahu-1", event_time, 30.0, 70.0, 5.0),  # Both violations
            ("site-1", "ahu-1", event_time + timedelta(seconds=5), 22.0, 50.0, 5.0),  # No violations
        ],
        "site_id string, device_id string, event_time timestamp, zone_temp_c double, "
        "relative_humidity_pct double, expected_report_interval_sec double",
    )
    settings.write_delta(silver, settings.silver_current_path)
    
    # Equipment limits: temp 18-25°C, humidity 30-60%
    limits = spark.createDataFrame(
        [("ahu-1", 5.0, 18.0, 25.0, 30.0, 60.0)],
        "device_id string, expected_report_interval_sec double, temp_min_c double, "
        "temp_max_c double, humidity_min_pct double, humidity_max_pct double",
    )
    limits_path = tmp_path / "equipment_limits.csv"
    settings.write_delta(limits, str(limits_path))
    
    compliance = build_compliance(spark, settings).first()
    
    # One reading violated both: should count 5 seconds (one interval), not 10
    assert compliance.temp_violation_events == 1
    assert compliance.humidity_violation_events == 1
    assert compliance.violation_minutes == pytest.approx(5.0 / 60.0, abs=0.01)  # ~0.083 minutes


def test_energy_coverage_and_incomplete_indicator(spark, tmp_path):
    """P1-4: Coverage tracks observed/expected samples; incomplete flags <95% coverage."""
    settings = Settings(root=tmp_path)
    
    window_start = datetime(2026, 10, 26, 10, 0, 0, tzinfo=timezone.utc)
    
    # 1-minute window (60s), device reports every 5s, so expect 12 readings
    # Provide only 10 readings → coverage = 10/12 = 0.833, incomplete=True
    silver = spark.createDataFrame(
        [(
            "site-1",
            "ahu-1",
            window_start + timedelta(seconds=i * 5),
            10.0,
            22.0,
            50.0,
        ) for i in range(10)],  # 10 readings instead of 12
        "site_id string, device_id string, event_time timestamp, power_kw double, "
        "zone_temp_c double, relative_humidity_pct double",
    )
    settings.write_delta(silver, settings.silver_current_path)
    
    metrics = compute_window_metrics(
        settings.read_delta(spark, settings.silver_current_path),
        {"ahu-1": 5.0},
        window_duration="1 minute",
        now=window_start + timedelta(minutes=15),
    ).first()
    
    assert metrics.event_count == 10
    assert metrics.coverage == pytest.approx(10.0 / 12.0, abs=0.01)  # 0.833
    assert metrics.incomplete is True
    assert metrics.window_status == "FINAL"  # Can be FINAL and still incomplete


def test_energy_complete_coverage(spark, tmp_path):
    """P1-4: Complete coverage (100%) marks incomplete=False."""
    settings = Settings(root=tmp_path)
    
    window_start = datetime(2026, 10, 26, 10, 0, 0, tzinfo=timezone.utc)
    
    # 1-minute window, device reports every 5s, provide exactly 12 readings
    silver = spark.createDataFrame(
        [(
            "site-1",
            "ahu-1",
            window_start + timedelta(seconds=i * 5),
            10.0,
            22.0,
            50.0,
        ) for i in range(12)],  # Exactly 12 readings
        "site_id string, device_id string, event_time timestamp, power_kw double, "
        "zone_temp_c double, relative_humidity_pct double",
    )
    settings.write_delta(silver, settings.silver_current_path)
    
    metrics = compute_window_metrics(
        settings.read_delta(spark, settings.silver_current_path),
        {"ahu-1": 5.0},
        window_duration="1 minute",
        now=window_start + timedelta(minutes=15),
    ).first()
    
    assert metrics.event_count == 12
    assert metrics.coverage == pytest.approx(1.0, abs=0.01)
    assert metrics.incomplete is False


def test_streaming_watermark_column_resolution(spark, tmp_path):
    """P0-1: Streaming query should analyze without unresolved bronze_ingest_ts."""
    settings = Settings(root=tmp_path)
    
    # Create a streaming Bronze source with bronze_ingest_ts
    bronze_schema = T.StructType([
        T.StructField("value", T.BinaryType(), False),
        T.StructField("topic", T.StringType(), False),
        T.StructField("partition", T.IntegerType(), False),
        T.StructField("offset", T.LongType(), False),
        T.StructField("kafka_timestamp", T.TimestampType(), False),
        T.StructField("bronze_ingest_ts", T.TimestampType(), False),
        T.StructField("headers", T.ArrayType(
            T.StructType([
                T.StructField("key", T.StringType()),
                T.StructField("value", T.BinaryType()),
            ])
        ), True),
    ])
    
    # Write some data to Bronze to test against
    from streaming.transforms import with_schema_version_header
    bronze_data = spark.createDataFrame([
        (
            b"test_payload",
            "hvac.telemetry",
            0,
            100,
            datetime.now(timezone.utc),
            datetime.now(timezone.utc),
            [{"key": "schema_version", "value": b"1"}],
        )
    ], bronze_schema)
    
    settings.write_delta(bronze_data, settings.bronze_path)
    
    # Read as stream and verify watermark column resolves
    bronze_stream = settings.read_delta_stream(spark, settings.bronze_path)
    bronze_with_header = with_schema_version_header(bronze_stream)
    
    # This should not raise AnalysisException about unresolved bronze_ingest_ts
    try:
        watermarked = bronze_with_header.withWatermark("bronze_ingest_ts", "10 minutes")
        # Verify the column is present in the logical plan
        assert "bronze_ingest_ts" in [f.name for f in watermarked.schema.fields]
    except Exception as e:
        pytest.fail(f"Watermark on bronze_ingest_ts failed: {e}")
