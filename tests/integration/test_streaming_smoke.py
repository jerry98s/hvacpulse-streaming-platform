"""Streaming smoke test: Bronze → Silver streaming query analyzes and runs.

This test catches unresolved-column errors that would break the streaming pipeline.
P0-1: Verify watermark column (bronze_ingest_ts) resolves correctly.
"""

import pytest

from streaming.config import load_settings
from streaming.spark_session import create_spark

pytestmark = pytest.mark.integration


def test_silver_streaming_query_analyzes_successfully():
    """Streaming query should analyze without unresolved column errors."""
    spark = create_spark("hvacpulse-streaming-smoke", delta=True)
    settings = load_settings()
    
    # This will fail at analysis time if bronze_ingest_ts is unresolved
    try:
        from pyspark.sql.functions import coalesce
        from pyspark.sql.functions import col
        from pyspark.sql.functions import concat
        from pyspark.sql.functions import lit

        from streaming.jobs import silver_transform
        from streaming.transforms import flatten_parsed
        from streaming.transforms import parse_bronze
        from streaming.transforms import with_schema_version_header
        from streaming.transforms import with_validation

        # Read known devices
        known_devices = silver_transform.read_known_devices(settings.known_devices_csv)
        
        # Build the streaming query without starting it
        bronze = settings.read_delta_stream(spark, settings.bronze_path)
        bronze = bronze.filter("topic = 'hvac.telemetry'")  # Filter out plant records
        
        validated = with_validation(
            flatten_parsed(parse_bronze(with_schema_version_header(bronze))),
            known_devices=known_devices,
        )

        # This is where the watermark would fail if bronze_ingest_ts doesn't exist
        deduped = (
            validated.withColumn(
                "_dedup_id",
                coalesce(
                    col("event_id"),
                    concat(
                        col("kafka_topic"),
                        lit("-"),
                        col("kafka_partition").cast("string"),
                        lit("-"),
                        col("kafka_offset").cast("string"),
                    ),
                ),
            )
            .withWatermark("bronze_ingest_ts", settings.watermark)
            .dropDuplicatesWithinWatermark(["_dedup_id", "payload_hash"])
        )
        
        # Verify the watermark is in the logical plan (won't start the stream)
        logical_plan = deduped._jdf.queryExecution().analyzed().toString()
        assert "bronze_ingest_ts" in logical_plan or "EventTimeWatermark" in logical_plan
        
    except Exception as e:
        pytest.fail(f"Streaming query analysis failed: {e}")
    finally:
        spark.stop()
