"""Kafka → Bronze Delta ingestion.

Bronze is an immutable, append-only raw-event layer: every Kafka record lands
exactly once (stable checkpoint + Delta transactional write), retaining valid,
invalid and unparseable payloads with full transport metadata.

Usage: python -m streaming.jobs.bronze_ingest [--once]
"""

from __future__ import annotations

import argparse

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import hex as hex_
from pyspark.sql.functions import sha2

from streaming.config import Settings
from streaming.config import load_settings
from streaming.ops import StageRun
from streaming.spark_session import create_spark
from streaming.transforms import with_schema_version_header


def build_bronze_stream(spark: SparkSession, settings: Settings) -> DataFrame:
    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap)
        .option("subscribe", settings.topic)
        .option("startingOffsets", settings.starting_offsets)
        .option("includeHeaders", "true")
    )
    if settings.max_offsets_per_trigger:
        kafka_df = kafka_df.option("maxOffsetsPerTrigger", settings.max_offsets_per_trigger)
    kafka_df = kafka_df.load()

    bronze = (
        kafka_df.select(
            col("key").alias("kafka_key"),
            col("value"),
            col("topic"),
            col("partition"),
            col("offset"),
            col("timestamp").alias("kafka_timestamp"),
            col("headers"),
        )
        .withColumn("ingest_ts", current_timestamp())
        .withColumn("ingest_date", col("ingest_ts").cast("date"))
        .withColumn("payload_hash", sha2(hex_(col("value")), 256))
    )
    return with_schema_version_header(bronze)


def run(settings: Settings, *, once: bool = True) -> None:
    spark = create_spark("hvacpulse-bronze-ingest", delta=True)
    with StageRun(spark, settings, "bronze_ingest", outputs=["bronze_telemetry"]) as stage:
        stream = build_bronze_stream(spark, settings)
        writer = (
            stream.writeStream.format("delta")
            .outputMode("append")
            .option("checkpointLocation", settings.checkpoint("bronze"))
        )
        if once:
            writer = writer.trigger(availableNow=True)
        if settings.is_table_target(settings.bronze_path):
            writer = writer.option("partitionOverwriteMode", "dynamic")
            query = writer.toTable(settings.bronze_path, partitionBy=["ingest_date"])
        else:
            query = writer.partitionBy("ingest_date").start(settings.bronze_path)
        query.awaitTermination()
        stage.record_stream(query)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Bronze ingestion job")
    parser.add_argument("--once", action="store_true", help="process available data then stop")
    args = parser.parse_args(argv)
    run(load_settings(), once=args.once)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
