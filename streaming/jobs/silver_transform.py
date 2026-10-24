"""Bronze → Silver / Quarantine transform.

Pipeline shape (README, "Idempotency Design"):

    bronze stream
      → parse Avro per schema_version header
      → hard validation (error_code; nothing dropped)
      → watermark(ingest_ts) + dropDuplicatesWithinWatermark(event_id, payload_hash)
      → foreachBatch:
            invalid rows                     → quarantine delta (idempotent merge)
            in-batch conflicting event_ids   → quarantine IDEMPOTENCY_CONFLICT
            vs silver history:
                new event_id                 → merge INSERT
                same payload_hash            → safe retry, no-op
                different payload_hash       → quarantine IDEMPOTENCY_CONFLICT
      → silver_current rebuilt from history (highest revision per logical id)

Azure Databricks runs this same job as a scheduled task over the Lakeflow
Bronze streaming table, with its checkpoint on a Unity Catalog volume.

Usage: python -m streaming.jobs.silver_transform [--once]
"""

from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce
from pyspark.sql.functions import col
from pyspark.sql.functions import concat
from pyspark.sql.functions import countDistinct
from pyspark.sql.functions import lit
from pyspark.sql.functions import row_number
from pyspark.sql.window import Window

from quality.rules import IDEMPOTENCY_CONFLICT
from streaming.config import Settings
from streaming.config import load_settings
from streaming.plant_transforms import plant_records
from streaming.spark_session import create_spark
from streaming.transforms import flatten_parsed
from streaming.transforms import parse_bronze
from streaming.transforms import silver_projection
from streaming.transforms import with_schema_version_header
from streaming.transforms import with_validation


def read_known_devices(csv_path: Path | str) -> set[str]:
    import csv

    with open(csv_path, newline="", encoding="utf-8") as fh:
        return {row["device_id"] for row in csv.DictReader(fh)}


def _merge_quarantine(
    spark: SparkSession, settings: Settings, df: DataFrame, path: str
) -> None:
    """Write each rejected Kafka record/reason once, including batch retries.
    
    Key includes topic+partition+offset to prevent corrupt records at the same
    offset in different partitions from colliding.
    """
    key = ["kafka_topic", "kafka_partition", "kafka_offset", "error_code"]
    rows = df.dropDuplicates(key)
    if rows.limit(1).count() == 0:
        return
    if not settings.delta_exists(spark, path):
        settings.write_delta(rows, path, partition_by=["ingest_date"])
        return
    target = settings.delta_table(spark, path)
    condition = " AND ".join(f"t.{column} <=> s.{column}" for column in key)
    target.alias("t").merge(rows.alias("s"), condition).whenNotMatchedInsertAll().execute()


def split_batch_against_history(
    batch_df: DataFrame, history_df: DataFrame
) -> tuple[DataFrame, DataFrame, DataFrame]:
    """Classify a deduplicated batch against history: (inserts, retries, conflicts)."""
    history_hashes = history_df.select(
        col("event_id"), col("payload_hash").alias("existing_hash")
    )
    matched = batch_df.join(history_hashes, "event_id", "left")
    inserts = matched.filter(col("existing_hash").isNull()).drop("existing_hash")
    retries = matched.filter(col("existing_hash") == col("payload_hash")).drop("existing_hash")
    conflicts = matched.filter(
        col("existing_hash").isNotNull() & (col("existing_hash") != col("payload_hash"))
    ).drop("existing_hash")
    return inserts, retries, conflicts


def _merge_inserts(
    spark: SparkSession, settings: Settings, inserts: DataFrame, path: str
) -> None:
    """Idempotent insert-only merge: retries match on event_id and do nothing."""
    if inserts.limit(1).count() == 0:
        return
    if not settings.delta_exists(spark, path):
        settings.write_delta(inserts, path, partition_by=["ingest_date"])
        return
    target = settings.delta_table(spark, path)
    (
        target.alias("t")
        .merge(inserts.alias("s"), "t.event_id = s.event_id")
        .whenNotMatchedInsertAll()
        .execute()
    )


def _update_current(spark: SparkSession, settings: Settings, inserted: DataFrame) -> None:
    """Update only affected logical IDs in current using revision-aware MERGE.
    
    Equal-revision conflicts are resolved deterministically by event_time,
    then bronze_ingest_ts, then event_id (lexicographically).
    """
    affected_ids = inserted.select("logical_event_id").distinct()
    history = settings.read_delta(spark, settings.silver_history_path)
    history_affected = history.join(affected_ids, "logical_event_id", "inner")
    
    latest_first = Window.partitionBy("logical_event_id").orderBy(
        col("revision_number").desc(),
        col("event_time").desc(),
        col("bronze_ingest_ts").desc(),
        col("event_id").desc(),
    )
    updates = history_affected.withColumn("_rn", row_number().over(latest_first)).filter("_rn = 1").drop("_rn")
    
    if not settings.delta_exists(spark, settings.silver_current_path):
        settings.write_delta(updates, settings.silver_current_path, partition_by=["ingest_date"])
        return
    
    target = settings.delta_table(spark, settings.silver_current_path)
    (
        target.alias("t")
        .merge(updates.alias("s"), "t.logical_event_id = s.logical_event_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def _process_microbatch(spark: SparkSession, settings: Settings, batch_df: DataFrame) -> None:
    batch_df = batch_df.persist()

    quarantine = batch_df.filter(col("error_code").isNotNull())
    valid = batch_df.filter(col("error_code").isNull())

    multi_hash_ids = (
        valid.groupBy("event_id")
        .agg(countDistinct("payload_hash").alias("_n"))
        .filter("_n > 1")
        .select("event_id")
    )
    in_batch_conflicts = valid.join(multi_hash_ids, "event_id", "inner").withColumn(
        "error_code", lit(IDEMPOTENCY_CONFLICT)
    )
    candidates = valid.join(multi_hash_ids, "event_id", "left_anti")

    _merge_quarantine(
        spark,
        settings,
        quarantine.unionByName(in_batch_conflicts, allowMissingColumns=True),
        settings.quarantine_path,
    )

    if settings.delta_exists(spark, settings.silver_history_path):
        history = settings.read_delta(spark, settings.silver_history_path)
        inserts, _retries, conflicts = split_batch_against_history(candidates, history)
        _merge_quarantine(
            spark,
            settings,
            conflicts.withColumn("error_code", lit(IDEMPOTENCY_CONFLICT)),
            settings.quarantine_path,
        )
    else:
        inserts = candidates

    projected_inserts = silver_projection(inserts)
    _merge_inserts(spark, settings, projected_inserts, settings.silver_history_path)
    if projected_inserts.limit(1).count() > 0:
        _update_current(spark, settings, projected_inserts)

    batch_df.unpersist()


def run(settings: Settings, *, once: bool = True) -> None:
    spark = create_spark("hvacpulse-silver-transform", delta=True)
    known_devices = read_known_devices(settings.known_devices_csv)

    bronze = settings.read_delta_stream(spark, settings.bronze_path)
    bronze = bronze.filter(~plant_records(bronze))
    validated = with_validation(
        flatten_parsed(parse_bronze(with_schema_version_header(bronze))),
        known_devices=known_devices,
    )
    # Bound streaming state by *arrival* time. Event-time lateness is a
    # business rule (EVENT_TOO_LATE quarantine), not a Spark drop.
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
        .withWatermark("ingest_ts", settings.watermark)
        .dropDuplicatesWithinWatermark(["_dedup_id", "payload_hash"])
    )

    def _handler(batch_df: DataFrame, batch_id: int) -> None:
        _process_microbatch(spark, settings, batch_df)

    writer = (
        deduped.writeStream.foreachBatch(_handler)
        .option("checkpointLocation", settings.checkpoint("silver"))
    )
    if once:
        writer = writer.trigger(availableNow=True)
    query = writer.start()
    query.awaitTermination()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Silver transform job")
    parser.add_argument("--once", action="store_true", help="process available data then stop")
    args = parser.parse_args(argv)
    run(load_settings(), once=args.once)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
